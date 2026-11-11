"""内存有界的清洗作业管道（纯标准库）。

设计要点（应对 256 MiB 内存上限）
--------------------------------
1. 上传文件先由路由分块落盘，本模块从**文件路径**流式读取，绝不把整份 CSV
   读进内存；
2. 解析时按机尾把行拆分到各自的 spool 文件（:class:`SpoolPool` 用 LRU 限制
   同时打开的文件句柄数），因此不同机尾的行可以任意交错、无需预先排序；
3. 逐机尾读取 spool 并调用 :func:`app.solver.solve_tail`，任意时刻内存里只有
   一架机尾的航段；完整明细写入 ``details/<idx>.json``，摘要列表只保留
   机尾级轻量字段；
4. 前端点开某架机尾时才读取对应的明细 JSON（懒加载）；
5. 导出时逐机尾重新求解、复核唯一性，并以生成器分块产出 CSV，
   全批导出也不缓冲整份结果。

作业目录结构::

    <root>/<job_id>/
      meta.json        # 文件名、机尾顺序与 tail -> 索引映射
      summary.json     # 全批统计 + 各机尾轻量摘要
      spool/<idx>.csv  # 每个机尾的原始行（带表头）
      details/<idx>.json  # 每个机尾的完整求解明细（按需读取）
"""
from __future__ import annotations

import csv
import io
import json
import os
import shutil
import time
import uuid
from collections import OrderedDict
from pathlib import Path

from .solver import (
    EXPORT_HEADER,
    REQUIRED_COLUMNS,
    CsvFormatError,
    iter_csv_rows,
    solve_tail,
    summarize_tail,
)

# 同时打开的 spool 句柄上限（LRU 回收），与机尾总数无关
OPEN_HANDLE_CAP = 64
# 最多保留的作业数，超出后淘汰最旧的作业
MAX_JOBS = 20
# 作业最长保留时间（秒）
JOB_TTL_SECONDS = 2 * 60 * 60


class JobNotFound(LookupError):
    """作业不存在或已被清理。"""


class ExportRejected(Exception):
    """导出被拒绝（存在非唯一解机尾），映射为 HTTP 409。"""


# ---------------------------------------------------------------------------
# 按机尾拆分的 spool 池（LRU 句柄）
# ---------------------------------------------------------------------------

class SpoolPool:
    """把交错到达的行按机尾写入各自的 spool CSV 文件。

    机尾数量可能上千，不能同时打开上千个句柄：超出 :data:`OPEN_HANDLE_CAP`
    时按 LRU 关闭最久未写入的句柄，需要时再以追加模式重开。
    """

    def __init__(self, spool_dir: Path, header: list[str]):
        self._dir = spool_dir
        self._header = header
        self._dir.mkdir(parents=True, exist_ok=True)
        self._tail_to_idx: dict[str, int] = {}
        self._handles: OrderedDict[int, object] = OrderedDict()

    def _open_new(self, idx: int):
        f = open(self._dir / f"{idx}.csv", "w", encoding="utf-8", newline="")
        csv.writer(f, lineterminator="\n").writerow(self._header)
        return f

    def _reopen(self, idx: int):
        return open(self._dir / f"{idx}.csv", "a", encoding="utf-8", newline="")

    def write_row(self, tail: str, row: list[str]):
        idx = self._tail_to_idx.get(tail)
        if idx is None:
            idx = len(self._tail_to_idx)
            self._tail_to_idx[tail] = idx
            self._handles[idx] = self._open_new(idx)
        f = self._handles.get(idx)
        if f is None:
            f = self._reopen(idx)
            self._handles[idx] = f
        else:
            self._handles.move_to_end(idx)
        csv.writer(f, lineterminator="\n").writerow(row)

        while len(self._handles) > OPEN_HANDLE_CAP:
            _, old = self._handles.popitem(last=False)
            old.close()

    def close(self):
        for f in self._handles.values():
            f.close()
        self._handles.clear()

    @property
    def tail_to_idx(self) -> dict[str, int]:
        return dict(self._tail_to_idx)

    def path(self, idx: int) -> Path:
        return self._dir / f"{idx}.csv"


# ---------------------------------------------------------------------------
# 作业存储
# ---------------------------------------------------------------------------

class JobStore:
    def __init__(self, root: str | os.PathLike | None = None):
        if root is None:
            root = os.environ.get("DST_CLEANER_JOB_DIR")
        if root:
            self.root = Path(root)
        else:
            self.root = Path(__file__).resolve().parent.parent / ".jobs"
        self.root.mkdir(parents=True, exist_ok=True)

    def create(self) -> tuple[str, Path]:
        self._evict()
        job_id = uuid.uuid4().hex
        job_dir = self.root / job_id
        (job_dir / "spool").mkdir(parents=True)
        (job_dir / "details").mkdir(parents=True)
        return job_id, job_dir

    def get(self, job_id: str) -> Path:
        # 只接受 uuid hex，杜绝路径穿越
        if not job_id or not all(c in "0123456789abcdef" for c in job_id):
            raise JobNotFound(job_id)
        job_dir = self.root / job_id
        if not job_dir.is_dir():
            raise JobNotFound(job_id)
        return job_dir

    def _evict(self):
        now = time.time()
        jobs = [p for p in self.root.iterdir() if p.is_dir()]
        for p in jobs:
            try:
                if now - p.stat().st_mtime > JOB_TTL_SECONDS:
                    _rmtree(p)
            except OSError:
                continue
        jobs = [p for p in self.root.iterdir() if p.is_dir()]
        if len(jobs) >= MAX_JOBS:
            jobs.sort(key=lambda p: p.stat().st_mtime)
            for p in jobs[: len(jobs) - MAX_JOBS + 1]:
                _rmtree(p)

    def wipe(self):
        """启动时清空残留作业。"""
        for p in self.root.iterdir():
            if p.is_dir():
                _rmtree(p)


def _rmtree(path: Path):
    """递归删除作业目录，忽略已被并发清理的情况。"""
    shutil.rmtree(path, ignore_errors=True)


# ---------------------------------------------------------------------------
# 作业处理
# ---------------------------------------------------------------------------

def _write_json(path: Path, payload: dict):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    tmp.replace(path)


def ingest_file(source_path: str | os.PathLike, job_dir: Path,
                original_name: str = "upload.csv") -> dict:
    """把已落盘的上传 CSV 流式拆分为机尾 spool，再逐机尾求解落盘。

    返回全批响应（统计 + 机尾摘要列表）。任意时刻内存只持有一架机尾的数据。
    抛出 :class:`CsvFormatError` 表示整份 CSV 无法解析。
    """
    spool_dir = job_dir / "spool"
    detail_dir = job_dir / "details"
    total_rows = 0
    pool: SpoolPool | None = None

    with open(source_path, "r", encoding="utf-8-sig", newline="") as src:
        reader = csv.reader(src)
        header_row = None
        for r in reader:
            if any(cell.strip() for cell in r):
                header_row = r
                break
        if header_row is None:
            raise CsvFormatError("CSV 内容为空")
        header = [c.strip().lower().lstrip("﻿") for c in header_row]
        missing = [c for c in REQUIRED_COLUMNS if c not in header]
        if missing:
            raise CsvFormatError(
                "缺少必需列: " + ", ".join(missing)
                + "。需要的表头为: " + ", ".join(REQUIRED_COLUMNS))
        tail_col = header.index("tail_number")
        pool = SpoolPool(spool_dir, header)

        tail_order: list[str] = []
        seen: set[str] = set()
        for r in reader:
            if not any(cell.strip() for cell in r):
                continue
            total_rows += 1
            # 机尾列定位；空机尾号照常 spool，由 solver 行级校验报错
            tail = r[tail_col].strip() if tail_col < len(r) else ""
            if tail not in seen:
                seen.add(tail)
                tail_order.append(tail)
            pool.write_row(tail, r)
        pool.close()
        tail_idx = pool.tail_to_idx

    if total_rows == 0:
        raise CsvFormatError("CSV 没有任何数据行")

    # 逐机尾求解：每次只有一架机尾的行与明细驻留内存
    counters = {"unique": 0, "no_solution": 0, "multiple": 0, "error": 0}
    tail_summaries = []
    for tail in tail_order:
        idx = tail_idx[tail]
        with open(pool.path(idx), "r", encoding="utf-8", newline="") as f:
            raws = list(iter_csv_rows(f))
        full = solve_tail(tail, raws)
        _write_json(detail_dir / f"{idx}.json", full)
        summary = summarize_tail(full)
        tail_summaries.append(summary)
        counters[summary["status"]] += 1
        del raws, full

    meta = {
        "filename": original_name,
        "created_at": time.time(),
        "rows": total_rows,
        "tail_order": tail_order,
        "tail_to_idx": tail_idx,
    }
    _write_json(job_dir / "meta.json", meta)
    payload = {
        "job_id": job_dir.name,
        "filename": original_name,
        "summary": {"rows": total_rows, "total_tails": len(tail_order), **counters},
        "tails": tail_summaries,
    }
    _write_json(job_dir / "summary.json", payload)
    return payload


def load_summary(job_dir: Path) -> dict:
    with open(job_dir / "summary.json", "r", encoding="utf-8") as f:
        return json.load(f)


def load_meta(job_dir: Path) -> dict:
    with open(job_dir / "meta.json", "r", encoding="utf-8") as f:
        return json.load(f)


def load_tail_detail(job_dir: Path, tail: str) -> dict:
    """懒加载单机尾完整明细；机尾不存在抛 :class:`JobNotFound`。"""
    meta = load_meta(job_dir)
    idx = meta["tail_to_idx"].get(tail)
    if idx is None:
        raise JobNotFound(tail)
    with open(job_dir / "details" / f"{idx}.json", "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# 流式导出（逐机尾重新求解并复核唯一性）
# ---------------------------------------------------------------------------

def resolve_export_targets(job_dir: Path, tail: str) -> tuple[list[str], str]:
    """根据机尾级摘要确定导出目标，非唯一解直接抛 :class:`ExportRejected`（409）。"""
    summary = load_summary(job_dir)
    by_tail = {t["tail"]: t for t in summary["tails"]}
    if tail == "__all__":
        targets = [t["tail"] for t in summary["tails"] if t["status"] == "unique"]
        if not targets:
            raise ExportRejected("没有可导出的机尾：仅整链唯一解的机尾允许导出")
        return targets, "utc_export_all.csv"

    match = by_tail.get(tail)
    if match is None:
        raise JobNotFound(tail)
    if match["status"] != "unique":
        raise ExportRejected(
            f"机尾 {tail} 状态为 {match['status']}，非整链唯一解，禁止导出")
    return [tail], f"utc_export_{tail}.csv"


def export_stream(job_dir: Path, targets: list[str]):
    """生成器：逐机尾**重新求解**并复核唯一性，分块产出 UTC CSV 文本。"""
    meta = load_meta(job_dir)
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerow(EXPORT_HEADER)
    yield buf.getvalue()

    for tail in targets:
        idx = meta["tail_to_idx"][tail]
        with open(job_dir / "spool" / f"{idx}.csv", "r",
                  encoding="utf-8", newline="") as f:
            raws = list(iter_csv_rows(f))
        full = solve_tail(tail, raws)
        # 导出时复核：只允许整链唯一解
        if full["status"] != "unique":
            raise ExportRejected(
                f"机尾 {tail} 重新求解结果为 {full['status']}，已中止导出")
        for item in full["solution"]:
            buf = io.StringIO()
            csv.writer(buf, lineterminator="\n").writerow(
                [tail, item["seq"], item["dep_utc"],
                 item["arr_utc"], item["duration_minutes"]])
            yield buf.getvalue()
        del raws, full

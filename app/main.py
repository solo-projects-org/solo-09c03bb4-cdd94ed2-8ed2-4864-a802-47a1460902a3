"""FastAPI 入口：航段时间清洗（DST 安全入仓，内存有界）。

内存策略
--------
- 上传按固定块流式落盘到临时文件（:func:`_stream_to_tempfile`），
  不会把整份 CSV 读进内存；
- 清洗在 :mod:`app.pipeline` 中按机尾拆分、逐机尾求解，
  只返回全批统计 + 各机尾轻量摘要；
- 单机尾明细按需通过 ``/api/jobs/{job_id}/tails/{tail}`` 懒加载；
- 导出为 :class:`StreamingResponse`，逐机尾重新求解并分块产出 CSV。

阻塞型端点都声明为普通 ``def``，FastAPI 会放到线程池执行，
大文件清洗期间事件循环仍可响应明细/健康检查请求。
"""
from __future__ import annotations

import contextlib
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse

from . import samplegen
from .pipeline import (
    ExportRejected,
    JobNotFound,
    JobStore,
    export_stream,
    ingest_file,
    load_summary,
    load_tail_detail,
    resolve_export_targets,
)
from .solver import CsvFormatError

BASE_DIR = Path(__file__).resolve().parent
SAMPLE_PATH = BASE_DIR.parent / "samples" / "dst_sample.csv"
INDEX_PATH = BASE_DIR / "static" / "index.html"
MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # 10 MB（10 万行验收文件约 7.4 MB）
UPLOAD_CHUNK = 1024 * 1024  # 1 MiB
# 在线生成样例的规模上限，防止演示接口被当成压测入口
MAX_SAMPLE_TAILS = 1200
MAX_SAMPLE_SEGMENTS = 200

store = JobStore()


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时清理上次进程残留的作业目录
    store.wipe()
    yield


app = FastAPI(title="航段时间清洗 · DST 安全入仓", version="2.0.0",
              lifespan=lifespan)


def _stream_to_tempfile(file: UploadFile) -> Path:
    """分块把上传内容写入临时文件并做大小限制，返回临时路径。

    直接读取底层 SpooledTemporaryFile（``file.file``），
    整个过程只保留 1 MiB 块，不把整份文件载入内存。
    """
    tmp = tempfile.NamedTemporaryFile(
        prefix="upload-", suffix=".csv", delete=False)
    path = Path(tmp.name)
    total = 0
    try:
        with tmp:
            src = file.file
            while True:
                chunk = src.read(UPLOAD_CHUNK)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_UPLOAD_BYTES:
                    raise HTTPException(413, "文件超过 10 MB 上限")
                tmp.write(chunk)
    except HTTPException:
        path.unlink(missing_ok=True)
        raise
    except Exception:
        path.unlink(missing_ok=True)
        raise HTTPException(400, "上传内容读取失败")
    if total == 0:
        path.unlink(missing_ok=True)
        raise HTTPException(400, "上传文件为空")
    return path


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(INDEX_PATH)


@app.get("/api/sample", include_in_schema=False)
async def sample():
    """内置夏令时样例（含唯一解/无解/多解/错误四类机尾）。"""
    return FileResponse(SAMPLE_PATH, media_type="text/csv; charset=utf-8",
                        filename="dst_sample.csv")


@app.get("/api/sample/interleaved", include_in_schema=False)
async def interleaved_sample(tails: int = 6, segments: int = 8):
    """在线流式生成「交错 + 无 DST 歧义」样例，可直接在前端一键载入。"""
    if not (1 <= tails <= MAX_SAMPLE_TAILS and 1 <= segments <= MAX_SAMPLE_SEGMENTS):
        raise HTTPException(
            400, f"参数越界：tails 需在 1..{MAX_SAMPLE_TAILS}，"
                 f"segments 需在 1..{MAX_SAMPLE_SEGMENTS}")

    def gen():
        yield from samplegen.iter_interleaved_csv(tails, segments)

    return StreamingResponse(
        gen(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition":
                 f'attachment; filename="interleaved_{tails}x{segments}.csv"'},
    )


@app.post("/api/solve")
def solve(file: UploadFile = File(...)):
    """上传 CSV：流式落盘 → 按机尾拆分 → 逐机尾求解。

    返回全批统计与各机尾**摘要**（不含明细，明细走懒加载接口）。
    同步端点：FastAPI 在线程池中执行，避免阻塞事件循环。
    """
    path = _stream_to_tempfile(file)
    job_id, job_dir = store.create()
    try:
        payload = ingest_file(path, job_dir, file.filename or "upload.csv")
    except CsvFormatError as exc:
        raise HTTPException(400, str(exc))
    finally:
        path.unlink(missing_ok=True)
    return payload


@app.get("/api/jobs/{job_id}")
def job_summary(job_id: str):
    """获取作业的全批统计与机尾摘要列表（刷新页面后仍可查看）。"""
    try:
        job_dir = store.get(job_id)
        return load_summary(job_dir)
    except JobNotFound:
        raise HTTPException(404, "作业不存在或已过期，请重新上传 CSV")


@app.get("/api/jobs/{job_id}/tails/{tail}")
def tail_detail(job_id: str, tail: str):
    """懒加载：点开某架机尾时才读取它的完整求解明细（候选/冲突/候选链）。"""
    try:
        job_dir = store.get(job_id)
        return load_tail_detail(job_dir, tail)
    except JobNotFound:
        raise HTTPException(404, f"作业中不存在机尾 {tail!r}，或作业已过期")


@app.get("/api/jobs/{job_id}/export")
def export(job_id: str, tail: str = "__all__"):
    """导出 UTC CSV（默认全批可导出机尾）。

    导出时逐机尾**重新求解并复核唯一性**，非唯一解返回 409；
    响应为流式分块输出，全批导出也不缓冲整份 CSV。
    """
    try:
        job_dir = store.get(job_id)
        targets, filename = resolve_export_targets(job_dir, tail)
    except JobNotFound:
        raise HTTPException(404, "作业不存在或已过期，请重新上传 CSV")
    except ExportRejected as exc:
        raise HTTPException(409, str(exc))

    def gen():
        try:
            yield from export_stream(job_dir, targets)
        except ExportRejected as exc:
            # 流式开始后复核失败（正常不会发生，求解是确定性的）：
            # 以注释行中止，避免静默产出看似完整的文件
            yield f"# 导出中止：{exc}\n"

    return StreamingResponse(
        gen(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/health", include_in_schema=False)
async def health():
    return {"status": "ok"}

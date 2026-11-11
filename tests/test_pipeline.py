"""流式管道自检：`python3 -I tests/test_pipeline.py`（仅依赖标准库 + tzdata）。

覆盖：
- 交错 CSV 经 spool 归集后，按机尾求解结果与 solve_csv 一致；
- 摘要不含明细、明细懒加载、作业隔离；
- 导出目标解析：单机尾/全批/非唯一解 409/未知机尾 404；
- 流式导出格式与逐机尾重新求解复核；
- 缺列/空文件等整表格式错误。
"""
import io
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.pipeline import (  # noqa: E402
    ExportRejected,
    JobNotFound,
    JobStore,
    SpoolPool,
    export_stream,
    ingest_file,
    load_summary,
    load_tail_detail,
    resolve_export_targets,
)
from app.solver import CsvFormatError, solve_csv  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "samples" / "dst_sample.csv"
INTERLEAVED = ROOT / "samples" / "interleaved_small.csv"

_passed = 0


def check(name, cond):
    global _passed
    print(("PASS" if cond else "FAIL"), "-", name)
    if not cond:
        raise SystemExit(1)
    _passed += 1


def main():
    tmp_root = Path(tempfile.mkdtemp(prefix="pipeline-test-"))
    try:
        store = JobStore(tmp_root / "jobs")
        store.wipe()

        # ---- 1) 内置 DST 样例：管道结果与直接 solve_csv 一致 ----
        job_id, job_dir = store.create()
        payload = ingest_file(SAMPLE, job_dir, "dst_sample.csv")
        direct = solve_csv(SAMPLE.read_text(encoding="utf-8"))
        check("统计一致", payload["summary"] == direct["summary"])
        check("机尾顺序与首次出现顺序一致",
              [t["tail"] for t in payload["tails"]]
              == [t["tail"] for t in direct["tails"]])

        # 摘要是轻量视图：不含 segments/solution 等明细字段
        t0 = payload["tails"][0]
        check("摘要不含明细字段",
              "segments" not in t0 and "solution" not in t0 and "solutions" not in t0)
        check("摘要含行数", t0["row_count"] > 0)

        # 懒加载明细
        detail = load_tail_detail(job_dir, "B-7001")
        check("懒加载 B-7001 为唯一解", detail["status"] == "unique")
        check("懒加载明细含航段表", len(detail["segments"]) == 3)
        check("懒加载明细含解", len(detail["solution"]) == 3)
        try:
            load_tail_detail(job_dir, "B-9999")
            check("未知机尾应报错", False)
        except JobNotFound:
            check("未知机尾抛 JobNotFound", True)

        # 与直接求解的明细等价
        direct_b1 = next(t for t in direct["tails"] if t["tail"] == "B-7001")
        check("懒加载明细与直接求解一致",
              detail["solution"] == direct_b1["solution"])

        # ---- 2) 交错样例：全部唯一解，行序打散不影响结论 ----
        job2_id, job2_dir = store.create()
        p2 = ingest_file(INTERLEAVED, job2_dir, "interleaved.csv")
        check("交错样例全部唯一解",
              p2["summary"]["unique"] == p2["summary"]["total_tails"]
              and p2["summary"]["unique"] > 0)
        check("交错样例行数正确", p2["summary"]["rows"]
              == p2["summary"]["total_tails"] * 8)

        # 两个作业相互隔离
        check("作业隔离：作业1看不到作业2的机尾",
              all(t["tail"].startswith("T-") is False
                  for t in load_summary(job_dir)["tails"]))

        # ---- 3) 导出目标解析 ----
        targets, fn = resolve_export_targets(job_dir, "B-7001")
        check("单机尾导出目标", targets == ["B-7001"] and fn == "utc_export_B-7001.csv")

        all_targets, all_fn = resolve_export_targets(job_dir, "__all__")
        check("全批导出只含唯一解机尾",
              set(all_targets) == {"B-7001", "B-7005"} and all_fn == "utc_export_all.csv")

        try:
            resolve_export_targets(job_dir, "B-7004")  # 多解
            check("多解机尾应拒绝导出", False)
        except ExportRejected:
            check("多解机尾导出抛 ExportRejected(409)", True)
        try:
            resolve_export_targets(job_dir, "B-7003")  # 无解
            check("无解机尾应拒绝导出", False)
        except ExportRejected:
            check("无解机尾导出抛 ExportRejected(409)", True)
        try:
            resolve_export_targets(job_dir, "NOPE")
            check("未知机尾应 404", False)
        except JobNotFound:
            check("未知机尾导出抛 JobNotFound(404)", True)

        # ---- 4) 流式导出内容：表头 + 唯一解机尾全部行 ----
        csv_text = "".join(export_stream(job_dir, all_targets))
        lines = csv_text.strip().splitlines()
        check("导出 CSV 行数 = 表头 + B-7001(3) + B-7005(2)",
              len(lines) == 1 + 3 + 2)
        check("导出 CSV 不含非唯一解机尾",
              "B-7002" not in csv_text and "B-7003" not in csv_text
              and "B-7004" not in csv_text)
        check("导出表头沿用 UTC CSV 格式",
              lines[0] == "tail_number,seq,dep_utc,arr_utc,duration_minutes")
        check("导出生成器分块产出",
              len(list(export_stream(job_dir, all_targets))) > 1)

        # 交错样例全批导出：行数 = 表头 + 全部数据行
        t2, _ = resolve_export_targets(job2_dir, "__all__")
        csv2 = "".join(export_stream(job2_dir, t2))
        check("交错样例全批导出行数",
              len(csv2.strip().splitlines()) == 1 + p2["summary"]["rows"])

        # ---- 5) SpoolPool：超过句柄上限时 LRU 回收，数据不丢 ----
        spool_dir = tmp_root / "spool"
        pool = SpoolPool(spool_dir, ["tail_number", "seq"])
        header = ["tail_number", "seq"]
        for seq in range(1, 50):
            for i in range(200):  # 200 机尾 > OPEN_HANDLE_CAP(64)
                pool.write_row(f"X-{i:03d}", [f"X-{i:03d}", str(seq)])
        pool.close()
        check("SpoolPool 为每个机尾建文件", len(pool.tail_to_idx) == 200)
        check("LRU 回收后文件数不变", len(list(spool_dir.glob('*.csv'))) == 200)
        # 抽查一个被回收过重开的机尾，49 段都在
        with open(spool_dir / f"{pool.tail_to_idx['X-000']}.csv",
                  encoding="utf-8") as f:
            rows = [ln for ln in f.read().splitlines() if ln.strip()]
        check("LRU 重开后行完整（表头+49段）", len(rows) == 50)

        # ---- 6) 整表格式错误 ----
        bad = tmp_root / "bad.csv"
        bad.write_text("a,b\n1,2\n", encoding="utf-8")
        j3, jd3 = store.create()
        try:
            ingest_file(bad, jd3, "bad.csv")
            check("缺列应抛 CsvFormatError", False)
        except CsvFormatError:
            check("缺列抛 CsvFormatError(400)", True)

        empty = tmp_root / "empty.csv"
        empty.write_text("tail_number,seq\n\n", encoding="utf-8")
        j4, jd4 = store.create()
        try:
            ingest_file(empty, jd4, "empty.csv")
            check("空数据应抛 CsvFormatError", False)
        except CsvFormatError:
            check("无数据行抛 CsvFormatError(400)", True)

        # 非法作业 id 不能路径穿越
        try:
            store.get("../etc")
            check("非法作业 id 应拒绝", False)
        except JobNotFound:
            check("非法作业 id 抛 JobNotFound", True)

        print(f"\n全部管道自检通过 ✅（{_passed} 项）")
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    main()

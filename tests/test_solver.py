"""求解器自检：直接运行 `python3 -I tests/test_solver.py`（仅依赖标准库 + tzdata）。"""
import io
import random
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.solver import (  # noqa: E402
    build_export_csv,
    export_rows,
    iter_csv_rows,
    read_rows,
    solve_csv,
    solve_tail,
    utc_candidates,
)

SAMPLE = Path(__file__).resolve().parents[1] / "samples" / "dst_sample.csv"
SMALL_INTERLEAVED = (
    Path(__file__).resolve().parents[1] / "samples" / "interleaved_small.csv"
)


def check(name, cond):
    print(("PASS" if cond else "FAIL"), "-", name)
    if not cond:
        raise SystemExit(1)


def main():
    # 1) 时区候选映射
    cands, err = utc_candidates(datetime(2026, 10, 25, 2, 30), "Europe/Berlin")
    check("柏林拨回：重复时刻保留两个候选", err is None and len(cands) == 2)

    cands, err = utc_candidates(datetime(2026, 3, 29, 2, 30), "Europe/Berlin")
    check("柏林拨快：不存在时刻报错", err == "nonexistent")

    cands, err = utc_candidates(datetime(2026, 6, 1, 12, 0), "Europe/Berlin")
    check("普通时刻：唯一候选", err is None and len(cands) == 1)

    cands, err = utc_candidates(datetime(2026, 1, 1, 0, 0), "Mars/Olympus")
    check("非法 IANA 时区", err == "unknown_tz")

    # 2) 样例整体求解
    result = solve_csv(SAMPLE.read_text(encoding="utf-8"))
    by_tail = {t["tail"]: t for t in result["tails"]}

    check("B-7001 整链唯一解", by_tail["B-7001"]["status"] == "unique")
    check("B-7002 不存在时刻 → error", by_tail["B-7002"]["status"] == "error")
    check("B-7003 链断裂 → no_solution", by_tail["B-7003"]["status"] == "no_solution")
    check("B-7003 冲突定位在 seq=2",
          by_tail["B-7003"]["conflict"]["segment_seq"] == 2)
    check("B-7004 多解", by_tail["B-7004"]["status"] == "multiple")
    check("B-7004 恰好 2 条候选链", by_tail["B-7004"]["solution_count"] == 2)
    check("B-7005 整链唯一解", by_tail["B-7005"]["status"] == "unique")

    # 3) 唯一解内容校验（B-7001 第一段：伦敦 00:30 BST → 23:30Z；
    #    柏林 02:15 双候选 00:15Z/01:15Z，时长 45/165 分钟，区间 [90,130] 只留下 01:15Z）
    sol1 = by_tail["B-7001"]["solution"][0]
    check("B-7001 seg1 起飞 UTC", sol1["dep_utc"] == "2026-10-24T23:30:00Z")
    check("B-7001 seg1 落地 UTC", sol1["arr_utc"] == "2026-10-25T01:15:00Z")

    # 4) 导出只包含唯一解机尾
    csv_out = build_export_csv(result, ["B-7001", "B-7004", "B-7005"])
    lines = [l for l in csv_out.strip().splitlines()]
    check("导出 CSV 行数 = 表头 + B-7001(3) + B-7005(2)", len(lines) == 1 + 3 + 2)
    check("导出 CSV 不含多解机尾 B-7004", "B-7004" not in csv_out)

    # 5) 流式生成器导出与一次性导出内容一致，且逐行产出
    streamed = "".join(export_rows(result, ["B-7001", "B-7004", "B-7005"]))
    check("流式导出生成器与一次性导出一致", streamed == csv_out)
    chunks = list(export_rows(result, ["B-7001"]))
    check("流式导出为多块逐行产出", len(chunks) > 1 and chunks[0].startswith("tail_number"))

    # 6) 流式 CSV 读取与一次性读取等价
    text = SAMPLE.read_text(encoding="utf-8")
    stream_rows = list(iter_csv_rows(io.StringIO(text)))
    check("iter_csv_rows 与 read_rows 行数一致",
          len(stream_rows) == len(read_rows(text)))
    check("流式行号从 1 连续编号",
          [r.row_number for r in stream_rows] == list(range(1, len(stream_rows) + 1)))

    # 7) 交错输入：随机打乱数据行后，逐机尾结果与原顺序完全一致
    raw_lines = text.splitlines(keepends=True)
    header, body = raw_lines[0], raw_lines[1:]
    rng = random.Random(20261111)
    rng.shuffle(body)
    shuffled = solve_csv(header + "".join(body))
    orig_by = {t["tail"]: t for t in result["tails"]}
    shuf_by = {t["tail"]: t for t in shuffled["tails"]}
    check("打乱行序后机尾集合不变",
          set(orig_by) == set(shuf_by))
    check("打乱行序后各机尾状态不变",
          all(orig_by[k]["status"] == shuf_by[k]["status"] for k in orig_by))
    check("打乱行序后唯一解内容不变",
          all(orig_by[k].get("solution") == shuf_by[k].get("solution")
              for k in orig_by if orig_by[k]["status"] == "unique"))

    # 8) 无 DST 歧义的交错样例：所有机尾整链唯一解
    if SMALL_INTERLEAVED.exists():
        il = solve_csv(SMALL_INTERLEAVED.read_text(encoding="utf-8"))
        check("交错样例全部为唯一解",
              il["summary"]["total_tails"] == il["summary"]["unique"]
              and il["summary"]["unique"] > 0)
        first_tail = il["tails"][0]["tail"]
        grouped = solve_tail(first_tail, [
            r for r in iter_csv_rows(io.StringIO(SMALL_INTERLEAVED.read_text()))
            if r.tail == first_tail])
        check("交错样例单机尾重算仍唯一解", grouped["status"] == "unique")

    print("\n全部自检通过 ✅")


if __name__ == "__main__":
    main()

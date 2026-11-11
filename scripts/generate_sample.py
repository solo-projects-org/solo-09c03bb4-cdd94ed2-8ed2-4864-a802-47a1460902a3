#!/usr/bin/env python3
"""生成「机尾行交错、无夏令时歧义」的历史航段补录样例 CSV。

用法
----
# 默认：1000 个机尾 × 每个 100 段 = 100000 行（验收压测规模）
python3 scripts/generate_sample.py samples/interleaved_sample.csv

# 网页演示小文件：6 个机尾 × 每个 8 段
python3 scripts/generate_sample.py samples/interleaved_small.csv --tails 6 --segments 8

仅依赖标准库。建议用 `python3 -I scripts/generate_sample.py ...` 运行。
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.samplegen import write_interleaved_csv  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="生成交错航段样例 CSV")
    parser.add_argument("path", help="输出 CSV 路径")
    parser.add_argument("--tails", type=int, default=1000,
                        help="机尾数量（默认 1000）")
    parser.add_argument("--segments", type=int, default=100,
                        help="每机尾航段数（默认 100）")
    args = parser.parse_args()

    if args.tails <= 0 or args.segments <= 0:
        parser.error("机尾数与航段数必须为正整数")

    out = Path(args.path)
    out.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    n = write_interleaved_csv(out, args.tails, args.segments)
    size_mb = out.stat().st_size / (1024 * 1024)
    print(f"已写入 {n} 行 → {out}（{size_mb:.2f} MiB，"
          f"耗时 {time.monotonic() - t0:.1f}s）")
    print("特点：机尾行按轮询交错、无需预排序；全部时区无夏令时，整链唯一解。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

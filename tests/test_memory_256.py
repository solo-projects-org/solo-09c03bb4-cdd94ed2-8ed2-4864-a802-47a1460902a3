"""256 MiB 内存上限验收脚本（纯标准库）。

用 ``RLIMIT_AS`` 把进程可用地址空间硬限制在 256 MiB（等价于容器
``mem_limit: 256m`` 的效果，且不允许 swap 兜底），在该限制下完成：

1. 1000 机尾 × 每机尾 100 段（10 万行）交错 CSV 的流式清洗；
2. 任意机尾明细的懒加载；
3. 全批唯一解机尾的流式导出。

用法::

    python3 -I tests/test_memory_256.py [csv路径] [MiB上限]

不传路径时，先用 app.samplegen 在线生成默认 1000×1000 交错 CSV。
若进程在限制内被 OOM 杀死，脚本以非零码退出。
"""
from __future__ import annotations

import os
import resource
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DEFAULT_LIMIT_MIB = 256


def main() -> int:
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    limit_mib = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_LIMIT_MIB

    # 先做导入，再收紧地址空间（import 阶段的一次性开销不计入业务峰值）
    from app.pipeline import (  # noqa: E402
        JobStore, export_stream, ingest_file,
        load_tail_detail, resolve_export_targets,
    )
    from app.samplegen import write_interleaved_csv  # noqa: E402

    work = Path(tempfile.mkdtemp(prefix="mem256-"))
    if csv_path is None:
        csv_path = work / "interleaved_1000x100.csv"
        print(f"[1/4] 生成验收 CSV: {csv_path} ...", flush=True)
        t0 = time.monotonic()
        n = write_interleaved_csv(csv_path, 1000, 100)
        print(f"      {n} 行，{csv_path.stat().st_size / 1024 / 1024:.2f} MiB，"
              f"{time.monotonic() - t0:.1f}s", flush=True)
    else:
        print(f"[1/4] 使用指定 CSV: {csv_path}", flush=True)

    # ---- 收紧地址空间：此后任何超出限制的内存申请都会抛 MemoryError / 被杀死 ----
    limit_bytes = limit_mib * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
    print(f"[2/4] RLIMIT_AS 已设为 {limit_mib} MiB", flush=True)

    store = JobStore(work / "jobs")
    store.wipe()
    job_id, job_dir = store.create()

    t0 = time.monotonic()
    payload = ingest_file(csv_path, job_dir, csv_path.name)
    dt = time.monotonic() - t0
    s = payload["summary"]
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print(f"[3/4] 清洗完成：{s['rows']} 行 / {s['total_tails']} 机尾，"
          f"唯一解 {s['unique']}，耗时 {dt:.1f}s，峰值 RSS ≈ {rss:.1f} MiB",
          flush=True)
    assert s["rows"] == 100_000, s
    assert s["total_tails"] == 1000, s
    assert s["unique"] == 1000, s
    assert s["no_solution"] == s["multiple"] == s["error"] == 0, s

    detail = load_tail_detail(job_dir, "T-0500")
    assert detail["status"] == "unique" and len(detail["segments"]) == 100
    print("      懒加载 T-0500 明细：100 段，OK", flush=True)

    t0 = time.monotonic()
    targets, _ = resolve_export_targets(job_dir, "__all__")
    out_path = work / "utc_export_all.csv"
    rows = 0
    with open(out_path, "w", encoding="utf-8") as out:
        for chunk in export_stream(job_dir, targets):
            out.write(chunk)
            rows += chunk.count("\n")
    dt = time.monotonic() - t0
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print(f"[4/4] 全批流式导出：{rows} 行（含表头），耗时 {dt:.1f}s，"
          f"峰值 RSS ≈ {rss:.1f} MiB", flush=True)
    assert rows == 100_001, rows
    assert out_path.stat().st_size > 0

    print(f"\n✅ 256 MiB 限制下清洗 + 明细懒加载 + 全批导出全部成功，"
          f"峰值 RSS ≈ {rss:.1f} MiB（上限 {limit_mib} MiB）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""交错航段样例生成器（纯标准库，无任何夏令时歧义）。

设计
----
- 所有机场时区均为**全年固定偏移、从无夏令时**的 IANA 时区（上海/新加坡/
  东京/迪拜/曼谷），因此每个当地时刻只有一个 UTC 候选，整链必然唯一解；
- 航段链以 **UTC 绝对时间**推进：每段落地 UTC 之后留 30~90 分钟过站，
  下一段起飞 UTC 不早于上一段落地 UTC（满足链约束），再按该段起飞机场的
  固定偏移换算成当地钟面时间写入 CSV；
- 飞行时长闭区间按实际飞行分钟数 ±10 分钟给出，实际值落在区间正中间，
  且当地时刻无歧义，保证整链恰好唯一解；
- 输出时按「轮询机尾」的方式交错写入：先所有机尾的第 1 段，再所有机尾的
  第 2 段……同一机尾的行在文件中被完全打散，用来演示无需预排序、
  按机尾归集后仍按 seq 做整链判定；
- 默认 1000 机尾 × 每机尾 100 段 = 10 万行，对应内存受限场景的验收规模，
  也可用小参数生成网页演示用的小文件。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

HEADER = ("tail_number,seq,dep_local,dep_tz,arr_local,arr_tz,"
          "duration_min_minutes,duration_max_minutes")

# (IANA 时区, UTC 偏移小时) —— 全部为无夏令时地区
_ZONES = [
    ("Asia/Shanghai", 8),
    ("Asia/Singapore", 8),
    ("Asia/Tokyo", 9),
    ("Asia/Dubai", 4),
    ("Asia/Bangkok", 7),
]
_FMT = "%Y-%m-%d %H:%M"
_TURN_MIN, _TURN_MAX = 30, 90      # 过站时间（分钟）
_WINDOW = 10                       # 时长区间相对实际值的对称余量（分钟）


def _zone_for(tail_idx: int, seq: int, side: str) -> tuple[str, int]:
    """为机尾/航段确定性地选取起降时区（同一时刻唯一 UTC 映射）。"""
    dep_i = (tail_idx * 3 + seq * 2) % len(_ZONES)
    arr_i = (dep_i + 1 + (seq % 2)) % len(_ZONES)
    return _ZONES[dep_i] if side == "dep" else _ZONES[arr_i]


def _to_local(dt_utc: datetime, offset_hours: int) -> datetime:
    return (dt_utc + timedelta(hours=offset_hours)).replace(tzinfo=None)


def build_chain(tail: str, tail_idx: int, n_segments: int) -> list[str]:
    """生成单机尾按 seq 升序的 CSV 行（UTC 时间严格递增、衔接合法）。"""
    # 不同机尾错开起始时刻，避免 1000 架机尾挤在同一时刻
    next_dep_utc = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc) + timedelta(
        days=tail_idx % 28, hours=(tail_idx * 7) % 18)
    rows = []
    for seq in range(1, n_segments + 1):
        dep_tz, dep_off = _zone_for(tail_idx, seq, "dep")
        arr_tz, arr_off = _zone_for(tail_idx, seq, "arr")

        dep_local = _to_local(next_dep_utc, dep_off)
        flight = 70 + ((tail_idx * 13 + seq * 17) % 191)  # 70~260 分钟
        arr_utc = next_dep_utc + timedelta(minutes=flight)
        arr_local = _to_local(arr_utc, arr_off)

        rows.append(",".join([
            tail, str(seq),
            dep_local.strftime(_FMT), dep_tz,
            arr_local.strftime(_FMT), arr_tz,
            str(flight - _WINDOW), str(flight + _WINDOW),
        ]))
        # 过站后才是下一段最早起飞时刻（UTC 绝对时间，链约束天然满足）
        turn = _TURN_MIN + ((tail_idx + seq * 5) %
                            (_TURN_MAX - _TURN_MIN + 1))
        next_dep_utc = arr_utc + timedelta(minutes=turn)
    return rows


def _build_segment(tail: str, tail_idx: int, seq: int) -> str:
    """按 :func:`build_chain` 同一套确定性规则，只计算指定 seq 的一行。

    从链首重算到该 seq（每段只做整数分钟运算，开销极小），
    使调用方可以流式逐行产出而不缓存任何机尾的完整链。
    """
    next_dep_utc = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc) + timedelta(
        days=tail_idx % 28, hours=(tail_idx * 7) % 18)
    row = ""
    for s in range(1, seq + 1):
        dep_tz, dep_off = _zone_for(tail_idx, s, "dep")
        arr_tz, arr_off = _zone_for(tail_idx, s, "arr")
        dep_local = _to_local(next_dep_utc, dep_off)
        flight = 70 + ((tail_idx * 13 + s * 17) % 191)
        arr_utc = next_dep_utc + timedelta(minutes=flight)
        arr_local = _to_local(arr_utc, arr_off)
        if s == seq:
            row = ",".join([
                tail, str(s),
                dep_local.strftime(_FMT), dep_tz,
                arr_local.strftime(_FMT), arr_tz,
                str(flight - _WINDOW), str(flight + _WINDOW),
            ])
        turn = _TURN_MIN + ((tail_idx + s * 5) %
                            (_TURN_MAX - _TURN_MIN + 1))
        next_dep_utc = arr_utc + timedelta(minutes=turn)
    return row


def iter_interleaved_csv(n_tails: int = 1000, n_segments: int = 100,
                         tail_prefix: str = "T-"):
    """生成器：逐行产出交错排列的 CSV 文本（含表头），内存占用恒定。

    写入顺序为「先所有机尾的第 1 段，再第 2 段……」，同一机尾的行被完全打散。
    """
    yield HEADER + "\n"
    for seq in range(1, n_segments + 1):
        for tail_idx in range(n_tails):
            tail = f"{tail_prefix}{tail_idx + 1:04d}"
            yield _build_segment(tail, tail_idx, seq) + "\n"


def write_interleaved_csv(path, n_tails: int = 1000,
                          n_segments: int = 100) -> int:
    """把交错 CSV 流式写入文件路径，返回写入的数据行数。"""
    n = 0
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(HEADER + "\n")
        for seq in range(1, n_segments + 1):
            for tail_idx in range(n_tails):
                tail = f"T-{tail_idx + 1:04d}"
                f.write(_build_segment(tail, tail_idx, seq) + "\n")
                n += 1
    return n

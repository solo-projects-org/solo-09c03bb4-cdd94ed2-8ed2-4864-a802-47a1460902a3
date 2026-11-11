# 航段时间清洗 · DST 安全入仓（内存有界版）

面向航司数据工程师的网页工具：把地服导出的**当地时间**航段链解析为唯一 **UTC** 时间链，
避免夏令时（DST）切换时被错误入仓。针对「内存有限的服务器补录历史航段」重构为
**流式管道**：1000 个机尾 × 每机尾 100 段（10 万行）在 **256 MiB** 内存上限下，
清洗预览与批量下载均可完成（实测业务进程峰值 RSS ≈ 16 MiB）。

## 快速启动

```bash
docker compose up --build
```

浏览器打开 <http://localhost:8000>。演示路径：

1. 点「🔀 载入交错样例（无 DST）」→「开始清洗」：先看到**全批统计 + 各机尾状态**列表；
2. 点任意机尾行，才会加载它的**原有明细**（候选/解/冲突，懒加载）；
3. 机尾卡片上「⬇ 导出该机尾 UTC CSV」，或顶部「⬇ 批量导出」。

容器内存被 `docker-compose.yml` 限制为 **256 MiB**（`mem_limit` + `deploy.resources`，
并禁用 swap 兜底）；作业 spool 挂在 64 MiB tmpfs 上。

本地开发（不用 Docker）：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## 内存受限下的处理模型

| 环节 | 旧做法（易 OOM） | 现做法（内存有界） |
| --- | --- | --- |
| 上传 | `await file.read()` 整份进内存 | 1 MiB 分块流式落盘，超限直接 413 |
| 解析分组 | 全量 `dict[tail] -> 所有行` 驻留 | 按机尾写入各自 spool 文件，LRU 句柄池（上限 64） |
| 求解 | 所有机尾结果一次性返回 | 逐机尾求解，任意时刻内存只有一架机尾；明细落盘 |
| 预览 | 首屏返回全部机尾明细 | 首屏只给**统计 + 机尾摘要**，点开机尾才懒加载明细 |
| 导出 | 重新求解全部并拼接整个 CSV | 逐机尾重新求解复核，`StreamingResponse` 分块产出 |

- **交错输入天然支持**：不同机尾的行可以任意交错、无需预先排序；
  spool 按机尾归集后，组内仍按 `seq` 升序做整链判定，规则不变。
- 作业目录结构：`meta.json`（机尾→索引映射）、`summary.json`（统计+摘要）、
  `spool/<idx>.csv`（该机尾原始行）、`details/<idx>.json`（完整明细，按需读）。
- 作业按数量（20 个）与 TTL（2 小时）自动清理，进程启动时清空残留。

## 输入 CSV 格式

UTF-8 编码，必需表头：

```csv
tail_number,seq,dep_local,dep_tz,arr_local,arr_tz,duration_min_minutes,duration_max_minutes
B-7001,1,2026-10-25 00:30,Europe/London,2026-10-25 02:15,Europe/Berlin,90,130
```

| 列 | 含义 |
| --- | --- |
| `tail_number` | 机尾号（同一机尾构成一条航段链） |
| `seq` | 航段顺序，整数，同一机尾内唯一，按升序链接 |
| `dep_local` / `arr_local` | 起飞/落地当地时间，`YYYY-MM-DD HH:MM`（也接受 `T` 分隔与秒） |
| `dep_tz` / `arr_tz` | 两端 IANA 时区名，如 `Europe/Berlin`、`America/New_York` |
| `duration_min_minutes` / `duration_max_minutes` | 飞行时长下/上限（分钟，**闭区间**） |

## 求解规则（与旧版完全一致）

1. 当地时间 → UTC 候选：
   - 普通时刻 → 1 个候选；
   - **重复时刻**（秋季拨回重叠，如柏林 2026-10-25 02:30）→ 保留 **2 个候选**参与整链求解；
   - **不存在的时刻**（春季拨快空洞，如柏林 2026-03-29 02:30）→ 该行报错，整架机尾禁止导出。
2. 每个航段的 (起飞, 落地) 候选组合必须满足：飞行时长 ∈ `[下限, 上限]`（闭区间）。
3. 按 `seq` 链接同一机尾：下一段起飞 **不得早于** 上一段落地（相等允许）。
4. 判定：
   - **整链唯一解** → 允许导出该机尾的 UTC CSV；
   - **无解** → 展示冲突航段（前向扫描定位第一个无法衔接的航段及其候选），禁止导出；
   - **多解** → 展示全部可行候选链（最多展示 20 条），禁止导出；
   - **行级错误**（不存在时刻 / 非法时区 / 格式错误 / seq 重复等）→ 禁止导出。

## 导出

- 网页上按机尾导出，或「批量导出（仅唯一解机尾）」；
- 沿用 UTC CSV 列：`tail_number,seq,dep_utc,arr_utc,duration_minutes`
  （UTC 为 ISO 8601 `Z` 格式）；
- 服务端在导出时**逐机尾重新求解并复核唯一性**，非唯一解返回 409，杜绝误导出；
- 批量导出同样是流式分块，只包含整链唯一解的机尾。

## 交错样例与生成脚本

- 网页「🔀 载入交错样例」调用在线生成接口（默认 6 机尾 × 8 段）；
- 仓库内置小文件 `samples/interleaved_small.csv`（机尾行轮询交错、全部使用
  上海/新加坡/东京/迪拜/曼谷等**无夏令时**时区，整链唯一解）；
- 离线生成任意规模（默认即验收规模 1000×100）：

```bash
python3 -I scripts/generate_sample.py samples/interleaved_sample.csv \
    --tails 1000 --segments 100
# 网页演示小文件
python3 -I scripts/generate_sample.py samples/interleaved_small.csv --tails 6 --segments 8
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 网页入口 |
| `GET` | `/api/sample` | 内置 DST 样例 CSV（含唯一/无解/多解/错误四类机尾） |
| `GET` | `/api/sample/interleaved?tails=&segments=` | 在线流式生成交错无歧义样例 |
| `POST` | `/api/solve` | 上传 CSV（multipart 字段 `file`，≤10 MB），返回作业 id、全批统计与各机尾**摘要** |
| `GET` | `/api/jobs/{job_id}` | 作业的统计与机尾摘要列表 |
| `GET` | `/api/jobs/{job_id}/tails/{tail}` | **懒加载**单机尾完整明细 |
| `GET` | `/api/jobs/{job_id}/export?tail=` | 流式导出 UTC CSV；`tail=__all__`（默认）为全部唯一解机尾；非唯一解 409 |
| `GET` | `/docs` | FastAPI 自动文档 |
| `GET` | `/api/health` | 健康检查 |

## 自检与验收（仅标准库，HTTP 冒烟另需 fastapi/httpx）

```bash
python3 -I tests/test_solver.py        # 求解规则 + 流式读取 + 打乱行序等价性
python3 -I tests/test_pipeline.py      # spool/懒加载/409/404/作业隔离（28 项）
python3 -I tests/test_memory_256.py /tmp/interleaved_1000x100.csv 256
# 256 MiB RLIMIT_AS 硬限制下完成清洗 + 明细 + 全批导出，实测峰值 RSS ≈ 16 MiB
```

## 目录结构

```
app/
  main.py            # FastAPI 路由（流式上传、作业式 API、StreamingResponse）
  pipeline.py        # 机尾 spool/LRU、逐机尾求解、懒加载、流式导出
  solver.py          # 候选生成 + 整链求解 + 冲突定位（纯标准库，含流式 CSV 入口）
  samplegen.py       # 交错无 DST 歧义样例生成器（恒定内存）
  static/index.html  # 单页前端（摘要列表 + 点击懒加载明细）
samples/
  dst_sample.csv            # 内置 DST 四类场景
  interleaved_small.csv     # 交错无歧义小样本（网页演示）
scripts/generate_sample.py # 离线样例生成脚本
tests/                       # solver / pipeline / 256MiB 验收
Dockerfile / docker-compose.yml / requirements.txt
```

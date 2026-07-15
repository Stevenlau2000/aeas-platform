# PE-7 · Operations Center Specification

> **Document ID:** OPS-SPEC
> **Layer:** S5 · Platform Engineering（PE-7）
> **Status:** Draft v0.1 · 2026-07-12
> **Connected to:** S7 Operations（Cloud/运营层）
> **Audience:** 平台运营者、SRE、企业管理员

---

## 0. 为什么需要 Operations Center

> 企业最终都会来到运营中心——查看 Running Apps、Capability Usage、Knowledge Usage、
> LLM Usage、Cost、Latency、Health、Alerts。
>
> PE-7 是 **S5 平台的最后一层**，也是 S7 Operations（多租户/云部署/运营后台）的前置。

---

## 1. 运营指标

| 指标 | 数据源 | 展示方式 |
|---|---|---|
| **System Health** | 最近 5 次 run 结果 | 绿/黄/红状态灯 |
| **Running Apps** | `registry/installed.yaml` | 安装包列表 + 状态 |
| **Runtime Events** | `.aeaos/run/events.jsonl` | 事件总数 + 分类计数 |
| **Foundation Events** | `.aeaos/run/metrics.jsonl` | foundation.* 分路由键计数 |
| **DLQ** | `run result` DLQ 字段 | DLQ 计数 |
| **Capability Usage** | events JSONL 中 `scheduler.task_started` | 各能力调用次数 |
| **Latency** | `run result` elapsed_s | 最新运行耗时 |
| **Audit Log** | events JSONL | 最近事件时间线 |

---

## 2. Dashboard 面板

| 面板 | 内容 | 列数 |
|---|---|---|
| System Health | 状态灯 + 最后运行时间 | 1 |
| Key Metrics | 6 张量化卡片（事件/指标/DLQ/Installed/Health Score） | 3 |
| Foundation Events | 各 routing_key 计数 | 1 |
| Capability Usage | 能力调用次数条形图 | 1 |
| Recent Events | 最近 20 条事件时间线 | 1 |

---

## 3. 数据采集器（ops/collector.py）

自动从以下位置采集数据：

| 源 | 路径 | 采集内容 |
|---|---|---|
| Runtime Events | `.aeaos/run/events.jsonl` | 全部运行时事件 |
| Foundation Metrics | `.aeaos/run/metrics.jsonl` | foundation.* 事件 |
| Installed Packages | `registry/installed.yaml` | 已安装包 |
| Capability Registry | `registry/capability.yaml` | 能力定义 |
| Marketplace | `marketplace/*.yaml` | 已发布包 |

输出：`ops/data.json`（结构化 JSON 供 Dashboard 消费）

---

## 4. 与 S7 Operations 的关系

- PE-7 是 S7 运营层的**数据基座**。
- S7 在此基础上增加：多租户 / 组织管理 / 计费 / 云部署 / 运营后台。
- PE-7 的采集器在 S7 成为独立的微服务，不再从本地 JSONL 读取。

---

## 5. 接生产事件流（任务 2 增量）

运维层默认从仓库内 `.aeaos/run/events.jsonl` 读取事件。要接入**真实生产事件流**，
只需让采集器指向真实事件源文件，无需改动 `data.json` schema 或采集逻辑。

### 5.1 事件源路径解析优先级（高 → 低）

| 优先级 | 来源 | 用法 |
|---|---|---|
| ① 最高 | CLI `--source <path>` | `python ops/collector.py --source /path/events.jsonl`<br/>`aeaos ops collect --source /path/events.jsonl` |
| ② 中 | 环境变量 `AEAOS_EVENTS_PATH` | `export AEAOS_EVENTS_PATH=/path/events.jsonl` 后直接 `aeaos ops collect` |
| ③ 默认回退 | 仓库内 `.aeaos/run/events.jsonl` | 不传任何参数时的行为（相对仓库根解析为绝对路径） |

- `--source` 接受**单个 JSONL 文件路径**（本期不接目录通配、不接消息总线）。
- 默认回退把仓库根解析为绝对路径后拼接，对真实仓库根等价于
  `.aeaos/run/events.jsonl`（绝对路径）；对临时/CI 仓库则停留在临时目录内，
  不会误读真实仓库事件流（零仓库污染）。

### 5.2 推荐接法

```bash
# 方式一：CLI --source（最明确，优先级最高）
aeaos ops collect --source /var/lib/aeaos/run/events.jsonl
python ops/collector.py --source /var/lib/aeaos/run/events.jsonl

# 方式二：环境变量（适合常驻 cron / systemd 定时采集）
export AEAOS_EVENTS_PATH=/var/lib/aeaos/run/events.jsonl
aeaos ops collect        # 自动读取该真实事件流

# 方式三：默认（不推荐生产，仅本地开发）
aeaos ops collect        # 读取仓库内 .aeaos/run/events.jsonl
```

> 真实事件流就是既有的 `.aeaos/run/events.jsonl` 格式（每行一个 EventBus 事件，
> 含 `trace_id` / `session_id` / `routing_key` / `payload`）。把该文件软链或拷贝到
> 部署机上，再以上述任一方式指向即可。

### 5.3 健壮性

- `--source` 指向文件**不存在 / 为空 / 含损坏行**时，采集器**不崩溃**，
  跳过损坏行并产出**零值/空壳 `data.json`**（与默认事件源缺失行为一致）。
- 前端 `ops/index.html` 对空壳 `data.json` 也能渲染（空列表/状态灯降级），
  不会出现白屏。

### 5.4 已知限制（P2，本期不实现）

- **消息总线（Kafka / NATS）接入**：因**零依赖约束**（仅标准库 + 既有 PyYAML，
  不引入第三方客户端），本期**不接消息总线**。实时消费生产事件流留作 **P2**，
  待生产 transport 就绪后通过环境变量/CLI 切换为「总线订阅」模式。
- **目录通配 / 多文件合并**：本期 `--source` 仅接受单个文件，不支持
  `events-*.jsonl` 通配或目录扫描（留作后续）。
- **DLQ 重放 / 告警自愈**：本期仅监控+定位（关联 `original_event_id`），
  重投/重试留 P2（见 design.md §8 待明确事项 6）。


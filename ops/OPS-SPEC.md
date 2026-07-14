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

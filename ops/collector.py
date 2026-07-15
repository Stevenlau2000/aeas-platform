#!/usr/bin/env python3
"""Operations Center 数据采集器（L7 Operations 层单一数据源）。

把既有 EventBus 事件流（`.aeaos/run/events.jsonl`）与既有 Registry YAML
（`registry/solution-promotions.yaml` / `registry/workspace-store.yaml` 等）加工成
一份结构化 `ops/data.json`，供前端 `ops/index.html` + `ops/app.js` 作为唯一数据源。
零新增第三方依赖：仅标准库 + 既有 `yaml`（PyYAML）。

数据流：
    events.jsonl + registry/*.yaml  →  OpsCollector.build()  →  ops/data.json
                                                        └→ ops/aep-history.jsonl（AEP 时序幂等合并）

设计：docs/operations/design.md §3（OpsData 树）。所有字段 snake_case。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

# AEP 阈值：低于该值的 run 计入 Health 扣分（design.md §8 待明确事项 3）。
# 优先复用 poc.orchestrator 的真实常量，失败则回退到文档约定值 0.70。
try:
    _ROOT_IMPORT = Path(__file__).resolve().parent.parent
    if str(_ROOT_IMPORT) not in sys.path:
        sys.path.insert(0, str(_ROOT_IMPORT))
    from poc.orchestrator import AEP_THRESHOLD  # type: ignore
except Exception:  # noqa: BLE001 - 采集器不应因运行时缺失而崩溃
    AEP_THRESHOLD = 0.70

# 事件 routing_key 常量（design.md §7.2）
RK_DEAD_LETTER = "event_bus.dead_letter"
RK_STATE_TRANSITION = "orchestrator.state_transition"
RK_LOOP_ENDED = "orchestrator.loop_ended"
RK_SCORE_COMPUTED = "evaluation.score_computed"
RK_ESCALATION = "policy.escalation_required"
RK_COACH_APPROVAL = "app.coach.approval"
RK_COACH_REQUESTED = "app.coach.requested"
RK_TASK_STARTED = "scheduler.task_started"

# Health 扣分参数（3b 增量：可通过环境变量配置，向后兼容）
_HEALTH_DEFAULTS = {
    "DLQ_PENALTY_PER": 5.0,
    "DLQ_PENALTY_CAP": 50.0,
    "AEP_PENALTY_PER": 2.0,
    "AEP_PENALTY_CAP": 30.0,
}
_HEALTH_ENV_MAP = {
    "DLQ_PENALTY_PER": "AEAOS_HEALTH_DLQ_PENALTY_PER",
    "DLQ_PENALTY_CAP": "AEAOS_HEALTH_DLQ_PENALTY_CAP",
    "AEP_PENALTY_PER": "AEAOS_HEALTH_AEP_PENALTY_PER",
    "AEP_PENALTY_CAP": "AEAOS_HEALTH_AEP_PENALTY_CAP",
}


def _load_health_config() -> dict:
    """从环境变量加载 Health 权重参数，未设置时使用默认值。

    环境变量对照：
      AEAOS_HEALTH_DLQ_PENALTY_PER  (默认 5.0) — 每条 DLQ 扣分
      AEAOS_HEALTH_DLQ_PENALTY_CAP   (默认 50.0) — DLQ 扣分上限
      AEAOS_HEALTH_AEP_PENALTY_PER   (默认 2.0)  — 每个低于阈值的 AEP run 扣分
      AEAOS_HEALTH_AEP_PENALTY_CAP   (默认 30.0) — AEP 扣分上限
    """
    config = dict(_HEALTH_DEFAULTS)
    for key, env_var in _HEALTH_ENV_MAP.items():
        val = os.environ.get(env_var, "").strip()
        if val:
            try:
                config[key] = float(val)
            except (ValueError, TypeError):
                pass  # 非法的环境变量值忽略，回退到默认
    return config


# Health 运行时配置（模块级缓存，避免每次 collect_health 都读 env）
_HEALTH_CONFIG = _load_health_config()

# recent_events 在前端展示的最大量（全量 trace 仍完整保留于 traces[].events）
RECENT_EVENTS_LIMIT = 500
# aep-history.jsonl 保留的最大点数（防止无限增长）
AEP_HISTORY_MAX = 5000

# 事件源路径配置化（任务 2 增量）
# 解析优先级（高→低）：CLI --source > 环境变量 AEAOS_EVENTS_PATH > 默认回退。
EVENTS_PATH_ENV = "AEAOS_EVENTS_PATH"


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    """按行读取 JSONL，跳过损坏行。文件不存在返回空列表。"""
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def resolve_events_path(
    explicit: Optional[Path] = None,
    repo: Optional[Path] = None,
) -> Path:
    """解析事件源 JSONL 路径，优先级（高→低）：

    1. 显式传入（CLI ``--source`` 或注入 ``events_path``）；
    2. 环境变量 ``AEAOS_EVENTS_PATH``；
    3. 默认回退：``<repo>/.aeaos/run/events.jsonl``。

    默认回退把仓库根解析为绝对路径后再拼接（对真实仓库根等价于 spec 给定的绝对
    路径 ``/Users/mac/WorkBuddy/Amaris Enterprise AI Agent Operating System（AEAOS）/.aeaos/run/events.jsonl``）；
    对临时仓库则停留在临时目录内，避免读取真实事件流（零仓库污染）。

    文件是否存在由读取层 ``_read_jsonl`` 兜底，此处不强制存在。
    """
    # ① 显式路径（--source）优先级最高
    if explicit is not None:
        return Path(explicit)

    # ② 环境变量 AEAOS_EVENTS_PATH
    env_val = os.environ.get(EVENTS_PATH_ENV, "").strip()
    if env_val:
        return Path(env_val)

    # ③ 默认回退：仓库根解析为绝对路径后拼接
    base = Path(repo) if repo else Path(__file__).resolve().parent.parent
    return base.resolve() / ".aeaos" / "run" / "events.jsonl"


def _safe_load_yaml(path: Path) -> Dict[str, Any]:
    """读取 YAML，失败返回空 dict。"""
    try:
        import yaml
        if path.exists():
            return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return {}
    except Exception:  # noqa: BLE001
        return {}


def _trim_event(e: Dict[str, Any]) -> Dict[str, Any]:
    """压缩事件体积：仅保留前端需要的字段（去掉 policy_context / version）。"""
    return {
        "event_id": e.get("event_id", ""),
        "ts": e.get("timestamp_utc", ""),
        "routing_key": e.get("routing_key", ""),
        "source": e.get("source", ""),
        "session_id": e.get("session_id", ""),
        "trace_id": e.get("trace_id", ""),
        "tenant_id": e.get("tenant_id", ""),
        "payload": e.get("payload", {}),
    }


class OpsCollector:
    """运维层采集器：读取真相源 → 计算 OpsData → 写出 data.json。

    依赖注入：events_path / solution_manager / workspace_manager / 各输出路径均可
    在测试中被覆盖，默认从仓库根推导（与既有 `ops/collector.py` 行为一致）。
    """

    def __init__(
        self,
        repo: Optional[Path] = None,
        events_path: Optional[Path] = None,
        solution_manager: Any = None,
        workspace_manager: Any = None,
        data_path: Optional[Path] = None,
        aep_history_path: Optional[Path] = None,
        hitl_decisions_path: Optional[Path] = None,
    ) -> None:
        self.repo = Path(repo) if repo else Path(__file__).resolve().parent.parent
        # 事件源路径：CLI --source > 环境变量 AEAOS_EVENTS_PATH > 默认回退
        self.events_path = resolve_events_path(explicit=events_path, repo=self.repo)
        self.data_path = Path(data_path) if data_path else self.repo / "ops" / "data.json"
        self.aep_history_path = (
            Path(aep_history_path) if aep_history_path
            else self.repo / "ops" / "aep-history.jsonl"
        )
        self.hitl_decisions_path = (
            Path(hitl_decisions_path) if hitl_decisions_path
            else self.repo / "ops" / "hitl-decisions.jsonl"
        )
        # 懒加载的管理器（可被测试注入）
        self._sm = solution_manager
        self._wm = workspace_manager

    # ── 管理器（按需构建，导入失败则降级为空） ────────────────────────────────
    @property
    def solution_manager(self) -> Any:
        if self._sm is None:
            try:
                from registry.solution import SolutionManager
                self._sm = SolutionManager(self.repo)
            except Exception:  # noqa: BLE001
                self._sm = _NullSolutionManager()
        return self._sm

    @property
    def workspace_manager(self) -> Any:
        if self._wm is None:
            try:
                from registry.workspace import WorkspaceManager
                self._wm = WorkspaceManager(self.repo)
            except Exception:  # noqa: BLE001
                self._wm = _NullWorkspaceManager()
        return self._wm

    # ── 读取事件 ─────────────────────────────────────────────────────────────
    def _read_events(self) -> List[Dict[str, Any]]:
        """读取全量事件流（list[dict]）。"""
        return _read_jsonl(self.events_path)

    # ── 各维度采集 ───────────────────────────────────────────────────────────
    def collect_runtime(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """运行态概览：事件总数、最近事件样本、按 routing_key 分布、capability 用量。"""
        total = len(events)
        # 倒序（最新在前），取最近 N 条用于前端 Event Explorer 默认视图
        ordered = sorted(events, key=lambda e: e.get("timestamp_utc", ""), reverse=True)
        recent = [_trim_event(e) for e in ordered[:RECENT_EVENTS_LIMIT]]

        by_key = dict(Counter(
            e.get("routing_key", "unknown") for e in events
        ).most_common(50))

        cap_usage: Counter = Counter()
        for e in events:
            rk = e.get("routing_key", "")
            if rk == RK_TASK_STARTED or "executor.capability_started" in rk:
                cap = (e.get("payload", {}) or {}).get("capability", "")
                if cap:
                    cap_usage[cap] += 1
        capability_usage = dict(cap_usage.most_common(30))

        return {
            "total_events": total,
            "recent_events": recent,
            "event_count_by_key": by_key,
            "capability_usage": capability_usage,
        }

    def collect_health(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """系统健康分：DLQ 与 AEP 低于阈值的 run 扣分（design.md §8）。"""
        dlq_count = sum(1 for e in events if e.get("routing_key") == RK_DEAD_LETTER)

        # 统计「低于阈值的 run」：按 session 取最后一次 evaluation.score_computed
        below_runs = 0
        last_score: Dict[str, float] = {}
        for e in events:
            if e.get("routing_key") == RK_SCORE_COMPUTED:
                sid = e.get("session_id", "")
                wt = (e.get("payload", {}) or {}).get("weighted_total")
                if sid and wt is not None:
                    last_score[sid] = float(wt)
        below_runs = sum(1 for v in last_score.values() if v < AEP_THRESHOLD)

        score = 100.0
        hc = _HEALTH_CONFIG  # 3b 增量：从环境变量加载的配置
        if dlq_count > 0:
            score -= min(hc["DLQ_PENALTY_CAP"], dlq_count * hc["DLQ_PENALTY_PER"])
        if below_runs > 0:
            score -= min(hc["AEP_PENALTY_CAP"], below_runs * hc["AEP_PENALTY_PER"])
        if not events:
            score = 0.0
        score = max(0.0, round(score, 1))

        if score >= 80:
            status = "normal"
        elif score >= 50:
            status = "degraded"
        else:
            status = "critical"

        last_ts = events[-1].get("timestamp_utc", "") if events else "never"

        return {
            "score": score,
            "status": status,
            "dlq_count": dlq_count,
            "aep_below_threshold_runs": below_runs,
            "last_event_time": last_ts,
        }

    def collect_kpis(
        self,
        events: List[Dict[str, Any]],
        health: Dict[str, Any],
        hitl: List[Dict[str, Any]],
        alerts: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """顶层 KPI 卡数据。"""
        total = len(events)

        # AEP 均值：按 session 取最后得分
        last_score: Dict[str, float] = {}
        for e in events:
            if e.get("routing_key") == RK_SCORE_COMPUTED:
                sid = e.get("session_id", "")
                wt = (e.get("payload", {}) or {}).get("weighted_total")
                if sid and wt is not None:
                    last_score[sid] = float(wt)
        aep_avg = round(sum(last_score.values()) / len(last_score), 4) if last_score else 0.0

        # 安装 / 晋升到 prod 的 solution 数
        installed = 0
        promoted_prod = 0
        try:
            installed = len(self.solution_manager.list_installed())
            for pv in self.collect_promotions(self.solution_manager):
                if pv.get("current_env") == "prod":
                    promoted_prod += 1
        except Exception:  # noqa: BLE001
            pass

        open_hitl = sum(1 for h in hitl if h.get("status") not in ("resolved",))
        open_alerts = len(alerts)

        return {
            "events": total,
            "aep_avg": aep_avg,
            "dlq": int(health.get("dlq_count", 0)),
            "installed": installed,
            "solutions_promoted_prod": promoted_prod,
            "health": float(health.get("score", 0.0)),
            "open_hitl": open_hitl,
            "open_alerts": open_alerts,
        }

    def collect_alerts(
        self,
        events: List[Dict[str, Any]],
        promotions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """从事件流派生的告警（DLQ / escalation / AEP 低于阈值 / P5 门未过）。"""
        alerts: List[Dict[str, Any]] = []

        # 1) DLQ 事件 → 高严重告警
        for e in events:
            if e.get("routing_key") == RK_DEAD_LETTER:
                pl = e.get("payload", {}) or {}
                alerts.append({
                    "id": e.get("event_id", ""),
                    "type": "dlq",
                    "severity": "high",
                    "title": "Dead Letter Queue",
                    "detail": f"consumer={pl.get('consumer','?')} routing_key={pl.get('routing_key','?')}",
                    "trace_id": e.get("trace_id", ""),
                    "session_id": e.get("session_id", ""),
                    "raised_at": e.get("timestamp_utc", ""),
                    "acknowledged": False,
                })

        # 2) policy.escalation_required → 高严重告警
        for e in events:
            if e.get("routing_key") == RK_ESCALATION:
                pl = e.get("payload", {}) or {}
                alerts.append({
                    "id": e.get("event_id", ""),
                    "type": "escalation",
                    "severity": "high",
                    "title": "Policy Escalation Required",
                    "detail": f"capability={pl.get('capability','?')} verdict={pl.get('verdict','?')}",
                    "trace_id": e.get("trace_id", ""),
                    "session_id": e.get("session_id", ""),
                    "raised_at": e.get("timestamp_utc", ""),
                    "acknowledged": False,
                })

        # 3) AEP 低于阈值的 session → 中严重告警
        last_score: Dict[str, tuple] = {}
        for e in events:
            if e.get("routing_key") == RK_SCORE_COMPUTED:
                sid = e.get("session_id", "")
                wt = (e.get("payload", {}) or {}).get("weighted_total")
                if sid and wt is not None:
                    last_score[sid] = (float(wt), e.get("trace_id", ""), e.get("timestamp_utc", ""))
        for sid, (wt, tid, ts) in last_score.items():
            if wt < AEP_THRESHOLD:
                alerts.append({
                    "id": f"aep-{sid}",
                    "type": "aep",
                    "severity": "medium",
                    "title": "AEP Below Threshold",
                    "detail": f"session={sid} weighted_total={wt} < {AEP_THRESHOLD}",
                    "trace_id": tid,
                    "session_id": sid,
                    "raised_at": ts,
                    "acknowledged": False,
                })

        # 4) P5 门未全过 → 中严重告警
        for pv in promotions:
            if not pv.get("all_gates_passed", True):
                alerts.append({
                    "id": f"promo-{pv.get('solution_id','')}",
                    "type": "promotion",
                    "severity": "medium",
                    "title": f"Promotion gate failed: {pv.get('solution_id','')}",
                    "detail": f"current_env={pv.get('current_env','?')} gates not all passed",
                    "trace_id": "",
                    "session_id": "",
                    "raised_at": "",
                    "acknowledged": False,
                })

        return alerts

    def collect_traces(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """按 trace_id 聚合完整调用链（事件已按时间排序）。"""
        by_trace: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for e in events:
            by_trace[e.get("trace_id", "")].append(e)

        traces: List[Dict[str, Any]] = []
        for tid, evs in by_trace.items():
            if not tid:
                continue
            evs_sorted = sorted(evs, key=lambda x: x.get("timestamp_utc", ""))
            session_id = evs_sorted[0].get("session_id", "")
            tenant_id = evs_sorted[0].get("tenant_id", "")
            end_state = ""
            passed = False
            aep = 0.0
            states: List[str] = []
            for e in evs_sorted:
                rk = e.get("routing_key", "")
                pl = e.get("payload", {}) or {}
                if rk == RK_LOOP_ENDED:
                    end_state = pl.get("end_state", "")
                    passed = bool(pl.get("passed", False))
                elif rk == RK_SCORE_COMPUTED:
                    aep = float(pl.get("weighted_total", 0.0))
                elif rk == RK_STATE_TRANSITION:
                    st = pl.get("state")
                    if st:
                        states.append(st)
            traces.append({
                "trace_id": tid,
                "session_id": session_id,
                "tenant_id": tenant_id,
                "event_count": len(evs_sorted),
                "end_state": end_state,
                "passed": passed,
                "aep": aep,
                "first_ts": evs_sorted[0].get("timestamp_utc", ""),
                "last_ts": evs_sorted[-1].get("timestamp_utc", ""),
                "states": states,
                "events": [_trim_event(e) for e in evs_sorted],
            })
        # 按最后时间倒序
        traces.sort(key=lambda t: t.get("last_ts", ""), reverse=True)
        return traces

    def collect_fsm_timelines(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """由 state_transition + loop_ended 还原每条 trace 的 9 态 FSM 时间线。"""
        by_trace: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for e in events:
            if e.get("routing_key") == RK_STATE_TRANSITION:
                by_trace[e.get("trace_id", "")].append(e)

        ended: Dict[str, Dict[str, Any]] = {}
        for e in events:
            if e.get("routing_key") == RK_LOOP_ENDED:
                ended[e.get("trace_id", "")] = e

        timelines: List[Dict[str, Any]] = []
        for tid, evs in by_trace.items():
            if not tid:
                continue
            evs_sorted = sorted(evs, key=lambda x: x.get("timestamp_utc", ""))
            steps = []
            for e in evs_sorted:
                pl = e.get("payload", {}) or {}
                steps.append({
                    "state": pl.get("state", ""),
                    "trigger": pl.get("trigger", ""),
                    "detail": pl.get("detail", ""),
                    "ts": e.get("timestamp_utc", ""),
                })
            end_ev = ended.get(tid, {})
            end_pl = end_ev.get("payload", {}) or {}
            timelines.append({
                "trace_id": tid,
                "session_id": evs_sorted[0].get("session_id", "") if evs_sorted else "",
                "steps": steps,
                "end_state": end_pl.get("end_state", ""),
                "passed": bool(end_pl.get("passed", False)),
            })
        timelines.sort(key=lambda t: t.get("trace_id", ""))
        return timelines

    def collect_promotions(self, sm: Any) -> List[Dict[str, Any]]:
        """从 Promotion Ledger（registry/solution-promotions.yaml）读取晋升状态 + P5 门。

        对每个 ledger key：current_env / current_version / history / gates / all_gates_passed。
        gates 优先用 SolutionManager.gates（真实 P5 计算），失败则回退到最近一次
        history 中记录的 gates（保证 scoping key 也能展示）。
        """
        promotions: List[Dict[str, Any]] = []
        try:
            ledger = sm._promotion._data.get("solutions", {})  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            try:
                from registry.promotion import EnvironmentPromotion
                ledger = EnvironmentPromotion(self.repo, sm)._data.get("solutions", {})
            except Exception:  # noqa: BLE001
                ledger = {}

        for key, rec in ledger.items():
            rec = rec or {}
            sid = key
            current_env = rec.get("current_env", "none")
            current_version = rec.get("current_version", "0.0.0")
            history = rec.get("history", []) or []

            # 尝试解析 name（非 scoped key 时从 solution 注册表取）
            name = rec.get("name") or sid
            try:
                sol = sm.resolve(sid)  # type: ignore[attr-defined]
                name = getattr(sol, "name", None) or name
            except Exception:  # noqa: BLE001
                pass

            # gates：真实 P5 计算 or history 回退
            gates: List[Dict[str, Any]] = []
            try:
                gates = sm.gates(sid)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                last = history[-1] if history else {}
                gates = last.get("gates", []) or []

            all_gates_passed = all(bool(g.get("passed")) for g in gates) if gates else True

            promotions.append({
                "solution_id": sid,
                "name": name,
                "current_env": current_env,
                "current_version": current_version,
                "history": list(history),
                "gates": [{
                    "gate": g.get("gate", ""),
                    "passed": bool(g.get("passed")),
                    "detail": g.get("detail", ""),
                } for g in gates],
                "all_gates_passed": all_gates_passed,
            })
        return promotions

    def collect_workspaces(self, wm: Any, sm: Any) -> List[Dict[str, Any]]:
        """Tenant → Org → Workspace 树 + 各 workspace 的 Solution 绑定与环境状态。"""
        tenants = []
        try:
            tenants = wm.list_tenants()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            return []

        tree: List[Dict[str, Any]] = []
        for t in tenants:
            orgs_out: List[Dict[str, Any]] = []
            for o in t.get("orgs", []) or []:
                wss_out: List[Dict[str, Any]] = []
                for ws in o.get("workspaces", []) or []:
                    ws_id = ws.get("id", "")
                    # 环境状态：env → enabled/disabled
                    envs = ws.get("environments", {}) or {}
                    allowed = []
                    try:
                        allowed = wm.allowed_envs(t["id"], ws_id)  # type: ignore[attr-defined]
                    except Exception:  # noqa: BLE001
                        pass
                    # Solution 绑定（按 (tenant,workspace) 隔离，零跨租户泄漏）
                    sols: List[Dict[str, Any]] = []
                    try:
                        bound = wm.list_solutions(t["id"], ws_id)  # type: ignore[attr-defined]
                        for b in bound:
                            sols.append({
                                "id": b.get("id", ""),
                                "env": b.get("env", ""),
                                "version": b.get("version", ""),
                                "status": b.get("status", ""),
                            })
                    except Exception:  # noqa: BLE001
                        pass
                    wss_out.append({
                        "id": ws_id,
                        "name": ws.get("name", ws_id),
                        "environments": dict(envs),
                        "allowed_envs": list(allowed),
                        "solutions": sols,
                    })
                orgs_out.append({"id": o.get("id", ""), "workspaces": wss_out})
            tree.append({"tenant": t.get("id", ""), "orgs": orgs_out})
        return tree

    def collect_hitl_queue(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """聚合 HITL 队列：escalation_required + app.coach.approval。

        红线类别（red_line）推导（design.md §7.5）：
          - app.coach.approval：由 category 映射
          - policy.escalation_required：由 capability 经 RegistryClient 解析 human_in_the_loop
            命中 reserved 类（Contract/Compliance/...）则取之，否则 Other。
        已被运维决策（ops/hitl-decisions.jsonl 含同 trace_id）的项标记为 resolved。
        """
        items: List[Dict[str, Any]] = []
        decisions = self._load_hitl_decisions()  # {trace_id: decision}

        # 红线类别查询器（按需构建）
        client = None

        def red_line_for_capability(cap: str) -> str:
            nonlocal client
            try:
                if client is None:
                    from registry.client import RegistryClient
                    client = RegistryClient(self.repo)
                hitl = client.resolve_capability(cap).get("human_in_the_loop", []) or []
                for reserved in ("Legal", "Financial", "Contract", "Compliance", "Risk", "Ethics"):
                    if reserved in hitl:
                        return reserved
                return "Other"
            except Exception:  # noqa: BLE001
                return "Other"

        def red_line_for_category(category: str) -> str:
            if category in ("deal_progression", "objection_handling", "negotiation"):
                return "Contract"
            if category in ("call_coaching", "pipeline_hygiene"):
                return "Risk"
            return "Other"

        for e in events:
            rk = e.get("routing_key", "")
            pl = e.get("payload", {}) or {}
            if rk == RK_ESCALATION:
                cap = pl.get("capability", "")
                items.append({
                    "item_id": e.get("event_id", ""),
                    "source": "orchestrator",
                    "red_line": red_line_for_capability(cap),
                    "capability": cap,
                    "category": "",
                    "policy": pl.get("verdict", ""),
                    "session_id": e.get("session_id", ""),
                    "trace_id": e.get("trace_id", ""),
                    "status": "open",
                    "raised_at": e.get("timestamp_utc", ""),
                    "decision": "",
                })
            elif rk == RK_COACH_APPROVAL:
                cat = pl.get("category", "")
                items.append({
                    "item_id": e.get("event_id", ""),
                    "source": "app",
                    "red_line": red_line_for_category(cat),
                    "capability": pl.get("capability", ""),
                    "category": cat,
                    "policy": pl.get("policy", ""),
                    "session_id": e.get("session_id", ""),
                    "trace_id": e.get("trace_id", ""),
                    "status": pl.get("status") or "pending",
                    "raised_at": e.get("timestamp_utc", ""),
                    "decision": "",
                })

        # 关联运维决策 → 标记 resolved
        for it in items:
            tid = it.get("trace_id", "")
            if tid and tid in decisions:
                it["status"] = "resolved"
                it["decision"] = decisions[tid]

        # 按 raised_at 倒序
        items.sort(key=lambda x: x.get("raised_at", ""), reverse=True)
        return items

    def _load_hitl_decisions(self) -> Dict[str, str]:
        """读取 ops/hitl-decisions.jsonl → {trace_id: decision}（最新决策覆盖）。"""
        out: Dict[str, str] = {}
        if not self.hitl_decisions_path.exists():
            return out
        for line in self.hitl_decisions_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            tid = d.get("trace_id", "")
            if tid:
                out[tid] = d.get("decision", "")
        return out

    def collect_dlq(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Dead Letter Queue 监控：计数 + 明细（关联 original_event_id 跳转 Event Explorer）。"""
        items: List[Dict[str, Any]] = []
        for e in events:
            if e.get("routing_key") == RK_DEAD_LETTER:
                pl = e.get("payload", {}) or {}
                items.append({
                    "event_id": e.get("event_id", ""),
                    "original_event_id": pl.get("original_event_id", ""),
                    "consumer": pl.get("consumer", ""),
                    "routing_key": pl.get("routing_key", ""),
                    "reason": pl.get("reason", ""),
                    "trace_id": e.get("trace_id", ""),
                    "session_id": e.get("session_id", ""),
                    "ts": e.get("timestamp_utc", ""),
                })
        return {"count": len(items), "items": items}

    def collect_learning_stream(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Learning Stream 数据采集（4a · OPS-021）。

        过滤 events 中 routing_key 含 learning/evolution/improve 的事件，
        加上 orchestrator.loop_ended 的 end_state=EVOLVE/LEARN。
        按时间倒序取最近 50 条。
        """
        learning_keys = {"learning", "evolution", "improve"}
        learning_items: List[Dict[str, Any]] = []

        for e in events:
            rk = e.get("routing_key", "")
            pl = e.get("payload", {}) or {}

            # 按 routing_key 含 learning/evolution/improve 过滤
            matched = any(k in rk for k in learning_keys)

            # 加上 orchestrator.loop_ended 且 end_state 为 EVOLVE/LEARN
            if not matched and rk == RK_LOOP_ENDED:
                end_state = pl.get("end_state", "")
                if end_state in ("EVOLVE", "LEARN"):
                    matched = True

            if not matched:
                continue

            # 构造 payload_summary（≤120 字）
            payload_str = json.dumps(pl, ensure_ascii=False)
            payload_summary = payload_str[:120]

            learning_items.append({
                "event_id": e.get("event_id", ""),
                "routing_key": rk,
                "session_id": e.get("session_id", ""),
                "trace_id": e.get("trace_id", ""),
                "ts": e.get("timestamp_utc", ""),
                "payload_summary": payload_summary,
            })

        # 按 ts 倒序，取最近 50 条
        learning_items.sort(key=lambda x: x.get("ts", ""), reverse=True)
        return learning_items[:50]

    def collect_foundation_panel(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Foundation 层指标面板（OPS-022）。

        统计 foundation 相关事件：routing_key 以 ``foundation`` 开头，
        或 source 为 ``foundation_bridge``（二者等价，均为 foundation 桥接产出）。
        输出 ``{total, by_type: {routing_key: count}, recent: [近 20 条]}``。
        """
        items = [
            e for e in events
            if (e.get("routing_key", "") or "").startswith("foundation")
            or e.get("source", "") == "foundation_bridge"
        ]

        by_type: Dict[str, int] = {}
        for e in items:
            rk = e.get("routing_key", "unknown")
            by_type[rk] = by_type.get(rk, 0) + 1

        # 按 ts 倒序，取最近 20 条
        ordered = sorted(items, key=lambda e: e.get("timestamp_utc", ""), reverse=True)
        recent: List[Dict[str, Any]] = []
        for e in ordered[:20]:
            pl = e.get("payload", {}) or {}
            payload_summary = json.dumps(pl, ensure_ascii=False)[:120]
            recent.append({
                "event_id": e.get("event_id", ""),
                "routing_key": e.get("routing_key", ""),
                "ts": e.get("timestamp_utc", ""),
                "payload_summary": payload_summary,
            })

        return {"total": len(items), "by_type": by_type, "recent": recent}

    def collect_billing(self, repo: Optional[Path] = None) -> Dict[str, Any]:
        """Billing 占位面板（OPS-024）。

        读取 ``registry/cloud-store.yaml`` 的 providers 与
        ``registry/resource-store.yaml`` 的 resources，仅展示配置态
        （不接真实 metering 流水，仅展示占位提示）。零写、纯只读。
        """
        repo = Path(repo) if repo else self.repo
        cloud = _safe_load_yaml(repo / "registry" / "cloud-store.yaml")
        resource = _safe_load_yaml(repo / "registry" / "resource-store.yaml")
        providers = cloud.get("providers", []) or []
        resources = resource.get("resources", []) or []
        return {
            "providers": providers,
            "resources": resources,
            "note": "占位：计量数据接入中",
        }

    def collect_aep_trend(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """每 session 取最终 evaluation.score_computed 作为 AEP 点。"""
        last: Dict[str, Dict[str, Any]] = {}
        for e in events:
            if e.get("routing_key") == RK_SCORE_COMPUTED:
                sid = e.get("session_id", "")
                if not sid:
                    continue
                wt = (e.get("payload", {}) or {}).get("weighted_total")
                if wt is None:
                    continue
                last[sid] = {
                    "session_id": sid,
                    "trace_id": e.get("trace_id", ""),
                    "weighted_total": round(float(wt), 4),
                    "end_state": "",
                    "ts": e.get("timestamp_utc", ""),
                    "below_threshold": float(wt) < AEP_THRESHOLD,
                }
        # 关联 end_state（来自 loop_ended）
        ended: Dict[str, str] = {}
        for e in events:
            if e.get("routing_key") == RK_LOOP_ENDED:
                sid = e.get("session_id", "")
                if sid:
                    ended[sid] = (e.get("payload", {}) or {}).get("end_state", "")
        for sid, pt in last.items():
            pt["end_state"] = ended.get(sid, "")

        points = list(last.values())
        points.sort(key=lambda p: p.get("ts", ""), reverse=True)
        return points

    def persist_aep_history(self, trend: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """把本批 AEP 点幂等合并进 ops/aep-history.jsonl（以 session_id 去重）。

        返回合并后的完整列表（前端 aep_trend 取最近 N 条）。重复运行不丢历史。
        """
        self.aep_history_path.parent.mkdir(parents=True, exist_ok=True)
        existing: Dict[str, Dict[str, Any]] = {}
        if self.aep_history_path.exists():
            for line in self.aep_history_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue
                sid = d.get("session_id", "")
                if sid:
                    existing[sid] = d

        for pt in trend:
            sid = pt.get("session_id", "")
            if sid:
                existing[sid] = pt  # 最新覆盖

        merged = list(existing.values())
        # 截断到上限，保留最近的点
        merged.sort(key=lambda p: p.get("ts", ""), reverse=True)
        if len(merged) > AEP_HISTORY_MAX:
            merged = merged[:AEP_HISTORY_MAX]

        lines = [json.dumps(p, ensure_ascii=False) for p in merged]
        self.aep_history_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        return merged

    # ── 顶层装配 ─────────────────────────────────────────────────────────────
    def build(self) -> Dict[str, Any]:
        """读取真相源 → 计算 OpsData → 写出 data.json → 返回 dict。"""
        events = self._read_events()
        health = self.collect_health(events)
        aep_trend = self.collect_aep_trend(events)
        merged_trend = self.persist_aep_history(aep_trend)

        sm = self.solution_manager
        wm = self.workspace_manager
        promotions = self.collect_promotions(sm)
        workspaces = self.collect_workspaces(wm, sm)
        hitl = self.collect_hitl_queue(events)
        alerts = self.collect_alerts(events, promotions)
        kpis = self.collect_kpis(events, health, hitl, alerts)
        runtime = self.collect_runtime(events)
        traces = self.collect_traces(events)
        fsm = self.collect_fsm_timelines(events)
        dlq = self.collect_dlq(events)
        learning = self.collect_learning_stream(events)
        foundation_panel = self.collect_foundation_panel(events)
        billing = self.collect_billing(self.repo)

        data: Dict[str, Any] = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": {
                "repo": str(self.repo),
                "events_path": str(self.events_path),
                "generated_by": "ops/collector.py",
            },
            "kpis": kpis,
            "health": health,
            "alerts": alerts,
            "recent_events": runtime["recent_events"],
            "traces": traces,
            "fsm_timelines": fsm,
            "promotions": promotions,
            "workspaces": workspaces,
            "hitl_queue": hitl,
            "dlq": dlq,
            "aep_trend": merged_trend,
            "capability_usage": runtime["capability_usage"],
            "learning_stream": learning,
            "foundation_panel": foundation_panel,
            "billing": billing,
        }

        self.data_path.parent.mkdir(parents=True, exist_ok=True)
        self.data_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        return data


# ── 降级空管理器（导入失败时使用，保证采集器不崩） ──────────────────────────────
class _NullSolutionManager:
    def current_env(self, *a, **k):
        return "none"

    def gates(self, *a, **k):
        return []

    def list_installed(self, *a, **k):
        return []

    def resolve(self, *a, **k):
        raise KeyError("solution manager unavailable")


class _NullWorkspaceManager:
    def list_tenants(self, *a, **k):
        return []

    def allowed_envs(self, *a, **k):
        return []

    def list_solutions(self, *a, **k):
        return []


def run(repo: Optional[Path] = None, events_path: Optional[Path] = None) -> int:
    """模块入口：构建并写出 data.json，返回进程退出码。

    Args:
        repo: 仓库根目录（默认自动推导）。
        events_path: 显式事件源路径（CLI --source；覆盖默认与环境变量）。
    """
    collector = OpsCollector(repo=repo, events_path=events_path)
    data = collector.build()
    k = data["kpis"]
    print(f"[ops] wrote {collector.data_path} "
          f"({(collector.data_path.stat().st_size if collector.data_path.exists() else 0)} bytes)")
    print(f"  source={collector.events_path}")
    print(f"  events={k['events']} aep_avg={k['aep_avg']} dlq={k['dlq']} "
          f"health={k['health']} traces={len(data['traces'])} "
          f"promotions={len(data['promotions'])} hitl={len(data['hitl_queue'])}")
    return 0


def _parse_collector_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """解析 ops/collector.py 入口参数（本期增量仅 --source / --repo）。"""
    p = argparse.ArgumentParser(
        prog="ops/collector.py",
        description="AEAOS OpsCollector — 读取事件源 → 写出 ops/data.json",
    )
    p.add_argument("--source", default=None,
                   help="事件源 JSONL 路径（覆盖默认 .aeaos/run/events.jsonl 与 "
                        "环境变量 AEAOS_EVENTS_PATH）")
    p.add_argument("--repo", default=None,
                   help="仓库根目录（默认：自动推导为采集器所在仓库根）")
    return p.parse_args(argv)


if __name__ == "__main__":
    _args = _parse_collector_args()
    raise SystemExit(run(
        repo=Path(_args.repo) if _args.repo else None,
        events_path=Path(_args.source) if _args.source else None,
    ))

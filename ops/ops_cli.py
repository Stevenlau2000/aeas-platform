"""Operations (L7) CLI 业务逻辑（aeaos ops collect / hitl-approve / hitl-reject / rollback）。

把真实动作集中在此模块，与 argparse 解耦，便于单元测试（design.md §3.2 / §4.2 / §4.3）。
仅依赖既有模块：apps.app_base / poc.app_runtime / ops.collector / registry.solution。
所有动作都「真正回调既有网关/管理器」，不重造策略引擎（design.md §1.1 / §7.8）。
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# 确保仓库根在 sys.path（ops / apps / registry 包导入）。
# poc/ 下的模块彼此以「顶层」方式互相导入（from scheduler / orchestrator /
# event_bus ...），因此 poc/ 也必须加入 sys.path，run_coach_session 才能运行。
_REPO_HINT = Path(__file__).resolve().parent.parent
if str(_REPO_HINT) not in sys.path:
    sys.path.insert(0, str(_REPO_HINT))
_POC_HINT = _REPO_HINT / "poc"
if str(_POC_HINT) not in sys.path:
    sys.path.insert(0, str(_POC_HINT))

from apps.app_base import ApprovalRequest, ForcedApprovalGateway  # noqa: E402
from ops.collector import OpsCollector  # noqa: E402


# ── 工具 ─────────────────────────────────────────────────────────
def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    """读取 JSONL，跳过损坏行；文件不存在返回空列表。"""
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def ops_collect(
    repo_root: Any = None,
    events_path: Any = None,
    data_path: Any = None,
    aep_history_path: Any = None,
    hitl_decisions_path: Any = None,
) -> Dict[str, Any]:
    """运行 OpsCollector.build() → 写 ops/data.json。等价于 `aeaos ops collect`。"""
    collector = OpsCollector(
        repo=Path(repo_root) if repo_root else None,
        events_path=Path(events_path) if events_path else None,
        data_path=Path(data_path) if data_path else None,
        aep_history_path=Path(aep_history_path) if aep_history_path else None,
        hitl_decisions_path=Path(hitl_decisions_path) if hitl_decisions_path else None,
    )
    return collector.build()


# ── HITL 审批：从 trace 还原 coaching_request ──────────────────────
def _coaching_request_from_trace(
    events: List[Dict[str, Any]], trace_id: str
) -> Optional[Dict[str, Any]]:
    """从 trace 事件中还原 coaching_request（category / role / tenant_id）。

    优先取 app.coach.requested / app.coach.approval / app.coach.advice 事件里的
    capability / category / role（design.md §7.5）。
    """
    category: Optional[str] = None
    role: Optional[str] = None
    tenant_id = "default"
    for e in events:
        if e.get("trace_id") != trace_id:
            continue
        rk = e.get("routing_key", "")
        pl = e.get("payload", {}) or {}
        if rk in ("app.coach.requested", "app.coach.approval", "app.coach.advice"):
            category = category or pl.get("category")
            role = role or pl.get("role")
            if e.get("tenant_id"):
                tenant_id = e["tenant_id"]
    if category is None:
        return None
    return {"category": category, "payload": {}, "role": role, "tenant_id": tenant_id}


def _extract_session_result(bus: Any) -> Dict[str, Any]:
    """从 run_coach_session 返回的 bus 中提取 AEP / 终态 / 审批状态。"""
    result: Dict[str, Any] = {
        "weighted_total": None,
        "end_state": "",
        "passed": False,
        "approval_status": None,
        "new_trace_id": "",
        "new_session_id": "",
    }
    try:
        from event_bus import ObservabilityConsumer  # type: ignore
    except Exception:  # noqa: BLE001
        ObservabilityConsumer = None  # type: ignore

    obs = None
    for c in getattr(bus, "consumers", []) or []:
        if ObservabilityConsumer is None:
            if getattr(c, "name", "") == "observability":
                obs = c
        elif isinstance(c, ObservabilityConsumer):
            obs = c
    for e in getattr(obs, "events", []) or []:
        rk = getattr(e, "routing_key", "")
        pl = getattr(e, "payload", {}) or {}
        if rk == "evaluation.score_computed":
            result["weighted_total"] = pl.get("weighted_total")
        elif rk == "orchestrator.loop_ended":
            result["end_state"] = pl.get("end_state", "")
            result["passed"] = bool(pl.get("passed", False))
        elif rk == "app.coach.approval":
            result["approval_status"] = pl.get("status")
        elif rk == "app.coach.requested":
            result["new_trace_id"] = getattr(e, "trace_id", "")
            result["new_session_id"] = getattr(e, "session_id", "")
    return result


def _append_audit(
    decision_path: Path,
    *,
    operator: str,
    decision: str,
    trace_id: str,
    reason: str = "",
) -> Dict[str, Any]:
    """追加一行审计日志到 ops/hitl-decisions.jsonl（design.md §2 / T4）。"""
    decision_path.parent.mkdir(parents=True, exist_ok=True)
    rec = {
        "operator": operator,
        "decision": decision,
        "trace_id": trace_id,
        "reason": reason,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with decision_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return rec


# ── HITL 审批 / 驳回 ─────────────────────────────────────────────
def ops_hitl_approve(
    repo_root: Any,
    trace_id: str,
    reviewer: str = "ops-console",
    events_path: Any = None,
    decision_log_path: Any = None,
) -> Dict[str, Any]:
    """注入 ForcedApprovalGateway("approved") 重跑 run_coach_session。

    返回结构化结果（供 CLI 打印与测试断言）：
        {decision, trace_id, category, new_trace_id, new_session_id,
         weighted_total, end_state, passed, approval_status, audit}
    审计写入 ops/hitl-decisions.jsonl（design.md §4.2）。
    """
    repo_root = Path(repo_root)
    events_path = Path(events_path) if events_path else repo_root / ".aeaos" / "run" / "events.jsonl"
    decision_log_path = (
        Path(decision_log_path) if decision_log_path else repo_root / "ops" / "hitl-decisions.jsonl"
    )

    events = _read_jsonl(events_path)
    req = _coaching_request_from_trace(events, trace_id)
    if req is None:
        raise ValueError(
            f"trace_id '{trace_id}' 在事件流中找不到 coaching 上下文"
            f"（需含 app.coach.requested/approval/advice 事件）"
        )

    gw = ForcedApprovalGateway("approved", reviewer=reviewer)
    from poc.app_runtime import run_coach_session  # 延迟导入，避免重负载
    session = asyncio.run(run_coach_session(repo_root, req, gw, tenant_id=req.get("tenant_id", "default")))
    res = _extract_session_result(session.get("bus"))

    audit = _append_audit(
        decision_log_path, operator=reviewer, decision="approved",
        trace_id=trace_id, reason=f"forced by {reviewer}",
    )
    return {
        "decision": "approved",
        "trace_id": trace_id,
        "category": req.get("category"),
        "new_trace_id": res["new_trace_id"],
        "new_session_id": res["new_session_id"],
        "weighted_total": res["weighted_total"],
        "end_state": res["end_state"],
        "passed": res["passed"],
        "approval_status": res["approval_status"],
        "audit": audit,
    }


def ops_hitl_reject(
    repo_root: Any,
    trace_id: str,
    reason: str = "ops-console rejected",
    reviewer: str = "ops-console",
    events_path: Any = None,
    decision_log_path: Any = None,
) -> Dict[str, Any]:
    """注入 ForcedApprovalGateway("rejected") 重跑 run_coach_session（design.md §4.2）。"""
    repo_root = Path(repo_root)
    events_path = Path(events_path) if events_path else repo_root / ".aeaos" / "run" / "events.jsonl"
    decision_log_path = (
        Path(decision_log_path) if decision_log_path else repo_root / "ops" / "hitl-decisions.jsonl"
    )

    events = _read_jsonl(events_path)
    req = _coaching_request_from_trace(events, trace_id)
    if req is None:
        raise ValueError(
            f"trace_id '{trace_id}' 在事件流中找不到 coaching 上下文"
            f"（需含 app.coach.requested/approval/advice 事件）"
        )

    gw = ForcedApprovalGateway("rejected", reviewer=reviewer)
    from poc.app_runtime import run_coach_session  # 延迟导入，避免重负载
    session = asyncio.run(run_coach_session(repo_root, req, gw, tenant_id=req.get("tenant_id", "default")))
    res = _extract_session_result(session.get("bus"))

    audit = _append_audit(
        decision_log_path, operator=reviewer, decision="rejected",
        trace_id=trace_id, reason=reason,
    )
    return {
        "decision": "rejected",
        "trace_id": trace_id,
        "category": req.get("category"),
        "new_trace_id": res["new_trace_id"],
        "new_session_id": res["new_session_id"],
        "weighted_total": res["weighted_total"],
        "end_state": res["end_state"],
        "passed": res["passed"],
        "approval_status": res["approval_status"],
        "audit": audit,
    }


# ── 回滚 ─────────────────────────────────────────────────────────
def ops_rollback(
    repo_root: Any,
    solution: str,
    tenant: Optional[str] = None,
    workspace: Optional[str] = None,
    note: str = "",
    solution_manager: Any = None,
    decision_log_path: Any = None,
) -> Dict[str, Any]:
    """调 SolutionManager.rollback_env 降级一步；追加审计日志（design.md §4.3 / T4）。

    solution_manager 可注入（测试用），否则从仓库构造真实 SolutionManager。
    """
    repo_root = Path(repo_root)
    decision_log_path = (
        Path(decision_log_path) if decision_log_path else repo_root / "ops" / "hitl-decisions.jsonl"
    )

    if solution_manager is None:
        from registry.solution import SolutionManager
        solution_manager = SolutionManager(repo_root)

    before_env = None
    try:
        before_env = solution_manager.current_env(solution, tenant, workspace)
    except Exception:  # noqa: BLE001
        before_env = None

    ok = bool(solution_manager.rollback_env(solution, note or "", tenant, workspace))

    after_env = None
    try:
        after_env = solution_manager.current_env(solution, tenant, workspace)
    except Exception:  # noqa: BLE001
        after_env = None

    audit = _append_audit(
        decision_log_path, operator="ops-console", decision="rollback",
        trace_id=solution, reason=note or "",
    )
    return {
        "solution": solution,
        "tenant": tenant,
        "workspace": workspace,
        "ok": ok,
        "before_env": before_env,
        "after_env": after_env,
        "audit": audit,
    }

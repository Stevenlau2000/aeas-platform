/* =============================================================
   AEAOS Operations (L7) · 前端看板逻辑（原生 JS · 零构建链）
   -------------------------------------------------------------
   数据契约见 docs/operations/design.md §3 (ops/data.json)。
   前端只 fetch('data.json') 并每 POLL_MS 轮询；过滤/分页/联动
   全部在浏览器内完成（数据已是全量）。
   所有字段均来自 data.json，不引入任何第三方库。
   ============================================================= */
'use strict';

const POLL_MS = 10000;          // design.md §8：默认 10s 轮询
const EVENTS_PER_PAGE = 200;    // Runtime Event Explorer 分页上限

/* ── 全局状态（跨轮询保留，避免刷新丢失交互） ── */
let DATA = null;
let ALL_EVENTS = [];            // 由 traces[].events 展开的全量事件流
let traceIndex = {};            // trace_id -> trace
let fsmIndex = {};              // trace_id -> fsm_timeline
let selectedTraceId = null;     // 当前联动选中的 trace
let runtimeFilters = { routing_key: '', session_id: '', trace_id: '', tenant_id: '', source: '' };
let runtimePage = 0;
let hitlFilter = 'all';         // all | Contract | Compliance | Risk
let promoFilter = '';
let wsTenant = '';
let wsWorkspace = '';
let currentPage = 'overview';

const AEP_THRESHOLD = 0.70;

/* ── DOM 小工具 ── */
const $ = (id) => document.getElementById(id);
function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function fmtNum(n) { return (n === null || n === undefined) ? '0' : Number(n).toLocaleString(); }
function fmtTs(s) { return (s || '').slice(0, 19).replace('T', ' '); }
function on(id, ev, fn) { const e = $(id); if (e) e.addEventListener(ev, fn); }

/* ── 徽章 ── */
function badge(text, kind) { return `<span class="badge badge-${kind}">${esc(text)}</span>`; }
function statusBadge(status) {
  if (status === 'normal') return badge('NORMAL', 'green');
  if (status === 'degraded') return badge('DEGRADED', 'yellow');
  if (status === 'critical') return badge('CRITICAL', 'red');
  return badge(status || '?', 'gray');
}
function envBadge(env) {
  const order = ['dev', 'test', 'staging', 'prod'];
  const idx = order.indexOf(env);
  const cur = order.indexOf(currentEnvOf(env));
  if (env === 'none' || idx < 0) return badge(env || 'none', 'gray');
  if (idx === order.length - 1) return badge(env, 'green');
  if (idx === 0) return badge(env, 'amber');
  return badge(env, 'blue');
}
function currentEnvOf(env) { return env; }

/* ═══════════════════════════════════════════════════════════
   加载 + 轮询
   ═══════════════════════════════════════════════════════════ */
async function load() {
  try {
    const r = await fetch('data.json', { cache: 'no-store' });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    DATA = await r.json();
    buildIndexes();
    applyUrlScope();
    renderTopbar();
    renderActivePage();
  } catch (e) {
    // 即使 data.json 暂缺也能渲染空壳
    $('page-overview').innerHTML = `<div class="empty-box">⚠ 未能加载 data.json（${esc(e.message)}）。请先运行 <code>aeaos ops collect</code>。</div>`;
    ['runtime', 'promo', 'workspaces', 'govern'].forEach(p => {
      const el = $('page-' + p); if (el) el.innerHTML = `<div class="empty-box">等待 data.json…</div>`;
    });
    renderTopbarEmpty();
  }
}

function buildIndexes() {
  ALL_EVENTS = [];
  traceIndex = {}; fsmIndex = {};
  (DATA.traces || []).forEach(t => {
    traceIndex[t.trace_id] = t;
    (t.events || []).forEach(e => ALL_EVENTS.push(e));
  });
  (DATA.fsm_timelines || []).forEach(f => { fsmIndex[f.trace_id] = f; });
}

function applyUrlScope() {
  const p = new URLSearchParams(location.search);
  wsTenant = p.get('tenant') || '';
  wsWorkspace = p.get('workspace') || '';
}

function renderTopbar() {
  const h = (DATA && DATA.health) || {};
  const score = h.score != null ? h.score : 0;
  const status = h.status || 'unknown';
  const light = $('tb-health-light');
  light.className = 'health-light' + (status === 'normal' ? '' : status === 'degraded' ? ' degraded' : ' critical');
  $('tb-health-txt').textContent = `Health ${score}`;
  $('tb-dlq-n').textContent = (DATA && DATA.health) ? DATA.health.dlq_count : 0;
  $('tb-tenant-txt').textContent = wsTenant ? `${wsTenant}${wsWorkspace ? '/' + wsWorkspace : ''}` : 'global';
  $('tb-updated').textContent = DATA && DATA.generated_at ? '更新 ' + fmtTs(DATA.generated_at) : '—';
}
function renderTopbarEmpty() {
  $('tb-health-txt').textContent = 'Health —';
  $('tb-dlq-n').textContent = '0';
  $('tb-updated').textContent = '—';
}

function renderActivePage() {
  if (!DATA) return;
  if (currentPage === 'overview') renderOverview();
  else if (currentPage === 'runtime') renderRuntime();
  else if (currentPage === 'promo') renderPromo();
  else if (currentPage === 'workspaces') renderWorkspaces();
  else if (currentPage === 'govern') renderGovern();
}

/* ═══════════════════════════════════════════════════════════
   导航
   ═══════════════════════════════════════════════════════════ */
function showPage(name) {
  currentPage = name;
  document.querySelectorAll('.sidebar a').forEach(a => a.classList.remove('active'));
  const a = document.querySelector(`.sidebar a[data-page="${name}"]`);
  if (a) a.classList.add('active');
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  const el = $('page-' + name);
  if (el) el.classList.add('active');
  renderActivePage();
}

function bindNav() {
  document.querySelectorAll('.sidebar a').forEach(a => {
    a.addEventListener('click', () => showPage(a.getAttribute('data-page')));
  });
}

/* ═══════════════════════════════════════════════════════════
   Overview
   ═══════════════════════════════════════════════════════════ */
function renderOverview() {
  const k = DATA.kpis || {};
  const h = DATA.health || {};
  const alerts = DATA.alerts || [];
  const cards = `
    <div class="cards">
      <div class="card"><div class="num">${fmtNum(k.events)}</div><div class="lbl">Events</div></div>
      <div class="card"><div class="num">${k.aep_avg != null ? k.aep_avg.toFixed(3) : '—'}</div><div class="lbl">AEP Avg</div><div class="sub">阈值 ${AEP_THRESHOLD}</div></div>
      <div class="card ${k.dlq > 0 ? 'alert-card' : 'ok-card'}"><div class="num">${fmtNum(k.dlq)}</div><div class="lbl">DLQ</div></div>
      <div class="card"><div class="num">${fmtNum(k.installed)}</div><div class="lbl">Installed</div></div>
      <div class="card"><div class="num">${fmtNum(k.solutions_promoted_prod)}</div><div class="lbl">Promoted Prod</div></div>
      <div class="card"><div class="num">${k.health != null ? k.health : '—'}</div><div class="lbl">Health</div></div>
      <div class="card ${k.open_hitl > 0 ? 'alert-card' : ''}"><div class="num">${fmtNum(k.open_hitl)}</div><div class="lbl">Open HITL</div></div>
      <div class="card ${k.open_alerts > 0 ? 'alert-card' : ''}"><div class="num">${fmtNum(k.open_alerts)}</div><div class="lbl">Open Alerts</div></div>
    </div>`;

  const ring = healthRing(h.score != null ? h.score : 0, h.status || 'unknown');

  const alertHtml = alerts.length
    ? alerts.slice(0, 12).map(a => `
      <div class="alert ${esc(a.severity)}">
        <div style="flex:1">
          <div class="a-title">${esc(a.title)} ${badge(a.type, a.severity === 'high' ? 'red' : a.severity === 'medium' ? 'yellow' : 'blue')}</div>
          <div class="a-detail">${esc(a.detail)}</div>
          <div class="a-meta">${fmtTs(a.raised_at)} · trace ${esc(a.trace_id || '—')}</div>
        </div>
      </div>`).join('')
    : `<div class="empty">无告警。</div>`;

  // AEP 趋势折线
  const aepChart = aepTrendChart(DATA.aep_trend || []);

  $('page-overview').innerHTML = `
    <h1><span class="accent">//</span> Overview</h1>
    ${cards}
    <div class="grid-2">
      <div class="glass">
        <h2>System Health</h2>
        ${ring}
        <p style="text-align:center;font-size:13px;margin-top:12px">${statusBadge(h.status)}</p>
        <p class="muted" style="text-align:center;font-size:12px;margin-top:6px">
          DLQ ${fmtNum(h.dlq_count)} · AEP 低于阈值 run ${fmtNum(h.aep_below_threshold_runs)} · 末事件 ${fmtTs(h.last_event_time)}
        </p>
      </div>
      <div class="glass">
        <h2>Alerts (${alerts.length})</h2>
        ${alertHtml}
      </div>
    </div>
    <div class="glass">
      <h2>AEP Trend <span class="muted" style="font-family:var(--sans);font-weight:400;text-transform:none">（阈值 ${AEP_THRESHOLD} · 点击低于阈值的点进入该 trace）</span></h2>
      ${aepChart}
    </div>`;
}

function healthRing(score, status) {
  const r = 70, c = 2 * Math.PI * r;
  const off = c * (1 - Math.max(0, Math.min(100, score)) / 100);
  const color = status === 'normal' ? 'var(--green)' : status === 'degraded' ? 'var(--yellow)' : 'var(--red)';
  return `<div class="health-ring"><div class="ring-wrap">
    <svg width="160" height="160" viewBox="0 0 160 160">
      <circle cx="80" cy="80" r="${r}" stroke="var(--surface2)" stroke-width="12" fill="none"/>
      <circle cx="80" cy="80" r="${r}" stroke="${color}" stroke-width="12" fill="none"
        stroke-linecap="round" stroke-dasharray="${c.toFixed(1)}" stroke-dashoffset="${off.toFixed(1)}" transform="rotate(-90 80 80)"/>
    </svg>
    <div class="ring-label"><div class="ring-num" style="color:${color}">${score}</div><div class="ring-sub">Health</div></div>
  </div></div>`;
}

/* ═══════════════════════════════════════════════════════════
   AEP 趋势折线（SVG，阈值线 0.70）
   ═══════════════════════════════════════════════════════════ */
function aepTrendChart(trend) {
  if (!trend || !trend.length) return `<div class="empty-box">暂无 AEP 数据点。</div>`;
  // 取最近 N 个（按 ts 倒序的前 200）
  const pts = trend.slice().sort((a, b) => (a.ts || '').localeCompare(b.ts || '')).slice(-200);
  const W = 900, H = 220, pad = 30;
  const n = pts.length;
  const x = (i) => pad + (W - 2 * pad) * (n === 1 ? 0.5 : i / (n - 1));
  const y = (v) => H - pad - (H - 2 * pad) * Math.max(0, Math.min(1, v));
  const thresholdY = y(AEP_THRESHOLD);

  let path = '';
  pts.forEach((p, i) => { path += (i === 0 ? 'M' : 'L') + x(i).toFixed(1) + ' ' + y(p.weighted_total || 0).toFixed(1) + ' '; });
  let dots = '';
  pts.forEach((p, i) => {
    const below = (p.weighted_total || 0) < AEP_THRESHOLD;
    dots += `<circle class="aep-dot ${below ? 'below' : 'above'}" cx="${x(i).toFixed(1)}" cy="${y(p.weighted_total || 0).toFixed(1)}" r="4"
      data-trace="${esc(p.trace_id || '')}" data-session="${esc(p.session_id || '')}"><title>AEP ${p.weighted_total} · ${esc(p.trace_id || '')}</title></circle>`;
  });

  return `<svg class="aep-chart" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
    <line class="aep-threshold" x1="${pad}" y1="${thresholdY.toFixed(1)}" x2="${W - pad}" y2="${thresholdY.toFixed(1)}"/>
    <text x="${pad}" y="${(thresholdY - 6).toFixed(1)}" fill="var(--amber)" font-size="11" font-family="var(--mono)">阈值 ${AEP_THRESHOLD}</text>
    <path d="${path}" fill="none" stroke="var(--cyan)" stroke-width="1.5" opacity="0.7"/>
    ${dots}
  </svg>`;
}

/* ═══════════════════════════════════════════════════════════
   Runtime — Event Explorer + FSM Timeline + Trace 调用链（三段联动）
   ═══════════════════════════════════════════════════════════ */
function renderRuntime() {
  const f = runtimeFilters;
  const filtered = ALL_EVENTS.filter(e =>
    (!f.routing_key || (e.routing_key || '').toLowerCase().includes(f.routing_key.toLowerCase())) &&
    (!f.session_id || (e.session_id || '').toLowerCase().includes(f.session_id.toLowerCase())) &&
    (!f.trace_id || (e.trace_id || '').toLowerCase().includes(f.trace_id.toLowerCase())) &&
    (!f.tenant_id || (e.tenant_id || '').toLowerCase().includes(f.tenant_id.toLowerCase())) &&
    (!f.source || (e.source || '').toLowerCase().includes(f.source.toLowerCase()))
  );
  const total = filtered.length;
  const pages = Math.max(1, Math.ceil(total / EVENTS_PER_PAGE));
  if (runtimePage >= pages) runtimePage = pages - 1;
  if (runtimePage < 0) runtimePage = 0;
  const start = runtimePage * EVENTS_PER_PAGE;
  const slice = filtered.slice(start, start + EVENTS_PER_PAGE);

  const rkOptions = Array.from(new Set(ALL_EVENTS.map(e => e.routing_key))).sort();
  const rkDatalist = `<datalist id="rk-list">${rkOptions.map(r => `<option value="${esc(r)}">`).join('')}</datalist>`;

  const eventRows = slice.length ? slice.map(e => `
    <div class="trace-event" data-trace="${esc(e.trace_id)}" data-eid="${esc(e.event_id)}">
      <span class="te-key">${esc(e.routing_key)}</span>
      <span class="te-src">${esc(e.source || '—')} · ses ${esc((e.session_id || '').slice(-8))} · ${esc((e.payload && (e.payload.capability || e.payload.state || '')) || '')}</span>
      <span class="te-ts">${fmtTs(e.ts)}</span>
    </div>`).join('') : `<div class="empty-box">无匹配事件。试试调整过滤器。</div>`;

  // 详情面板（选中 trace 后联动）
  let detail = '';
  if (selectedTraceId && traceIndex[selectedTraceId]) {
    detail = runtimeDetail(selectedTraceId);
  } else {
    detail = `<div class="empty-box">点击上方事件或输入 trace_id，查看其 FSM 时间线与调用链（事件 → 时间线 → 调用链）。</div>`;
  }

  $('page-runtime').innerHTML = `
    <h1><span class="accent">//</span> Runtime <span class="muted" style="font-family:var(--sans);font-weight:400">（事件 → 时间线 → 调用链）</span></h1>
    <div class="filters">
      <div class="fgroup grow"><label>trace_id 跳转</label>
        <input id="rt-trace" placeholder="输入 trace_id 回车" value="${esc(f.trace_id)}"></div>
      <div class="fgroup grow"><label>routing_key</label>
        <input id="rt-rk" list="rk-list" placeholder="事件类型" value="${esc(f.routing_key)}"></div>
      <div class="fgroup grow"><label>session_id</label>
        <input id="rt-sid" placeholder="会话 id" value="${esc(f.session_id)}"></div>
      <div class="fgroup grow"><label>tenant_id</label>
        <input id="rt-tenant" placeholder="租户" value="${esc(f.tenant_id)}"></div>
      <div class="fgroup grow"><label>source</label>
        <input id="rt-src" placeholder="来源" value="${esc(f.source)}"></div>
      ${rkDatalist}
    </div>
    <div class="row between" style="margin-bottom:10px">
      <span class="muted">共 ${fmtNum(total)} 事件 · 第 ${runtimePage + 1}/${pages} 页（每页≤${EVENTS_PER_PAGE}）</span>
      <span>
        <button class="btn btn-sm" id="rt-prev">‹ 上一页</button>
        <button class="btn btn-sm" id="rt-next">下一页 ›</button>
      </span>
    </div>
    <div class="panel" style="margin-bottom:20px">${eventRows}</div>
    ${detail}`;

  // 绑定
  on('rt-rk', 'input', e => { runtimeFilters.routing_key = e.target.value; runtimePage = 0; renderRuntime(); });
  on('rt-sid', 'input', e => { runtimeFilters.session_id = e.target.value; runtimePage = 0; renderRuntime(); });
  on('rt-tenant', 'input', e => { runtimeFilters.tenant_id = e.target.value; runtimePage = 0; renderRuntime(); });
  on('rt-src', 'input', e => { runtimeFilters.source = e.target.value; runtimePage = 0; renderRuntime(); });
  on('rt-trace', 'input', e => { runtimeFilters.trace_id = e.target.value; });
  on('rt-trace', 'keydown', e => { if (e.key === 'Enter') { runtimePage = 0; renderRuntime(); } });
  on('rt-prev', 'click', () => { runtimePage--; renderRuntime(); });
  on('rt-next', 'click', () => { runtimePage++; renderRuntime(); });

  // 事件点击 → 选中 trace（联动）
  document.querySelectorAll('#page-runtime .trace-event').forEach(row => {
    row.addEventListener('click', () => {
      selectedTraceId = row.getAttribute('data-trace');
      renderRuntime();
    });
  });
  bindRuntimeDetail();
}

function runtimeDetail(traceId) {
  const t = traceIndex[traceId];
  const f = fsmIndex[traceId];
  const fsm = f ? fsmFlow(f) : `<div class="empty">该 trace 无 FSM 时间线（可能非编排 trace）。</div>`;
  const chain = (t && t.events) ? t.events.map((e, i) => traceEventRow(e, i)).join('') : `<div class="empty">无事件。</div>`;
  const endState = (t && t.end_state) || (f && f.end_state) || '—';
  return `
    <div class="grid-2">
      <div class="glass">
        <h2>FSM Timeline — ${esc(traceId.slice(-12))}</h2>
        <div class="row" style="margin-bottom:8px">${badge('end: ' + endState, endState === 'EVOLVE' ? 'green' : endState === 'LEARN' ? 'red' : 'blue')}
          ${badge('passed: ' + ((t && t.passed) || (f && f.passed) || false), 'gray')}
          ${badge('AEP ' + ((t && t.aep) != null ? t.aep : '—'), 'amber')}
          <span class="link" onclick="copyTraceId('${esc(traceId)}')">复制 trace_id</span>
        </div>
        ${fsm}
      </div>
      <div class="glass">
        <h2>Trace 调用链（${t ? t.event_count : 0} 事件）</h2>
        <div class="panel" style="margin-bottom:0;max-height:420px;overflow:auto">${chain}</div>
      </div>
    </div>`;
}

function fsmFlow(f) {
  if (!f.steps || !f.steps.length) return `<div class="empty">无状态步骤。</div>`;
  const steps = f.steps.map(s => {
    const cls = (s.state === 'EVOLVE') ? 'evolve' : (s.state === 'LEARN') ? 'learn' : '';
    return `<div class="fsm-step ${cls}" title="${esc(s.detail || '')}">
      <span class="sname">${esc(s.state)}</span>
      <span class="strig">${esc(s.trigger || '')}</span>
      <span class="sdetail">${esc(s.detail || '')}</span>
    </div>`;
  }).join('<span class="fsm-arrow">→</span>');
  return `<div class="fsm-flow">${steps}</div>`;
}

function traceEventRow(e, i) {
  const cap = (e.payload && (e.payload.capability || e.payload.state || e.payload.category || '')) || '';
  return `<div class="trace-event" data-eid="${esc(e.event_id)}" onclick="togglePayload(this)">
    <span class="te-key">${esc(e.routing_key)}</span>
    <span class="te-src">${esc(e.source || '—')}${cap ? ' · ' + esc(cap) : ''}</span>
    <span class="te-ts">${fmtTs(e.ts)}</span>
    <div style="grid-column:1/-1;display:none" class="payload-box"><pre>${esc(JSON.stringify(e.payload || {}, null, 2))}</pre></div>
  </div>`;
}
function togglePayload(row) {
  const box = row.querySelector('.payload-box');
  if (box) box.style.display = (box.style.display === 'none' || !box.style.display) ? 'block' : 'none';
}
function copyTraceId(id) {
  navigator.clipboard && navigator.clipboard.writeText(id);
}

function bindRuntimeDetail() { /* 事件点击已在 renderRuntime 中绑定；traceEventRow 用内联 onclick */ }

/* ═══════════════════════════════════════════════════════════
   Promo — 晋升阶梯 + P5 门下钻 + 回滚
   ═══════════════════════════════════════════════════════════ */
function renderPromo() {
  const promos = (DATA.promotions || []).filter(p =>
    !promoFilter || (p.solution_id || '').toLowerCase().includes(promoFilter.toLowerCase()) ||
    (p.name || '').toLowerCase().includes(promoFilter.toLowerCase()));
  const ENVS = ['dev', 'test', 'staging', 'prod'];

  const cards = promos.length ? promos.map(p => {
    const curIdx = ENVS.indexOf(p.current_env);
    const ladder = ENVS.map((env, i) => {
      let cls = 'future';
      if (i < curIdx) cls = 'done';
      else if (i === curIdx) cls = 'current';
      return `<div class="promo-step ${cls}"><div class="pname">${env}</div><div>${i < curIdx ? '✓' : i === curIdx ? '●' : ''}</div></div>`;
    }).join('');

    const gates = (p.gates || []).map(g => {
      const fail = !g.passed;
      // security 门高亮缺 idempotency / HITL 的 capability
      const isSecurity = (g.gate === 'security');
      const flag = isSecurity && /missing|fail|idempot|hitl/i.test(g.detail || '');
      return `<div class="gate ${fail ? 'fail' : 'pass'} ${flag ? 'security-flag' : ''}">
        <div class="gname">${esc(g.gate)}</div>
        <div class="gdetail">${esc(g.detail || (fail ? '未通过' : '通过'))}</div>
      </div>`;
    }).join('');

    const rollbackCmd = `aeaos ops rollback ${esc(p.solution_id)}${wsTenant ? ' --tenant ' + esc(wsTenant) : ''}${wsWorkspace ? ' --workspace ' + esc(wsWorkspace) : ''} --note "ops-console rollback"`;

    return `<div class="promo-card">
      <div class="promo-head">
        <span class="name">${esc(p.name || p.solution_id)}</span>
        <span>${badge(p.current_env, 'amber')} ${badge('v' + (p.current_version || '0.0.0'), 'blue')}
          ${p.all_gates_passed ? badge('P5 全过', 'green') : badge('P5 未全过', 'red')}</span>
      </div>
      <div class="promo-ladder">${ladder}</div>
      <h3>P5 Promotion Gates (${p.gates ? p.gates.length : 0})</h3>
      <div class="gate-list">${gates || '<span class="muted">无门记录</span>'}</div>
      <div class="row between" style="margin-top:14px">
        <span class="muted">solution_id: <span class="mono">${esc(p.solution_id)}</span></span>
        <button class="btn btn-amber btn-sm" onclick="confirmRollback('${esc(p.solution_id)}')">↩ 回滚一步</button>
      </div>
    </div>`;
  }).join('') : `<div class="empty-box">无 Promotion 记录（registry/solution-promotions.yaml 为空）。</div>`;

  $('page-promo').innerHTML = `
    <h1><span class="accent">//</span> Promotion Board</h1>
    <div class="filters">
      <div class="fgroup grow"><label>过滤 solution / name</label>
        <input id="pm-filter" placeholder="关键字" value="${esc(promoFilter)}"></div>
    </div>
    ${cards}`;
  on('pm-filter', 'input', e => { promoFilter = e.target.value; renderPromo(); });
}

function confirmRollback(solutionId) {
  openModal(
    '确认回滚',
    `即将对 <span class="mono">${esc(solutionId)}</span> 执行环境降级一步（如 prod→staging）。` +
    `本控制台不直接执行，仅渲染运维命令供复制运行。`,
    () => renderCmd('回滚命令', `aeaos ops rollback ${esc(solutionId)}${wsTenant ? ' --tenant ' + esc(wsTenant) : ''}${wsWorkspace ? ' --workspace ' + esc(wsWorkspace) : ''} --note "ops-console rollback"`)
  );
}

/* ═══════════════════════════════════════════════════════════
   Workspaces — Tenant → Org → Workspace 树 + Solution 绑定
   ═══════════════════════════════════════════════════════════ */
function renderWorkspaces() {
  let tenants = DATA.workspaces || [];
  // ?tenant=&workspace= 展示层隔离（design.md §7.6）
  if (wsTenant) tenants = tenants.filter(t => (t.tenant || '') === wsTenant);

  const tree = tenants.length ? tenants.map(t => {
    let orgs = t.orgs || [];
    return `<div class="tree-tenant">
      <div class="t-name">${esc(t.tenant)}</div>
      ${orgs.map(o => {
        let wss = o.workspaces || [];
        if (wsWorkspace) wss = wss.filter(w => (w.id || '') === wsWorkspace);
        return `<div class="tree-org"><div class="o-name">org: ${esc(o.id)}</div>
          ${wss.map(w => workspaceCard(w)).join('')}
        </div>`;
      }).join('')}
    </div>`;
  }).join('') : `<div class="empty-box">无租户/工作区数据（registry/workspace-store.yaml 为空）。${wsTenant ? '当前 scope: ' + esc(wsTenant) + (wsWorkspace ? '/' + esc(wsWorkspace) : '') : ''}</div>`;

  $('page-workspaces').innerHTML = `
    <h1><span class="accent">//</span> Workspaces
      <span class="muted" style="font-family:var(--sans);font-weight:400">（scope: ${wsTenant ? esc(wsTenant) + (wsWorkspace ? '/' + esc(wsWorkspace) : '') : 'global'}</span></h1>
    ${tree}`;
}

function workspaceCard(w) {
  const envs = w.environments || {};
  const cur = w.allowed_envs && w.allowed_envs.length ? w.allowed_envs[w.allowed_envs.length - 1] : null;
  const envChips = Object.keys(envs).length ? Object.keys(envs).map(e => {
    const enabled = envs[e];
    const cls = !enabled ? 'disabled' : (e === cur ? 'current' : 'enabled');
    return `<span class="env-chip ${cls}">${e}${enabled ? '' : ' (off)'}</span>`;
  }).join('') : '<span class="muted">无环境</span>';

  const sols = (w.solutions || []).map(s => `
    <div class="sol-binding">
      ${badge(s.env || '?', 'blue')}
      <span class="s-id">${esc(s.id)}</span>
      <span class="muted">v${esc(s.version || '?')}</span>
      ${badge(s.status || '', 'gray')}
    </div>`).join('') || '<span class="muted">未绑定 Solution</span>';

  return `<div class="tree-ws">
    <div class="w-name">${esc(w.name || w.id)} <span class="muted mono">(${esc(w.id)})</span></div>
    <div class="w-envs">${envChips}</div>
    <h3>Solution 绑定</h3>
    ${sols}
  </div>`;
}

/* ═══════════════════════════════════════════════════════════
   Govern — HITL 队列 + 审批/驳回 + DLQ 监控
   ═══════════════════════════════════════════════════════════ */
function renderGovern() {
  const queue = (DATA.hitl_queue || []).filter(it =>
    hitlFilter === 'all' || (it.red_line || '') === hitlFilter);
  const open = queue.filter(it => (it.status || '') !== 'resolved');

  const tabs = ['all', 'Contract', 'Compliance', 'Risk'].map(r => {
    const n = r === 'all' ? queue.length : queue.filter(it => (it.red_line || '') === r).length;
    return `<button class="btn btn-sm ${hitlFilter === r ? 'btn-amber' : ''}" data-hf="${r}">${r === 'all' ? '全部' : r} (${n})</button>`;
  }).join(' ');

  const items = queue.length ? queue.map(it => {
    const resolved = (it.status || '') === 'resolved';
    const rl = it.red_line || 'Other';
    const rlKind = rl === 'Contract' ? 'amber' : rl === 'Compliance' ? 'blue' : rl === 'Risk' ? 'yellow' : 'gray';
    const approveCmd = `aeaos ops hitl-approve ${esc(it.trace_id)} --reviewer ops-console`;
    const rejectCmd = `aeaos ops hitl-reject ${esc(it.trace_id)} --reason "ops-console rejected"`;
    return `<div class="alert ${resolved ? 'low' : 'high'}">
      <div style="flex:1">
        <div class="a-title">${esc(it.capability || it.category || 'HITL')} ${badge(rl, rlKind)} ${badge(it.source || '', 'gray')}</div>
        <div class="a-detail">policy: ${esc(it.policy || '—')} · status: ${esc(it.status || 'open')}${resolved ? ' · 决策: ' + esc(it.decision) : ''}</div>
        <div class="a-meta">trace ${esc(it.trace_id || '—')} · ${fmtTs(it.raised_at)}</div>
        ${resolved ? '' : `<div class="row" style="margin-top:8px">
          <button class="btn btn-green btn-sm" onclick="renderCmd('审批命令', '${approveCmd.replace(/'/g, "\\'")}')">✓ 审批</button>
          <button class="btn btn-red btn-sm" onclick="renderCmd('驳回命令', '${rejectCmd.replace(/'/g, "\\'")}')">✗ 驳回</button>
        </div>`}
      </div>
    </div>`;
  }).join('') : `<div class="empty-box">HITL 队列为空。</div>`;

  // DLQ 监控
  const dlq = DATA.dlq || { count: 0, items: [] };
  const dlqRows = (dlq.items || []).map(d => `
    <tr>
      <td class="mono">${esc(d.event_id)}</td>
      <td class="mono"><span class="link" onclick="jumpToEvent('${esc(d.original_event_id)}')">${esc(d.original_event_id)}</span></td>
      <td class="mono">${esc(d.consumer || '—')}</td>
      <td class="mono">${esc(d.routing_key || '—')}</td>
      <td class="muted">${esc(d.reason || '')}</td>
      <td class="mono">${fmtTs(d.ts)}</td>
    </tr>`).join('') || `<tr><td colspan="6" class="muted">无死信事件。</td></tr>`;

  $('page-govern').innerHTML = `
    <h1><span class="accent">//</span> Governance</h1>
    <div class="grid-2">
      <div class="glass">
        <h2>HITL Queue <span class="muted" style="font-family:var(--sans);font-weight:400">（未决 ${open.length}）</span></h2>
        <div class="row" style="margin-bottom:12px">${tabs}</div>
        ${items}
      </div>
      <div class="glass">
        <h2>Dead Letter Queue <span class="muted" style="font-family:var(--sans);font-weight:400">（${dlq.count}）</span></h2>
        <table><thead><tr><th>DLQ Event</th><th>Original</th><th>Consumer</th><th>Key</th><th>Reason</th><th>Time</th></tr></thead>
        <tbody>${dlqRows}</tbody></table>
        <p class="muted" style="font-size:12px;margin-top:10px">点击 Original 事件 id 跳转至 Runtime 事件浏览器定位原事件。</p>
      </div>
    </div>`;

  document.querySelectorAll('#page-govern [data-hf]').forEach(b => {
    b.addEventListener('click', () => { hitlFilter = b.getAttribute('data-hf'); renderGovern(); });
  });
}

function jumpToEvent(eventId) {
  const ev = ALL_EVENTS.find(e => (e.event_id || '') === eventId);
  runtimeFilters = { routing_key: '', session_id: '', trace_id: ev ? ev.trace_id : eventId, tenant_id: '', source: '' };
  runtimePage = 0;
  if (ev) selectedTraceId = ev.trace_id;
  showPage('runtime');
}

/* ═══════════════════════════════════════════════════════════
   命令渲染 + 二次确认弹窗（前端只渲染命令，不执行，design.md §7.8）
   ═══════════════════════════════════════════════════════════ */
function renderCmd(title, cmd) {
  // 在 Governance 页底部临时插入命令块（也可改为弹窗）。此处用弹窗展示并支持复制。
  openModal(title, `<div class="cmd-block" style="margin:0"><code>${esc(cmd)}</code>
    <button class="btn btn-sm copy" onclick="copyText(this)">复制</button></div>
    <p class="muted" style="margin-top:10px">复制后在仓库根目录执行该命令（需具备运维权限）。</p>`, null);
}
function copyText(btn) {
  const code = btn.parentElement.querySelector('code').textContent;
  navigator.clipboard && navigator.clipboard.writeText(code);
  const old = btn.textContent; btn.textContent = '已复制'; setTimeout(() => btn.textContent = old, 1200);
}

function openModal(title, bodyHtml, onOk) {
  $('modal-title').textContent = title;
  $('modal-body').innerHTML = bodyHtml;
  const mask = $('modal-mask');
  mask.classList.add('show');
  const ok = $('modal-ok');
  const cancel = $('modal-cancel');
  ok.onclick = () => { mask.classList.remove('show'); if (onOk) onOk(); };
  cancel.onclick = () => mask.classList.remove('show');
  mask.onclick = (e) => { if (e.target === mask) mask.classList.remove('show'); };
}
function closeModal() { $('modal-mask').classList.remove('show'); }

/* ═══════════════════════════════════════════════════════════
   启动
   ═══════════════════════════════════════════════════════════ */
bindNav();
on('modal-cancel', 'click', closeModal);
load();
setInterval(load, POLL_MS);

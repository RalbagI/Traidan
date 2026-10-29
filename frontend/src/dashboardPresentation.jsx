import { useId, useState } from 'react';

import { MIN_OPERATOR_TOKEN_LENGTH } from './adminApi.js';
import {
  DASHBOARD_TIME_ZONE,
  DASHBOARD_TIME_ZONE_LABEL,
  formatDashboardClockTime,
  formatDashboardTimestamp,
} from './dateTime.js';
import {
  CYCLE_STATUS_LABELS,
  FRESHNESS_LABELS,
  ORDER_SIDE_LABELS,
  ORDER_STATUS_LABELS,
  SESSION_LABELS,
  gatePath,
} from './dashboardShell.js';
export function createWaitingCycle(dashboard, nowMs) {
  const offline = dashboard.service.state === 'bad';
  return {
    id: dashboard.service.cycleId === 'none' ? 'waiting-for-cycle' : dashboard.service.cycleId,
    seq: 0,
    asOf: nowMs,
    session: dashboard.service.session ?? 'closed',
    sessionLabel: SESSION_LABELS[dashboard.service.session] ?? 'סגור',
    status: offline ? 'held' : 'reviewing',
    thesis: dashboard.service.detail || dashboard.latestReasoning.thesis,
    model: 'GPT-6 Astra xhigh',
    commands: [],
    evidence: [],
    risk: {
      summary: dashboard.latestReasoning.risk,
      verdict: 'not_evaluated',
      verdictLabel: 'לא נבדק',
      notes: '',
    },
    blockedGate: null,
    gates: gatePath(null).map((gate) => ({ ...gate, state: 'na', detail: 'לא נבדק' })),
    orders: [],
    memory: {
      conclusion: dashboard.latestReasoning.memory,
      watch: [],
    },
    waiting: true,
  };
}

function Topbar({
  dashboard,
  connectionStatus,
  adminApiStatus,
  nowMs,
}) {
  return (
    <header className="topbar">
      <div className="brand">
        <span className="brand-mark"><BrandMark /></span>
        <div className="brand-text">
          <p className="eyebrow">Traidan</p>
          <h1>דסק מניות LLM</h1>
        </div>
      </div>
      <div className="topbar-right">
        <span className="session-tag">
          {SESSION_LABELS[dashboard.service.session] ?? 'סגור'}
          <b className="num" title={DASHBOARD_TIME_ZONE}>{formatDashboardClockTime(nowMs)}</b>
          <span className="tz-label">{DASHBOARD_TIME_ZONE_LABEL}</span>
        </span>
        <div className="status-cluster" aria-label="סטטוס חיבור">
          <StatusPill
            label="זרם"
            value={connectionStatusLabel(connectionStatus)}
            state={connectionStatus === 'connected' ? 'good' : connectionStatus === 'disconnected' ? 'bad' : 'warn'}
            live={connectionStatus === 'connected'}
          />
          <StatusPill label="שירות" value={dashboard.service.label} state={dashboard.service.state} />
          <StatusPill label="Admin API" value={adminApiStatusLabel(adminApiStatus)} state={adminApiPillState(adminApiStatus)} />
        </div>
      </div>
    </header>
  );
}

function IncidentBanner({ health, reconnectDelayMs }) {
  const incident = health.incident;
  if (!incident) return null;
  const retrySeconds = Number.isFinite(reconnectDelayMs)
    ? Math.ceil(reconnectDelayMs / 1000)
    : null;
  return (
    <section
      className={`incident-banner ${incident.tone}`}
      role={incident.tone === 'critical' ? 'alert' : 'status'}
      aria-labelledby="incident-title"
    >
      <span className="incident-icon"><Icon name={incident.tone === 'critical' ? 'alert' : 'clock'} size={20} /></span>
      <div className="incident-copy">
        <strong id="incident-title">{incident.title}</strong>
        <span dir="auto">{incident.detail}</span>
      </div>
      <div className="incident-meta">
        {Number.isFinite(health.lastUpdateAtMs) ? (
          <span>עדכון אחרון <bdi dir="ltr">{formatDashboardTimestamp(health.lastUpdateAtMs)}</bdi></span>
        ) : <span>טרם התקבל עדכון מאומת</span>}
        {retrySeconds ? <span>ניסיון נוסף בעוד <bdi dir="ltr">{retrySeconds}s</bdi></span> : null}
        <a href="#system-access">לפרטי מערכת</a>
      </div>
    </section>
  );
}

function BrandMark() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" aria-hidden="true">
      <g fill="oklch(0.18 0.01 248)">
        <rect x="4" y="13" width="3.4" height="7" rx="1" />
        <rect x="10.3" y="8" width="3.4" height="12" rx="1" />
        <rect x="16.6" y="4" width="3.4" height="16" rx="1" />
      </g>
    </svg>
  );
}

function StatusPill({
  label,
  value,
  state,
  live = false,
}) {
  return (
    <span className={`status-pill ${state || ''}`} aria-label={`${label}: ${value}`}>
      <span className={`led ${live ? 'live' : ''}`} />
      <span className="lab">{label}</span>
      <strong className="val">{value}</strong>
    </span>
  );
}

function Panel({
  title,
  meta,
  children,
  className = '',
  panelState = null,
  id,
}) {
  return (
    <section id={id} className={`panel ${className} ${panelState ? `data-${panelState.status}` : ''}`}>
      <div className="panel-header">
        <h2 className="panel-title"><span className="panel-key" />{title}</h2>
        <div className="panel-header-meta">
          {panelState ? <DataStateBadge panelState={panelState} /> : null}
          {meta != null ? <span className="panel-meta">{meta}</span> : null}
        </div>
      </div>
      <div className="panel-body">{children}</div>
    </section>
  );
}

function DataStateBadge({ panelState }) {
  const label = {
    loading: 'טוען',
    ready: 'עדכני',
    stale: 'מידע שמור',
    empty: 'אין פעילות',
    error: 'לא זמין',
  }[panelState.status] ?? panelState.status;
  const detail = Number.isFinite(panelState.updatedAtMs)
    ? `עודכן ${formatDashboardTimestamp(panelState.updatedAtMs)}`
    : label;
  return <span className={`data-state ${panelState.status}`} title={detail}><span className="dot" />{label}</span>;
}

function PanelStateMessage({ status, loading, empty, error }) {
  const config = status === 'error'
    ? { icon: 'alert', title: 'הנתונים אינם זמינים', body: error }
    : status === 'loading'
      ? { icon: 'clock', title: 'ממתין לנתונים מאומתים', body: loading }
      : { icon: 'list', title: 'אין פעילות להצגה', body: empty };
  return (
    <div className={`panel-state-message ${status}`}>
      <Icon name={config.icon} size={20} />
      <span><strong>{config.title}</strong><span>{config.body}</span></span>
    </div>
  );
}

function DecisionHero({ cycle, panelState, onTrace }) {
  return (
    <section className={`panel hero-panel data-${panelState.status}`} aria-label="החלטת LLM אחרונה">
      <div className="panel-header">
        <h2 className="panel-title"><span className="panel-key" />החלטה אחרונה</h2>
        <div className="panel-header-meta">
          <DataStateBadge panelState={panelState} />
          <button className="link-button" type="button" onClick={() => onTrace(cycle)}>
            פתיחת מעקב מלא
            <Icon name="arrow" size={14} />
          </button>
        </div>
      </div>

      <div className="decision-top">
        <div className="decision-lead">
          <div className="verdict-row">
            <VerdictBadge status={cycle.status} label={cycle.statusLabel} />
            <span className="cycle-id">
              מחזור <bdi dir="ltr">#{cycle.seq || 'בהמתנה'}</bdi> · {ageLabel(cycle.asOf)} · {SESSION_LABELS[cycle.session] ?? cycle.sessionLabel}
            </span>
          </div>
          <p className="thesis" dir="auto">{cycle.thesis}</p>
        </div>
        <div className="model-chip">
          <span>מודל</span>
          <bdi className="model-value" dir="ltr">{cycle.model}</bdi>
        </div>
      </div>

      <div className="commands">
        {cycle.allocation?.length ? (
          <div className="command-refs" aria-label="הקצאת תיק מלאה">
            {cycle.allocation.map((holding) => (
              <span className="ref-chip" key={holding.symbol}><bdi dir="ltr">{holding.symbol} · {holding.weight_bps / 100}%</bdi></span>
            ))}
          </div>
        ) : cycle.commands.length === 0 ? (
          <div className="empty-block">עדיין אין פקודה שעברה את סריקת ה-LLM.</div>
        ) : cycle.commands.map((command) => (
          <CommandCard key={`${cycle.id}-${command.symbol}-${command.action}`} command={command} onTrace={() => onTrace(cycle)} />
        ))}
      </div>

      <GateFlow gates={cycle.gates} blockedGate={cycle.blockedGate} status={cycle.status} waiting={cycle.waiting} researchOnly={cycle.researchOnly} />
    </section>
  );
}

function VerdictBadge({ status, label }) {
  const visibleLabel = label ?? CYCLE_STATUS_LABELS[status] ?? status;
  return (
    <span className={`verdict-badge ${status}`}>
      <span className="dot" />
      {visibleLabel}
    </span>
  );
}

function CommandCard({ command, onTrace }) {
  return (
    <button className="command-card" type="button" onClick={onTrace}>
      <span className="command-symbol">
        <bdi dir="ltr"><strong>{command.symbol}</strong></bdi>
        <span className={`command-action ${command.actionClass}`}>{command.actionLabel}</span>
      </span>
      <span className="command-copy">
        <span className="command-rationale" dir="auto">{command.rationale}</span>
        <span className="command-refs">
          {command.refs.map((reference) => <span className="ref-chip" key={reference}>{reference}</span>)}
        </span>
      </span>
      <span className="command-right">
        <bdi className="command-notional" dir="ltr"><strong>{command.maxNotional > 0 ? `≤ ${formatMoney0(command.maxNotional)}` : '—'}</strong></bdi>
        <Confidence value={command.confidence} />
      </span>
    </button>
  );
}

function Confidence({ value }) {
  const pct = Math.round(value * 100);
  return (
    <span className="confidence" title={`רמת ביטחון ${pct}%`}>
      <span className="confidence-track"><span className="confidence-fill" style={{ width: `${pct}%` }} /></span>
      <span className="confidence-value num">{pct}%</span>
    </span>
  );
}

function GateFlow({
  gates,
  blockedGate,
  status,
  waiting,
  researchOnly,
}) {
  const allPass = !blockedGate && status !== 'held' && !waiting && !researchOnly;
  const heldNoOrder = status === 'held' && !waiting;
  return (
    <div className="pipeline" aria-label="מסלול סמכות Rust">
      <div className="pipeline-head">
        <span className="pipeline-title"><Icon name="brain" size={14} /> כוונה - סמכות - יומן</span>
        <span className="authority">LLM מציע - <b>סמכות Rust</b> מאשרת</span>
      </div>
      <div className="flow" role="list">
        <FlowNode node={{ key: 'intent', label: 'כוונת LLM', sub: 'ייעוץ', icon: 'brain' }} state={waiting || researchOnly ? 'na' : 'origin'} />
        {gates.map((gate) => (
          <FlowNode key={gate.key} node={gate} state={gate.state} />
        ))}
        <FlowNode
          node={{ key: 'order', label: heldNoOrder ? 'ללא פקודה' : 'יומן נייר', sub: heldNoOrder ? 'החזקה' : 'מילוי', icon: 'ledger' }}
          state={allPass ? 'lit' : 'pending'}
          endpoint
        />
      </div>
      <BlockedBanner gates={gates} blockedGate={blockedGate} status={status} waiting={waiting} researchOnly={researchOnly} />
    </div>
  );
}

function FlowNode({
  node,
  state,
  endpoint = false,
}) {
  let className = 'flow-node';
  if (state === 'pass' || state === 'lit' || state === 'origin') className += ' lit';
  else if (state === 'block') className += ' blocked';
  else className += ' pending';
  if (endpoint) className += ' endpoint';
  return (
    <div className={className} role="listitem" aria-label={`${node.label}: ${node.detail || node.sub || 'לא נבדק'}`}>
      <span className="gate-orb"><Icon name={state === 'block' ? 'block' : node.icon} size={18} /></span>
      <span className="gate-name">{node.label}</span>
      <span className="gate-sub">{state === 'block' ? 'חסום' : state === 'na' ? '-' : node.sub}</span>
      <span className="gate-detail" dir="auto">{node.detail || (state === 'na' ? 'לא נבדק' : node.sub)}</span>
    </div>
  );
}

function BlockedBanner({
  gates,
  blockedGate,
  status,
  waiting,
  researchOnly,
}) {
  if (waiting) {
    return (
      <div className="blocked-banner pending">
        <span className="banner-icon"><Icon name="clock" size={16} /></span>
        <span>ממתין לסריקה הבאה שתושלם. מסלול הסמכות עדיין לא נבדק.</span>
      </div>
    );
  }
  if (researchOnly) {
    return (
      <div className="blocked-banner pending">
        <span className="banner-icon"><Icon name="list" size={16} /></span>
        <span>סריקת המחקר הסתיימה ללא הסלמה להחלטת LLM; מסלול סמכות Rust לא הופעל.</span>
      </div>
    );
  }
  if (!blockedGate) {
    if (status === 'held') {
      return (
        <div className="blocked-banner ok">
          <span className="banner-icon"><Icon name="check" size={16} /></span>
          <span>כל השערים עברו; לא נבנתה פקודה. החשיפה הקיימת נשמרת.</span>
        </div>
      );
    }
    return (
      <div className="blocked-banner ok">
        <span className="banner-icon"><Icon name="check" size={16} /></span>
        <span>כל השערים התפעוליים עברו. הכוונה אושרה ונרשמה ביומן הנייר.</span>
      </div>
    );
  }
  const gate = gates.find((candidate) => candidate.key === blockedGate);
  return (
    <div className="blocked-banner">
      <span className="banner-icon"><Icon name="alert" size={16} /></span>
      <span>נעצר בשער <b>{gate?.label}</b> - <code>{gate?.detail}</code>. אין הרשאת מילוי.</span>
    </div>
  );
}

function CycleTimeline({
  cycles,
  activeId,
  panelState,
  onSelect,
}) {
  const checkCount = cycles.reduce(
    (total, cycle) => total + Math.max(1, Number(cycle.repeatCount) || 1),
    0,
  );
  const activityMeta = checkCount === cycles.length
    ? formatHebrewCount(cycles.length, 'סריקה אחת', 'סריקות')
    : `${formatHebrewCount(cycles.length, 'הזדמנות אחת', 'הזדמנויות')} · ${formatHebrewCount(checkCount, 'בדיקה אחת', 'בדיקות')}`;
  return (
    <Panel title="פעילות מחזורים" meta={activityMeta} className="cycles-panel" panelState={panelState}>
      {cycles.length === 0 ? (
        <PanelStateMessage
          status={panelState.status}
          loading="הסריקה הראשונה טרם פורסמה לזרם המפעיל."
          empty="אין עדיין מחזורים שהושלמו."
          error="לא ניתן לאמת מחזורים חדשים בזמן שהזרם מנותק."
        />
      ) : (
        <div className="cycle-list">
          {cycles.slice(0, 20).map((cycle) => {
            const command = cycle.commands[0];
            const action = cycle.allocation?.length
              ? `הקצאת תיק · ${cycle.allocation.length} אחזקות`
              : cycle.commands.length > 1
                ? `${cycle.commands.filter((row) => row.action === 'buy_notional').length} קניות · ${cycle.commands.filter((row) => row.action === 'sell_notional').length} מכירות`
                : command ? `${command.symbol} · ${command.actionLabel}`
                  : cycle.status === 'held' ? 'ללא שינוי בתיק' : 'אין פקודה';
            const blocker = cycle.blockedGate
              ? cycle.gates.find((gate) => gate.key === cycle.blockedGate)?.label
              : null;
            const notionalLine = Number.isFinite(cycle.actualNotional)
              ? `בוצע ${cycle.actualNotionalLabel} · מינימום ${cycle.requiredNotionalLabel}`
              : null;
            return (
              <button key={cycle.id} className={`cycle-list-item ${cycle.id === activeId ? 'active' : ''}`} type="button" onClick={() => onSelect(cycle)}>
                <span className={`timeline-status ${cycle.status}`}><span className="dot" />{cycle.statusLabel ?? CYCLE_STATUS_LABELS[cycle.status] ?? cycle.status}</span>
                <span className="cycle-list-main"><strong><bdi dir="ltr">{action}</bdi></strong><span dir="auto">{blocker ? `נעצר בשער ${blocker}` : cycle.thesis}</span>{notionalLine ? <em><bdi dir="ltr">{notionalLine}</bdi></em> : null}</span>
                <span className="cycle-list-time"><bdi>{formatDashboardTimestamp(cycle.asOf)}</bdi><span>{SESSION_LABELS[cycle.session] ?? cycle.sessionLabel}</span></span>
                <Icon name="arrow" size={16} />
              </button>
            );
          })}
        </div>
      )}
    </Panel>
  );
}

function FreshnessPanel({ rows, panelState }) {
  return (
    <Panel title="טריות נתונים" meta={formatHebrewCount(panelState.status === 'ready' || panelState.status === 'stale' ? rows.length : 0, 'מקור אחד', 'מקורות')} className="freshness-panel" panelState={panelState}>
      {panelState.status === 'loading' || panelState.status === 'empty' || panelState.status === 'error' ? (
        <PanelStateMessage
          status={panelState.status}
          loading="ממתין ל-market research snapshot הראשון."
          empty="סריקת החדשות הציבורית הסתיימה; snapshot שוק מלא נשלף רק לאחר שאירוע עובר את סף ההסלמה."
          error="אין snapshot מאומת שאפשר להציג בזמן הניתוק."
        />
      ) : (
        <div className="fresh-grid">
          {rows.map((row, index) => (
            <div className="fresh-row" key={`${row.source}-${row.freshness}-${index}`}>
              <span className={`fresh-led ${row.freshness}`} />
              <span>
                <bdi dir="ltr"><strong>{row.source}</strong></bdi>
                <span dir="auto">{row.detail}</span>
              </span>
              <em className={`fresh-state ${row.freshness}`}>{row.freshnessLabel ?? FRESHNESS_LABELS[row.freshness] ?? row.freshness}</em>
            </div>
          ))}
        </div>
      )}
    </Panel>
  );
}

function OrdersPanel({ orders, panelState, onSelect }) {
  return (
    <Panel title="פקודות סימולציה" meta={formatHebrewCount(orders.length, 'פקודה אחת', 'פקודות')} className="orders-panel" panelState={panelState}>
      {orders.length === 0 ? (
        <PanelStateMessage
          status={panelState.status}
          loading="ממתין לתוצאת פקודת הסימולציה הראשונה."
          empty="לא נוצרו פקודות סימולציה במחזורים שהתקבלו."
          error="לא ניתן לאמת פקודות חדשות בזמן שהזרם מנותק."
        />
      ) : (
        <div className="orders-list">
          {orders.slice(0, 8).map((order) => (
            <button className="order-row" type="button" key={order.id} onClick={() => onSelect(order)}>
              <span className={`order-side ${order.side}`}>{order.sideLabel ?? ORDER_SIDE_LABELS[order.side] ?? order.side}</span>
              <span className="order-main">
                <strong><bdi dir="ltr">{order.symbol} · {formatNumber(order.qty, 2)} @ {formatMoney(order.avgPrice)}</bdi></strong>
                <span dir="auto">{order.reason}</span>
              </span>
              <span className={`order-status ${order.status}`}>{order.statusLabel ?? ORDER_STATUS_LABELS[order.status] ?? order.status}</span>
              <bdi className="order-notional" dir="ltr"><strong>{formatMoney0(order.notional)}</strong></bdi>
            </button>
          ))}
        </div>
      )}
    </Panel>
  );
}

function MemoryPanel({ rows, panelState }) {
  return (
    <Panel title="זיכרון הדסק" meta={formatHebrewCount(rows.length, 'עדכון אחד', 'עדכונים')} className="memory-panel" panelState={panelState}>
      {rows.length === 0 ? (
        <PanelStateMessage
          status={panelState.status}
          loading="הזיכרון יוצג רק לאחר כתיבה אמיתית מהשירות."
          empty="לא נכתב עדכון זיכרון במחזורים שהתקבלו."
          error="לא ניתן לאמת עדכוני זיכרון בזמן שהזרם מנותק."
        />
      ) : (
        <div>
          {rows.slice(0, 5).map((item) => (
            <article className="memory-item" key={item.id}>
              <bdi className="memory-id" dir="ltr">{shortCycleId(item.cycleId)}</bdi>
              <p dir="auto">{item.conclusion}</p>
              {item.watchItems?.length ? (
                <div className="watch-list">
                  {item.watchItems.map((watchItem) => <span className="watch-chip" key={watchItem}>{watchItem}</span>)}
                </div>
              ) : null}
            </article>
          ))}
        </div>
      )}
    </Panel>
  );
}

function ConnectionPanel({
  service,
  reconnectDelayMs,
  adminToken,
  adminApiStatus,
  onAdminTokenChange,
  onClearAdminToken,
  onRetryAdminToken,
}) {
  const [showToken, setShowToken] = useState(false);
  const tokenHintId = useId();
  const tokenStatusId = useId();
  const tokenReady = adminToken.trim().length >= MIN_OPERATOR_TOKEN_LENGTH;
  const submitToken = (event) => {
    event.preventDefault();
    if (tokenReady) onRetryAdminToken();
  };
  return (
    <Panel id="system-access" title="מערכת וגישה" meta={service.ageLabel} className="connection-panel">
      <div className="connection">
        <p dir="auto">{service.detail}</p>
        <div className="connection-meta"><span>מחזור אחרון</span><bdi dir="ltr"><strong>{shortCycleId(service.cycleId)}</strong></bdi></div>
        {reconnectDelayMs ? (
          <div className="connection-meta warning"><span>חיבור מחדש</span><bdi dir="ltr"><strong>{Math.ceil(reconnectDelayMs / 1_000)}s</strong></bdi></div>
        ) : null}
        <div className="connection-meta">
          <span>Admin API</span>
          <strong className={adminApiStatus === 'unauthorized' ? 'bad' : ''}>{adminApiStatusLabel(adminApiStatus)}</strong>
        </div>
        <form className="token-form" onSubmit={submitToken}>
          <div className="token-form-heading">
            <label htmlFor="admin-token">טוקן מפעיל מקומי</label>
            <span id={tokenHintId}>נשמר ב-sessionStorage ונמחק עם סיום סשן הדפדפן.</span>
          </div>
          <div className="token-row">
            <input
              id="admin-token"
              aria-describedby={`${tokenHintId} ${tokenStatusId}`}
              aria-invalid={adminApiStatus === 'unauthorized'}
              type={showToken ? 'text' : 'password'}
              value={adminToken}
              minLength={MIN_OPERATOR_TOKEN_LENGTH}
              autoComplete="off"
              spellCheck="false"
              dir="ltr"
              onChange={onAdminTokenChange}
              placeholder="Admin API token"
            />
            <button type="button" className="token-secondary" onClick={() => setShowToken((value) => !value)} aria-pressed={showToken}>
              {showToken ? 'הסתרה' : 'הצגה'}
            </button>
            <button type="submit" disabled={!tokenReady || adminApiStatus === 'checking'}>
              {adminApiStatus === 'checking' ? 'בודק…' : 'בדיקה חוזרת'}
            </button>
            <button type="button" className="token-secondary" onClick={onClearAdminToken} disabled={!adminToken}>ניקוי</button>
          </div>
          <p id={tokenStatusId} className={`token-status ${adminApiPillState(adminApiStatus)}`} role={adminApiStatus === 'unauthorized' ? 'alert' : 'status'}>
            {adminApiStatusDescription(adminApiStatus)}
          </p>
        </form>
      </div>
    </Panel>
  );
}


function Icon({ name, size = 16 }) {
  const props = {
    width: size,
    height: size,
    viewBox: '0 0 24 24',
    fill: 'none',
    stroke: 'currentColor',
    strokeWidth: 1.7,
    strokeLinecap: 'round',
    strokeLinejoin: 'round',
    'aria-hidden': 'true',
  };
  switch (name) {
    case 'doc': return <svg {...props}><path d="M7 3h7l4 4v14H7z" /><path d="M14 3v4h4" /><path d="M10 12h5M10 16h5" /></svg>;
    case 'wave': return <svg {...props}><path d="M3 12c2 0 2-4 4-4s2 8 4 8 2-8 4-8 2 4 4 4" /></svg>;
    case 'clock': return <svg {...props}><circle cx="12" cy="12" r="8" /><path d="M12 8v4l3 2" /></svg>;
    case 'list': return <svg {...props}><path d="M8 7h11M8 12h11M8 17h11" /><circle cx="4" cy="7" r="1" /><circle cx="4" cy="12" r="1" /><circle cx="4" cy="17" r="1" /></svg>;
    case 'shield': return <svg {...props}><path d="M12 3l7 3v5c0 5-3.5 8-7 9-3.5-1-7-4-7-9V6z" /><path d="M9 12l2 2 4-4" /></svg>;
    case 'loop': return <svg {...props}><path d="M4 9a8 8 0 0 1 14-3l2 2" /><path d="M20 5v4h-4" /><path d="M20 15a8 8 0 0 1-14 3l-2-2" /><path d="M4 19v-4h4" /></svg>;
    case 'ledger': return <svg {...props}><rect x="4" y="4" width="16" height="16" rx="2" /><path d="M4 9h16M9 14h6M9 17h3" /></svg>;
    case 'check': return <svg {...props}><path d="M5 12l4 4 10-10" /></svg>;
    case 'block': return <svg {...props}><circle cx="12" cy="12" r="8" /><path d="M7 7l10 10" /></svg>;
    case 'alert': return <svg {...props}><path d="M12 4l9 16H3z" /><path d="M12 10v4M12 17h.01" /></svg>;
    case 'close': return <svg {...props}><path d="M6 6l12 12M18 6 6 18" /></svg>;
    case 'brain': return <svg {...props}><path d="M9 4a3 3 0 0 0-3 3 3 3 0 0 0-1 5 3 3 0 0 0 2 5 3 3 0 0 0 5 1V4.5A2.5 2.5 0 0 0 9 4z" /><path d="M15 4a3 3 0 0 1 3 3 3 3 0 0 1 1 5 3 3 0 0 1-2 5 3 3 0 0 1-5 1" /></svg>;
    case 'arrow': return <svg {...props}><path d="M5 12h14M13 6l6 6-6 6" /></svg>;
    default: return null;
  }
}

function adminApiStatusLabel(status) {
  if (status === 'available') return 'זמין';
  if (status === 'checking') return 'בודק';
  if (status === 'unauthorized') return 'נדחה';
  if (status === 'unavailable') return 'מקומי בלבד';
  return 'נדרש';
}

function adminApiStatusDescription(status) {
  if (status === 'available') return 'הטוקן אומת ו-Admin API זמין לסשן הנוכחי.';
  if (status === 'checking') return 'בודק את הטוקן מול Admin API המקומי.';
  if (status === 'unauthorized') return 'הטוקן נדחה. הדשבורד נשאר במצב קריאה.';
  if (status === 'unavailable') return 'Admin API אינו נגיש כעת; נתוני הזרם ממשיכים לפעול.';
  return `נדרש טוקן באורך ${MIN_OPERATOR_TOKEN_LENGTH} תווים לפחות.`;
}

function adminApiPillState(status) {
  if (status === 'available') return 'good';
  if (status === 'unauthorized') return 'bad';
  return 'warn';
}

function connectionStatusLabel(status) {
  if (status === 'connected') return 'מחובר';
  if (status === 'disconnected') return 'מנותק';
  if (status === 'connecting') return 'מתחבר';
  return status;
}

function formatHebrewCount(count, singular, plural) {
  return count === 1 ? singular : `${count} ${plural}`;
}

function ageLabel(ms) {
  const age = Math.max(0, Date.now() - ms);
  if (age < 1000) return 'עכשיו';
  if (age < 60_000) return `לפני ${Math.floor(age / 1000)} שנ׳`;
  if (age < 3_600_000) return `לפני ${Math.floor(age / 60_000)} דק׳`;
  if (age < 86_400_000) return `לפני ${Math.floor(age / 3_600_000)} שע׳`;
  return formatDashboardTimestamp(ms);
}

function shortCycleId(id) {
  if (!id || id === 'none') return 'none';
  return String(id).replace('llm-equity-', '');
}

function formatMoney(value) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  }).format(normalizedFinite(value));
}

function formatMoney0(value) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(normalizedFinite(value));
}

function formatSignedMoney(value) {
  return `${value >= 0 ? '+' : '-'}${formatMoney(Math.abs(value))}`;
}

function formatPct(value, digits = 2) {
  return new Intl.NumberFormat('en-US', {
    style: 'percent',
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(normalizedFinite(value));
}

function formatSignedPct(value, digits = 2) {
  return `${value >= 0 ? '+' : '-'}${formatPct(Math.abs(value), digits)}`;
}

function formatNumber(value, digits = 4) {
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: digits,
  }).format(normalizedFinite(value));
}

function normalizedFinite(value) {
  const number = Number.isFinite(value) ? value : 0;
  return Object.is(number, -0) || Math.abs(number) < 0.005 ? 0 : number;
}

export {
  ConnectionPanel,
  CycleTimeline,
  DecisionHero,
  FreshnessPanel,
  IncidentBanner,
  Icon,
  MemoryPanel,
  OrdersPanel,
  Panel,
  PanelStateMessage,
  Topbar,
  VerdictBadge,
  formatMoney,
  formatMoney0,
  formatNumber,
  formatPct,
  formatSignedMoney,
  formatSignedPct,
};

import { useEffect, useRef, useState } from 'react';

import { formatDashboardTimestamp } from './dateTime.js';
import {
  FRESHNESS_LABELS,
  ORDER_SIDE_LABELS,
  ORDER_STATUS_LABELS,
  RISK_VERDICT_LABELS,
  SESSION_LABELS,
} from './dashboardShell.js';
import {
  Icon,
  VerdictBadge,
  formatMoney,
  formatMoney0,
  formatNumber,
} from './dashboardPresentation.jsx';
import { createTraceCopy } from './traceCopy.js';

const DIALOG_FOCUS_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

export default function TraceDrawer({ cycle, order, onClose }) {
  const open = Boolean(cycle);
  const [renderedCycle, setRenderedCycle] = useState(cycle);
  const drawerRef = useRef(null);
  const previousFocusRef = useRef(null);

  useEffect(() => {
    if (cycle) setRenderedCycle(cycle);
  }, [cycle]);

  useEffect(() => {
    if (!open) return undefined;

    previousFocusRef.current = document.activeElement;
    const drawer = drawerRef.current;
    const focusTarget = drawer?.querySelector(DIALOG_FOCUS_SELECTOR) ?? drawer;
    focusTarget?.focus({ preventScroll: true });

    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        onClose();
        return;
      }
      if (event.key !== 'Tab' || !drawerRef.current) return;

      const focusable = [...drawerRef.current.querySelectorAll(DIALOG_FOCUS_SELECTOR)]
        .filter((element) => element.offsetParent !== null);
      if (focusable.length === 0) {
        event.preventDefault();
        drawerRef.current.focus({ preventScroll: true });
        return;
      }

      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus({ preventScroll: true });
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus({ preventScroll: true });
      }
    };

    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('keydown', onKeyDown);
      previousFocusRef.current?.focus?.({ preventScroll: true });
      previousFocusRef.current = null;
    };
  }, [onClose, open]);

  return (
    <>
      <div className={`scrim ${open ? 'open' : ''}`} aria-hidden="true" onClick={onClose} />
      <aside
        ref={drawerRef}
        className={`drawer ${open ? 'open' : ''}`}
        aria-hidden={!open}
        aria-labelledby="trace-drawer-title"
        aria-modal={open ? 'true' : undefined}
        role="dialog"
        tabIndex={-1}
        onTransitionEnd={(event) => {
          if (event.target === event.currentTarget && event.propertyName === 'transform' && !open) {
            setRenderedCycle(null);
          }
        }}
      >
        {cycle || renderedCycle ? (
          <CycleTrace cycle={cycle ?? renderedCycle} order={order} onClose={onClose} />
        ) : null}
      </aside>
    </>
  );
}

function CycleTrace({ cycle, onClose }) {
  const copy = createTraceCopy(cycle);
  const riskIcon = cycle.risk.verdict === 'blocked'
    ? 'block'
    : cycle.risk.verdict === 'reduced'
      ? 'alert'
      : cycle.risk.verdict === 'within_limits'
        ? 'check'
        : 'clock';

  return (
    <>
      <div className="drawer-header">
        <h2 id="trace-drawer-title">מעקב · מחזור <bdi dir="ltr">#{cycle.seq || 'בהמתנה'}</bdi></h2>
        <button className="drawer-close" type="button" onClick={onClose} aria-label="סגירה">
          <Icon name="close" size={16} />
        </button>
      </div>
      <div className="drawer-body">
        <div className="verdict-row">
          <VerdictBadge status={cycle.status} label={cycle.statusLabel} />
          <span className="cycle-id"><bdi>{formatDashboardTimestamp(cycle.asOf)}</bdi> · <bdi dir="ltr">{cycle.model}</bdi> · {SESSION_LABELS[cycle.session] ?? cycle.sessionLabel}</span>
        </div>

        <TraceStep label="1 - ראיות שנאספו">
          {cycle.evidence.length === 0 ? (
            <p className="trace-body muted">לא התקבלה מסגרת ראיות למחזור הזה.</p>
          ) : cycle.evidence.map((item) => (
            <div className="evidence-card" key={item.id}>
              <div className="evidence-source">
                <strong>{item.source}</strong>
                <span className={`fresh-state ${item.freshness}`}>{item.freshnessLabel ?? FRESHNESS_LABELS[item.freshness] ?? item.freshness}</span>
              </div>
              <p dir="auto">{item.summary}</p>
              <div className="command-refs">
                <span className="ref-chip">{item.id}</span>
                <span className="ref-chip">נשלף {formatDashboardTimestamp(item.retrievedAt, 'לא ידוע')}</span>
              </div>
            </div>
          ))}
        </TraceStep>

        <TraceStep label={copy.thesisLabel}><p className="trace-body" dir="auto">{cycle.thesis}</p></TraceStep>

        <TraceStep label={copy.intentLabel}>
          {cycle.allocation?.length ? (
            <div className="command-refs" aria-label="הקצאת תיק מלאה">
              {cycle.allocation.map((holding) => (
                <span className="ref-chip" key={holding.symbol}><bdi dir="ltr">{holding.symbol} · {holding.weight_bps / 100}%</bdi></span>
              ))}
            </div>
          ) : cycle.commands.length === 0 ? (
            <p className="trace-body muted">{copy.intentEmpty}</p>
          ) : cycle.commands.map((command) => (
            <div className="evidence-card" key={`${command.symbol}-${command.action}`}>
              <div className="evidence-source">
                <strong>{command.symbol} - {command.actionLabel}</strong>
                <span className="num">{Math.round(command.confidence * 100)}% ביטחון - {'≤ '}{formatMoney0(command.maxNotional)}</span>
              </div>
              <p dir="auto">{command.rationale}</p>
            </div>
          ))}
        </TraceStep>

        <TraceStep label={copy.gateLabel} tone={cycle.researchOnly ? '' : cycle.blockedGate ? 'bad' : 'go'}>
          <div className="mini-gates">
            {cycle.gates.map((gate) => (
              <span key={gate.key} className={`mini-gate ${gate.state === 'pass' ? 'pass' : gate.state === 'block' ? 'block' : 'na'}`}>
                <span><span className="dot" />{gate.label}</span><small dir="auto">{gate.detail}</small>
              </span>
            ))}
          </div>
        </TraceStep>

        <TraceStep label="5 - הערכת סיכון" tone={cycle.risk.verdict === 'blocked' ? 'bad' : cycle.risk.verdict === 'within_limits' ? 'go' : ''}>
          <div className="risk-box">
            <div className={`risk-verdict ${cycle.risk.verdict}`} data-icon={riskIcon}>
              <Icon name={riskIcon} size={14} />
              {cycle.risk.verdictLabel ?? RISK_VERDICT_LABELS[cycle.risk.verdict] ?? cycle.risk.verdict.replace('_', ' ')}
            </div>
            <p dir="auto">{cycle.risk.summary}</p>
            {cycle.risk.notes ? <span>{cycle.risk.notes}</span> : null}
          </div>
        </TraceStep>

        <TraceStep label={copy.resultLabel} tone={cycle.orders.some((item) => item.status === 'rejected') ? 'bad' : cycle.orders.length ? 'go' : ''}>
          {cycle.orders.length === 0 ? (
            <p className="trace-body muted">{copy.resultEmpty}</p>
          ) : cycle.orders.map((item) => (
            <div className="trace-order" key={item.id}>
              <span className={`order-side ${item.side}`}>{item.sideLabel ?? ORDER_SIDE_LABELS[item.side] ?? item.side}</span>
              <span><strong>{item.symbol} · {formatNumber(item.qty, 2)} @ {formatMoney(item.avgPrice)}</strong><em>{item.reason}</em></span>
              <span className={`order-status ${item.status}`}>{item.statusLabel ?? ORDER_STATUS_LABELS[item.status] ?? item.status}</span>
              <strong className="order-notional">{formatMoney0(item.notional)}</strong>
            </div>
          ))}
        </TraceStep>

        <TraceStep label={copy.memoryLabel}>
          <p className="trace-body" dir="auto">{cycle.memory.conclusion}</p>
          {cycle.memory.watch.length ? (
            <div className="watch-list">{cycle.memory.watch.map((item) => <span className="watch-chip" key={item}>{item}</span>)}</div>
          ) : null}
        </TraceStep>
      </div>
    </>
  );
}

function TraceStep({ label, tone = '', children }) {
  return (
    <div className="trace-step">
      <span className={`trace-dot ${tone}`} />
      <div className="trace-label">{label}</div>
      {children}
    </div>
  );
}

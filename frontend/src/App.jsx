import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { AdminApiClient, DEFAULT_ADMIN_API_URL, MIN_OPERATOR_TOKEN_LENGTH } from './adminApi.js';
import { createDashboardViewModel } from './dashboardShell.js';
import {
  ConnectionPanel,
  CycleTimeline,
  DecisionHero,
  FreshnessPanel,
  IncidentBanner,
  MemoryPanel,
  OrdersPanel,
  Topbar,
  createWaitingCycle,
} from './dashboardPresentation.jsx';
import PortfolioPanel from './PortfolioPanel.jsx';
import TraceDrawer from './TraceDrawer.jsx';
import {
  forgetPersistedOperatorToken,
  persistOperatorToken,
  readPersistedOperatorToken,
} from './operatorTokenStorage.js';
import { applyEngineMessage, createOperationsState } from './operationsState.js';
import WebSocketClient from './WebSocketClient.jsx';

const DEFAULT_WS_URL = 'ws://localhost:8080/stream';
const WS_URL = import.meta.env.VITE_TRAIDAN_WS_URL ?? DEFAULT_WS_URL;
const ADMIN_API_URL = import.meta.env.VITE_TRAIDAN_ADMIN_API_URL ?? DEFAULT_ADMIN_API_URL;

export default function App() {
  const [operationsState, setOperationsState] = useState(createOperationsState);
  const [connectionStatus, setConnectionStatus] = useState('connecting');
  const [reconnectDelayMs, setReconnectDelayMs] = useState(null);
  const [adminToken, setAdminToken] = useState(() => readPersistedOperatorToken());
  const [adminApiStatus, setAdminApiStatus] = useState('token_required');
  const [adminCheckNonce, setAdminCheckNonce] = useState(0);
  const [nowMs, setNowMs] = useState(() => Date.now());
  const [trace, setTrace] = useState(null);
  const streamGenerationRef = useRef(0);
  const [streamGeneration, setStreamGeneration] = useState(0);

  useEffect(() => {
    const ws = new WebSocketClient(WS_URL, {
      onReconnectSchedule: setReconnectDelayMs,
      onStatusChange: (status) => {
        if (status === 'connecting') {
          streamGenerationRef.current += 1;
          setStreamGeneration(streamGenerationRef.current);
        }
        setConnectionStatus(status);
        if (status !== 'disconnected') setReconnectDelayMs(null);
      },
      onMessage: (message) => {
        setOperationsState((current) => applyEngineMessage(
          current,
          message,
          Date.now(),
          streamGenerationRef.current,
        ));
      },
    });
    return () => ws.close();
  }, []);

  useEffect(() => {
    const interval = window.setInterval(() => setNowMs(Date.now()), 1_000);
    return () => window.clearInterval(interval);
  }, []);

  useEffect(() => {
    const token = adminToken.trim();
    if (token.length < MIN_OPERATOR_TOKEN_LENGTH) {
      setAdminApiStatus('token_required');
      return undefined;
    }
    let canceled = false;
    setAdminApiStatus('checking');
    const timer = window.setTimeout(async () => {
      const result = await new AdminApiClient({ baseUrl: ADMIN_API_URL, token }).status();
      if (canceled) return;
      if (result.status === 'ok') {
        setAdminApiStatus('available');
      } else if (result.httpStatus === 401 || result.reason === 'unauthorized') {
        setAdminApiStatus('unauthorized');
      } else {
        setAdminApiStatus('unavailable');
      }
    }, 250);
    return () => {
      canceled = true;
      window.clearTimeout(timer);
    };
  }, [adminCheckNonce, adminToken]);

  const dashboard = useMemo(
    () => createDashboardViewModel({
      operationsState,
      connectionStatus,
      adminApiStatus,
      reconnectDelayMs,
      streamGeneration,
      nowMs,
    }),
    [operationsState, connectionStatus, adminApiStatus, reconnectDelayMs, streamGeneration, nowMs],
  );

  const currentCycle = dashboard.currentCycle ?? createWaitingCycle(dashboard, nowMs);

  const updateAdminToken = useCallback((event) => {
    const value = event.target.value;
    setAdminToken(value);
    persistOperatorToken(value);
  }, []);
  const clearAdminToken = useCallback(() => {
    forgetPersistedOperatorToken();
    setAdminToken('');
    setAdminApiStatus('token_required');
  }, []);
  const retryAdminToken = useCallback(() => setAdminCheckNonce((value) => value + 1), []);
  const openCycleTrace = useCallback((cycle) => setTrace({ cycle }), []);
  const openOrderTrace = useCallback((order) => {
    const cycle = dashboard.cycles.find((candidate) => candidate.id === order.cycleId) ?? currentCycle;
    setTrace({ cycle, order });
  }, [currentCycle, dashboard.cycles]);
  const closeTrace = useCallback(() => setTrace(null), []);

  return (
    <>
      <main className="app-shell" aria-hidden={trace ? 'true' : undefined} inert={trace ? '' : undefined}>
        <Topbar dashboard={dashboard} connectionStatus={connectionStatus} adminApiStatus={adminApiStatus} nowMs={nowMs} />
        <IncidentBanner health={dashboard.health} reconnectDelayMs={reconnectDelayMs} />
        <div className="sr-only" aria-live="polite" aria-atomic="true">
          {dashboard.health.incident?.title ?? `סטטוס המערכת: ${dashboard.service.label}`}
        </div>

        <section className="desk-grid" aria-label="דסק מניות LLM">
          <PortfolioPanel portfolio={dashboard.portfolio} navSeries={dashboard.navSeries} panelState={dashboard.panels.portfolio} />
          <DecisionHero cycle={currentCycle} panelState={dashboard.panels.decision} onTrace={openCycleTrace} />
          <CycleTimeline cycles={dashboard.cycles} activeId={trace?.cycle?.id} panelState={dashboard.panels.cycles} onSelect={openCycleTrace} />
          <FreshnessPanel rows={dashboard.freshness} panelState={dashboard.panels.freshness} />
          <OrdersPanel orders={dashboard.orders} panelState={dashboard.panels.orders} onSelect={openOrderTrace} />
          <MemoryPanel rows={dashboard.memory} panelState={dashboard.panels.memory} />
          <ConnectionPanel
            service={dashboard.service}
            reconnectDelayMs={reconnectDelayMs}
            adminToken={adminToken}
            adminApiStatus={adminApiStatus}
            onAdminTokenChange={updateAdminToken}
            onClearAdminToken={clearAdminToken}
            onRetryAdminToken={retryAdminToken}
          />
        </section>
      </main>
      <TraceDrawer cycle={trace?.cycle} order={trace?.order} onClose={closeTrace} />
    </>
  );
}

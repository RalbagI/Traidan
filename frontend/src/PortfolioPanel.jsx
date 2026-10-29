import { useCallback, useId, useMemo, useRef, useState } from 'react';

import { formatDashboardDateTime } from './dateTime.js';
import {
  Icon,
  Panel,
  PanelStateMessage,
  formatMoney,
  formatMoney0,
  formatNumber,
  formatPct,
  formatSignedMoney,
  formatSignedPct,
} from './dashboardPresentation.jsx';
import { buildNavChart } from './portfolioNavChartModel.js';

const BENCHMARK_STATUS_LABELS = Object.freeze({
  live: 'חי',
  stale: 'שמור',
  unavailable: 'לא זמין',
});

export default function PortfolioPanel({ portfolio, navSeries, panelState }) {
  const pnlClass = portfolio.totalPnl > 0.5 ? 'up' : portfolio.totalPnl < -0.5 ? 'down' : 'flat';
  const pnlPct = portfolio.initialEquity ? portfolio.totalPnl / portfolio.initialEquity : 0;
  const navImpaired = portfolio.available && portfolio.nav <= 0;
  const benchmarkStatus = portfolio.benchmark?.status ?? 'unavailable';
  const benchmarkPrice = portfolio.benchmark?.price;
  const benchmarkStatusLabel = BENCHMARK_STATUS_LABELS[benchmarkStatus] ?? benchmarkStatus;
  const benchmarkValueLabel = benchmarkStatus === 'live' && Number.isFinite(benchmarkPrice)
    ? formatMoney(benchmarkPrice)
    : benchmarkStatusLabel;
  return (
    <Panel
      title="תיק ו-P&L"
      meta={portfolio.available ? (navImpaired ? 'NAV סימולציה פגוע' : 'חשבון השקעות בסימולציה') : null}
      className="portfolio-panel"
      panelState={panelState}
    >
      {!portfolio.available ? (
        <PanelStateMessage
          status={panelState.status}
          loading="הכרטיס יוצג רק לאחר קבלת frame אמיתי של מצב התיק."
          empty="עדיין לא נוצרה תמונת תיק לסימולציה."
          error="אין תמונת תיק מאומתת שאפשר להציג בזמן הניתוק."
        />
      ) : (
        <>
          {panelState.status === 'stale' ? (
            <div className="stale-notice"><Icon name="clock" size={15} /> הנתונים נשמרו מהעדכון האחרון ואינם מייצגים מצב חי.</div>
          ) : null}
          <div className="portfolio-headline">
            <bdi className={`portfolio-nav num ${navImpaired ? 'down' : ''}`} dir="ltr">{portfolio.navLabel}</bdi>
            <bdi className={`portfolio-delta ${pnlClass}`} dir="ltr">{formatSignedMoney(portfolio.totalPnl)} · {formatSignedPct(pnlPct)}</bdi>
          </div>
          <div className="portfolio-sub">שווי נכסי נטו · P&amp;L כולל מהתחלה <bdi className={pnlClass} dir="ltr">{formatSignedMoney(portfolio.totalPnl)}</bdi> · התחלה <bdi dir="ltr">{formatMoney0(portfolio.initialEquity)}</bdi></div>
          <div className={`benchmark-status ${benchmarkStatus}`}>
            <span>S&amp;P500 / SPY</span>
            <bdi dir={benchmarkStatus === 'live' && Number.isFinite(benchmarkPrice) ? 'ltr' : 'rtl'}>{benchmarkValueLabel}</bdi>
          </div>
          <PortfolioNavChart series={navSeries} />
          <div className="portfolio-metrics">
            <Metric label="מזומן" value={portfolio.cashLabel} />
            <Metric label="שיעור השקעה" value={portfolio.nav > 0 ? formatPct(portfolio.gross / portfolio.nav, 1) : '—'} />
            <Metric label="חשיפה ברוטו" value={portfolio.exposureLabel} />
            <Metric label="P&L כולל" value={portfolio.pnlLabel} valueClass={pnlClass} />
            <Metric label="P&L יומי" value={portfolio.dailyPnlLabel} valueClass={portfolio.dailyPnl >= 0 ? 'up' : 'down'} />
            <Metric label="ממומש" value={portfolio.realizedPnlLabel} valueClass={portfolio.realizedPnl >= 0 ? 'up' : 'down'} />
            <Metric label="לא ממומש" value={portfolio.unrealizedPnlLabel} valueClass={portfolio.unrealizedPnl >= 0 ? 'up' : 'down'} />
            <Metric label="ירידה מקסימלית" value={portfolio.drawdownLabel} />
          </div>

          <table className="positions-table">
            <caption className="sr-only">פוזיציות פתוחות בתיק הסימולציה</caption>
            <thead>
              <tr><th scope="col">פוזיציה</th><th scope="col">כמות</th><th scope="col">שווי</th><th scope="col">עלות</th><th scope="col">משקל</th><th scope="col">P&amp;L</th></tr>
            </thead>
            <tbody>
              {portfolio.positions.length === 0 ? (
                <tr><td className="cash-only" colSpan="6">מזומן בלבד · אין פוזיציות פתוחות</td></tr>
              ) : portfolio.positions.map((position) => (
                <tr key={position.symbol}>
                  <th scope="row" data-label="פוזיציה"><bdi dir="ltr">{position.symbol}</bdi></th>
                  <td data-label="כמות"><bdi dir="ltr">{formatNumber(position.qty, 2)}</bdi></td>
                  <td data-label="שווי"><bdi dir="ltr">{position.marketValueLabel}</bdi></td>
                  <td data-label="עלות"><bdi dir="ltr">{position.costBasisLabel}</bdi></td>
                  <td data-label="משקל" className="position-weight">
                    <span className="position-weight-track" aria-hidden="true">
                      <span className="position-weight-fill" style={{ width: `${Math.min(100, Math.abs(position.weightPct) * 100)}%` }} />
                    </span>
                    <bdi className="position-pct" dir="ltr">{formatPct(position.weightPct, 0)}</bdi>
                  </td>
                  <td data-label="P&L" className={`position-pnl ${position.unrealizedPnl >= 0 ? 'up' : 'down'}`}><bdi dir="ltr">{formatSignedMoney(position.unrealizedPnl)} · {formatSignedPct(position.pnlPct, 1)}</bdi></td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </Panel>
  );
}

function Metric({
  label,
  value,
  valueClass = '',
}) {
  return (
    <div className="metric">
      <span>{label}</span>
      <bdi dir="ltr"><strong className={valueClass}>{value}</strong></bdi>
    </div>
  );
}

function PortfolioNavChart({ series }) {
  const gradientId = useId();
  const summaryId = useId();
  const svgRef = useRef(null);
  const [activeIndex, setActiveIndex] = useState(null);
  const [showTable, setShowTable] = useState(false);
  const chart = useMemo(() => buildNavChart(series), [series]);
  const activePoint = activeIndex === null ? null : chart.series[activeIndex];
  const activeRenderPoint = activePoint ? chart.pointForSeriesIndex(activeIndex) : null;

  const selectNearestPoint = useCallback((event) => {
    if (!svgRef.current || chart.renderPoints.length === 0) return;
    const rect = svgRef.current.getBoundingClientRect();
    const x = ((event.clientX - rect.left) / rect.width) * chart.width;
    const nearest = chart.renderPoints.reduce((best, point) => (
      Math.abs(point.x - x) < Math.abs(best.x - x) ? point : best
    ), chart.renderPoints[0]);
    setActiveIndex(nearest.seriesIndex);
  }, [chart.renderPoints, chart.width]);

  const handleKeyDown = useCallback((event) => {
    if (chart.series.length === 0 || (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight')) return;
    event.preventDefault();
    setActiveIndex((current) => {
      const fallback = event.key === 'ArrowLeft' ? chart.series.length - 1 : 0;
      const next = current === null ? fallback : current + (event.key === 'ArrowLeft' ? -1 : 1);
      return Math.max(0, Math.min(chart.series.length - 1, next));
    });
  }, [chart.series.length]);

  if (!chart.ready) {
    return (
      <div className="portfolio-nav-chart" tabIndex={0} role="img" aria-label="גרף היסטוריית NAV של התיק">
        <svg className="portfolio-nav-svg" viewBox="0 0 640 126" preserveAspectRatio="none" aria-hidden="true" />
      </div>
    );
  }

  const up = chart.series[chart.series.length - 1].nav >= chart.series[0].nav;
  const stroke = up ? 'var(--go)' : 'var(--bad)';
  const benchmarkStroke = 'var(--accent)';
  const hasTradeMarkers = chart.tradeMarkers.length > 0;
  const benchmarkVisible = chart.benchmarkKnown || chart.benchmarkReady;
  const latest = chart.series[chart.series.length - 1];
  const navValues = chart.series.map((point) => point.nav).filter(Number.isFinite);
  const summary = `NAV נוכחי ${formatMoney(latest.nav)}. שינוי מההתחלה ${formatSignedMoney(latest.nav - chart.initialNav)}. טווח ${formatMoney(Math.min(...navValues))} עד ${formatMoney(Math.max(...navValues))}.`;
  return (
    <div className="portfolio-chart-block">
      <p className="sr-only" id={summaryId}>{summary}</p>
      <div className="portfolio-chart-toolbar">
        <div className="portfolio-nav-legend" aria-label="מקרא גרף">
          <span><i className="legend-line nav" style={{ borderColor: stroke }} />Traidan NAV</span>
          {benchmarkVisible ? (
            <span><i className="legend-line benchmark" />{chart.benchmarkLabel}{chart.benchmarkReady ? '' : ` · ${BENCHMARK_STATUS_LABELS[chart.benchmarkStatus] ?? chart.benchmarkStatus}`}</span>
          ) : null}
          {hasTradeMarkers ? (
            <>
              <span><i className="legend-trade buy" />קנייה/לונג</span>
              <span><i className="legend-trade sell" />מכירה/שורט</span>
            </>
          ) : null}
        </div>
        <button className="chart-table-toggle" type="button" aria-expanded={showTable} onClick={() => setShowTable((value) => !value)}>
          {showTable ? 'הסתרת נתונים' : 'טבלת נתונים'}
        </button>
      </div>
      <div
        className="portfolio-nav-chart"
        tabIndex={0}
        role="img"
        aria-describedby={summaryId}
        aria-label={[
          benchmarkVisible ? 'גרף NAV של התיק מול S&P500.' : 'גרף היסטוריית NAV של התיק.',
          hasTradeMarkers ? 'כולל סמני עסקאות LLM.' : '',
          'אפשר להשתמש בחיצים ימינה ושמאלה כדי לבדוק נקודות.',
        ].filter(Boolean).join(' ')}
        onBlur={() => setActiveIndex(null)}
        onFocus={() => setActiveIndex((current) => current ?? chart.series.length - 1)}
        onKeyDown={handleKeyDown}
      >
        <svg
          ref={svgRef}
          className="portfolio-nav-svg"
          viewBox={`0 0 ${chart.width} ${chart.height}`}
          preserveAspectRatio="none"
          aria-hidden="true"
          onPointerDown={selectNearestPoint}
          onPointerLeave={() => setActiveIndex(null)}
          onPointerMove={selectNearestPoint}
        >
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={stroke} stopOpacity="0.2" />
              <stop offset="100%" stopColor={stroke} stopOpacity="0" />
            </linearGradient>
          </defs>
          <line className="portfolio-nav-baseline" x1={chart.padX} x2={chart.width - chart.padX} y1={chart.baselineY} y2={chart.baselineY} />
          <path d={chart.areaPath} fill={`url(#${gradientId})`} />
          <path className="portfolio-nav-line" d={chart.linePath} stroke={stroke} />
          {chart.benchmarkReady ? <path className="portfolio-benchmark-line" d={chart.benchmarkLinePath} stroke={benchmarkStroke} /> : null}
          {chart.tradeMarkers.map((marker) => <path key={marker.id} className={`portfolio-trade-marker ${marker.side} ${marker.exposure}`} d={tradeMarkerPath(marker)} />)}
          <line className="portfolio-nav-axis" x1={chart.padX} x2={chart.width - chart.padX} y1={chart.axisY} y2={chart.axisY} />
          {chart.ticks.map((tick) => (
            <g className="portfolio-nav-tick" key={`${tick.label}-${tick.x}`}>
              <line x1={tick.x} x2={tick.x} y1={chart.axisY} y2={chart.axisY + 5} />
              <text x={tick.x} y={chart.axisY + 17} textAnchor={tick.anchor}>{tick.label}</text>
            </g>
          ))}
          {activeRenderPoint ? (
            <>
              <line className="portfolio-nav-crosshair" x1={activeRenderPoint.x} x2={activeRenderPoint.x} y1={chart.padY} y2={chart.axisY} />
              <circle className="portfolio-nav-marker" cx={activeRenderPoint.x} cy={activeRenderPoint.y} r="4" fill={stroke} />
              {chart.benchmarkReady && Number.isFinite(activeRenderPoint.benchmarkY) ? <circle className="portfolio-nav-marker benchmark" cx={activeRenderPoint.x} cy={activeRenderPoint.benchmarkY} r="3.5" fill={benchmarkStroke} /> : null}
            </>
          ) : (
            <>
              <circle className="portfolio-nav-marker latest" cx={chart.renderPoints[chart.renderPoints.length - 1].x} cy={chart.renderPoints[chart.renderPoints.length - 1].y} r="3" fill={stroke} />
              {chart.benchmarkReady ? <circle className="portfolio-nav-marker latest benchmark" cx={chart.benchmarkRenderPoints[chart.benchmarkRenderPoints.length - 1].x} cy={chart.benchmarkRenderPoints[chart.benchmarkRenderPoints.length - 1].benchmarkY} r="3" fill={benchmarkStroke} /> : null}
            </>
          )}
        </svg>
        {activePoint && activeRenderPoint ? (
          <PortfolioNavTooltip chart={chart} point={activePoint} renderPoint={activeRenderPoint} hasBenchmark={chart.benchmarkReady} />
        ) : null}
      </div>
      {showTable ? (
        <div className="chart-data-table-wrap">
          <table className="chart-data-table">
            <caption>נתוני NAV לפי נקודת זמן</caption>
            <thead><tr><th scope="col">זמן</th><th scope="col">NAV</th><th scope="col">מזומן</th><th scope="col">חשיפה</th></tr></thead>
            <tbody>
              {chart.series.map((point) => (
                <tr key={point.id}><th scope="row">{formatNavPointTime(point)}</th><td><bdi dir="ltr">{formatMoney(point.nav)}</bdi></td><td><bdi dir="ltr">{formatMoney(point.cash)}</bdi></td><td><bdi dir="ltr">{formatMoney(point.grossExposure)}</bdi></td></tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}

function PortfolioNavTooltip({
  chart,
  point,
  renderPoint,
  hasBenchmark,
}) {
  const delta = point.nav - chart.initialNav;
  const benchmarkNav = Number.isFinite(point.benchmarkNav) ? point.benchmarkNav : null;
  const vsBenchmark = benchmarkNav === null ? null : point.nav - benchmarkNav;
  const rawLeft = (renderPoint.x / chart.width) * 100;
  const tooltipShift = rawLeft < 38 ? '0%' : rawLeft > 62 ? '-100%' : '-50%';
  const left = tooltipShift === '0%' ? '8px' : tooltipShift === '-100%' ? 'calc(100% - 8px)' : `${rawLeft}%`;
  const hasTrades = (point.tradeMarkers ?? []).length > 0;
  return (
    <div
      className="portfolio-nav-tooltip"
      style={{
        left,
        top: hasBenchmark && !hasTrades ? '28px' : '8px',
        '--tooltip-shift': tooltipShift,
      }}
      role="status"
    >
      <strong>{formatNavPointTime(point)}</strong>
      <dl>
        <div><dt>NAV</dt><dd>{formatMoney(point.nav)}</dd></div>
        {hasBenchmark ? (
          <>
            <div><dt>{chart.benchmarkLabel}</dt><dd>{benchmarkNav === null ? 'לא זמין' : formatMoney(benchmarkNav)}</dd></div>
            <div><dt>מול {chart.benchmarkLabel}</dt><dd className={vsBenchmark === null ? 'flat' : vsBenchmark >= 0 ? 'up' : 'down'}>{vsBenchmark === null ? 'לא זמין' : formatSignedMoney(vsBenchmark)}</dd></div>
          </>
        ) : null}
        <div><dt>מההתחלה</dt><dd className={delta >= 0 ? 'up' : 'down'}>{formatSignedMoney(delta)}</dd></div>
        <div><dt>מזומן</dt><dd>{formatMoney(point.cash)}</dd></div>
        <div><dt>חשיפה ברוטו</dt><dd>{formatMoney(point.grossExposure)}</dd></div>
        <div><dt>P&amp;L יומי</dt><dd>{formatSignedMoney(point.dailyPnl)}</dd></div>
        <div><dt>ירידה מקס׳</dt><dd>{formatPct(point.maxDrawdownPct)}</dd></div>
        <div><dt>פוזיציות</dt><dd>{formatNumber(point.positionCount, 0)}</dd></div>
        {hasTrades ? (
          <div className="portfolio-nav-trades"><dt>עסקת LLM</dt><dd>{point.tradeMarkers.map(formatTradeMarker).join('; ')}</dd></div>
        ) : null}
      </dl>
    </div>
  );
}

function tradeMarkerPath(marker) {
  const stackOffset = Math.min(3, marker.markerIndex ?? 0) * 7;
  const size = 5.8;
  if (marker.side === 'sell') {
    const y = marker.y + 9 + stackOffset;
    return `M${marker.x.toFixed(1)} ${y.toFixed(1)} l${size.toFixed(1)} -${(size * 1.55).toFixed(1)} h-${(size * 2).toFixed(1)} Z`;
  }
  const y = marker.y - 9 - stackOffset;
  return `M${marker.x.toFixed(1)} ${y.toFixed(1)} l${size.toFixed(1)} ${(size * 1.55).toFixed(1)} h-${(size * 2).toFixed(1)} Z`;
}

function formatTradeMarker(marker) {
  const notional = Number.isFinite(marker.notional) && marker.notional > 0
    ? ` ${formatMoney(marker.notional)}`
    : '';
  return `${marker.label ?? marker.side} ${marker.symbol}${notional}`;
}

function formatNavPointTime(point) {
  if (point.isBaseline) return 'תחילה';
  const ms = point.timestampMs ?? point.receivedAtMs;
  if (!Number.isFinite(ms)) return 'חותמת זמן לא זמינה';
  return formatDashboardDateTime(ms);
}

import { formatDashboardChartTick } from './dateTime.js';

export const NAV_CHART_RENDER_LIMIT = 420;

export function buildNavChart(rawSeries) {
  const series = Array.isArray(rawSeries)
    ? rawSeries.filter((point) => Number.isFinite(point?.nav))
    : [];
  const width = 640;
  const height = 126;
  const padX = 16;
  const padY = 8;
  const axisY = 96;
  if (series.length < 2) {
    return { ready: false, series, renderPoints: [], width, height, padX, padY, axisY };
  }

  const values = series.flatMap((point) => [
    point.nav,
    Number.isFinite(point.benchmarkNav) ? point.benchmarkNav : null,
  ]).filter(Number.isFinite);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const plotHeight = axisY - padY - 6;
  const xForIndex = (index) => padX + (index / (series.length - 1)) * (width - padX * 2);
  const yForValue = (value) => axisY - 6 - ((value - min) / span) * plotHeight;
  const renderPoints = downsampleNavSeries(series, NAV_CHART_RENDER_LIMIT).map(({ point, seriesIndex }) => ({
    ...point,
    seriesIndex,
    x: xForIndex(seriesIndex),
    y: yForValue(point.nav),
    benchmarkY: Number.isFinite(point.benchmarkNav) ? yForValue(point.benchmarkNav) : null,
  }));
  const benchmarkRenderPoints = renderPoints.filter((point) => (
    Number.isFinite(point.benchmarkNav) && Number.isFinite(point.benchmarkY)
  ));
  const tradeMarkers = renderPoints.flatMap((point) => (
    (point.tradeMarkers ?? []).map((marker, markerIndex) => ({
      ...marker,
      markerIndex,
      seriesIndex: point.seriesIndex,
      x: point.x,
      y: point.y,
    }))
  ));
  const linePath = renderPoints.map((point, index) => `${index === 0 ? 'M' : 'L'}${point.x.toFixed(1)} ${point.y.toFixed(1)}`).join(' ');
  const benchmarkLinePath = benchmarkRenderPoints.map((point, index) => `${index === 0 ? 'M' : 'L'}${point.x.toFixed(1)} ${point.benchmarkY.toFixed(1)}`).join(' ');
  const areaPath = `${linePath} L${renderPoints[renderPoints.length - 1].x.toFixed(1)} ${axisY.toFixed(1)} L${renderPoints[0].x.toFixed(1)} ${axisY.toFixed(1)} Z`;
  const benchmarkPoint = series.find((point) => (
    Number.isFinite(point.benchmarkNav)
    || point.benchmarkSymbol
    || point.benchmarkLabel
    || point.benchmarkStatus
  ));
  return {
    ready: true,
    series,
    renderPoints,
    benchmarkRenderPoints,
    tradeMarkers,
    width,
    height,
    padX,
    padY,
    axisY,
    linePath,
    benchmarkLinePath,
    benchmarkReady: benchmarkRenderPoints.length >= 2,
    benchmarkKnown: Boolean(benchmarkPoint),
    benchmarkLabel: benchmarkPoint?.benchmarkLabel ?? 'S&P500',
    benchmarkSymbol: benchmarkPoint?.benchmarkSymbol ?? 'SPY',
    benchmarkStatus: benchmarkPoint?.benchmarkStatus ?? (
      benchmarkRenderPoints.length >= 2 ? 'live' : 'unavailable'
    ),
    areaPath,
    baselineY: yForValue(series[0].nav),
    initialNav: series[0].nav,
    pointForSeriesIndex: (seriesIndex) => ({
      ...series[seriesIndex],
      seriesIndex,
      x: xForIndex(seriesIndex),
      y: yForValue(series[seriesIndex].nav),
      benchmarkY: Number.isFinite(series[seriesIndex].benchmarkNav)
        ? yForValue(series[seriesIndex].benchmarkNav)
        : null,
    }),
    ticks: navTimelineTicks(series, xForIndex),
  };
}

export function downsampleNavSeries(series, limit) {
  const indexed = series.map((point, seriesIndex) => ({ point, seriesIndex }));
  if (indexed.length <= limit) return indexed;
  const hasBenchmark = indexed.some((item) => Number.isFinite(item.point.benchmarkNav));
  const innerMarked = indexed
    .slice(1, -1)
    .filter((item) => hasTradeMarkers(item.point));
  const markedIndexes = new Set(evenlySampleItems(innerMarked, Math.max(0, limit - 2))
    .map((item) => item.seriesIndex));
  const remainingLimit = Math.max(0, limit - 2 - markedIndexes.size);
  const maxBucketPoints = hasBenchmark ? 4 : 2;
  const bucketCount = Math.floor(remainingLimit / maxBucketPoints);
  const innerCount = indexed.length - 2;
  const result = [indexed[0], ...indexed.filter((item) => markedIndexes.has(item.seriesIndex))];
  const seen = new Set(result.map((item) => item.seriesIndex));
  for (let bucketIndex = 0; bucketIndex < bucketCount; bucketIndex += 1) {
    const start = 1 + Math.floor((bucketIndex / bucketCount) * innerCount);
    const end = 1 + Math.floor(((bucketIndex + 1) / bucketCount) * innerCount);
    const bucket = indexed.slice(start, end);
    if (bucket.length === 0) continue;
    const selected = [
      minByFinite(bucket, (item) => item.point.nav),
      maxByFinite(bucket, (item) => item.point.nav),
      minByFinite(bucket, (item) => item.point.benchmarkNav),
      maxByFinite(bucket, (item) => item.point.benchmarkNav),
    ].filter(Boolean).sort((left, right) => left.seriesIndex - right.seriesIndex);
    for (const point of selected) {
      if (!seen.has(point.seriesIndex)) {
        result.push(point);
        seen.add(point.seriesIndex);
      }
    }
  }
  const last = indexed[indexed.length - 1];
  if (!seen.has(last.seriesIndex)) result.push(last);
  return result.sort((left, right) => left.seriesIndex - right.seriesIndex);
}

function evenlySampleItems(items, limit) {
  if (items.length <= limit) return items;
  if (limit <= 0) return [];
  if (limit === 1) return [items[Math.floor((items.length - 1) / 2)]];
  const selected = [];
  const seen = new Set();
  for (let slot = 0; slot < limit; slot += 1) {
    const index = Math.round((slot / (limit - 1)) * (items.length - 1));
    const item = items[index];
    if (!seen.has(item.seriesIndex)) {
      selected.push(item);
      seen.add(item.seriesIndex);
    }
  }
  return selected;
}

function hasTradeMarkers(point) {
  return Array.isArray(point?.tradeMarkers) && point.tradeMarkers.length > 0;
}

function minByFinite(items, valueForItem) {
  return items.reduce((best, item) => {
    const value = valueForItem(item);
    if (!Number.isFinite(value)) return best;
    if (!best) return item;
    return value < valueForItem(best) ? item : best;
  }, null);
}

function maxByFinite(items, valueForItem) {
  return items.reduce((best, item) => {
    const value = valueForItem(item);
    if (!Number.isFinite(value)) return best;
    if (!best) return item;
    return value > valueForItem(best) ? item : best;
  }, null);
}

export function nearestRenderPoint(renderPoints, seriesIndex) {
  return renderPoints.reduce((best, point) => (
    Math.abs(point.seriesIndex - seriesIndex) < Math.abs(best.seriesIndex - seriesIndex) ? point : best
  ), renderPoints[0]);
}

function navTimelineTicks(series, xForIndex) {
  const indexes = [0, Math.floor((series.length - 1) / 2), series.length - 1];
  return [...new Set(indexes)].map((index, tickIndex, ticks) => ({
    x: xForIndex(index),
    label: formatNavTick(series[index], tickIndex === 1 ? 'אמצע' : 'אחרון'),
    anchor: tickIndex === 0 ? 'start' : tickIndex === ticks.length - 1 ? 'end' : 'middle',
  }));
}

function formatNavTick(point, fallback) {
  if (point.isBaseline) return 'תחילה';
  const ms = point.timestampMs ?? point.receivedAtMs;
  if (!Number.isFinite(ms)) return fallback;
  return formatDashboardChartTick(ms) ?? fallback;
}

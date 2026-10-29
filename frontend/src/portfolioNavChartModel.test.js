import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildNavChart,
  downsampleNavSeries,
  NAV_CHART_RENDER_LIMIT,
} from './portfolioNavChartModel.js';

test('portfolio nav chart downsamples large histories while preserving boundary and bucket extremes', () => {
  const series = Array.from({ length: 1_500 }, (_, index) => ({
    nav: 10_000 + index * 0.1,
    timestampMs: 1_700_000_000_000 + index * 60_000,
  }));
  series[740] = { ...series[740], nav: 8_900 };
  series[741] = { ...series[741], nav: 11_100 };

  const sampled = downsampleNavSeries(series, NAV_CHART_RENDER_LIMIT);
  const chart = buildNavChart(series);

  assert.ok(sampled.length <= NAV_CHART_RENDER_LIMIT);
  assert.equal(sampled[0].seriesIndex, 0);
  assert.equal(sampled[sampled.length - 1].seriesIndex, series.length - 1);
  assert.equal(sampled.filter((item) => item.seriesIndex === 740).length, 1);
  assert.equal(sampled.filter((item) => item.seriesIndex === 741).length, 1);

  assert.equal(chart.ready, true);
  assert.ok(chart.renderPoints.length <= NAV_CHART_RENDER_LIMIT);
  assert.equal(chart.renderPoints[0].seriesIndex, 0);
  assert.equal(chart.renderPoints[chart.renderPoints.length - 1].seriesIndex, series.length - 1);
  assert.equal(chart.pointForSeriesIndex(740).seriesIndex, 740);
  assert.equal(chart.pointForSeriesIndex(740).nav, 8_900);
});

test('portfolio nav chart keeps finite non-positive NAV points', () => {
  const series = [
    { nav: 10_000, timestampMs: 1_700_000_000_000 },
    { nav: 0, timestampMs: 1_700_086_400_000 },
    { nav: -25, timestampMs: 1_700_172_800_000 },
  ];

  const chart = buildNavChart(series);

  assert.equal(chart.ready, true);
  assert.deepEqual(chart.series.map((point) => point.nav), [10_000, 0, -25]);
  assert.equal(chart.renderPoints[chart.renderPoints.length - 1].nav, -25);
  assert.equal(chart.initialNav, 10_000);
});

test('portfolio nav chart renders normalized S&P500 comparison line', () => {
  const series = [
    {
      nav: 10_000,
      benchmarkNav: 10_000,
      benchmarkSymbol: 'SPY',
      benchmarkLabel: 'S&P500',
      timestampMs: 1_700_000_000_000,
    },
    {
      nav: 10_100,
      benchmarkNav: 10_200,
      benchmarkSymbol: 'SPY',
      benchmarkLabel: 'S&P500',
      timestampMs: 1_700_086_400_000,
    },
    {
      nav: 10_050,
      benchmarkNav: 10_300,
      benchmarkSymbol: 'SPY',
      benchmarkLabel: 'S&P500',
      timestampMs: 1_700_172_800_000,
    },
  ];

  const chart = buildNavChart(series);

  assert.equal(chart.ready, true);
  assert.equal(chart.benchmarkReady, true);
  assert.equal(chart.benchmarkLabel, 'S&P500');
  assert.equal(chart.benchmarkSymbol, 'SPY');
  assert.match(chart.benchmarkLinePath, /^M/);
  assert.equal(chart.benchmarkRenderPoints.length, 3);
  assert.equal(chart.pointForSeriesIndex(1).benchmarkNav, 10_200);
  assert.equal(Number.isFinite(chart.pointForSeriesIndex(1).benchmarkY), true);
});

test('portfolio nav chart does not render benchmark metadata without normalized values', () => {
  const series = [
    {
      nav: 10_000,
      timestampMs: 1_700_000_000_000,
    },
    {
      nav: 10_100,
      benchmarkSymbol: 'SPY',
      benchmarkLabel: 'S&P500',
      benchmarkPrice: 505,
      timestampMs: 1_700_086_400_000,
    },
    {
      nav: 10_050,
      benchmarkSymbol: 'SPY',
      benchmarkLabel: 'S&P500',
      benchmarkPrice: 510,
      timestampMs: 1_700_172_800_000,
    },
  ];

  const chart = buildNavChart(series);

  assert.equal(chart.ready, true);
  assert.equal(chart.benchmarkReady, false);
  assert.equal(chart.benchmarkLinePath, '');
  assert.equal(chart.benchmarkRenderPoints.length, 0);
});

test('portfolio nav chart downsamples benchmark histories while preserving benchmark extremes', () => {
  const series = Array.from({ length: 1_500 }, (_, index) => ({
    nav: 10_000 + index * 0.1,
    benchmarkNav: 10_000 + index * 0.05,
    timestampMs: 1_700_000_000_000 + index * 60_000,
  }));
  series[640] = { ...series[640], benchmarkNav: 8_700 };
  series[641] = { ...series[641], benchmarkNav: 11_300 };

  const sampled = downsampleNavSeries(series, NAV_CHART_RENDER_LIMIT);
  const chart = buildNavChart(series);

  assert.ok(sampled.length <= NAV_CHART_RENDER_LIMIT);
  assert.equal(sampled.filter((item) => item.seriesIndex === 640).length, 1);
  assert.equal(sampled.filter((item) => item.seriesIndex === 641).length, 1);
  assert.equal(chart.benchmarkReady, true);
  assert.ok(chart.benchmarkRenderPoints.length <= NAV_CHART_RENDER_LIMIT);
});

test('portfolio nav chart keeps LLM trade marker coordinates through downsampling', () => {
  const series = Array.from({ length: 1_500 }, (_, index) => ({
    nav: 10_000 + index * 0.1,
    timestampMs: 1_700_000_000_000 + index * 60_000,
  }));
  series[777] = {
    ...series[777],
    tradeMarkers: [{
      id: 'trade-cycle-1-spy',
      label: 'קניית לונג',
      symbol: 'SPY',
      side: 'buy',
      exposure: 'long',
      notional: 500,
    }],
  };

  const sampled = downsampleNavSeries(series, NAV_CHART_RENDER_LIMIT);
  const chart = buildNavChart(series);

  assert.equal(sampled.filter((item) => item.seriesIndex === 777).length, 1);
  assert.equal(chart.tradeMarkers.length, 1);
  assert.equal(chart.tradeMarkers[0].seriesIndex, 777);
  assert.equal(chart.tradeMarkers[0].label, 'קניית לונג');
  assert.equal(Number.isFinite(chart.tradeMarkers[0].x), true);
  assert.equal(Number.isFinite(chart.tradeMarkers[0].y), true);
});

test('portfolio nav chart caps dense LLM trade markers to the render limit', () => {
  const series = Array.from({ length: 1_500 }, (_, index) => ({
    nav: 10_000 + index * 0.1,
    timestampMs: 1_700_000_000_000 + index * 60_000,
    tradeMarkers: index > 0 && index < 1_499 ? [{
      id: `trade-${index}`,
      label: 'קניית לונג',
      symbol: 'SPY',
      side: 'buy',
      exposure: 'long',
      notional: 25,
    }] : undefined,
  }));

  const sampled = downsampleNavSeries(series, NAV_CHART_RENDER_LIMIT);
  const chart = buildNavChart(series);

  assert.ok(sampled.length <= NAV_CHART_RENDER_LIMIT);
  assert.ok(chart.renderPoints.length <= NAV_CHART_RENDER_LIMIT);
  assert.equal(sampled[0].seriesIndex, 0);
  assert.equal(sampled[sampled.length - 1].seriesIndex, series.length - 1);
  assert.ok(chart.tradeMarkers.length > 0);
  assert.ok(chart.tradeMarkers.length <= NAV_CHART_RENDER_LIMIT - 2);
});

test('portfolio nav chart tick labels render in Israel time', () => {
  const series = [
    { nav: 10_000, timestampMs: Date.UTC(2026, 4, 28, 14, 30) },
    { nav: 10_025, timestampMs: Date.UTC(2026, 4, 28, 15, 30) },
    { nav: 10_050, timestampMs: Date.UTC(2026, 4, 29, 14, 30) },
  ];

  const chart = buildNavChart(series);

  assert.deepEqual(chart.ticks.map((tick) => tick.label), [
    '28.05 17:30',
    '28.05 18:30',
    '29.05 17:30',
  ]);
});

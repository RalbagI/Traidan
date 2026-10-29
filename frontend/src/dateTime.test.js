import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DASHBOARD_TIME_ZONE,
  formatDashboardChartTick,
  formatDashboardClockTime,
  formatDashboardDateTime,
  formatDashboardTimestamp,
} from './dateTime.js';

test('dashboard clock renders UTC timestamps in Israel time', () => {
  assert.equal(DASHBOARD_TIME_ZONE, 'Asia/Jerusalem');
  assert.equal(formatDashboardClockTime(Date.UTC(2026, 4, 28, 14, 30)), '17:30');
  assert.equal(formatDashboardClockTime(Date.UTC(2026, 0, 1, 12, 0)), '14:00');
});

test('dashboard date labels use Israel time for chart and tooltip displays', () => {
  const timestamp = Date.UTC(2026, 4, 28, 14, 30);

  assert.equal(formatDashboardChartTick(timestamp), '28.05 17:30');
  assert.equal(formatDashboardDateTime(timestamp), '28 במאי, 17:30');
});

test('dashboard date labels tolerate missing timestamps', () => {
  assert.equal(formatDashboardClockTime(null), '--:--');
  assert.equal(formatDashboardChartTick(Number.NaN), null);
  assert.equal(formatDashboardDateTime(undefined), 'חותמת זמן לא זמינה');
});

test('dashboard timestamp labels parse backend ISO strings in Israel time', () => {
  assert.equal(formatDashboardTimestamp('2026-05-28T14:30:00Z', 'לא ידוע'), '28 במאי, 17:30');
  assert.equal(formatDashboardTimestamp('not-a-date', 'לא ידוע'), 'לא ידוע');
});

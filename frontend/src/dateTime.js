export const DASHBOARD_TIME_ZONE = 'Asia/Jerusalem';
export const DASHBOARD_TIME_ZONE_LABEL = 'שעון ישראל';

const CLOCK_FORMAT = new Intl.DateTimeFormat('he-IL', {
  timeZone: DASHBOARD_TIME_ZONE,
  hour: '2-digit',
  minute: '2-digit',
  hourCycle: 'h23',
});

const CHART_TICK_FORMAT = new Intl.DateTimeFormat('he-IL', {
  timeZone: DASHBOARD_TIME_ZONE,
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  hourCycle: 'h23',
});

const DATE_TIME_FORMAT = new Intl.DateTimeFormat('he-IL', {
  timeZone: DASHBOARD_TIME_ZONE,
  month: 'long',
  day: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
  hourCycle: 'h23',
});

export function formatDashboardClockTime(ms) {
  const parts = dateParts(CLOCK_FORMAT, ms);
  if (!parts) return '--:--';
  return `${parts.hour}:${parts.minute}`;
}

export function formatDashboardChartTick(ms) {
  const parts = dateParts(CHART_TICK_FORMAT, ms);
  if (!parts) return null;
  return `${parts.day}.${parts.month} ${parts.hour}:${parts.minute}`;
}

export function formatDashboardDateTime(ms) {
  const parts = dateParts(DATE_TIME_FORMAT, ms);
  if (!parts) return 'חותמת זמן לא זמינה';
  return `${Number(parts.day)} ב${parts.month}, ${parts.hour}:${parts.minute}`;
}

export function formatDashboardTimestamp(value, fallback = 'חותמת זמן לא זמינה') {
  const ms = timestampMs(value);
  if (ms === null) return fallback;
  return formatDashboardDateTime(ms);
}

function timestampMs(value) {
  if (Number.isFinite(value)) return value;
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  const parsed = Date.parse(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function dateParts(formatter, ms) {
  if (!Number.isFinite(ms)) return null;
  const date = new Date(ms);
  if (Number.isNaN(date.getTime())) return null;
  return Object.fromEntries(
    formatter
      .formatToParts(date)
      .filter((part) => part.type !== 'literal')
      .map((part) => [part.type, part.value]),
  );
}

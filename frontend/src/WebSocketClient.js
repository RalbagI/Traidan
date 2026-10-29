import { parseEngineMessage } from './messages.js';

const DEFAULT_RECONNECT_DELAY_MS = 2000;
const DEFAULT_MAX_RECONNECT_DELAY_MS = 30_000;
const DEFAULT_MIN_RECONNECT_DELAY_MS = 250;

export default class WebSocketClient {
  constructor(url, options = {}) {
    const normalized = typeof options === 'function' ? { onMessage: options } : options;
    this._url = url;
    this._onMessage = normalized.onMessage ?? (() => {});
    this._onStatusChange = normalized.onStatusChange ?? (() => {});
    this._logger = normalized.logger ?? console;
    this._reconnectDelayMs = normalized.reconnectDelayMs ?? DEFAULT_RECONNECT_DELAY_MS;
    this._maxReconnectDelayMs = normalized.maxReconnectDelayMs ?? DEFAULT_MAX_RECONNECT_DELAY_MS;
    this._minReconnectDelayMs = normalized.minReconnectDelayMs ?? DEFAULT_MIN_RECONNECT_DELAY_MS;
    this._random = normalized.random ?? Math.random;
    this._onReconnectSchedule = normalized.onReconnectSchedule ?? (() => {});
    this._WebSocket = normalized.WebSocket ?? WebSocket;
    this._reconnectAttempt = 0;
    this._shouldReconnect = true;
    this._reconnectTimer = null;
    this._ws = null;
    this._connect();
  }

  _connect() {
    if (!this._shouldReconnect) {
      return;
    }

    this._setStatus('connecting');
    this._ws = new this._WebSocket(this._url);
    this._ws.onopen = () => {
      this._logger.info?.('[WS] connected to Rust engine');
      this._reconnectAttempt = 0;
      this._onReconnectSchedule(null);
      this._setStatus('connected');
    };
    this._ws.onclose = () => {
      if (!this._shouldReconnect || this._reconnectTimer) {
        return;
      }
      const delayMs = this._nextReconnectDelayMs();
      this._logger.warn?.(`[WS] disconnected; reconnecting in ${formatDelay(delayMs)}`);
      this._setStatus('disconnected');
      this._scheduleReconnect(delayMs);
    };
    this._ws.onerror = (error) => {
      if (this._shouldReconnect) {
        this._logger.error?.('[WS] error', error);
      }
    };
    this._ws.onmessage = (event) => {
      const message = parseEngineMessage(event.data, { logger: this._logger });
      if (message) {
        this._onMessage(message);
      }
    };
  }

  _scheduleReconnect(delayMs) {
    if (!this._shouldReconnect || this._reconnectTimer) {
      return;
    }

    this._onReconnectSchedule(delayMs);
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this._connect();
    }, delayMs);
  }

  _nextReconnectDelayMs() {
    const capped = Math.min(
      this._reconnectDelayMs * (2 ** this._reconnectAttempt),
      this._maxReconnectDelayMs,
    );
    this._reconnectAttempt += 1;
    const jittered = Math.round(capped * clampRandom(this._random()));
    return Math.max(this._minReconnectDelayMs, jittered);
  }

  _setStatus(status) {
    this._onStatusChange(status);
  }

  send(payload) {
    if (this._ws?.readyState === this._WebSocket.OPEN) {
      this._ws.send(JSON.stringify(payload));
    }
  }

  close() {
    this._shouldReconnect = false;
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this._ws) {
      const socket = this._ws;
      this._ws = null;
      socket.onclose = null;
      socket.onmessage = null;
      socket.onerror = null;
      if (socket.readyState === this._WebSocket.CONNECTING) {
        socket.onopen = () => {
          socket.onopen = null;
          socket.close();
        };
        return;
      }
      socket.onopen = null;
      if (socket.readyState !== this._WebSocket.CLOSED) {
        socket.close();
      }
    }
  }
}

function clampRandom(value) {
  return Number.isFinite(value) ? Math.min(1, Math.max(0, value)) : 1;
}

function formatDelay(delayMs) {
  const seconds = delayMs / 1_000;
  return `${seconds % 1 === 0 ? seconds.toFixed(0) : seconds.toFixed(1)} s`;
}

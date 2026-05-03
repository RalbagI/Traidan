/**
 * WebSocket client — maintains a persistent connection to the Rust engine's
 * JSON broadcast endpoint and dispatches messages to a caller-provided handler.
 * See architecture.md §7 — Live Operations UI.
 */
export default class WebSocketClient {
  constructor(url, onMessage) {
    this._url = url;
    this._onMessage = onMessage;
    this._connect();
  }

  _connect() {
    this._ws = new WebSocket(this._url);

    this._ws.onopen = () => console.log('[WS] connected to Rust engine');
    this._ws.onclose = () => {
      console.warn('[WS] disconnected — reconnecting in 2 s');
      setTimeout(() => this._connect(), 2000);
    };
    this._ws.onerror = (e) => console.error('[WS] error', e);
    this._ws.onmessage = (event) => {
      try {
        this._onMessage(JSON.parse(event.data));
      } catch (e) {
        console.error('[WS] parse error', e);
      }
    };
  }

  send(payload) {
    if (this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(payload));
    }
  }

  close() {
    this._ws.onclose = null;
    this._ws.close();
  }
}

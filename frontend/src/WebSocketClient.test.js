import assert from 'node:assert/strict';
import test from 'node:test';

import WebSocketClient from './WebSocketClient.js';

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSED = 3;
  static instances = [];

  constructor(url) {
    this.url = url;
    this.readyState = FakeWebSocket.CONNECTING;
    this.closeCalls = 0;
    FakeWebSocket.instances.push(this);
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.onopen?.();
  }

  fail(error) {
    this.onerror?.(error);
  }

  close() {
    this.closeCalls += 1;
    this.readyState = FakeWebSocket.CLOSED;
  }
}

function createClient(logger = {}) {
  FakeWebSocket.instances = [];
  const client = new WebSocketClient('ws://localhost:8080/stream', {
    WebSocket: FakeWebSocket,
    logger,
  });
  return { client, socket: FakeWebSocket.instances[0] };
}

test('intentional teardown waits for a connecting socket before closing it', () => {
  const errors = [];
  const { client, socket } = createClient({ error: (...args) => errors.push(args) });

  client.close();

  assert.equal(socket.closeCalls, 0);
  assert.equal(socket.onerror, null);
  assert.equal(socket.onmessage, null);

  socket.open();

  assert.equal(socket.closeCalls, 1);
  assert.deepEqual(errors, []);
});

test('active transport failures are still logged', () => {
  const errors = [];
  const { client, socket } = createClient({ error: (...args) => errors.push(args) });
  const failure = new Error('connection failed');

  socket.open();
  socket.fail(failure);

  assert.deepEqual(errors, [['[WS] error', failure]]);
  client.close();
  assert.equal(socket.closeCalls, 1);
});

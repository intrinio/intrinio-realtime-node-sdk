'use strict'

const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const test = require('node:test');
const https = require('node:https');

const wsPath = require.resolve('ws');
const originalWebSocket = require(wsPath);
const originalHttpsGet = https.get;
const { RealtimeClient } = require('../index');

function waitFor(predicate, message, timeoutMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    const check = () => {
      if (predicate()) {
        resolve();
        return;
      }
      if (Date.now() >= deadline) {
        reject(new Error(message));
        return;
      }
      setTimeout(check, 5);
    };
    check();
  });
}

function createAuthGet(results, calls) {
  return (_url, _options, callback) => {
    const request = new EventEmitter();
    request.destroyed = false;
    request.setTimeout = (_milliseconds, timeoutCallback) => {
      request.timeoutCallback = timeoutCallback;
    };
    request.destroy = error => {
      if (request.destroyed) return;
      request.destroyed = true;
      setImmediate(() => request.emit('error', error || new Error('destroyed')));
    };

    const result = results.length > 1 ? results.shift() : results[0];
    calls.push({ result, request });
    if (result && result.hang) return request;

    setImmediate(() => {
      if (request.destroyed) return;
      const response = new EventEmitter();
      response.statusCode = result.status;
      response.resume = () => {};
      callback(response);
      if (result.status === 200) {
        for (const chunk of result.chunks || ['token']) response.emit('data', Buffer.from(chunk));
        response.emit('end');
      }
    });
    return request;
  };
}

function createFakeWebSocket(scenarios) {
  return class FakeWebSocket extends EventEmitter {
    static instances = [];

    constructor(url, options) {
      super();
      this.url = url;
      this.options = options;
      this.bufferedAmount = 0;
      this.sent = [];
      this.terminated = false;
      this.closed = false;
      this.readyState = 0;
      this.scenario = scenarios.length > 1 ? scenarios.shift() : scenarios[0];
      FakeWebSocket.instances.push(this);

      if (this.scenario.open) setImmediate(() => {
        this.readyState = 1;
        this.emit('open');
      });
      if (this.scenario.error) setImmediate(() => this.emit('error', new Error(this.scenario.error)));
    }

    send(message) {
      if (this.scenario.failSend) {
        this.scenario.failSend = false;
        throw new Error('send failed');
      }
      this.sent.push(Uint8Array.from(message));
    }

    close(code = 1000) {
      if (this.closed) return;
      this.closed = true;
      this.readyState = 2;
      if (this.scenario.silentClose) return;
      setImmediate(() => {
        this.readyState = 3;
        this.emit('close', code, Buffer.alloc(0));
      });
    }

    terminate() {
      this.terminated = true;
      this.readyState = 3;
      if (this.closed) return;
      this.closed = true;
      setImmediate(() => this.emit('close', 1006, Buffer.alloc(0)));
    }
  };
}

function installFakes(t, authResults, socketScenarios) {
  const authCalls = [];
  const FakeWebSocket = createFakeWebSocket(socketScenarios);
  https.get = createAuthGet(authResults, authCalls);
  require.cache[wsPath].exports = FakeWebSocket;

  t.after(() => {
    https.get = originalHttpsGet;
    require.cache[wsPath].exports = originalWebSocket;
  });
  return { authCalls, FakeWebSocket };
}

function createClient(onTrade = () => {}, onQuote = () => {}) {
  const client = new RealtimeClient('key', onTrade, onQuote, { provider: 'IEX' });
  client._selfHealBackoffs = [1, 2, 5];
  client._connectTimeoutMs = 20;
  return client;
}

function buildMessage(type, length) {
  const message = new Uint8Array(length);
  message[0] = type;
  message[1] = length;
  return message;
}

function buildFrame(messages) {
  let size = 1;
  for (const message of messages) size += message.length;
  const frame = new Uint8Array(size);
  frame[0] = messages.length;
  let offset = 1;
  for (const message of messages) {
    frame.set(message, offset);
    offset += message.length;
  }
  return frame;
}

test('drains the trade queue before the quote queue and contains callback failures', async t => {
  installFakes(t, [{ status: 200 }], [{ open: true }]);
  const processed = [];
  const client = createClient(
      event => {
        processed.push(event.kind);
        throw new Error('consumer failure');
      },
      event => processed.push(event.kind));
  t.after(() => client.stop());
  await waitFor(() => client._isReady, 'client did not become ready');

  client._parseTrade = () => ({ kind: 'trade' });
  client._parseQuote = () => ({ kind: 'quote' });
  client._parseSocketMessage(buildFrame([buildMessage(1, 23), buildMessage(0, 27)]));

  await waitFor(() => processed.length === 2, 'queued events were not processed');
  assert.deepEqual(processed, ['trade', 'quote']);
  assert.equal(client._tradeQueue._items[0], undefined);
  assert.equal(client._quoteQueue._items[0], undefined);
});

test('reconnects after a clean remote close, rejoins, and ignores stale socket events', async t => {
  const { FakeWebSocket } = installFakes(t, [{ status: 200 }], [{ open: true }, { open: true }]);
  const client = createClient();
  t.after(() => client.stop());
  await waitFor(() => client._isReady, 'initial connection did not open');
  await client.join('AAPL');

  const first = FakeWebSocket.instances[0];
  first.emit('message', 'slow down', false);
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(FakeWebSocket.instances.length, 1);
  assert.equal(client._isReady, true);
  first.emit('close', 1000, Buffer.alloc(0));
  const joinWhileDown = client.join('MSFT');
  await waitFor(() => FakeWebSocket.instances.length === 2 && client._isReady, 'clean close did not reconnect');
  await joinWhileDown;

  const second = FakeWebSocket.instances[1];
  assert.equal(second.sent.filter(message => message[0] === 74).length, 2);
  first.emit('close', 1006, Buffer.alloc(0));
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(FakeWebSocket.instances.length, 2);
  assert.equal(client._isReady, true);
  assert.ok(second.options.headers['Client-Information']);
  assert.equal(second.options.headers.UseNewEquitiesFormat, 'v2');
});

test('recovers from a websocket error without starting competing reconnect workers', async t => {
  const { FakeWebSocket } = installFakes(t, [{ status: 200 }], [{ open: true }, { open: true }]);
  const client = createClient();
  t.after(() => client.stop());
  await waitFor(() => client._isReady, 'initial connection did not open');

  const first = FakeWebSocket.instances[0];
  first.emit('error', new Error('connection reset'));
  first.emit('close', 1006, Buffer.alloc(0));
  first.emit('close', 1006, Buffer.alloc(0));
  await waitFor(() => FakeWebSocket.instances.length === 2 && client._isReady, 'socket error did not recover');
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(FakeWebSocket.instances.length, 2);
});

test('retries auth failures and abandons a hung websocket handshake', async t => {
  const { authCalls, FakeWebSocket } = installFakes(
      t,
      [{ status: 503 }, { status: 200, chunks: ['to', 'ken'] }],
      [{ hang: true }, { open: true }]);
  const client = createClient();
  t.after(() => client.stop());

  await waitFor(() => client._isReady, 'client did not recover from auth and handshake failures');
  assert.equal(authCalls.length, 2);
  assert.equal(FakeWebSocket.instances.length, 2);
  assert.equal(FakeWebSocket.instances[0].terminated, true);
  assert.match(FakeWebSocket.instances[1].url, /token=token/);
});

test('continues reconnecting when a channel rejoin send fails', async t => {
  const { FakeWebSocket } = installFakes(
      t,
      [{ status: 200 }],
      [{ open: true }, { open: true, failSend: true }, { open: true }]);
  const client = createClient();
  t.after(() => client.stop());
  await waitFor(() => client._isReady, 'initial connection did not open');
  await client.join('AAPL');

  FakeWebSocket.instances[0].emit('close', 1006, Buffer.alloc(0));
  await waitFor(
      () => FakeWebSocket.instances.length === 3 && client._isReady,
      'client did not recover after a rejoin send failure');
  assert.equal(FakeWebSocket.instances[1].terminated, true);
  assert.equal(FakeWebSocket.instances[2].sent.filter(message => message[0] === 74).length, 1);
});

test('stop cancels reconnect backoff', async t => {
  const { authCalls, FakeWebSocket } = installFakes(t, [{ status: 503 }, { hang: true }], [{ open: true }]);
  const client = createClient();
  client._selfHealBackoffs = [10000];

  await waitFor(() => authCalls.length === 1 && client._cancelBackoff !== null, 'client did not enter auth retry backoff');
  const started = Date.now();
  await client.stop();
  assert.ok(Date.now() - started < 500, 'stop should interrupt reconnect backoff');
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(authCalls.length, 1);
  assert.equal(FakeWebSocket.instances.length, 0);
});

test('stop aborts a hung auth request', async t => {
  const { authCalls, FakeWebSocket } = installFakes(t, [{ hang: true }], [{ open: true }]);
  const client = createClient();
  await waitFor(() => authCalls.length === 1, 'auth request did not start');

  const started = Date.now();
  await client.stop();
  assert.ok(Date.now() - started < 500, 'stop should interrupt hung auth');
  assert.equal(authCalls[0].request.destroyed, true);
  assert.equal(FakeWebSocket.instances.length, 0);
});

test('stop cancels a hung websocket handshake', async t => {
  const { FakeWebSocket } = installFakes(t, [{ status: 200 }], [{ hang: true }]);
  const client = createClient();
  await waitFor(() => FakeWebSocket.instances.length === 1, 'websocket attempt did not start');

  const started = Date.now();
  await client.stop();
  assert.ok(Date.now() - started < 500, 'stop should interrupt a hung handshake');
  assert.equal(FakeWebSocket.instances[0].terminated, true);
});

test('drops new quotes once the quote queue is at its cap', async t => {
  installFakes(t, [{ status: 200 }], [{ open: true }]);
  const client = createClient();
  client._quoteQueueCap = 2;
  t.after(() => client.stop());
  await waitFor(() => client._isReady, 'client did not become ready');

  let parsedQuotes = 0;
  client._parseQuote = () => {
    parsedQuotes++;
    return { kind: 'quote' };
  };
  client._parseTrade = () => ({ kind: 'trade' });
  client._parseSocketMessage(buildFrame([
    buildMessage(1, 23),
    buildMessage(2, 23),
    buildMessage(1, 23),
    buildMessage(0, 27)
  ]));

  assert.equal(client._quoteQueue.length, 2);
  assert.equal(parsedQuotes, 2);
  assert.equal(client.getDroppedQuoteCount(), 1);
  assert.equal(client._tradeQueue.length, 1);

  await waitFor(() => client._quoteQueue.length === 0, 'quote queue did not drain');
  client._parseSocketMessage(buildFrame([buildMessage(1, 23)]));
  assert.equal(client._quoteQueue.length, 1);
  assert.equal(client.getDroppedQuoteCount(), 1);
});

test('drops a short frame and still delivers a later complete message', async t => {
  installFakes(t, [{ status: 200 }], [{ open: true }]);
  const trades = [];
  const quotes = [];
  const client = createClient(trade => trades.push(trade), quote => quotes.push(quote));
  t.after(() => client.stop());
  await waitFor(() => client._isReady, 'client did not become ready');

  client._parseSocketMessage(Uint8Array.from([1, 0, 2]));
  client._parseSocketMessage(buildFrame([
    buildMessage(0, 2),
    buildMessage(0, 27),
    buildMessage(1, 23)
  ]));

  await waitFor(() => trades.length === 1 && quotes.length === 1, 'valid messages were not delivered');
  assert.equal(trades[0].Type, 'Trade');
  assert.equal(quotes[0].Type, 'Ask');
  assert.equal(client._tradeQueue.length, 0);
  assert.equal(client._quoteQueue.length, 0);
});

test('retries a dropped session once immediately, then applies backoff', async t => {
  const warnings = [];
  const infos = [];
  const originalWarn = console.warn;
  const originalInfo = console.info;
  console.warn = (...args) => warnings.push(args.join(' '));
  console.info = (...args) => infos.push(args.join(' '));
  t.after(() => {
    console.warn = originalWarn;
    console.info = originalInfo;
  });

  const { FakeWebSocket } = installFakes(t, [{ status: 200 }], [{ open: true }]);
  const client = createClient();
  client._selfHealBackoffs = [80, 200, 1000];
  client._stableSessionMs = 60000;
  t.after(() => client.stop());
  await waitFor(() => client._isReady, 'initial connection did not open');

  let started = Date.now();
  FakeWebSocket.instances[0].emit('close', 1000, Buffer.alloc(0));
  await waitFor(() => FakeWebSocket.instances.length === 2 && client._isReady, 'immediate retry did not reconnect');
  assert.ok(Date.now() - started < 40, 'first drop after open should retry immediately');

  client._stableSessionMs = 40;
  started = Date.now();
  FakeWebSocket.instances[1].emit('close', 1000, Buffer.alloc(0));
  await waitFor(() => FakeWebSocket.instances.length === 3 && client._isReady, 'backoff retry did not reconnect');
  const climbed = Date.now() - started;
  assert.ok(climbed >= 60, `second drop should back off, took ${climbed}ms`);

  await new Promise(resolve => setTimeout(resolve, 60));
  started = Date.now();
  FakeWebSocket.instances[2].emit('close', 1000, Buffer.alloc(0));
  await waitFor(() => FakeWebSocket.instances.length === 4 && client._isReady, 'stable session did not retry immediately');
  assert.ok(Date.now() - started < 40, 'a session that stayed up should retry immediately again');

  assert.equal(warnings.some(line => line.includes('Connection attempt failed')), false);
  assert.ok(infos.some(line => line.includes('Websocket closed')));
});

test('stop waits for a graceful close and terminates only if it is not acknowledged', async t => {
  const graceful = installFakes(t, [{ status: 200 }], [{ open: true }]);
  const gracefulClient = createClient();
  t.after(() => gracefulClient.stop());
  await waitFor(() => gracefulClient._isReady, 'graceful client did not become ready');
  await gracefulClient.join('AAPL');
  await gracefulClient.stop();
  assert.equal(graceful.FakeWebSocket.instances[0].closed, true);
  assert.equal(graceful.FakeWebSocket.instances[0].terminated, false);
  assert.ok(graceful.FakeWebSocket.instances[0].sent.some(message => message[0] === 76));

  const silent = installFakes(t, [{ status: 200 }], [{ open: true, silentClose: true }]);
  const silentClient = createClient();
  silentClient._closeAckTimeoutMs = 30;
  t.after(() => silentClient.stop());
  await waitFor(() => silentClient._isReady, 'silent client did not become ready');
  const started = Date.now();
  await silentClient.stop();
  const elapsed = Date.now() - started;
  assert.ok(elapsed >= 20, 'stop should wait for the close acknowledgement');
  assert.ok(elapsed < 500, 'stop should not wait for the full drain timeout');
  assert.equal(silent.FakeWebSocket.instances[0].terminated, true);
});

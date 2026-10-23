'use strict'

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf8");
const unicodeDecoder = new TextDecoder("utf-16le");

const SELF_HEAL_BACKOFFS = [10000, 30000, 60000, 300000, 600000];
const CONNECT_TIMEOUT_MS = 30000;
const TOKEN_MAX_AGE_MS = 23 * 60 * 60 * 1000;
const PROCESSING_BATCH_SIZE = 1000;
const QUOTE_QUEUE_CAP = 500000;
const STOP_DRAIN_TIMEOUT_MS = 5000;
const CLOSE_ACK_TIMEOUT_MS = 1000;
const STABLE_SESSION_MS = 1000;
const SOCKET_CLOSED = 3;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const CLIENT_INFO_HEADER_KEY = "Client-Information";
const CLIENT_INFO_HEADER_VALUE = "IntrinioRealtimeNodeSDKv5.8";
const MESSAGE_VERSION_HEADER_KEY = "UseNewEquitiesFormat";
const MESSAGE_VERSION_HEADER_VALUE = "v2";
const EVENT_BUFFER_SIZE = 100;

class EventQueue {
  constructor() {
    this._items = [];
    this._head = 0;
  }

  get length() {
    return this._items.length - this._head;
  }

  enqueue(item) {
    this._items.push(item);
  }

  dequeue() {
    if (this._head >= this._items.length)
      return undefined;

    const index = this._head++;
    const item = this._items[index];
    this._items[index] = undefined;
    if (this._head >= 1024 && this._head * 2 >= this._items.length) {
      this._items = this._items.slice(this._head);
      this._head = 0;
    }
    return item;
  }

  clear() {
    this._items.length = 0;
    this._head = 0;
  }
}

function scheduleWork(callback) {
  if (typeof setImmediate === "function")
    setImmediate(callback);
  else
    setTimeout(callback, 0);
}

function readString(bytes, startPos, endPos) {
  if (startPos < 0) startPos = 0;
  else if (startPos >= bytes.length) return '';
  else startPos |= 0;
  if (endPos === undefined || endPos > bytes.length) endPos = bytes.length;
  else endPos |= 0;
  if (endPos <= startPos) return '';
  const chunk = bytes.slice(startPos, endPos);
  return decoder.decode(chunk);
}

function readUnicodeString(bytes, startPos, endPos) {
  if (startPos < 0) startPos = 0;
  else if (startPos >= bytes.length) return '';
  else startPos |= 0;
  if (endPos === undefined || endPos > bytes.length) endPos = bytes.length;
  else endPos |= 0;
  if (endPos <= startPos) return '';
  const chunk = bytes.slice(startPos, endPos);
  return unicodeDecoder.decode(chunk);
}

function readInt32(bytes, startPos = 0) {
  const first = bytes[startPos];
  const last = bytes[startPos + 3];
  if (first === undefined || last === undefined)
    console.error("Intrinio Realtime Client - Cannot read UInt32");
  return (
      first +
      (bytes[++startPos] * 256) +
      (bytes[++startPos] * 65536) +
      (last << 24)
  );
}

function readUInt32(bytes, startPos = 0) {
  const first = bytes[startPos];
  const last = bytes[startPos + 3];
  if (first === undefined || last === undefined)
    console.error("Intrinio Realtime Client - Cannot read UInt32");
  return (
      first +
      (bytes[++startPos] * 256) +
      (bytes[++startPos] * 65536) +
      (last * 16777216)
  );
}

function readFloat32(bytes, float32Array, backingByteArray, startPos = 0) {
  const first = bytes[startPos];
  const last = bytes[startPos + 3];
  if (first === undefined || last === undefined)
    console.error("Intrinio Realtime Client - Cannot read Float32");
  backingByteArray[0] = first;
  backingByteArray[1] = bytes[++startPos];
  backingByteArray[2] = bytes[++startPos];
  backingByteArray[3] = last;
  let parsed = parseFloat(float32Array[0].toFixed(4));
  return parsed < 0 ? 0 : parsed;
}

function readUInt64(bytes, startPos = 0) {
  const first = bytes[startPos];
  const last = bytes[startPos + 7];
  if (first === undefined || last === undefined)
    console.error("Intrinio Realtime Client - Cannot read UInt64");
  const lower =
      first +
      (bytes[++startPos] * 256) +
      (bytes[++startPos] * 65536) +
      (bytes[++startPos] * 16777216);
  const upper =
      bytes[++startPos] +
      (bytes[++startPos] * 256) +
      (bytes[++startPos] * 65536) +
      (last * 16777216);
  return (BigInt(lower) + (BigInt(upper) << 32n));
}

function writeString(bytes, string, startPos) {
  if (startPos === undefined || startPos < 0 || startPos > bytes.length - 1) return bytes;
  const encodedString = encoder.encode(string);
  const bytesAvailable = bytes.length - startPos;
  if (bytesAvailable < string.length) {
    const trimmedEncodedString = encodedString.slice(0, bytesAvailable);
    bytes.set(trimmedEncodedString, startPos);
  }
  else bytes.set(encodedString, startPos);
}

function copyInto(source, destination, destinationStartIndex){
  for (let i = 0; i < source.length; i++){
    destination[destinationStartIndex + i] = source[i]
  }
}

async function readFileChunk(fileStream, chunkSize){
  const buffer = Buffer.alloc(chunkSize);
  const buff = await fileStream.read(buffer, 0, chunkSize, null);
  if (buff === undefined || buff === null || buff.bytesRead === 0 || buff.buffer === undefined || buff.buffer === null){
    return null;
  }
  else
    return buff.buffer;
}

async function * replayTickFileWithoutDelay(filePath){
  if (require('fs').existsSync(filePath)) {
    const fileStream = await require('fs').promises.open(filePath, 'r');
    let readResult = await readFileChunk(fileStream, 1);
    while (readResult !== undefined && readResult !== null){
      let eventBytes= Buffer.alloc(EVENT_BUFFER_SIZE);
      eventBytes[0] = 1; // This is the number of messages in the group
      eventBytes[1] = readResult[0]; // This is message type
      eventBytes[2] = (await readFileChunk(fileStream, 1))[0]; // This is message length, including this and the previous byte.
      copyInto(await readFileChunk(fileStream, eventBytes[2] - 2), eventBytes, 3); // read the rest of the message
      let timeReceivedBytes = await readFileChunk(fileStream, 8);
      let timeReceived = readUInt64(timeReceivedBytes, 0);
      yield new Tick(timeReceived, eventBytes);
      readResult = await readFileChunk(fileStream, 1);
    }
    fileStream.close();
  }
  else
    yield null;
}

async function fillNextTicks(enumerators, nextTicks){
  for (let i = 0; i < nextTicks.length; i++){
    if (nextTicks[i] === undefined || nextTicks[i] === null){
      let next = await enumerators[i].next();
      if (!next.done) {
        nextTicks[i] = next.value;
      }
      else{
        nextTicks[i] = null;
      }
    }
  }
}

function pullNextTick(nextTicks){
  let pullIndex = 0;
  let t = 9223372036854775806;  //max value
  for (let i = 0; i < nextTicks.length; i++){
    if (nextTicks[i] !== undefined && nextTicks[i] !== null && nextTicks[i].timeReceived < t){
      pullIndex = i;
      t = nextTicks[i].timeReceived;
    }
  }
  let pulledTick = nextTicks[pullIndex];
  nextTicks[pullIndex] = null;
  return pulledTick;
}

function hasAnyValue(nextTicks){
  let hasValue = false;
  for (let i = 0; i < nextTicks.length; i++){
    if (nextTicks[i] !== undefined && nextTicks[i] !== null){
      hasValue = true;
    }
  }
  return hasValue;
}

async function * replayFileGroupWithoutDelay(tickGroup) {
  let nextTicks = Array(tickGroup.length);
  for(let i = 0; i < tickGroup.length; i++){
    nextTicks[i] = null;
  }
  let enumerators = Array(tickGroup.length);
  for(let i = 0; i < tickGroup.length; i++){
    nextTicks[i] = null;
  }
  for (let i = 0; i < tickGroup.length; i++) {
    enumerators[i] = tickGroup[i]
  }
  await fillNextTicks(enumerators, nextTicks)
  while (hasAnyValue(nextTicks)){
    let nextTick = pullNextTick(nextTicks);
    if (nextTick !== undefined && nextTick !== null){
      yield nextTick;
    }
    await fillNextTicks(enumerators, nextTicks);
  }
}

async function * replayFileGroupWithDelay(allTicks){
  const multiplier = BigInt(1000000);
  const start = BigInt(new Date().getTime()) * multiplier; //getTime returns milliseconds since epoch
  let offset = BigInt(0);

  let aggregatedTickIterator = await replayFileGroupWithoutDelay(allTicks);
  let next = await aggregatedTickIterator.next();
  if (offset === BigInt(0)) {
    offset = start - next.value.timeReceived;
  }
  yield next.value;
  while(!next.done){
    next = await aggregatedTickIterator.next();
    // sleep until the tick happens
    const now = BigInt(new Date().getTime()) * multiplier;
    if ((next.value.timeReceived + offset) <= now) {
      let sleepTime = (now - (next.value.timeReceived + offset)) / multiplier
      await new Promise(r => setTimeout(r, Number(sleepTime))); //input is milliseconds
    }
    yield next.value
  }
}

function writeCsvHeaderRow(csvWriter){
  csvWriter.write("\"Type\",\"Symbol\",\"Price\",\"Size\",\"Timestamp\",\"TotalVolume\",\"SubProvider\",\"MarketCenter\",\"Condition\"\r\n");
}

function writeCsvTradeRow(csvWriter, trade){
  csvWriter.write(`\"${trade.Type}\",\"${trade.Symbol}\",\"${trade.Price}\",\"${trade.Size}\",\"${trade.Timestamp.toString()}\",\"${trade.TotalVolume}\",\"${trade.SubProvider}\",\"${trade.MarketCenter}\",\"${trade.Condition}\"\r\n`);
}

function writeCsvQuoteRow(csvWriter, quote){
  csvWriter.write(`\"${quote.Type}\",\"${quote.Symbol}\",\"${quote.Price}\",\"${quote.Size}\",\"${quote.Timestamp.toString()}\",\"\",\"${quote.SubProvider}\",\"${quote.MarketCenter}\",\"${quote.Condition}\"\r\n`);
}

async function replayToCsv(outputFilePath, config, channels, tradesOnly, apiKey){
  console.log("Creating file " + outputFilePath);
  let csvWriter = require('fs').createWriteStream(outputFilePath, {flags: 'a'});
  writeCsvHeaderRow(csvWriter);
  function onTrade(trade){
    writeCsvTradeRow(csvWriter, trade);
  }
  function onQuote(quote){
    writeCsvQuoteRow(csvWriter, quote);
  }

  let client = await new this.ReplayClient(apiKey, onTrade, onQuote, config);
  await client.join(channels, tradesOnly);
}

const defaultConfig = {
  provider: 'IEX', //IEX (same as) REALTIME, or DELAYED_SIP, or NASDAQ_BASIC, or CBOE_ONE, EQUITIES_EDGE, or MANUAL
  ipAddress: undefined,
  tradesOnly: false,
  isPublicKey: false,
  delayed: false //set to true if you have realtime access and want to force delayed mode. Otherwise, if you only have delayed, you'll get delayed not matter if you set this or not.
};

const defaultReplayConfig = {
  provider: 'IEX', //IEX (same as) REALTIME, or DELAYED_SIP, or NASDAQ_BASIC, or CBOE_ONE, EQUITIES_EDGE, or MANUAL
  ipAddress: undefined,
  tradesOnly: false,
  isPublicKey: false,
  replayDate: '2025-01-08',
  replayAsIfLive: false,
  replayDeleteFileWhenDone: true
};

class IntrinioRealtime {
  constructor(accessKey, onTrade, onQuote, config = {}) {
    this._accessKey = accessKey;
    this._config = Object.assign({}, defaultConfig, config);
    this._token = null;
    this._tokenObtainedAt = 0;
    this._websocket = null;
    this._isReady = false;
    this._attemptingReconnect = false;
    this._stopped = false;
    this._stopPromise = null;
    this._connectionLoopPromise = null;
    this._cancelActiveConnection = null;
    this._failActiveConnection = null;
    this._cancelBackoff = null;
    this._cancelAuth = null;
    this._authRequest = null;
    this._socketGeneration = 0;
    this._startupLogged = false;
    this._sigintHandler = null;
    this._readyWaiters = new Set();
    this._lastReset = Date.now();
    this._msgCount = 0;
    this._channels = new Map();
    this._onTrade = (onTrade && (typeof onTrade === "function")) ? onTrade : (_) => {};
    this._onQuote = (onQuote && (typeof onQuote === "function")) ? onQuote : (_) => {};
    this._float32Array = new Float32Array(1);
    this._backingByteArray = new Uint8Array(this._float32Array.buffer);
    this._tradeQueue = new EventQueue();
    this._quoteQueue = new EventQueue();
    this._quoteQueueCap = QUOTE_QUEUE_CAP;
    this._droppedQuoteCount = 0;
    this._queueDrainScheduled = false;
    this._selfHealBackoffs = SELF_HEAL_BACKOFFS;
    this._connectTimeoutMs = CONNECT_TIMEOUT_MS;
    this._closeAckTimeoutMs = CLOSE_ACK_TIMEOUT_MS;
    this._stableSessionMs = STABLE_SESSION_MS;
    this._stableSessionTimer = null;
    this._noteStableSession = null;

    if ((!this._accessKey) || (this._accessKey === "")) {
      throw "Intrinio Realtime Client - Access Key is required";
    }

    if (!this._config.provider) {
      throw "Intrinio Realtime Client - 'config.provider' must be specified";
    }
    else if ((this._config.provider !== "IEX") && (this._config.provider !== "CBOE_ONE") && (this._config.provider !== "EQUITIES_EDGE") && (this._config.provider !== "REALTIME") && (this._config.provider !== "MANUAL")
        && (this._config.provider !== "DELAYED_SIP") && (this._config.provider !== "NASDAQ_BASIC")) {
      throw "Intrinio Realtime Client - 'config.provider' must be either 'IEX' or 'REALTIME' or 'CBOE_ONE' or 'EQUITIES_EDGE' or 'MANUAL' or 'DELAYED_SIP' or 'NASDAQ_BASIC'";
    }

    if ((this._config.provider === "MANUAL") && ((!this._config.ipAddress) || (this._config.ipAddress === ""))) {
      throw "Intrinio Realtime Client - 'config.ipAddress' must be specified for manual configuration";
    }

    if(!onTrade) {
      throw "Intrinio Realtime Client - 'onTrade' callback is required";
    }

    if(!onQuote){
      this._config.tradesOnly = true;
    }

    this._connectionLoopPromise = this._runConnectionLoop();
  }

  _getAuthUrl() {
    switch(this._config.provider) {
      case "REALTIME":
        if (this._config.isPublicKey) return "https://realtime-mx.intrinio.com/auth";
        else return "https://realtime-mx.intrinio.com/auth?api_key=" + this._accessKey;
      case "IEX":
        if (this._config.isPublicKey) return "https://realtime-mx.intrinio.com/auth";
        else return "https://realtime-mx.intrinio.com/auth?api_key=" + this._accessKey;
      case "DELAYED_SIP":
        if (this._config.isPublicKey) return "https://realtime-delayed-sip.intrinio.com/auth";
        else return "https://realtime-delayed-sip.intrinio.com/auth?api_key=" + this._accessKey;
      case "NASDAQ_BASIC":
        if (this._config.isPublicKey) return "https://realtime-nasdaq-basic.intrinio.com/auth";
        else return "https://realtime-nasdaq-basic.intrinio.com/auth?api_key=" + this._accessKey;
      case "CBOE_ONE":
        if (this._config.isPublicKey) return "https://cboe-one.intrinio.com/auth";
        else return "https://cboe-one.intrinio.com/auth?api_key=" + this._accessKey;
      case "EQUITIES_EDGE":
        if (this._config.isPublicKey) return "https://equities-edge.intrinio.com/auth";
        else return "https://equities-edge.intrinio.com/auth?api_key=" + this._accessKey;
      case "MANUAL":
        if (this._config.isPublicKey) return "http://" + this._config.ipAddress + "/auth";
        else return "http://" + this._config.ipAddress + "/auth?api_key=" + this._accessKey;
      default: throw "Intrinio Realtime Client - 'config.provider' not specified!";
    }
  }

  _getWebSocketUrl() {
    let delayed_part = this._config.delayed ? "&delayed=true" : "";

    switch(this._config.provider) {
      case "REALTIME": return `wss://realtime-mx.intrinio.com/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}${delayed_part}`;
      case "IEX": return `wss://realtime-mx.intrinio.com/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}${delayed_part}`;
      case "DELAYED_SIP": return `wss://realtime-delayed-sip.intrinio.com/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}${delayed_part}`;
      case "NASDAQ_BASIC": return `wss://realtime-nasdaq-basic.intrinio.com/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}${delayed_part}`;
      case "CBOE_ONE": return `wss://cboe-one.intrinio.com/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}${delayed_part}`;
      case "EQUITIES_EDGE": return `wss://equities-edge.intrinio.com/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}${delayed_part}`;
      case "MANUAL": return "ws://" + this._config.ipAddress + `/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}${delayed_part}`;
      default: throw "Intrinio Realtime Client - 'config.provider' not specified!";
    }
  }

  _getMessageType(val){
    switch(val) {
      case 0:
        return 'Trade';
        break;
      case 1:
        return 'Ask';
        break;
      case 2:
        return 'Bid';
        break;
      default:
        return '';
        break;
    }
  }

  _getSubProvider(val){
    switch(val) {
      case 0:
        return 'NONE';
        break;
      case 1:
        return 'CTA_A';
        break;
      case 2:
        return 'CTA_B';
        break;
      case 3:
        return 'UTP';
        break;
      case 4:
        return 'OTC';
        break;
      case 5:
        return 'NASDAQ_BASIC';
        break;
      case 6:
        return 'IEX';
        break;
      case 7:
        return 'CBOE_ONE';
        break;
      case 8:
        return 'EQUITIES_EDGE';
        break;
      default:
        return 'NONE';
        break;
    }
  }

  _parseTrade(bytes) {
    let symbolLength = bytes[2];
    let conditionLength = bytes[26 + symbolLength];
    let marketCenter = readUnicodeString(bytes, 4 + symbolLength, 6 + symbolLength);
    let subProvider = this._getSubProvider(bytes[3 + symbolLength]);
    let isDarkpool = false;
    switch(subProvider) {
      case 'CTA_A':
      case 'CTA_B':
      case 'UTP':
      case 'OTC':
        isDarkpool = (!marketCenter) || marketCenter == "\0" || marketCenter == "D" || marketCenter == "E" || marketCenter.trim().length === 0;
        break;
      case 'NASDAQ_BASIC':
        isDarkpool = (!marketCenter) || marketCenter == "\0" || marketCenter == "L" || marketCenter == "2" || marketCenter.trim().length === 0;
        break;
      default:
        isDarkpool = false;
        break;
    }

    return {
      Type: this._getMessageType(bytes[0]),
      Symbol: readString(bytes, 3, 3 + symbolLength),
      Price: readFloat32(bytes, this._float32Array, this._backingByteArray, 6 + symbolLength),
      Size: readUInt32(bytes, 10 + symbolLength),
      Timestamp: readUInt64(bytes, 14 + symbolLength),
      TotalVolume: readUInt32(bytes, 22 + symbolLength),
      SubProvider: subProvider,
      MarketCenter: marketCenter,
      Condition: conditionLength > 0 ? readString(bytes, 27 + symbolLength, 27 + symbolLength + conditionLength) : "",
      IsDarkpool: isDarkpool
    }
  }

  _parseQuote (bytes) {
    let symbolLength = bytes[2];
    let conditionLength = bytes[22 + symbolLength];
    let marketCenter = readUnicodeString(bytes, 4 + symbolLength, 6 + symbolLength);
    return {
      Type: this._getMessageType(bytes[0]),
      Symbol: readString(bytes, 3, 3 + symbolLength),
      Price: readFloat32(bytes, this._float32Array, this._backingByteArray, 6 + symbolLength),
      Size: readUInt32(bytes, 10 + symbolLength),
      Timestamp: readUInt64(bytes, 14 + symbolLength),
      SubProvider: this._getSubProvider(bytes[3 + symbolLength]),
      MarketCenter: marketCenter,
      Condition: conditionLength > 0 ? readString(bytes, 23 + symbolLength, 23 + symbolLength + conditionLength) : ""
    }
  }

  _messageIsComplete(bytes, msgType) {
    if (bytes.length < 3) return false;
    const symbolLength = bytes[2];
    const conditionIndex = (msgType === 0 ? 26 : 22) + symbolLength;
    if (conditionIndex >= bytes.length) return false;
    return bytes.length >= conditionIndex + 1 + bytes[conditionIndex];
  }

  _parseSocketMessage(data) {
    let bytes = new Uint8Array(data);
    let msgCount = bytes[0];
    let startIndex = 1;
    for (let i = 0; i < msgCount; i++) {
      if (startIndex + 1 >= bytes.length) {
        console.warn("Intrinio Realtime Client - Invalid message length: %i", bytes[startIndex + 1]);
        break;
      }
      let msgType = bytes[startIndex];
      let msgLength = bytes[startIndex + 1];
      let endIndex = startIndex + msgLength;
      if (msgLength < 2 || endIndex > bytes.length) {
        console.warn("Intrinio Realtime Client - Invalid message length: %i", msgLength);
        break;
      }
      let chunk = bytes.slice(startIndex, endIndex);
      if ((msgType === 0 || msgType === 1 || msgType === 2) && !this._messageIsComplete(chunk, msgType)) {
        console.warn("Intrinio Realtime Client - Invalid message length: %i", msgLength);
        startIndex = endIndex;
        continue;
      }
      try {
        switch(msgType) {
          case 0:
            this._tradeQueue.enqueue(this._parseTrade(chunk));
            break;
          case 1:
          case 2:
            if (this._quoteQueue.length >= this._quoteQueueCap) this._droppedQuoteCount++;
            else this._quoteQueue.enqueue(this._parseQuote(chunk));
            break;
          default: console.warn("Intrinio Realtime Client - Invalid message type: %i", msgType);
        }
      }
      catch (error) {
        console.error("Intrinio Realtime Client - Error parsing websocket message:", error);
      }
      startIndex = endIndex;
    }
    this._scheduleQueueDrain();
  }

  _scheduleQueueDrain() {
    if (this._stopped || this._queueDrainScheduled || (this._tradeQueue.length === 0 && this._quoteQueue.length === 0))
      return;

    this._queueDrainScheduled = true;
    scheduleWork(() => this._drainEventQueues());
  }

  _drainEventQueues() {
    this._queueDrainScheduled = false;
    if (this._stopped) {
      this._clearEventQueues();
      return;
    }

    let processed = 0;
    while (processed < PROCESSING_BATCH_SIZE) {
      let callback;
      let event;
      if (this._tradeQueue.length > 0) {
        callback = this._onTrade;
        event = this._tradeQueue.dequeue();
      }
      else if (this._quoteQueue.length > 0) {
        callback = this._onQuote;
        event = this._quoteQueue.dequeue();
      }
      else {
        break;
      }

      this._invokeCallback(callback, event);
      processed++;
    }

    this._scheduleQueueDrain();
  }

  _invokeCallback(callback, event) {
    try {
      const result = callback(event);
      if (result && typeof result.catch === "function") {
        result.catch(error => console.error("Intrinio Realtime Client - Event callback failed:", error));
      }
    }
    catch (error) {
      console.error("Intrinio Realtime Client - Event callback failed:", error);
    }
  }

  _clearEventQueues() {
    this._tradeQueue.clear();
    this._quoteQueue.clear();
  }

  _trySetToken() {
    if (this._config.isPublicKey) {
      return new Promise((fulfill, reject) => {
        let settled = false;
        let xhr = null;
        const cancel = () => fail(new Error("Auth request canceled"));
        const succeed = token => {
          if (settled) return;
          settled = true;
          if (this._authRequest === xhr) this._authRequest = null;
          if (this._cancelAuth === cancel) this._cancelAuth = null;
          this._token = token;
          this._tokenObtainedAt = Date.now();
          console.log("Intrinio Realtime Client - Authorized (public key)");
          fulfill();
        };
        const fail = error => {
          if (settled) return;
          settled = true;
          if (this._authRequest === xhr) this._authRequest = null;
          if (this._cancelAuth === cancel) this._cancelAuth = null;
          reject(error instanceof Error ? error : new Error("Unable to acquire auth token"));
        };
        try {
          console.log("Intrinio Realtime Client - Authorizing (public key)...");
          const url = this._getAuthUrl();
          xhr = new XMLHttpRequest();
          this._authRequest = xhr;
          this._cancelAuth = cancel;
          xhr.timeout = this._connectTimeoutMs;
          xhr.onerror = (error) => {
            console.error("Intrinio Realtime Client - Error getting public key auth token: ", error);
            fail(error);
          };
          xhr.ontimeout = () => {
            console.error("Intrinio Realtime Client - Timed out trying to get auth token.");
            fail(new Error("Auth request timed out"));
          };
          xhr.onabort = () => {
            fail(new Error("Auth request aborted"));
          };
          xhr.onreadystatechange = () => {
            if (xhr.readyState === 4) {
              if (xhr.status === 401) {
                console.error("Intrinio Realtime Client - Unable to authorize (public key)");
                fail(new Error("Unable to authorize"));
              }
              else if (xhr.status !== 200) {
                console.error("Intrinio Realtime Client - Could not get public key auth token: Status code (%i)", xhr.status);
                fail(new Error(`Auth request failed with status ${xhr.status}`));
              }
              else succeed(xhr.responseText);
            }
          };
          xhr.open("GET", url, true);
          xhr.overrideMimeType("text/html");
          xhr.setRequestHeader('Content-Type', 'application/json');
          xhr.setRequestHeader('Authorization', 'Public ' + this._accessKey);
          xhr.setRequestHeader(CLIENT_INFO_HEADER_KEY, CLIENT_INFO_HEADER_VALUE);
          xhr.send();
        }
        catch (error) {
          console.error("Intrinio Realtime Client - Error in authorization (%s)", error);
          fail(error);
        }
      });
    }

    return new Promise((fulfill, reject) => {
      let settled = false;
      let request = null;
      const cancel = () => fail(new Error("Auth request canceled"));
      const succeed = token => {
        if (settled) return;
        settled = true;
        if (this._authRequest === request) this._authRequest = null;
        if (this._cancelAuth === cancel) this._cancelAuth = null;
        this._token = token;
        this._tokenObtainedAt = Date.now();
        console.log("Intrinio Realtime Client - Authorized");
        fulfill();
      };
      const fail = error => {
        if (settled) return;
        settled = true;
        if (this._authRequest === request) this._authRequest = null;
        if (this._cancelAuth === cancel) this._cancelAuth = null;
        reject(error instanceof Error ? error : new Error("Unable to acquire auth token"));
      };

      try {
        const protocol = (this._config.provider === "MANUAL") ? require('http') : require('https');
        console.log("Intrinio Realtime Client - Authorizing...");
        const url = this._getAuthUrl();
        const options = {
          headers: {
            [CLIENT_INFO_HEADER_KEY]: CLIENT_INFO_HEADER_VALUE
          }
        };
        request = protocol.get(url, options, response => {
          if (response.statusCode === 401) {
            console.error("Intrinio Realtime Client - Unable to authorize");
            if (typeof response.resume === "function") response.resume();
            fail(new Error("Unable to authorize"));
            return;
          }
          if (response.statusCode !== 200) {
            console.error("Intrinio Realtime Client - Could not get auth token: Status code (%i)", response.statusCode);
            if (typeof response.resume === "function") response.resume();
            fail(new Error(`Auth request failed with status ${response.statusCode}`));
            return;
          }

          const chunks = [];
          response.on("data", data => chunks.push(Buffer.isBuffer(data) ? data : Buffer.from(data)));
          response.on("end", () => succeed(Buffer.concat(chunks).toString("utf8")));
          response.on("error", fail);
        });
        this._authRequest = request;
        this._cancelAuth = cancel;
        if (typeof request.setTimeout === "function") {
          request.setTimeout(this._connectTimeoutMs, () => {
            const error = new Error("Auth request timed out");
            console.error("Intrinio Realtime Client - Timed out trying to get auth token.");
            if (typeof request.destroy === "function") request.destroy(error);
            fail(error);
          });
        }
        request.on("error", error => {
          if (!this._stopped)
            console.error("Intrinio Realtime Client - Error getting auth token: ", error);
          fail(error);
        });
      }
      catch (error) {
        console.error("Intrinio Realtime Client - Error in authorization (%s)", error);
        fail(error);
      }
    });
  }

  async _ensureToken() {
    if (!this._token || (Date.now() - this._tokenObtainedAt) >= TOKEN_MAX_AGE_MS)
      await this._trySetToken();
  }

  _makeJoinMessage(tradesOnly, symbol) {
    let message = null;
    switch (symbol) {
      case "$lobby":
        message = new Uint8Array(11);
        message[0] = 74;
        if (tradesOnly) {
          message[1] = 1;
        } else {
          message[1] = 0;
        }
        writeString(message, "$FIREHOSE", 2);
        return message;
      default:
        message = new Uint8Array(2 + symbol.length);
        message[0] = 74;
        if (tradesOnly) {
          message[1] = 1;
        } else {
          message[1] = 0;
        }
        writeString(message, symbol, 2);
        return message;
    }
  }

  _makeLeaveMessage(symbol) {
    let message = null;
    switch (symbol) {
      case "$lobby":
        message = new Uint8Array(10);
        message[0] = 76;
        writeString(message, "$FIREHOSE", 1);
        return message;
      default:
        message = new Uint8Array(1 + symbol.length);
        message[0] = 76;
        writeString(message, symbol, 1);
        return message;
    }
  }

  async _runConnectionLoop() {
    let backoffIndex = 0;
    let gaveImmediateRetry = false;
    this._noteStableSession = () => {
      gaveImmediateRetry = false;
      backoffIndex = 0;
    };
    try {
      while (!this._stopped) {
        try {
          await this._ensureToken();
          if (this._stopped) return;
          await this._connectOnce();
          if (this._stopped) return;
        }
        catch (error) {
          if (this._stopped) return;
          const wasOpen = !!(error && error.wasOpen);
          this._markNotReady();
          this._attemptingReconnect = true;
          console.info("Intrinio Realtime Client - Websocket reconnecting...");
          if (wasOpen)
            console.info("Intrinio Realtime Client - Websocket closed: %s", error && error.message ? error.message : error);
          else
            console.warn("Intrinio Realtime Client - Connection attempt failed: %s", error && error.message ? error.message : error);

          if (wasOpen && !gaveImmediateRetry) {
            gaveImmediateRetry = true;
            backoffIndex = 0;
            if (!(await this._sleepForReconnect(0))) return;
            continue;
          }

          const backoff = this._selfHealBackoffs[Math.min(backoffIndex, this._selfHealBackoffs.length - 1)];
          console.log("Intrinio Realtime Client - Sleeping for %isec", backoff / 1000);
          if (!(await this._sleepForReconnect(backoff))) return;
          backoffIndex = Math.min(backoffIndex + 1, this._selfHealBackoffs.length - 1);
        }
      }
    }
    finally {
      this._noteStableSession = null;
      this._clearStableSessionTimer();
    }
  }

  _scheduleStableSession() {
    this._clearStableSessionTimer();
    this._stableSessionTimer = setTimeout(() => {
      this._stableSessionTimer = null;
      if (this._stopped) return;
      if (typeof this._noteStableSession === "function") this._noteStableSession();
    }, this._stableSessionMs);
    if (typeof this._stableSessionTimer.unref === "function") this._stableSessionTimer.unref();
  }

  _clearStableSessionTimer() {
    if (!this._stableSessionTimer) return;
    clearTimeout(this._stableSessionTimer);
    this._stableSessionTimer = null;
  }

  _sleepForReconnect(milliseconds) {
    if (this._stopped) return Promise.resolve(false);
    return new Promise(resolve => {
      let settled = false;
      const finish = value => {
        if (settled) return;
        settled = true;
        clearTimeout(timeoutId);
        if (this._cancelBackoff === cancel) this._cancelBackoff = null;
        resolve(value);
      };
      const cancel = () => finish(false);
      const timeoutId = setTimeout(() => finish(true), milliseconds);
      this._cancelBackoff = cancel;
    });
  }

  _createWebSocket() {
    const wsUrl = this._getWebSocketUrl();
    if (this._config.isPublicKey) {
      console.info("Intrinio Realtime Client - Websocket initializing (public key)");
      return new WebSocket(wsUrl);
    }

    console.info("Intrinio Realtime Client - Websocket initializing");
    const WebSocketClient = require('ws');
    return new WebSocketClient(wsUrl, {
      perMessageDeflate: false,
      handshakeTimeout: this._connectTimeoutMs,
      headers: {
        [CLIENT_INFO_HEADER_KEY]: CLIENT_INFO_HEADER_VALUE,
        [MESSAGE_VERSION_HEADER_KEY]: MESSAGE_VERSION_HEADER_VALUE
      }
    });
  }

  _connectOnce() {
    return new Promise((fulfill, reject) => {
      let socket;
      let opened = false;
      let settled = false;
      let timeoutId = null;
      const generation = ++this._socketGeneration;
      const isCurrent = () => this._websocket === socket && this._socketGeneration === generation;
      const cleanup = () => {
        if (timeoutId !== null) clearTimeout(timeoutId);
        this._clearStableSessionTimer();
        if (this._cancelActiveConnection === cancel) this._cancelActiveConnection = null;
        if (this._failActiveConnection === fail) this._failActiveConnection = null;
      };
      const detachCurrent = () => {
        if (isCurrent()) this._websocket = null;
      };
      const succeed = value => {
        if (settled) return;
        settled = true;
        cleanup();
        detachCurrent();
        fulfill(value);
      };
      const fail = error => {
        if (settled) return;
        settled = true;
        cleanup();
        if (isCurrent()) this._markNotReady();
        detachCurrent();
        const connectionError = error instanceof Error ? error : new Error("Websocket connection failed");
        connectionError.wasOpen = opened;
        reject(connectionError);
      };
      const cancel = () => {
        if (settled) return;
        const current = isCurrent();
        succeed(false);
        if (current) this._abortWebsocket(socket);
      };

      this._cancelActiveConnection = cancel;
      this._failActiveConnection = fail;

      try {
        const previous = this._websocket;
        socket = this._createWebSocket();
        this._websocket = socket;
        this._abortWebsocket(previous);
        socket.binaryType = "arraybuffer";

        timeoutId = setTimeout(() => {
          if (!isCurrent() || opened) return;
          const error = new Error(`Websocket connect timed out after ${this._connectTimeoutMs}ms`);
          fail(error);
          this._abortWebsocket(socket);
        }, this._connectTimeoutMs);

        const onOpen = () => {
          if (!isCurrent() || settled) return;
          if (this._stopped) {
            cancel();
            return;
          }

          if (timeoutId !== null) {
            clearTimeout(timeoutId);
            timeoutId = null;
          }
          try {
            for (const [channel, tradesOnly] of this._channels) {
              console.info("Intrinio Realtime Client - Joining channel: %s (trades only = %s)", channel, tradesOnly);
              socket.send(this._makeJoinMessage(tradesOnly, channel));
            }
          }
          catch (error) {
            fail(error);
            this._abortWebsocket(socket);
            return;
          }

          opened = true;
          this._scheduleStableSession();
          this._lastReset = Date.now();
          this._attemptingReconnect = false;
          this._markReady();
          console.log("Intrinio Realtime Client - Websocket connected%s", this._config.isPublicKey ? " (public key)" : "");
          this._logStartupOnce();
        };
        const onClose = closeEventOrCode => {
          if (!isCurrent() || settled) return;
          const code = typeof closeEventOrCode === "number" ? closeEventOrCode : closeEventOrCode && closeEventOrCode.code;
          console.info("Intrinio Realtime Client - Websocket closed (code: %o)", code);
          this._markNotReady();
          if (this._stopped) succeed(false);
          else fail(new Error(`Websocket closed${code === undefined ? "" : ` with code ${code}`}`));
        };
        const onError = error => {
          if (!isCurrent() || settled) return;
          console.error("Intrinio Realtime Client - Websocket error: %s", error && error.message ? error.message : error);
          fail(error);
          this._abortWebsocket(socket);
        };
        const onMessage = (message, isBinary) => {
          if (!isCurrent() || settled || !opened || this._stopped) return;
          const data = message && typeof message === "object" && "data" in message ? message.data : message;
          this._handleIncomingMessage(data, isBinary, socket, generation);
        };

        if (this._config.isPublicKey) {
          socket.onopen = onOpen;
          socket.onclose = onClose;
          socket.onerror = onError;
          socket.onmessage = onMessage;
        }
        else {
          socket.on("open", onOpen);
          socket.on("close", onClose);
          socket.on("error", onError);
          socket.on("message", onMessage);
        }
      }
      catch (error) {
        fail(error);
        this._abortWebsocket(socket);
      }
    });
  }

  _handleIncomingMessage(data, isBinary, socket, generation) {
    const isCurrent = () => this._websocket === socket && this._socketGeneration === generation && !this._stopped;
    if (typeof data === "string" || isBinary === false) {
      console.log("Intrinio Realtime Client - Message: %s", data);
      return;
    }
    if (typeof Blob !== "undefined" && data instanceof Blob) {
      data.arrayBuffer().then(buffer => {
        if (isCurrent()) {
          this._msgCount++;
          this._parseSocketMessage(buffer);
        }
      }).catch(error => console.error("Intrinio Realtime Client - Error reading websocket message:", error));
      return;
    }
    if (data instanceof ArrayBuffer || ArrayBuffer.isView(data)) {
      this._msgCount++;
      this._parseSocketMessage(data);
    }
    else {
      console.log("Intrinio Realtime Client - Message: %s", data);
    }
  }

  _isSocketClosed(socket) {
    return !socket || socket.readyState === SOCKET_CLOSED;
  }

  _waitForSocketClose(socket, timeoutMs) {
    if (this._isSocketClosed(socket)) return Promise.resolve(true);
    return new Promise(resolve => {
      let settled = false;
      const finish = closed => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (typeof socket.removeListener === "function") socket.removeListener("close", onClose);
        resolve(closed);
      };
      const onClose = () => finish(true);
      const timer = setTimeout(() => finish(this._isSocketClosed(socket)), timeoutMs);
      if (typeof socket.on === "function") socket.on("close", onClose);
      else {
        const previous = socket.onclose;
        socket.onclose = event => {
          if (typeof previous === "function") previous(event);
          onClose();
        };
      }
    });
  }

  _abortWebsocket(socket) {
    if (!socket || this._isSocketClosed(socket)) return;
    try {
      if (typeof socket.terminate === "function") socket.terminate();
      else if (typeof socket.close === "function") socket.close();
    }
    catch (_) {}
  }

  _markReady() {
    this._isReady = true;
    for (const waiter of this._readyWaiters) waiter();
    this._readyWaiters.clear();
  }

  _markNotReady() {
    this._isReady = false;
  }

  _waitUntilReady() {
    if (this._isReady || this._stopped) return Promise.resolve();
    return new Promise(resolve => this._readyWaiters.add(resolve));
  }

  _handleSocketFailure(error) {
    const socket = this._websocket;
    this._markNotReady();
    if (this._failActiveConnection) this._failActiveConnection(error);
    this._abortWebsocket(socket);
  }

  _logStartupOnce() {
    if (this._startupLogged) return;
    this._startupLogged = true;
    console.log("Intrinio Realtime Client - Startup succeeded");
    if (!this._config.isPublicKey && typeof process !== "undefined" && process && typeof process.on === "function") {
      this._sigintHandler = () => {
        console.log("Intrinio Realtime Client - Shutdown detected");
        this.stop().finally(() => process.kill(process.pid, 'SIGTERM'));
      };
      process.on('SIGINT', this._sigintHandler);
    }
  }

  _join(symbol, tradesOnly) {
    if (this._channels.has("$lobby")) {
      console.warn("Intrinio Realtime Client - $lobby channel already joined. Other channels not necessary.");
    }
    if (!this._channels.has(symbol)) {
      this._channels.set(symbol, tradesOnly);
      if (!this._isReady || !this._websocket) return false;
      try {
        let message = this._makeJoinMessage(tradesOnly, symbol);
        console.info("Intrinio Realtime Client - Joining channel: %s (trades only = %s)", symbol, tradesOnly);
        this._websocket.send(message);
      }
      catch (error) {
        this._handleSocketFailure(error);
        return false;
      }
    }
    return true;
  }

  _leave(symbol) {
    if (this._channels.has(symbol)) {
      this._channels.delete(symbol);
      if (this._isReady && this._websocket) {
        try {
          let message = this._makeLeaveMessage(symbol);
          console.info("Intrinio Realtime Client - Leaving channel: %s", symbol);
          this._websocket.send(message);
        }
        catch (error) {
          this._handleSocketFailure(error);
        }
      }
    }
  }

  async join(symbols, tradesOnly) {
    await this._waitUntilReady();
    if (this._stopped) return;
    let _tradesOnly = (this._config.tradesOnly ? this._config.tradesOnly : false) || (tradesOnly ? tradesOnly : false);
    let sent = true;
    if (symbols instanceof Array) {
      for (const symbol of symbols) {
        if (!this._channels.has(symbol)){
          sent = this._join(symbol, _tradesOnly) && sent;
        }
      }
    }
    else if (typeof symbols === "string") {
      if (!this._channels.has(symbols)){
        sent = this._join(symbols, _tradesOnly);
      }
    }
    else if ((typeof tradesOnly !== "undefined") || (typeof tradesOnly !== "boolean")) {
      console.error("Intrinio Realtime Client - If provided, 'tradesOnly' must be of type 'boolean', not '%s'", typeof tradesOnly);
    }
    else {
      console.error("Intrinio Realtime Client - Invalid use of 'join'");
    }
    if (!sent && !this._stopped) await this._waitUntilReady();
  }

  leave(symbols) {
    if (symbols instanceof Array) {
      for (const symbol of symbols) {
        if (this._channels.has(symbol)) {
          this._leave(symbol)
        }
      }
    }
    else if (typeof symbols === "string") {
      if (this._channels.has(symbols)) {
        this._leave(symbols);
      }
    }
    else {
      if (arguments.length == 0) {
        for (const channel of this._channels.keys()) {
          this._leave(channel);
        }
      }
      else {
        console.error("Intrinio Realtime Client - Invalid use of 'leave'");
      }
    }
  }

  async stop() {
    if (this._stopPromise) return this._stopPromise;
    this._stopPromise = this._stopCore();
    return this._stopPromise;
  }

  async _stopCore() {
    const socket = this._websocket;
    const wasReady = this._isReady;
    this._stopped = true;
    this._markNotReady();
    for (const waiter of this._readyWaiters) waiter();
    this._readyWaiters.clear();
    this._clearEventQueues();
    this._clearStableSessionTimer();

    console.log("Intrinio Realtime Client - Leaving subscribed channels");
    if (wasReady && socket) {
      for (const channel of this._channels.keys()) {
        try { socket.send(this._makeLeaveMessage(channel)); }
        catch (_) { break; }
      }
    }
    this._channels.clear();

    if (this._cancelBackoff) this._cancelBackoff();
    const authRequest = this._authRequest;
    if (this._cancelAuth) this._cancelAuth();
    if (authRequest) {
      try {
        if (typeof authRequest.abort === "function") authRequest.abort();
        else if (typeof authRequest.destroy === "function") authRequest.destroy(new Error("Client stopped"));
      }
      catch (_) {}
      this._authRequest = null;
    }

    if (this._sigintHandler && typeof process !== "undefined" && process && typeof process.removeListener === "function") {
      process.removeListener('SIGINT', this._sigintHandler);
      this._sigintHandler = null;
    }

    const deadline = Date.now() + STOP_DRAIN_TIMEOUT_MS;
    while (wasReady && socket && socket.bufferedAmount > 0 && Date.now() < deadline)
      await sleep(50);

    console.log("Intrinio Realtime Client - Websocket closing");
    if (wasReady && socket && !this._isSocketClosed(socket)) {
      const pendingClose = this._waitForSocketClose(socket, this._closeAckTimeoutMs);
      try { socket.close(1000, "Terminated by client"); }
      catch (_) {}
      const acknowledged = (await pendingClose) || this._isSocketClosed(socket);
      if (!acknowledged) {
        if (this._cancelActiveConnection) this._cancelActiveConnection();
        else this._abortWebsocket(socket);
      }
    }
    else if (!wasReady) {
      if (socket && !this._isSocketClosed(socket)) {
        try { socket.close(1000, "Terminated by client"); }
        catch (_) {}
      }
      if (this._cancelActiveConnection) this._cancelActiveConnection();
      this._abortWebsocket(socket);
    }
    else if (socket && this._isSocketClosed(socket) && this._cancelActiveConnection) {
      this._cancelActiveConnection();
    }

    if (this._connectionLoopPromise) await this._connectionLoopPromise;
  }

  getTotalMsgCount() {
    return this._msgCount;
  }

  getDroppedQuoteCount() {
    return this._droppedQuoteCount;
  }
}

class Tick{
  constructor(timeReceived, data){
    this.timeReceived = timeReceived;
    this.data = data;
  }
}

class IntrinioRealtimeReplayClient {
  constructor(accessKey, onTrade, onQuote, config = {}) {
    this._accessKey = accessKey;
    this._config = Object.assign({}, defaultReplayConfig, config);
    this._msgCount = 0;
    this._channels = new Map();
    this._onTrade = (onTrade && (typeof onTrade === "function")) ? onTrade : (_) => {};
    this._onQuote = (onQuote && (typeof onQuote === "function")) ? onQuote : (_) => {};
    this._float32Array = new Float32Array(1);
    this._backingByteArray = new Uint8Array(this._float32Array.buffer);

    if (this._config.isPublicKey){
      throw "Intrinio Replay Client - Replay Client is only available in a node.js environment.";
    }

    if ((!this._accessKey) || (this._accessKey === "")) {
      throw "Intrinio Replay Client - Access Key is required";
    }

    if (!this._config.provider) {
      throw "Intrinio Replay Client - 'config.provider' must be specified";
    }
    else if ((this._config.provider !== "IEX") && (this._config.provider !== "CBOE_ONE") && (this._config.provider !== "EQUITIES_EDGE") && (this._config.provider !== "REALTIME") && (this._config.provider !== "MANUAL")
          && (this._config.provider !== "DELAYED_SIP") && (this._config.provider !== "NASDAQ_BASIC")) {
        throw "Intrinio Replay Client - 'config.provider' must be either 'IEX' or 'REALTIME' or 'CBOE_ONE' or 'EQUITIES_EDGE' or 'MANUAL' or 'DELAYED_SIP' or 'NASDAQ_BASIC'";
    }

    if(!onTrade) {
      throw "Intrinio Replay Client - 'onTrade' callback is required";
    }

    if(!onQuote){
      this._config.tradesOnly = true;
    }

    this._start();
  }

  async _getAllApiDownloadResponses(urls){
    let responses = [];
    for (let i = 0; i < urls.length; i++){
      try{
        let apiResponse = await this._getApiDownloadResponse(urls[i]);
        if (apiResponse !== undefined && apiResponse !== null){ responses.push(apiResponse); }
      }
      catch (ex){
        //not all customers have access to all subsources within a source.  Ignore errors if they don't.
      }
    }
    return responses;
  }

  async _getAllFilePaths(responses){
    let filePaths = [];
    //download the files to a temp dir
    for (let i = 0; i < responses.length; i++){
      let filePath = await this._downloadFile(this._decodeUrl(responses[i].url), responses[i].name)
      if (filePath !== undefined && filePath !== null && filePath !== ""){ filePaths.push(filePath); }
    }
    return filePaths;
  }

  _deleteReplayFiles(filePaths){
    const fs = require('fs');
    for (let i = 0; i < filePaths.length; i++) {
      if (fs.existsSync(filePaths[i])){
        console.log("Intrinio Replay Client - Deleting file: " + filePath);
        fs.unlinkSync(filePaths[i]); //delete file
      }
    }
  }

  async _getAllFileIterators(filePaths){
    let enumerators = [];
    for (let i = 0; i < filePaths.length; i++){
      enumerators.push(await replayTickFileWithoutDelay(filePaths[i]));
    }
    return enumerators;
  }

  async _getAggregateTickIterator(fileGroup){
    if (this._config.replayAsIfLive){
      return await replayFileGroupWithDelay(fileGroup);
    }
    else{
      return await replayFileGroupWithoutDelay(fileGroup);
    }
  }

  async _start(){
    let urls = await this._getApiReplayUrls();
    let responses = await this._getAllApiDownloadResponses(urls);
    let filePaths = await this._getAllFilePaths(responses);
    let fileGroup = await this._getAllFileIterators(filePaths);

    let aggregatedTickIterator = await this._getAggregateTickIterator(fileGroup);

    console.log("Starting to read files...")

    let next = await aggregatedTickIterator.next();
    await this._parseSocketMessage(next.value.data);
    while(!next.done){
      next = await aggregatedTickIterator.next();
      await this._parseSocketMessage(next.value.data);
    }

    if (this._config.replayDeleteFileWhenDone){
      console.log("cleaning up files");
      this._deleteReplayFiles(filePaths, fs)
    }
  }

  _decodeUrl(url){
    if (url !== undefined){
      return url.replace('\u0026', '&');
    }
    return url;
  }

  async _downloadFile(url, fileName){
    const os = require('os');
    const fs = require('fs');
    const https = require('https');
    const path = require('path');
    let tempDir = os.tmpdir();
    let filePath = path.join(tempDir, fileName);

    console.log("Intrinio Replay Client - Starting to download to " + filePath + ".");
    return new Promise((fulfill, reject) => {
      try {
        const request = https.get(url, {}, response => {
          if (response.statusCode != 200) {
            console.error("Intrinio Replay Client - Could not download " + fileName + ": Status code (%i)", response.statusCode);
            reject();
          }
          else {
            const fileStream = fs.createWriteStream(filePath);
            response.pipe(fileStream);
            fileStream.on("finish", () => {
              fileStream.close();
              console.log("Intrinio Replay Client - Successfully downloaded " + filePath + ".");
              fulfill(filePath);
            });
          }
        })
        request.on("timeout", () => {
          console.error("Intrinio Replay Client - Timed out trying to download " + fileName + ".");
          reject();
        });
        request.on("error", error => {
          console.error("Intrinio Replay Client - Error downloading  " + fileName + ": (%s)", error);
          reject();
        });
      }
      catch (error) {
        console.error("Intrinio Replay Client - Error downloading " + fileName + ": (%s)", error);
        reject();
      }
    });
  }

  async _getApiDownloadResponse(url) {
    return new Promise((fulfill, reject) => {
      try {
        const protocol = require('https');
        const request = protocol.get(url, {}, response => {
          if (response.statusCode != 200) {
            //console.error("Intrinio Replay Client - Could not fetch download URL from API: Status code (%i)", response.statusCode);
            reject();
          }
          else {
            response.on("data", data => {
              let apiResponse = JSON.parse(data);
              console.log("Intrinio Replay Client - Successfully fetched download URL from API");
              fulfill(apiResponse);
            });
          }
        })
        request.on("timeout", () => {
          //console.error("Intrinio Replay Client - Timed out trying to fetched download URL from API.");
          reject();
        });
        request.on("error", error => {
          console.error("Intrinio Replay Client - Error fetching download URL from API: ", error);
          reject();
        });
      }
      catch (error) {
        console.error("Intrinio Replay Client - Error in fetching download URL (%s)", error);
        reject();
      }
    });
  }

  _getApiReplayUrls() {
    switch(this._config.provider) {
      case "REALTIME":
        return ["https://api-v2.intrinio.com/securities/replay?subsource=iex&date=" + this._config.replayDate + "&api_key=" + this._accessKey];
      case "IEX":
        return ["https://api-v2.intrinio.com/securities/replay?subsource=iex&date=" + this._config.replayDate + "&api_key=" + this._accessKey];
      case "DELAYED_SIP":
        return ["https://api-v2.intrinio.com/securities/replay?subsource=utp_delayed&date=" + this._config.replayDate + "&api_key=" + this._accessKey,
                "https://api-v2.intrinio.com/securities/replay?subsource=cta_a_delayed&date=" + this._config.replayDate + "&api_key=" + this._accessKey,
                "https://api-v2.intrinio.com/securities/replay?subsource=cta_b_delayed&date=" + this._config.replayDate + "&api_key=" + this._accessKey,
                "https://api-v2.intrinio.com/securities/replay?subsource=otc_delayed&date=" + this._config.replayDate + "&api_key=" + this._accessKey,
        ];
      case "NASDAQ_BASIC":
        return ["https://api-v2.intrinio.com/securities/replay?subsource=nasdaq_basic&date=" + this._config.replayDate + "&api_key=" + this._accessKey];
      case "CBOE_ONE":
        return ["https://api-v2.intrinio.com/securities/replay?subsource=cboe_one&date=" + this._config.replayDate + "&api_key=" + this._accessKey];
      case "EQUITIES_EDGE":
        return ["https://api-v2.intrinio.com/securities/replay?subsource=equities_edge&date=" + this._config.replayDate + "&api_key=" + this._accessKey];
      default: throw "Intrinio Replay Client - 'config.provider' not specified!";
    }
  }

  _getMessageType(val){
    switch(val) {
      case 0:
        return 'Trade';
        break;
      case 1:
        return 'Ask';
        break;
      case 2:
        return 'Bid';
        break;
      default:
        return '';
        break;
    }
  }

  _getSubProvider(val){
    switch(val) {
      case 0:
        return 'NONE';
        break;
      case 1:
        return 'CTA_A';
        break;
      case 2:
        return 'CTA_B';
        break;
      case 3:
        return 'UTP';
        break;
      case 4:
        return 'OTC';
        break;
      case 5:
        return 'NASDAQ_BASIC';
        break;
      case 6:
        return 'IEX';
        break;
      case 7:
        return 'CBOE_ONE';
        break;
      case 8:
        return 'EQUITIES_EDGE';
        break;
      default:
        return 'NONE';
        break;
    }
  }

  _parseTrade(bytes) {
    let symbolLength = bytes[2];
    let conditionLength = bytes[26 + symbolLength];
    return {
      Type: this._getMessageType(bytes[0]),
      Symbol: readString(bytes, 3, 3 + symbolLength),
      Price: readFloat32(bytes, this._float32Array, this._backingByteArray, 6 + symbolLength),
      Size: readUInt32(bytes, 10 + symbolLength),
      Timestamp: readUInt64(bytes, 14 + symbolLength),
      TotalVolume: readUInt32(bytes, 22 + symbolLength),
      SubProvider: this._getSubProvider(bytes[3 + symbolLength]),
      MarketCenter: readUnicodeString(bytes, 4 + symbolLength, 6 + symbolLength),
      Condition: conditionLength > 0 ? readString(bytes, 27 + symbolLength, 27 + symbolLength + conditionLength) : ""
    }
  }

  _parseQuote (bytes) {
    let symbolLength = bytes[2];
    let conditionLength = bytes[22 + symbolLength];
    return {
      Type: this._getMessageType(bytes[0]),
      Symbol: readString(bytes, 3, 3 + symbolLength),
      Price: readFloat32(bytes, this._float32Array, this._backingByteArray, 6 + symbolLength),
      Size: readUInt32(bytes, 10 + symbolLength),
      Timestamp: readUInt64(bytes, 14 + symbolLength),
      SubProvider: this._getSubProvider(bytes[3 + symbolLength]),
      MarketCenter: readUnicodeString(bytes, 4 + symbolLength, 6 + symbolLength),
      Condition: conditionLength > 0 ? readString(bytes, 23 + symbolLength, 23 + symbolLength + conditionLength) : ""
    }
  }

  async _parseSocketMessage(data) {
    let bytes = new Uint8Array(data);
    let msgCount = bytes[0];
    let startIndex = 1;
    for (let i = 0; i < msgCount; i++) {
      let msgType = bytes[startIndex]
      let msgLength = bytes[startIndex + 1]
      let endIndex = startIndex + msgLength
      let chunk = bytes.slice(startIndex, endIndex)
      switch(msgType) {
        case 0:
          let trade = this._parseTrade(chunk);
          if (this._channels.has("$lobby") || this._channels.has(trade.Symbol)) {
            await this._onTrade(trade);
          }
          break;
        case 1:
        case 2:
          let quote = this._parseQuote(chunk);
          if ((!this._config.tradesOnly) && (this._channels.has("$lobby") || this._channels.has(quote.Symbol))) {
            await this._onQuote(quote);
          }
          break;
        default: console.warn("Intrinio Replay Client - Invalid message type: %i", msgType)
      }
      startIndex = endIndex
    }
  }

  _join(symbol, tradesOnly) {
    if (this._channels.has("$lobby")) {
      console.warn("Intrinio Replay Client - $lobby channel already joined. Other channels not necessary.");
    }
    if (!this._channels.has(symbol)) {
      this._channels.set(symbol, tradesOnly);
      console.info("Intrinio Replay Client - Joining channel: %s (trades only = %s)", symbol, tradesOnly);
    }
  }

  _leave(symbol) {
    if (this._channels.has(symbol)) {
      this._channels.delete(symbol);
      console.info("Intrinio Replay Client - Leaving channel: %s", symbol);
    }
  }

  async join(symbols, tradesOnly) {
    let _tradesOnly = (this._config.tradesOnly ? this._config.tradesOnly : false) || (tradesOnly ? tradesOnly : false);
    if (symbols instanceof Array) {
      for (const symbol of symbols) {
        if (!this._channels.has(symbol)){
          this._join(symbol, _tradesOnly);
        }
      }
    }
    else if (typeof symbols === "string") {
      if (!this._channels.has(symbols)){
        this._join(symbols, _tradesOnly);
      }
    }
    else if ((typeof tradesOnly !== "undefined") || (typeof tradesOnly !== "boolean")) {
      console.error("Intrinio Replay Client - If provided, 'tradesOnly' must be of type 'boolean', not '%s'", typeof tradesOnly);
    }
    else {
      console.error("Intrinio Replay Client - Invalid use of 'join'");
    }
  }

  leave(symbols) {
    if (symbols instanceof Array) {
      for (const symbol of symbols) {
        if (this._channels.has(symbol)) {
          this._leave(symbol)
        }
      }
    }
    else if (typeof symbols === "string") {
      if (this._channels.has(symbols)) {
        this._leave(symbols);
      }
    }
    else {
      if (arguments.length == 0) {
        for (const channel of this._channels.keys()) {
          this._leave(channel);
        }
      }
      else {
        console.error("Intrinio Replay Client - Invalid use of 'leave'");
      }
    }
  }

  async stop() {
    console.log("Intrinio Replay Client - Leaving subscribed channels");
    for (const channel of this._channels.keys()) {
      this._leave(channel);
    }
  }

  getTotalMsgCount() {
    return this._msgCount;
  }
}

if (typeof window === 'undefined') {
  module.exports = { RealtimeClient: IntrinioRealtime, ReplayClient: IntrinioRealtimeReplayClient, replayToCsv: replayToCsv };
}

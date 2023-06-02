'use strict'

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf8");
const unicodeDecoder = new TextDecoder("utf-16be");

const SELF_HEAL_BACKOFFS = [10000, 30000, 60000, 300000, 600000];

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const CLIENT_INFO_HEADER_KEY = "Client-Information";
const CLIENT_INFO_HEADER_VALUE = "IntrinioRealtimeNodeSDKv4.2";
const MESSAGE_VERSION_HEADER_KEY = "UseNewEquitiesFormat";
const MESSAGE_VERSION_HEADER_VALUE = "v2";

async function doBackoff(context, callback) {
  let i = 0;
  let backoff = SELF_HEAL_BACKOFFS[i];
  let success = true;
  await callback.call(context).catch(() => success = false);
  while (!success) {
    console.log("Intrinio Realtime Client - Sleeping for %isec", (backoff/1000));
    await sleep(backoff);
    i = Math.min(i + 1, SELF_HEAL_BACKOFFS.length - 1);
    backoff = SELF_HEAL_BACKOFFS[i];
    success = true;
    await callback.call(context).catch(() => success = false);
  }
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

const defaultConfig = {
  provider: 'REALTIME', //REALTIME or DELAYED_SIP or NASDAQ_BASIC or MANUAL
  ipAddress: undefined,
  tradesOnly: false,
  isPublicKey: false
};

class IntrinioRealtime {
  constructor(accessKey, onTrade, onQuote, config = {}) {
    this._accessKey = accessKey;
    this._config = Object.assign(defaultConfig, config);
    this._token = null;
    this._websocket = null;
    this._isReady = false;
    this._attemptingReconnect = false;
    this._lastReset = Date.now();
    this._msgCount = 0;
    this._channels = new Map();
    this._onTrade = (onTrade && (typeof onTrade === "function")) ? onTrade : (_) => {};
    this._onQuote = (onQuote && (typeof onQuote === "function")) ? onQuote : (_) => {};
    this._float32Array = new Float32Array(1);
    this._backingByteArray = new Uint8Array(this._float32Array.buffer);

    if ((!this._accessKey) || (this._accessKey === "")) {
      throw "Intrinio Realtime Client - Access Key is required";
    }

    if (!this._config.provider) {
      throw "Intrinio Realtime Client - 'config.provider' must be specified";
    }
    else if ((this._config.provider !== "REALTIME") && (this._config.provider !== "MANUAL")
        && (this._config.provider !== "DELAYED_SIP") && (this._config.provider !== "NASDAQ_BASIC")) {
      throw "Intrinio Realtime Client - 'config.provider' must be either 'REALTIME' or 'MANUAL' or 'DELAYED_SIP' or 'NASDAQ_BASIC'";
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

    doBackoff(this, this._trySetToken).then(
        () => {doBackoff(this, this._resetWebsocket).then(
            () => {
              console.log("Intrinio Realtime Client - Startup succeeded");
              if (!this._config.isPublicKey) {
                process.on('SIGINT', () => {
                  console.log("Intrinio Realtime Client - Shutdown detected");
                  this.stop();
                  process.kill(process.pid, 'SIGINT');})
              }
            },
            () => {console.error("Intrinio Realtime Client - Startup failed. Unable to establish websocket connection.");})},
        () => {console.error("Intrinio Realtime Client - Startup failed. Unable to acquire auth token.");});
  }

  _getAuthUrl() {
    switch(this._config.provider) {
      case "REALTIME":
        if (this._config.isPublicKey) return "https://realtime-mx.intrinio.com/auth";
        else return "https://realtime-mx.intrinio.com/auth?api_key=" + this._accessKey;
      case "DELAYED_SIP":
        if (this._config.isPublicKey) return "https://realtime-delayed-sip.intrinio.com/auth";
        else return "https://realtime-delayed-sip.intrinio.com/auth?api_key=" + this._accessKey;
      case "NASDAQ_BASIC":
        if (this._config.isPublicKey) return "https://realtime-nasdaq-basic.intrinio.com/auth";
        else return "https://realtime-nasdaq-basic.intrinio.com/auth?api_key=" + this._accessKey;
      case "MANUAL":
        if (this._config.isPublicKey) return "http://" + this._config.ipAddress + "/auth";
        else return "http://" + this._config.ipAddress + "/auth?api_key=" + this._accessKey;
      default: throw "Intrinio Realtime Client - 'config.provider' not specified!";
    }
  }

  _getWebSocketUrl() {
    switch(this._config.provider) {
      case "REALTIME": return `wss://realtime-mx.intrinio.com/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}`;
      case "DELAYED_SIP": return `wss://realtime-delayed-sip.intrinio.com/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}`;
      case "NASDAQ_BASIC": return `wss://realtime-nasdaq-basic.intrinio.com/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}`;
      case "MANUAL": return "ws://" + this._config.ipAddress + `/socket/websocket?vsn=1.0.0&token=${this._token}&${CLIENT_INFO_HEADER_KEY}=${CLIENT_INFO_HEADER_VALUE}&${MESSAGE_VERSION_HEADER_KEY}=${MESSAGE_VERSION_HEADER_VALUE}`;
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

  _parseSocketMessage(data) {
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
          let trade = this._parseTrade(chunk)
          this._onTrade(trade)
          break;
        case 1:
        case 2:
          let quote = this._parseQuote(chunk)
          this._onQuote(quote)
          break;
        default: console.warn("Intrinio Realtime Client - Invalid message type: %i", msgType)
      }
      startIndex = endIndex
    }
  }

  _trySetToken() {
    if (this._config.isPublicKey)
      return new Promise((fulfill, reject) => {
        try {
          console.log("Intrinio Realtime Client - Authorizing (public key)...");
          const url = this._getAuthUrl();
          const xhr = new XMLHttpRequest();
          //TODO - add header
          xhr.onerror = (error) => {
            console.error("Intrinio Realtime Client - Error getting public key auth token: ", error);
            reject();
          };
          xhr.ontimeout = () => {
            console.error("Intrinio Realtime Client - Timed out trying to get auth token.");
            reject();
          };
          xhr.onabort = () => {
            console.error("Intrinio Realtime Client - Aborted attempt to get auth token.");
            reject();
          };
          xhr.onreadystatechange = () => {
            if (xhr.readyState === 4) {
              if (xhr.status === 401) {
                console.error("Intrinio Realtime Client - Unable to authorize (public key)");
                reject();
              }
              else if (xhr.status !== 200) {
                console.error("Intrinio Realtime Client - Could not get public key auth token: Status code (%i)", xhr.status);
                reject();
              }
              else {
                console.log("Intrinio Realtime Client - Authorized (public key)");
                this._token = xhr.responseText;
                fulfill();
              }
            }
          }
          xhr.open("GET", url, true);
          xhr.overrideMimeType("text/html");
          xhr.setRequestHeader('Content-Type', 'application/json');
          xhr.setRequestHeader('Authorization', 'Public ' + this._accessKey);
          xhr.setRequestHeader('Client-Information', "IntrinioRealtimeWebSDKv4.2.0");
          xhr.send();
        }
        catch (error) {
          console.error("Intrinio Realtime Client - Error in authorization (%s)", error);
          reject();
        }
      })
    else
      return new Promise((fulfill, reject) => {
        try {
          const protocol = (this._config.provider === "MANUAL") ? require('http') : require('https');
          console.log("Intrinio Realtime Client - Authorizing...");
          const url = this._getAuthUrl();
          const options = {
            headers: {
              [CLIENT_INFO_HEADER_KEY]: CLIENT_INFO_HEADER_VALUE
            }
          };
          const request = protocol.get(url, options, response => {
            if (response.statusCode == 401) {
              console.error("Intrinio Realtime Client - Unable to authorize");
              reject();
            }
            else if (response.statusCode != 200) {
              console.error("Intrinio Realtime Client - Could not get auth token: Status code (%i)", response.statusCode);
              reject();
            }
            else {
              response.on("data", data => {
                this._token = decoder.decode(data);
                console.log("Intrinio Realtime Client - Authorized");
                fulfill();
              });
            }
          })
          request.on("timeout", () => {
            console.error("Intrinio Realtime Client - Timed out trying to get auth token.");
            reject();
          });
          request.on("error", error => {
            console.error("Intrinio Realtime Client - Error getting auth token: ", error);
            reject();
          });
        }
        catch (error) {
          console.error("Intrinio Realtime Client - Error in authorization (%s)", error);
          reject();
        }
      });
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

  _resetWebsocket() {
    if (this._config.isPublicKey)
      return new Promise((fulfill, reject) => {
        try {
          console.info("Intrinio Realtime Client - Websocket initializing (public key)");
          let wsUrl = this._getWebSocketUrl();
          this._websocket = new WebSocket(wsUrl, {perMessageDeflate: false}, {headers: {
              [CLIENT_INFO_HEADER_KEY]: CLIENT_INFO_HEADER_VALUE,
              [MESSAGE_VERSION_HEADER_KEY]: MESSAGE_VERSION_HEADER_VALUE
            }});
          this._websocket.binaryType = "arraybuffer";
          this._websocket.onopen = () => {
            console.log("Intrinio Realtime Client - Websocket connected (public key)");
            if (this._channels.size > 0) {
              for (const [channel, tradesOnly] of this._channels) {
                let message = this._makeJoinMessage(tradesOnly, channel);
                console.info("Intrinio Realtime Client - Joining channel: %s (trades only = %s)", channel, tradesOnly);
                this._websocket.send(message);
              }
            }
            this._lastReset = Date.now();
            this._isReady = true;
            this._isReconnecting = false;
            fulfill(true);
          };
          this._websocket.onclose = (code) => {
            if (!this._attemptingReconnect) {
              this._isReady = false;
              console.info("Intrinio Realtime Client - Websocket closed (code: %o)", code);
              if (code != 1000) {
                console.info("Intrinio Realtime Client - Websocket reconnecting...");
                if (!this._isReady) {
                  this._attemptingReconnect = true;
                  if ((Date.now() - this._lastReset) > 86400000) {
                    doBackoff(this, this._trySetToken).then(() => {doBackoff(this, this._resetWebsocket)});
                  }
                  doBackoff(this, this._resetWebsocket);
                }
              }
            }
            else reject();
          }
          this._websocket.onerror = (error) => {
            console.error("Intrinio Realtime Client - Websocket error: %s", error);
            reject();
          }
          this._websocket.onmessage = (message) => {
            this._msgCount++;
            if (message.data instanceof ArrayBuffer)
              this._parseSocketMessage(message.data);
            else
              console.log("Intrinio Realtime Client - Message: %s", message.data);
          };
        }
        catch (error) {
          console.error("Intrinio Realtime Client - Error establishing public key websocket connection (%s)", error);
          reject();
        }
      });
    else
      return new Promise((fulfill, reject) => {
        try {
          console.info("Intrinio Realtime Client - Websocket initializing");
          const WebSocket = require('ws');
          let wsUrl = this._getWebSocketUrl();
          this._websocket = new WebSocket(wsUrl, {perMessageDeflate: false}, {headers: {
              [CLIENT_INFO_HEADER_KEY]: CLIENT_INFO_HEADER_VALUE,
              [MESSAGE_VERSION_HEADER_KEY]: MESSAGE_VERSION_HEADER_VALUE
            }});
          this._websocket.binaryType = "arraybuffer";
          this._websocket.on("open", () => {
            console.log("Intrinio Realtime Client - Websocket connected");
            if (this._channels.size > 0) {
              for (const [channel, tradesOnly] of this._channels) {
                let message = this._makeJoinMessage(tradesOnly, channel);
                console.info("Intrinio Realtime Client - Joining channel: %s (trades only = %s)", channel, tradesOnly);
                this._websocket.send(message);
              }
            }
            this._lastReset = Date.now();
            this._isReady = true;
            this._isReconnecting = false;
            fulfill(true);
          });
          this._websocket.on("close", (code, reason) => {
            if (!this._attemptingReconnect) {
              this._isReady = false;
              console.info("Intrinio Realtime Client - Websocket closed (code: %o)", code);
              if (code != 1000) {
                console.info("Intrinio Realtime Client - Websocket reconnecting...");
                if (!this._isReady) {
                  this._attemptingReconnect = true;
                  if ((Date.now() - this._lastReset) > 86400000) {
                    doBackoff(this, this._trySetToken).then(() => {doBackoff(this, this._resetWebsocket)});
                  }
                  doBackoff(this, this._resetWebsocket);
                }
              }
            }
            else reject();
          });
          this._websocket.on("error", (error) => {
            console.error("Intrinio Realtime Client - Websocket error: %s", error);
            reject();
          });
          this._websocket.on("message", (message) => {
            this._msgCount++;
            this._parseSocketMessage(message);
          });
        }
        catch (error) {
          console.error("Intrinio Realtime Client - Error establishing websocket connection (%s)", error);
          reject();
        }
      });
  }

  _join(symbol, tradesOnly) {
    if (this._channels.has("$lobby")) {
      console.warn("Intrinio Realtime Client - $lobby channel already joined. Other channels not necessary.");
    }
    if (!this._channels.has(symbol)) {
      this._channels.set(symbol, tradesOnly);
      let message = this._makeJoinMessage(tradesOnly, symbol);
      console.info("Intrinio Realtime Client - Joining channel: %s (trades only = %s)", symbol, tradesOnly);
      this._websocket.send(message);
    }
  }

  _leave(symbol) {
    if (this._channels.has(symbol)) {
      this._channels.delete(symbol);
      let message = this._makeLeaveMessage(symbol);
      console.info("Intrinio Realtime Client - Leaving channel: %s", symbol);
      this._websocket.send(message);
    }
  }

  async join(symbols, tradesOnly) {
    while (!this._isReady) {
      await sleep(1000);
    }
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
      console.error("Intrinio Realtime Client - If provided, 'tradesOnly' must be of type 'boolean', not '%s'", typeof tradesOnly);
    }
    else {
      console.error("Intrinio Realtime Client - Invalid use of 'join'");
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
        console.error("Intrinio Realtime Client - Invalid use of 'leave'");
      }
    }
  }

  async stop() {
    this._isReady = false;
    console.log("Intrinio Realtime Client - Leaving subscribed channels");
    for (const channel of this._channels.keys()) {
      this._leave(channel);
    }
    while (this._websocket.bufferedAmount > 0) {
      await sleep(500);
    }
    console.log("Intrinio Realtime Client - Websocket closing");
    if (this._websocket) {
      this._websocket.close(1000, "Terminated by client");
    }
  }

  getTotalMsgCount() {
    return this._msgCount;
  }
}

if (typeof window === 'undefined') {
  module.exports = IntrinioRealtime;
}
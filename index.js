'use strict'

const Promise = require('promise')
const WebSocket = require('ws')

const Encoder = new TextEncoder()
const Decoder = new TextDecoder("utf-8")

const HEARTBEAT_INTERVAL = 1000 * 20 // 20 seconds
const SELF_HEAL_BACKOFFS = [10000, 30000, 60000, 300000, 600000]

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function doBackoff(context, callback) {
  let i = 0
  let backoff = SELF_HEAL_BACKOFFS[i]
  let success = true
  await callback.call(context).catch(() => success = false)
  while (!success) {
    console.log("Intrinio Realtime Client - Sleeping for %isec", (backoff/1000))
    await sleep(backoff)
    i = Math.min(i + 1, SELF_HEAL_BACKOFFS.length - 1)
    backoff = SELF_HEAL_BACKOFFS[i]
    success = true
    await callback.call(context).catch(() => success = false)
  }
}

function readString(bytes, startPos, endPos) {
  if (startPos < 0) startPos = 0
  else if (startPos >= bytes.length) return ''
  else startPos |= 0
  if (endPos === undefined || endPos > bytes.length) endPos = bytes.length
  else endPos |= 0
  if (end <= start) return ''
  const chunk = bytes.slice(startPos, endPos)
  //return String.fromCharCode(chunk)
  Decoder.decode(chunk)
}

function readInt32(bytes, startPos = 0) {
  const first = bytes[startPos]
  const last = bytes[startPos + 3]
  if (first === undefined || last === undefined)
    console.error("Intrinio Realtime Client - Cannot read UInt32")
  return (
    first +
    (bytes[++startPos] * 256) +
    (bytes[++startPos] * 65536) +
    (last << 24)
  )
}

function readUInt32(bytes, startPos = 0) {
  const first = bytes[startPos]
  const last = bytes[startPos + 3]
  if (first === undefined || last === undefined)
    console.error("Intrinio Realtime Client - Cannot read UInt32")
  return (
    first +
    (bytes[++startPos] * 256) +
    (bytes[++startPos] * 65536) +
    (last * 16777216)
  )
}

function readUInt64(bytes, startPos = 0) {
  const first = bytes[startPos]
  const last = bytes[startPos + 7]
  if (first === undefined || last === undefined)
    console.error("Intrinio Realtime Client - Cannot read UInt64")
  const lower =
    first +
    (bytes[++startPos] * 256) +
    (bytes[++startPos] * 65536) +
    (bytes[++startPos] * 16777216)
  const upper =
    bytes[++startPos]
    (bytes[++startPos] * 256) +
    (bytes[++startPos] * 65536) +
    (last * 16777216)
  return (BigInt(lower) + (BigInt(upper) << 32))
}

function writeString(bytes, string, startPos) {
  if (startPos === undefined || startPos < 0 || startPos > bytes.length - 1) return bytes
  const encodedString = Encoder.encode(string)
  const bytesAvailable = bytes.length - startPos
  if (bytesAvailable < string.length) {
    const trimmedEncodedString = encodedString.slice(0, bytesAvailable)
    bytes.set(trimmedEncodedString, startPos)
  }
  else bytes.set(encodedString, startPos)
}

const defaultConfig = {
  provider: 'REALTIME', 
  ipAddress: undefined,
  tradesOnly: false,
  isPublicKey: false
}

class IntrinioRealtime {
  constructor(accessKey, onTrade, onQuote, config = {}) {
    this._accessKey = accessKey
    this._config = Object.assign(defaultConfig, config)
    this._token = null
    this._websocket = null
    this._isReady = false
    this._attemptingReconnect = false
    this._lastReset = Date.now()
    this._msgCount = 0
    this._channels = new Map()
    this._heartbeat = null
    this._onTrade = (onTrade && (typeof onTrade === "function")) ? onTrade : (_) => {}
    this._onQuote = (onQuote && (typeof onQuote === "function")) ? onQuote : (_) => {}

    if ((!this._accessKey) || (this._accessKey === "")) {
      throw "Intrinio Realtime Client - Access Key is required"
    }

    if (!this._config.provider) {
      throw "Intrinio Realtime Client - 'config.provider' must be specified"
    }
    else if ((this._config.provider !== "REALTIME") && (this._config.provider !== "MANUAL")) {
      throw "Intrinio Realtime Client - 'config.provider' must be either 'REALTIME' or 'MANUAL'"
    }

    if ((this._config.provider === "MANUAL") && ((!this._config.ipAddress) || (this._config.ipAddress === ""))) {
      throw "Intrinio Realtime Client - 'config.ipAddress' must be specified for manual configuration"
    }

    if(!onTrade) {
      throw "Intrinio Realtime Client - 'onTrade' callback is required"
    }

    if(!onQuote){
      this._config.tradesOnly = true
    }

    doBackoff(this, this._trySetToken).then(
      () => {doBackoff(this, this._resetWebsocket).then(
        () => {
          console.log("Intrinio Realtime Client - Startup succeeded")
          process.on('SIGINT', () => {
            console.log("Intrinio Realtime Client - Shutdown detected")
            this.stop()
            process.kill(process.pid, 'SIGINT')})
          },
        () => {console.error("Intrinio Realtime Client - Startup failed. Unable to establish websocket connection.")})},
      () => {console.error("Intrinio Realtime Client - Startup failed. Unable to acquire auth token.")})
  }

  _getAuthUrl() {
    switch(this._config.provider) {
      case "REALTIME":
        if (this._config.isPublicKey) return "https://realtime-mx.intrinio.com/auth"
        else return "https://realtime-mx.intrinio.com/auth?api_key=" + this._accessKey
      case "MANUAL":
        if (this._config.isPublicKey) return "http://" + this._config.ipAddress + "/auth"
        else return "http://" + this._config.ipAddress + "/auth?api_key=" + this._accessKey
      default: throw "Intrinio Realtime Client - 'config.provider' not specified!"
    }
  }

  _getWebSocketUrl() {
    switch(this._config.provider) {
      case "REALTIME": return "wss://realtime-mx.intrinio.com/socket/websocket?vsn=1.0.0&token=" + this._token
      case "MANUAL": return "ws://" + this._config.ipAddress + "/socket/websocket?vsn=1.0.0&token=" + this._token
      default: throw "Intrinio Realtime Client - 'config.provider' not specified!"
    }
  }

  _parseTrade(bytes, symbolLength) {
    return {
      Symbol: readString(bytes, 2, 2 + symbolLength),
      Price: readInt32(bytes, 2 + symbolLength) / 10000.0,
      Size: readUInt32(bytes, 6 + symbolLength),
      Timestamp: readUInt64(bytes, 10 + symbolLength),
      TotalVolume: readUInt32(bytes, 18 + symbolLength)
    }
  }

  _parseQuote (bytes, symbolLength) {
    return {
      Type: bytes[0],
      Symbol: readString(bytes, 2, 2 + symbolLength),
      Price: readInt32(bytes, 2 + symbolLength) / 10000.0,
      Size: readUInt32(bytes, 6 + symbolLength),
      Timestamp: readUInt64(bytes, 10 + symbolLength)
    }
  }

  _parseSocketMessage(data) {
    let bytes = Uint8Array.from(data)
    let msgCount = bytes[0]
    let startIndex = 1
    for (let i = 0; i < msgCount; i++) {
      let msgType = bytes[startIndex]
      let symbolLength = bytes[startIndex + 1]
      let endIndex = startIndex + symbolLength
      let chunk = null
      switch(msgType) {
        case 0:
          endIndex = endIndex + 22
          chunk = bytes.slice(startIndex, endIndex)
          let trade = this._parseTrade(chunk, symbolLength)
          startIndex = endIndex
          this._onTrade(trade)
          break;
        case 1:
        case 2:
          endIndex = endIndex + 18
          chunk = bytes.slice(startIndex, endIndex)
          let quote = this._parseQuote(chunk, symbolLength)
          startIndex = endIndex
          this._onQuote(quote)
          break;
        default: console.warn("Intrinio Realtime Client - Invalid message type: %i", msgType)
      }
    }
  }

  _trySetToken() {
    return new Promise((fulfill, reject) => {
      console.log("Intrinio Realtime Client - Authorizing...")
      const url = this._getAuthUrl()
      const xhr = new XMLHttpRequest()
      xhr.onreadystatechange = () => {
        if (xhr.readyState === 4) {
          if (xhr.status === 401) {
            console.error("Intrinio Realtime Client - Unable to authorize")
            reject()
          }
          else if (xhr.status !== 200) {
            console.error("Intrinio Realtime Client - Could not get auth token: Status code (%i)", response.statusCode)
            reject()
          }
          else {
            this._token = xhr.responseText
            console.log("Intrinio Realtime Client - Authorized")
            fulfill()
          }
        }
      }
      xhr.open("GET", url, true)
      xhr.overrideMimeType("text/html")
      xhr.setRequestHeader('Content-Type', 'application/json')
      if (this._config.isPublicKey) xhr.setRequestHeader('Authorization', 'Public ' + public_key)
      xhr.send()
    })
  }

  _makeJoinMessage(tradesOnly, symbol) {
    let message = null
    switch (symbol) {
      case "$lobby":
        message = new Uint8Array(11)
        message[0] = 74
        if (tradesOnly) {
          message[1] = 1
        } else {
          message[1] = 0
        }
        writeString(message, "$FIREHOSE", 2)
        return message
      default:
        message = new Uint8Array(2 + symbol.length)
        message[0] = 74
        if (tradesOnly) {
          message[1] = 1
        } else {
          message[1] = 0
        }
        writeString(message, symbol, 2)
        return message
    }
  }

  _makeLeaveMessage(symbol) {
    let message = null
    switch (symbol) {
      case "$lobby":
        message = new Uint8Array(10)
        message[0] = 76
        writeString(message, "$FIREHOSE", 1)
        return message
      default:
        message = new Uint8Array(1 + symbol.length)
        message[0] = 76
        writeString(message, symbol, 1)
        return message
    }
  }

  _resetWebsocket() {
    const self = this
    return new Promise((fulfill, reject) => {
      console.info("Intrinio Realtime Client - Websocket initializing")
      let wsUrl = self._getWebSocketUrl()
      self._websocket = new WebSocket(wsUrl, {perMessageDeflate: false})
      self._websocket.on("open", () => {
        console.log("Intrinio Realtime Client - Websocket connected")
        if (!self._heartbeat) {
          console.log("Intrinio Realtime Client - Starting heartbeat")
          self._heartbeat = setInterval(() => {
            if ((self._websocket) && (self._isReady)) {
              this._websocket.send("")
            }
          }, HEARTBEAT_INTERVAL)
        }
        if (self._channels.size > 0) {
          for (const [channel, tradesOnly] of self._channels) {
            let message = self._makeJoinMessage(tradesOnly, channel)
            console.info("Intrinio Realtime Client - Joining channel: %s (trades only = %s)", channel, tradesOnly)
            self._websocket.send(message)
          }
        }
        self._lastReset = Date.now()
        self._isReady = true
        self._isReconnecting = false
        fulfill(true)
      })
      self._websocket.on("close", (code, reason) => {
        if (!self._attemptingReconnect) {
          self._isReady = false
          clearInterval(self._heartbeat)
          self._heartbeat = null
          console.info("Intrinio Realtime Client - Websocket closed (code: %o)", code)
          if (code != 1000) {
            console.info("Intrinio Realtime Client - Websocket reconnecting...")
            if (!self._isReady) {
              self._attemptingReconnect = true
              if ((Date.now() - self._lastReset) > 86400000) {
                doBackoff(self, self._trySetToken).then(() => {doBackoff(self, self._resetWebsocket)})
                this._updateToken()
              }
              doBackoff(self, self._resetWebsocket)
            }
          }
        }
        else reject()
      })
      self._websocket.on("error", (error) => {
        console.error("Intrinio Realtime Client - Websocket error: %s", error)
        reject(false)
      })
      self._websocket.on("message", (message) => {
        self._msgCount++
        self._parseSocketMessage(message)
      })
    })
  }

  _join(symbol, tradesOnly) {
    if (this._channels.has("$lobby")) {
      console.warn("Intrinio Realtime Client - $lobby channel already joined. Other channels not necessary.")
    }
    if (!this._channels.has(symbol)) {
      this._channels.set(symbol, tradesOnly)
      let message = this._makeJoinMessage(tradesOnly, symbol)
      console.info("Intrinio Realtime Client - Joining channel: %s (trades only = %s)", symbol, tradesOnly)
      this._websocket.send(message)
    }
  }

  _leave(symbol) {
    if (this._channels.has(symbol)) {
      this._channels.delete(symbol)
      let message = this._makeLeaveMessage(symbol)
      console.info("Intrinio Realtime Client - Leaving channel: %s", symbol)
      this._websocket.send(message)
    }
  }

  async join(symbols, tradesOnly) {
    while (!this._isReady) {
      await sleep(1000)
    }
    let _tradesOnly = (this._config.tradesOnly ? this._config.tradesOnly : false) || (tradesOnly ? tradesOnly : false)
    if (symbols instanceof Array) {
      for (const symbol of symbols) {
        if (!this._channels.has(symbol)){
          this._join(symbol, _tradesOnly)
        }
      }
    }
    else if (typeof symbols === "string") {
      if (!this._channels.has(symbols)){
        this._join(symbols, _tradesOnly)
      }
    }
    else if ((typeof tradesOnly !== "undefined") || (typeof tradesOnly !== "boolean")) {
      console.error("Intrinio Realtime Client - If provided, 'tradesOnly' must be of type 'boolean', not '%s'", typeof tradesOnly)
    }
    else {
        console.error("Intrinio Realtime Client - Invalid use of 'join'")
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
        this._leave(symbols)
      }
    }
    else {
      if (arguments.length == 0) {
        for (const channel of this._channels.keys()) {
          this._leave(channel)
        }
      }
      else {
        console.error("Intrinio Realtime Client - Invalid use of 'leave'")
      }
    }
  }

  async stop() {
    this._isReady = false
    this._doReconnect = false
    console.log("Intrinio Realtime Client - Leaving subscribed channels")
    for (const channel of this._channels.keys()) {
      this._leave(channel)
    }
    while (this._websocket.bufferedAmount > 0) {
      await sleep(500)
    }
    console.log("Intrinio Realtime Client - Websocket closing")
    if (this._websocket) {
      this._websocket.close(1000, "Terminated by client")
    }
  }

  getTotalMsgCount() {
    return this._msgCount
  }
}

module.exports = IntrinioRealtime

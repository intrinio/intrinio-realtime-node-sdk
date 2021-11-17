'use strict'

const https = require('https')
const Promise = require('promise')
const WebSocket = require('ws')

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

const defaultConfig = {
  provider: 'REALTIME', 
  ipAddress: undefined,
  tradesOnly: false
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
      case "REALTIME": return "https://realtime-mx.intrinio.com/auth?api_key=" + this._accessKey
      case "MANUAL": return "http://" + this._config.ipAddress + "/auth?api_key=" + this._accessKey
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

  _parseTrade (buffer, symbolLength) {
    return {
      Symbol: buffer.toString("ascii", 2, 2 + symbolLength),
      Price: buffer.readInt32LE(2 + symbolLength) / 10000.0,
      Size: buffer.readUInt32LE(6 + symbolLength),
      Timestamp: buffer.readBigUInt64LE(10 + symbolLength),
      TotalVolume: buffer.readUInt32LE(18 + symbolLength)
    }
  }

  _parseQuote (buffer, symbolLength) {
    return {
      Type: buffer[0],
      Symbol: buffer.toString("ascii", 2, 2 + symbolLength),
      Price: buffer.readInt32LE(2 + symbolLength) / 10000.0,
      Size: buffer.readUInt32LE(6 + symbolLength),
      Timestamp: buffer.readBigUInt64LE(10 + symbolLength)
    }
  }

  _parseSocketMessage(bytes) {
    let buffer = Buffer.from(bytes)
    let msgCount = buffer[0]
    let startIndex = 1
    for (let i = 0; i < msgCount; i++) {
      let msgType = buffer[startIndex]
      let symbolLength = buffer[startIndex + 1]
      let endIndex = startIndex + symbolLength
      let chunk = null
      switch(msgType) {
        case 0:
          endIndex = endIndex + 22
          chunk = buffer.subarray(startIndex, endIndex)
          let trade = this._parseTrade(chunk, symbolLength)
          startIndex = endIndex
          this._onTrade(trade)
          break;
        case 1:
        case 2:
          endIndex = endIndex + 18
          chunk = buffer.subarray(startIndex, endIndex)
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
      let url = this._getAuthUrl()
      let request = https.get(url, response => {
        if (response.statusCode == 401) {
          console.error("Intrinio Realtime Client - Unable to authorize")
          reject(false)
        }
        else if (response.statusCode != 200) {
          console.error("Intrinio Realtime Client - Could not get auth token: Status code (%i)", response.statusCode)
          reject(false)
        }
        else {
          response.on("data", data => {
              this._token = Buffer.from(data).toString("utf8")
              console.log("Intrinio Realtime Client - Authorized")
              fulfill(true)
          })
        }
      })
      request.on("timeout", () => {
        console.error("Intrinio Realtime Client - Timed out trying to get auth token.")
        reject(false)
      })
      request.on("error", error => {
        console.error("Intrinio Realtime Client - Unable to get auth token (%s) ", error.String)
        reject(false)
      })
      request.end()
    })
  }

  _makeJoinMessage(tradesOnly, symbol) {
    let message = null
    switch (symbol) {
      case "$lobby":
        message = Buffer.alloc(11)
        message.writeUInt8(74, 0)
        if (tradesOnly) {
          message.writeUInt8(1, 1)
        } else {
          message.writeUInt8(0, 1)
        }
        message.write("$FIREHOSE", 2, "ascii")
        return message
      default:
        message = Buffer.alloc(2 + symbol.length)
        message.writeUInt8(74, 0)
        if (tradesOnly) {
          message.writeUInt8(1, 1)
        } else {
          message.writeUInt8(0, 1)
        }
        message.write(symbol, 2, "ascii")
        return message
    }
  }

  _makeLeaveMessage(symbol) {
    let message = null
    switch (symbol) {
      case "$lobby":
        message = Buffer.alloc(10)
        message.writeUInt8(76, 0)
        message.write("$FIREHOSE", 1, "ascii")
        return message
      default:
        message = Buffer.alloc(1 + symbol.length)
        message.writeUInt8(76, 0)
        message.write(symbol, 1, "ascii")
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

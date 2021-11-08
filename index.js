'use strict'

const https = require('https')
const Promise = require('promise')
const WebSocket = require('ws')
const Config = require('./config.json')

const HEARTBEAT_INTERVAL = 1000 * 20 // 20 seconds
const SELF_HEAL_BACKOFFS = [10000, 30000, 60000, 300000, 600000]

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

class IntrinioRealtime {
  constructor(onTrade, onQuote) {
    this._token = null
    this._tokenTime = null
    this._websocket = null
    this._isReady = false
    this._isReconnecting = false
    this._lastReset = Date.now()
    this._msgCount = 0
    this._channels = new Map()
    this._heartbeat = null
    this._onTrade = (onTrade && (typeof onTrade === "function")) ? onTrade : (_) => {}
    this._onQuote = (onQuote && (typeof onQuote === "function")) ? onQuote : (_) => {}

    if (!Config) {
      throw "Intrinio Realtime Client - config.json not found"
    }

    if ((!Config.ApiKey) || (Config.ApiKey === "")) {
      throw "Intrinio Realtime Client - API Key is required"
    }

    if ((!Config.Provider) || ((Config.Provider != "REALTIME") || (Config.Provider != "MANUAL"))){
      throw "Intrinio Realtime Client - 'Provider' must be specified and valid"
    }

    if ((Config.Provider = "MANUAL") && ((!Config.IPAddress) || (Config.IPAddress === ""))) {
      throw "Intrinio Realtime Client - 'IPAddress' must be specified for manual configuration"
    }
  }

  _getAuthUrl() {
    switch(Config.Provider) {
      case "REALTIME": return "https://realtime-mx.intrinio.com/auth?api_key=" + Config.ApiKey
      case "MANUAL": return "http://" + Config.IPAddress + "/auth?api_key=" + Config.ApiKey
      default: throw "Intrinio Realtime Client - 'Provider' not specified!"
    }
  }

  _getWebSocketUrl() {
    switch(Config.Provider) {
      case "REALTIME": return "wss://realtime-mx.intrinio.com/socket/websocket?vsn=1.0.0&token=" + this._token
      case "MANUAL": return "ws://" + Config.IPAddress + "/socket/websocket?vsn=1.0.0&token=" + this._token
      default: throw "Intrinio Realtime Client - 'Provider' not specified!"
    }
  }

  _parseTrade (buffer, symbolLength) {
    return {
      Symbol: buffer.toString("ascii", 2, symbolLength),
      Price: float (buffer.readInt32LE(2 + symbolLength)) / 10000.0,
      Size: buffer.readUInt32LE(6 + symbolLength),
      Timestamp: buffer.readUInt64LE(10 + symbolLength),
      TotalVolume: buffer.readUInt32LE(18 + symbolLength)
    }
  }

  _parseQuote (buffer, symbolLength) {
    return {
      Type: int(buffer[0]),
      Symbol: buffer.toString("ascii", 2, symbolLength),
      Price: float(buffer.readInt32LE(2 + symbolLength)) / 10000.0,
      Size: buffer.readUInt32LE(6 + symbolLength),
      Timestamp: buffer.readUInt64LE(10 + symbolLength)
    }
  }

  _parseSocketMessage(bytes) {
    let buffer = Buffer.from(bytes)
    let msgCount = int(buffer[0])
    let startIndex = 1
    for (let i = 0; i < msgCount; i++) {
      let msgType = int(buffer[startIndex])
      let symbolLength = int(buffer[startIndex + 1])
      switch(msgType) {
        case 0:
          let endIndex = startIndex + 22 + symbolLength
          let chunk = buffer.subarray(startIndex, endIndex)
          let trade = this._parseTrade(chunk, symbolLength)
          startIndex = endIndex
          this._onTrade(trade)
          break;
        case 1:
        case 2:
          let endIndex = startIndex + 18 + symbolLength
          let chunk = buffer.subarray(startIndex, endIndex)
          let trade = this._parseQuote(chunk, symbolLength)
          startIndex = endIndex
          this._onQuote(trade)
          break;
        default: console.warn("Intrinio Realtime Client - Invalid message type: {0}", msgType)
      }
    }
  }

  _heartbeatFn() {
    if ((this._websocket) && (this._isReady)) {
      this._websocket.send("")
    }
  }

  async _doBackoff(callback) {
    let i = 0
    let backoff = SELF_HEAL_BACKOFFS[i]
    let success = callback()
    while (!success) {
      sleep(backoff)
      i = Math.min(i + 1, SELF_HEAL_BACKOFFS.length - 1)
      backoff = SELF_HEAL_BACKOFFS[i]
      success = callback()
    }
  }

  _trySetToken() {
    console.log("Intrinio Realtime Client - Authorizing...")
    let url = this._getAuthUrl()
    let request = https.get(url, response => {
      if (response.statusCode == 401) {
        console.error("Intrinio Realtime Client - Unable to authorize")
        return false
      }
      else if (response.statusCode != 200) {
        console.error("Intrinio Realtime Client - Could not get auth token: Status code " + response.statusCode)
        return false
      }
      else {
        response.on("data", data => {
            this._token = Buffer.from(data).toString("utf8")
            console.log("Authorized")
            return true
        })
      }
    })
    request.on("timeout", () => {
      console.error("Intrinio Realtime Client - Timed out trying to get auth token.")
      return false
    })
    request.on("error", error => {
      console.error("Intrinio Realtime Client - Unable to get auth token: "+ error)
      return false
    })
    request.end()
  }

  _updateToken() {
    if (this._tokenTime < (Date.now.getDate() - 1)) {
      this._doBackoff(this._trySetToken)
    }
  }

  _makeJoinMessage(tradesOnly, symbol) {
    switch (symbol) {
      case "lobby":
        let message = Buffer.alloc(11)
        message.writeUInt8(74, 0)
        if (tradesOnly) {
          message.writeUInt8(1, 1)
        } else {
          message.writeUInt8(0, 1)
        }
        message.write("$FIREHOSE", "ascii", 2)
        return message
      default:
        let message = Buffer.alloc(2 + symbol.length)
        message.writeUInt8(74, 0)
        if (tradesOnly) {
          message.writeUInt8(1, 1)
        } else {
          message.writeUInt8(0, 1)
        }
        message.write(symbol, "ascii", 2)
        return message
    }
  }

  _makeLeaveMessage(symbol) {
    switch (symbol) {
      case "lobby":
        let message = Buffer.alloc(10)
        message.writeUInt8(76, 0)
        message.write("$FIREHOSE", "ascii", 1)
        return message
      default:
        let message = Buffer.alloc(1 + symbol.length)
        message.writeUInt8(76, 0)
        message.write(symbol, "ascii", 1)
        return message
    }
  }  

  _onOpen(_) {
    console.log("Intrinio Realtime Client - Websocket connected.")
    this._isReady = true
    this._isReconnecting = false
    if (!this._heartbeat) {
      this._heartbeat = setInterval(this._heartbeatFn, HEARTBEAT_INTERVAL)
    }
    if (this._channels.size > 0) {
      for (const [channel, tradesOnly] of this._channels) {
        let message = this._makeJoinMessage(tradesOnly, channel)
        console.info("Intrinio Realtime Client - Joining channel: {0} (trades only = {1})", channel, tradesOnly)
        this._websocket.send(message)
      }
    }
  }

  _onClose(code, reason) {
    if (!this._isReconnecting) {
      console.info("Intrinio Realtime Client - Websocket closed ({0}): {1}", code, reason)
      this._isReady = false
      this._tryReconnect()
    }
  }

  _onError(error) {
    console.error("Intrinio Realtime Client - Websocket error: {0}", error)
  }

  _onMessage(message) {
    this._msgCount++
    this._parseSocketMessage(message)
  }

  _resetWebsocket() {
    console.info("Intrinio Realtime Client - Websocket initializing")
    let wsUrl = this._getWebSocketUrl()
    let ws = new WebSocket(wsUrl, {perMessageDeflate: false})
    ws.on("open", this._onOpen)
    ws.on("close", this._onClose)
    ws.on("error", this._onError)
    ws.on("message", this._onMessage)
    this._websocket = ws
    this._lastReset = Date.now()
  }

  _reconnectFn() {
    console.info("Intrinio Realtime Client - Websocket reconnecting...")
    if (this._isReady) true
    else {
      this._isReconnecting = true
      if (this._lastReset < (Date.now.getDate() - 5)) {
        this._updateToken()
      }
      this._resetWebsocket()
    }
  }

  _tryReconnect() {
    this._doBackoff(this._reconnectFn)
  }

  _join(symbol, tradesOnly) {
    if (!this._channels.has(symbol)) {
      this._channels.set(symbol, tradesOnly)
      let message = this._makeJoinMessage(tradesOnly, symbol)
      console.info("Intrinio Realtime Client - Joining channel: {0} (trades only = {1})", symbol, tradesOnly)
      this.websocket.send(message, (_) => {this._channels.delete(symbol)})
    }
  }

  _leave(symbol, tradesOnly) {
    if (this._channels.has(symbol)) {
      this._channels.delete(symbol)
      let message = this._makeLeaveMessage(tradesOnly, symbol)
      console.info("Intrinio Realtime Client - Leaving channel: {0} (trades only = {1})", symbol, tradesOnly)
      this._websocket.send(message)
    }
  }

  async join() {
    while (!this._isReady) {
      await sleep(1000)
    }
    let tradesOnly = Config.TradesOnly ? Config.TradesOnly : false
    for (const symbol of Config.Symbols) {
      if (!this._channels.has(symbol)){
        this._join(symbol, tradesOnly)
      }
    }
  } 
  
  async join(symbol, tradesOnly) {
    while (!this._isReady) {
      await sleep(1000)
    }
    let _tradesOnly = (Config.TradesOnly ? Config.TradesOnly : false) || tradesOnly
    if (!this._channels.has(symbol)){
      this._join(symbol, _tradesOnly)
    }
  }

  join(symbols, tradesOnly) {
    while (!this._isReady) {
      await sleep(1000)
    }
    let _tradesOnly = (Config.TradesOnly ? Config.TradesOnly : false) || tradesOnly
    for (const symbol of symbols) {
      if (!this._channels.has(symbol)){
        this._join(symbol, _tradesOnly)
      }
    }
  }

  leave() {
    for (const [channel, tradesOnly] of this._channels) {
      this._leave(channel, tradesOnly)
    }
  }

  leave(symbol) {
    if (this._channels.has(symbol)) {
      this._leave(symbol, this._channels.get(symbol))
    }
  }

  leave(symbols) {
    for (const symbol of symbols) {
      if (this._channels.has(symbol)) {
        this._leave(symbol, this._channels.get(symbol))
      }
    }
  }

  stop() {
    clearInterval(this._heartbeat)
    this._isReady = false
    for (const channel of this._channels.keys()) {
      this._leave(channel)
    }
    console.log("Intrinio Realtime Client - Websocket closing")
    if (this._websocket) {
      this._websocket.close(1000, "Terminated by client")
    }
    console.log("Intrinio Realtime Client - Websocket closed")
  }

  getTotalMsgCount() {
    this._msgCount
  }
}

module.exports = IntrinioRealtime

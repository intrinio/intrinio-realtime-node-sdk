'use strict'

const https = require('https')
const Promise = require('promise')
const { threadId } = require('worker_threads')
const WebSocket = require('ws')
const Config = require('./config.json')

const HEARTBEAT_INTERVAL = 1000 * 20 // 20 seconds
const SELF_HEAL_BACKOFFS = [10000, 30000, 60000, 300000, 600000]

class IntrinioRealtime {
  constructor(apiKey, onTrade, onQuote) {
    this.token = null
    this.tokenTime = null
    this.websocket = null
    this.isReady = false
    this.isReconnecting = false
    this.lastReset = Date.now()
    this.dataMsgCount = 0
    this.textMsgCount = 0
    this.channels = {}

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

  getAuthUrl() {
    switch(Config.Provider) {
      case "REALTIME": return "https://realtime-mx.intrinio.com/auth?api_key=" + Config.ApiKey
      case "MANUAL": return "http://" + Config.IPAddress + "/auth?api_key=" + Config.ApiKey
      default: throw "Intrinio Realtime Client - 'Provider' not specified!"
    }
  }

  getWebSocketUrl(token) {
    switch(Config.Provider) {
      case "REALTIME": return "wss://realtime-mx.intrinio.com/socket/websocket?vsn=1.0.0&token=" + token
      case "MANUAL": return "ws://" + Config.IPAddress + "/socket/websocket?vsn=1.0.0&token=" + token
      default: throw "Intrinio Realtime Client - 'Provider' not specified!"
    }
  }

  parseTrade (buffer, symbolLength) {
    return {
      Symbol: buffer.toString("ascii", 2, symbolLength),
      Price: float (buffer.readInt32LE(2 + symbolLength)) / 10000.0,
      Size: buffer.readUInt32LE(6 + symbolLength),
      Timestamp: buffer.readUInt64LE(10 + symbolLength),
      TotalVolume: buffer.readUInt32LE(18 + symbolLength)
    }
  }

  parseQuote (buffer, symbolLength) {
    return {
      Type: int(buffer[0]),
      Symbol: buffer.toString("ascii", 2, symbolLength),
      Price: float(buffer.readInt32LE(2 + symbolLength)) / 10000.0,
      Size: buffer.readUInt32LE(6 + symbolLength),
      Timestamp: buffer.readUInt64LE(10 + symbolLength)
    }
  }

  parseSocketMessage(bytes) {
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
          let trade = this.parseTrade(chunk, symbolLength)
          startIndex = endIndex
          onTrade(trade)
          break;
        case 1:
        case 2:
          let endIndex = startIndex + 18 + symbolLength
          let chunk = buffer.subarray(startIndex, endIndex)
          let trade = this.parseQuote(chunk, symbolLength)
          startIndex = endIndex
          onQuote(trade)
          break;
        default: console.warn("Intrinio Realtime Client - Invalid message type: {0}", msgType)
      }
    }
  }

  doBackoff(callback) {
    let i = 0
    let backoff = SELF_HEAL_BACKOFFS[i]
    let success = callback()
    while (!success) {
      i = Math.min(i + 1, SELF_HEAL_BACKOFFS.length - 1)
      backoff = SELF_HEAL_BACKOFFS[i]
      success = callback()
    }
  }

  trySetToken() {
    console.log("Intrinio Realtime Client - Authorizing...")
    let url = this.getAuthUrl()
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
            this.token = Buffer.from(data).toString("utf8")
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

  getToken() {
    if ((Date.now.getDate() - 1) > this.tokenTime) {
      return token
    } else {
      this.doBackoff(this.trySetToken)
      return token
    }
  }

  makeJoinMessage(tradesOnly, symbol) {
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

  makeLeaveMessage(symbol) {
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






    // Establish connection
    this._connect()

    // Send heartbeat at intervals
    this.heartbeat_ref = setInterval(()=> {
      this._heartbeat()
    }, HEARTBEAT_INTERVAL)
  }

  _log(...parts) {
    var message = "IntrinioRealtime | "
    parts.forEach(part => {
      if (typeof part === 'object') { part = JSON.stringify(part) }
      message += part
    })
    console.log(message)
  }

  _debug(...parts) {
    if (this.options.debug) {
      this._log(...parts)
    }
  }

  _throw(e) {
    let handled = false
    if (typeof e === 'string') {
      e = "IntrinioRealtime | " + e
    }
    if (typeof this.error_callback === 'function') {
      this.error_callback(e)
      handled = true
    }
    if (this.listenerCount('error') > 0) {
      this.emit('error', e)
      handled = true
    }
    if (!handled) {
      throw e
    }
  }

  _connect() {
    this._debug("Connecting...")

    this.afterConnected = new Promise((fulfill, reject) => {
      this._refreshToken().then(() => {
        this._refreshWebsocket().then(() => {
          fulfill()
        }, reject)
      }, reject)
    })

    this.afterConnected.done(() => {
      this.ready = true
      this.emit('connect')
      this._stopSelfHeal()
      if (["iex", "cryptoquote", "fxcm"].includes(this.options.provider)) {
        this._refreshChannels() 
      }
    },
    () => {
      this.ready = false
      this._trySelfHeal()
    })

    return this.afterConnected
  }
  
  _makeAuthUrl() {
    var auth_url = {
      host: "",
      path: ""
    }

    if (this.options.provider == "iex") {
      auth_url = {
        host: "realtime.intrinio.com",
        path: "/auth"
      }
    }
    else if (this.options.provider == "quodd") {
      auth_url = {
        host: "api.intrinio.com",
        path: "/token?type=QUODD"
      }
    }
    else if (this.options.provider == "cryptoquote") {
      auth_url = {
        host: "crypto.intrinio.com",
        path: "/auth"
      }
    }
    else if (this.options.provider == "fxcm") {
      auth_url = {
        host: "fxcm.intrinio.com",
        path: "/auth"
      }
    }

    if (this.options.api_key) {
      auth_url = this._makeAPIAuthUrl(auth_url)
    }

    return auth_url
  }

  _makeAPIAuthUrl(auth_url) {
    var path = auth_url.path

    if (path.includes("?")) {
      path = path + "&"
    }
    else {
      path = path + "?"
    }

    auth_url.path = path + "api_key=" + this.options.api_key
    return auth_url
  }

  _makeHeaders() {
    if (this.options.api_key) {
      return {
        'Content-Type': 'application/json'
      }
    }
    else {
      var { username, password } = this.options

      return {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + new Buffer((username + ':' + password)).toString('base64')
      }
    }
  }

  _refreshToken() {
    this._debug("Requesting auth token...")

    return new Promise((fulfill, reject) => {
      var agent = this.options.agent || false
      var { host, path } = this._makeAuthUrl()
      var headers = this._makeHeaders()

      // Get token
      var options = {
        host: host,
        path: path,
        agent: agent,
        headers: headers
      }
      
      var req = https.get(options, res => {
        if (res.statusCode == 401) {
          this._throw("Unable to authorize")
          reject()
        }
        else if (res.statusCode != 200) {
          console.error("IntrinioRealtime | Could not get auth token: Status code " + res.statusCode)
          reject()
        }
        else {
          res.on('data', data => {
            this.token = new Buffer(data, 'base64').toString()
            this._debug("Received auth token!")
            fulfill()
          })
        }
      })

      req.on('error', (e) => {
        console.error("IntrinioRealtime | Could not get auth token: " + e)
        reject(e)
      })

      req.end()
    })
  }

  _makeSocketUrl() {
    if (this.options.provider == "iex") {
      return 'wss://realtime.intrinio.com/socket/websocket?vsn=1.0.0&token=' + encodeURIComponent(this.token)
    }
    else if (this.options.provider == "quodd") {
      return 'wss://www5.quodd.com/websocket/webStreamer/intrinio/' + encodeURIComponent(this.token)
    }
    else if (this.options.provider == "cryptoquote") {
      return 'wss://crypto.intrinio.com/socket/websocket?vsn=1.0.0&token=' + encodeURIComponent(this.token)
    }
    else if (this.options.provider == "fxcm") {
      return 'wss://fxcm.intrinio.com/socket/websocket?vsn=1.0.0&token=' + encodeURIComponent(this.token)
    }
  }
  
  _refreshWebsocket() {
    this._debug("Establishing websocket...")

    return new Promise((fulfill, reject) => {
      if (this.websocket) {
        this.websocket.close(WS_CLOSE_REASON_USER, "User terminated")
      }

      var socket_url = this._makeSocketUrl()
      this.websocket = new WebSocket(socket_url, {perMessageDeflate: false, agent: this.options.agent})

      this.websocket.on('open', () => {
        this._debug("Websocket connected!")
        fulfill()
      })

      this.websocket.on('close', (code, reason) => {
        this._debug("Websocket closed!")
        if (code != WS_CLOSE_REASON_USER) {
          this.joinedChannels = {}
          this._trySelfHeal()
        }
      })

      this.websocket.on('error', e => {
        console.error("IntrinioRealtime | Websocket error: " + e)
        this.joinedChannels = {}
        reject(e)
      })

      this.websocket.on('message', (data, flags) => {
        try {
          var message = JSON.parse(data)
        }
        catch (e) {
          this._debug('Non-quote message: ', data)
          return
        }

        var quote = null

        if (message.event == "phx_reply" && message.payload.status == "error") {
          var error = message.payload.response
          console.error("IntrinioRealtime | Websocket data error: " + error)
          this._throw(error)
        }
        else if (this.options.provider == "iex") {
          if (message.event === 'quote') {
            quote = message.payload
          }
        }
        else if (this.options.provider == "quodd") {
          if (message.event === 'info' && message.data.message === 'Connected') {
            this._refreshChannels()
          }
          else if (message.event === 'quote' || message.event == 'trade') {
            quote = message.data
          }
        }
        else if (this.options.provider == "cryptoquote") {
          if (message.event === 'book_update' || message.event === 'ticker' || message.event === 'trade') {
            quote = message.payload
          }
        }
        else if (this.options.provider == "fxcm") {
          if (message.event === 'price_update') {
            quote = message.payload
          }
        }
        
        if (quote) {
          if (typeof this.quote_callback === 'function') {
            this.quote_callback(quote)
          }
          this.emit('quote', quote)
          this._debug('Quote: ', quote)
        }
        else {
          this._debug('Non-quote message: ', data)
        }
      })
    })
  }

  _trySelfHeal() {
    this._log("No connection! Retrying...")

    var time = this.self_heal_backoff[0]
    if (this.self_heal_backoff.length > 1) {
      time = this.self_heal_backoff.shift()
    }

    if (this.self_heal_ref) { clearTimeout(this.self_heal_ref) }

    this.self_heal_ref = setTimeout(() => {
      this._connect()
    }, time)
  }

  _stopSelfHeal() {
    this.self_heal_backoff = Array.from(SELF_HEAL_BACKOFFS)

    if (this.self_heal_ref) {
      clearTimeout(this.self_heal_ref)
      this.self_heal_ref = null
    }
  }

  _refreshChannels() {
    if (!this.ready) {
      return
    }
    
    // Join new channels
    for (var channel in this.channels) {
      if (!this.joinedChannels[channel]) {
        this.websocket.send(JSON.stringify(this._makeJoinMessage(channel)))
        this._debug('Joined channel: ', channel)
      }
    }
    
    // Leave old channels
    for (var channel in this.joinedChannels) {
      if (!this.channels[channel]) {
        this.websocket.send(JSON.stringify(this._makeLeaveMessage(channel)))
        this._debug('Left channel: ', channel)
      }
    }
    
    this.joinedChannels = {}
    for (var channel in this.channels) {
      this.joinedChannels[channel] = true
    }
  }

  _makeHeartbeatMessage() {
    if (this.options.provider == "quodd") {
      return {event: 'heartbeat', data: {action: 'heartbeat', ticker: Date.now()}}
    }
    else if (["iex", "cryptoquote", "fxcm"].includes(this.options.provider)) {
      return {topic: 'phoenix', event: 'heartbeat', payload: {}, ref: null}
    }
  }

  _heartbeat() {
    this.afterConnected.then(() => {
      var message = JSON.stringify(this._makeHeartbeatMessage())
      this._debug("Heartbeat: ", message)
      this.websocket.send(message)
    })
  }

  _parseChannels(args) {
    var channels = []

    args.forEach(arg => {
      if (Array.isArray(arg)) {
        arg.forEach(sub_arg => {
          if (typeof sub_arg === 'string') {
            channels.push(sub_arg.trim())
          }
          else {
            this._throw("Invalid channel provided")
          }
        })
      }
      else if (typeof arg === 'string') {
        channels.push(arg.trim())
      }
      else {
        this._throw("Invalid channel provided")
      }
    })

    channels.forEach(channel => {
      if (channel.length == 0) {
        this._throw("Invalid channel provided")
      }
    })

    return channels
  }
  
  _makeJoinMessage(channel) {
    if (this.options.provider == "iex") {
      return {
        topic: this._parseIexTopic(channel),
        event: 'phx_join',
        payload: {},
        ref: null
      }
    }
    else if (this.options.provider == "quodd") {
      return {
        event: "subscribe",
        data: {
          ticker: channel,
          action: "subscribe"
        }
      }
    }
    else if (["cryptoquote", "fxcm"].includes(this.options.provider)) {
      return {
        topic: channel,
        event: 'phx_join',
        payload: {},
        ref: null
      }
    }
  }
  
  _makeLeaveMessage(channel) {
    if (this.options.provider == "iex") {
      return {
        topic: this._parseIexTopic(channel),
        event: 'phx_leave',
        payload: {},
        ref: null
      }
    }
    else if (this.options.provider == "quodd") {
      return {
        event: "unsubscribe",
        data: {
          ticker: channel,
          action: "unsubscribe"
        }
      }
    }
    else if (["cryptoquote", "fxcm"].includes(this.options.provider)) {
      return {
        topic: channel,
        event: 'phx_leave',
        payload: {},
        ref: null
      }
    }
  }

  _parseIexTopic(channel) {
    var topic = null
    if (channel == "$lobby") {
      topic = "iex:lobby"
    }
    else if (channel == "$lobby_last_price") {
      topic = "iex:lobby:last_price"
    }
    else {
      topic = "iex:securities:" + channel
    }
    return topic
  }

  _validAPIKey(api_key) {
    if (typeof api_key !== 'string') {
      return false
    }

    if (api_key === "") {
      return false
    }

    return true
  }

  destroy() {
    if (this.token_expiration_ref) {
      clearInterval(this.token_expiration_ref)
    }

    if (this.heartbeat_ref) {
      clearInterval(this.heartbeat_ref)
    }

    if (this.self_heal_ref) {
      clearTimeout(this.self_heal_ref)
    }

    if (this.websocket) {
      this.websocket.close(WS_CLOSE_REASON_USER, "User terminated")
    }
  }

  onError(callback) {
    this.error_callback = callback
  }

  onQuote(callback) {
    this.quote_callback = callback
  }

  join(...channels) {
    var channels = this._parseChannels(channels)
    channels.forEach(channel => {
      this.channels[channel] = true
    })
    
    this._refreshChannels()
  }

  leave(...channels) {
    var channels = this._parseChannels(channels)
    channels.forEach(channel => {
      delete this.channels[channel]
    })
    
    this._refreshChannels()
  }

  leaveAll() {
    this.channels = {}
    this._refreshChannels()
  }

  listConnectedChannels() {
    var channels = []
    for (var channel in this.joinedChannels) {
      channels.push(channel)
    }
    return channels
  }
}

module.exports = IntrinioRealtime

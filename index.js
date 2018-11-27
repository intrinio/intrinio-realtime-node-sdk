'use strict'

const https = require('https')
const Promise = require('promise')
const WebSocket = require('ws')
const EventEmitter = require('events').EventEmitter

const HEARTBEAT_INTERVAL = 1000 * 3 // 3 seconds
const SELF_HEAL_BACKOFFS = [0,100,500,1000,2000]
const WS_CLOSE_REASON_USER = 1000

class IntrinioRealtime extends EventEmitter {
  constructor(options) {
    super()
    
    this.options = options
    this.token = null
    this.websocket = null
    this.ready = false
    this.channels = {}
    this.joinedChannels = {}
    this.afterConnected = null // Promise
    this.self_heal_backoff = Array.from(SELF_HEAL_BACKOFFS)
    this.self_heal_ref = null
    this.quote_callback = null
    this.error_callback = null

    // Parse options
    if (!options) {
      this._throw("Need a valid options parameter")
    }

    if (!options.username) {
      this._throw("Need a valid username")
    }

    if (!options.password) {
      this._throw("Need a valid password")
    }
    
    if (!options.provider || (options.provider != "iex" && options.provider != "quodd")) {
      this._throw("Need a valid provider: iex or quodd")
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
      if (this.options.provider == "iex") { 
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
    if (this.options.provider == "iex") {
      return {
        host: "realtime.intrinio.com",
        path: "/auth"
      }
    }
    else if (this.options.provider == "quodd") {
      return {
        host: "api.intrinio.com",
        path: "/token?type=QUODD"
      }
    }
  }

  _refreshToken() {
    this._debug("Requesting auth token...")

    return new Promise((fulfill, reject) => {
      var { username, password } = this.options
      var agent = this.options.agent || false
      var { host, path } = this._makeAuthUrl()

      // Get token
      var options = {
        host: host,
        path: path,
        agent: agent,
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Basic ' + new Buffer((username + ':' + password)).toString('base64')
        }
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
          this._trySelfHeal()
        }
      })

      this.websocket.on('error', e => {
        console.error("IntrinioRealtime | Websocket error: " + e)
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
        
        if (this.options.provider == "iex") {
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
    if (this.options.provider == "iex") {
      return {topic: 'phoenix', event: 'heartbeat', payload: {}, ref: null}
    }
    else if (this.options.provider == "quodd") {
      return {event: 'heartbeat', data: {action: 'heartbeat', ticker: Date.now()}}
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
      if (channel.length == 0 || channel.length > 20) {
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

'use strict'

const https = require('https')
const Promise = require('promise')
const WebSocket = require('ws')
const EventEmitter = require('events').EventEmitter

const TOKEN_EXPIRATION_INTERVAL = 1000 * 60 * 60 * 24 * 7 // 1 week
const HEARTBEAT_INTERVAL = 1000 * 20 // 20 seconds
const SELF_HEAL_BACKOFFS = [0,100,500,1000,2000,5000]
const WS_CLOSE_REASON_USER = 1000
const HOST = "realtime.intrinio.com"
const PORT = 443
const WS_PROTOCOL = "wss"

class IntrinioRealtime extends EventEmitter {
  constructor(options) {
    super()
    
    this.options = options
    this.token = null
    this.websocket = null
    this.channels = {}
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

    // Establish connection
    this._connect()

    // Refresh token every week
    this.token_expiration_ref = setInterval(() => {
      this._connect()
    }, TOKEN_EXPIRATION_INTERVAL)

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

  _connect(rejoin=false) {
    this._debug("Connecting...")

    this.afterConnected = new Promise((fulfill, reject) => {
      this._refreshToken().then(() => {
        this._refreshWebsocket().then(() => {
          fulfill()
        }, reject)
      }, reject)
    })

    this.afterConnected.done(() => {
      this.emit('connect')
      this._stopSelfHeal()
      if (rejoin) { this._rejoinChannels() }
    },
    () => {
      this._trySelfHeal()
    })

    return this.afterConnected
  }

  _refreshToken() {
    this._debug("Requesting auth token...")

    return new Promise((fulfill, reject) => {
      var { username, password } = this.options

      // Get token
      var options = {
        host: HOST,
        port: PORT,
        path: '/auth',
        method: 'GET',
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

  _refreshWebsocket() {
    this._debug("Establishing websocket...")

    return new Promise((fulfill, reject) => {
      if (this.websocket) {
        this.websocket.close(WS_CLOSE_REASON_USER, "User terminated")
      }

      var socket_url = WS_PROTOCOL+'://'+HOST+":"+PORT+'/socket/websocket?vsn=1.0.0&token=' + encodeURIComponent(this.token)
      this.websocket = new WebSocket(socket_url, {perMessageDeflate: false})

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
        var message = JSON.parse(data)
        if (message.event === 'quote') {
          var quote = message.payload
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
      this._connect(true)
    }, time)
  }

  _stopSelfHeal() {
    this.self_heal_backoff = Array.from(SELF_HEAL_BACKOFFS)

    if (this.self_heal_ref) {
      clearTimeout(this.self_heal_ref)
      this.self_heal_ref = null
    }
  }

  _rejoinChannels() {
    for (var channel in this.channels) {
      this.websocket.send(JSON.stringify({
        topic: this._parseTopic(channel),
        event: 'phx_join',
        payload: {},
        ref: null
      }))

      this._debug('Rejoined channel: ', channel)
    }
  }

  _heartbeat() {
    this.afterConnected.then(() => {
      this.websocket.send(JSON.stringify({
        topic: 'phoenix',
        event: 'heartbeat',
        payload: {},
        ref: null
      }))
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

  _parseTopic(channel) {
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

    return this.afterConnected.then(() => {
      channels.forEach(channel => {
        this.channels[channel] = true

        this.websocket.send(JSON.stringify({
          topic: this._parseTopic(channel),
          event: 'phx_join',
          payload: {},
          ref: null
        }))

        this._debug('Joined channel: ', channel)
      })
    })
  }

  leave(...channels) {
    var channels = this._parseChannels(channels)

    return this.afterConnected.then(() => {
      channels.forEach(channel => {
        delete this.channels[channel]

        this.websocket.send(JSON.stringify({
          topic: this._parseTopic(channel),
          event: 'phx_leave',
          payload: {},
          ref: null
        }))

        this._debug('Left channel: ', channel)
      })
    })
  }

  leaveAll() {
    return this.afterConnected.then(() => {
      for (var channel in this.channels) {
        delete this.channels[channel]

        this.websocket.send(JSON.stringify({
          topic: this._parseTopic(channel),
          event: 'phx_leave',
          payload: {},
          ref: null
        }))

        this._debug('Left channel: ', channel)
      }
    })
  }

  listConnectedChannels() {
    var channels = []
    for (var channel in this.channels) {
      channels.push(channel)
    }
    return channels
  }
}

module.exports = IntrinioRealtime

# Intrinio NodeJS SDK for Real-Time Stock Prices

[Intrinio](https://intrinio.com/) provides real-time stock prices from the [IEX stock exchange](https://iextrading.com/), via a two-way WebSocket connection. To get started, [subscribe here](https://intrinio.com/data/realtime-stock-prices) and follow the instructions below.

## Requirements

- NodeJS 6.9.5

## Features

* Receive streaming, real-time price quotes from the IEX stock exchange
  * Bid, ask, and last price
  * Order size
  * Order execution timestamp
* Subscribe to updates from individual securities
* Subscribe to updates for all securities (contact us for special access)

## Installation
```
npm install intrinio-realtime --save
```

## Example Usage
```javascript
var IntrinioRealtime = require('intrinio-realtime')

// Create an IntrinioRealtime instance
var ir = new IntrinioRealtime({
  username: "INTRINIO_API_USERNAME",
  password: "INTRINIO_API_PASSWORD",
})

// Listen for quotes
ir.onQuote(quote => {
  var { ticker, type, price, size, timestamp } = quote
  console.log("QUOTE: ", ticker, type, price, size, timestamp)
})

// Join channels
ir.join("AAPL", "MSFT", "GE")
```

## Quote Fields

```javascript
{ type: 'ask',
  timestamp: 1493409509.3932788,
  ticker: 'GE',
  size: 13750,
  price: 28.97 }
```

*   **type** - the quote type
  *    **`last`** - represents the last traded price
  *    **`bid`** - represents the top-of-book bid price
  *    **`ask`** - represents the top-of-book ask price
*   **timestamp** - a Unix timestamp (with microsecond precision)
*   **ticker** - the ticker of the security
*   **size** - the size of the `last` trade, or total volume of orders at the top-of-book `bid` or `ask` price
*   **price** - the price in USD

## Channels

To receive price quotes from the Intrinio Real-Time API, you need to instruct the client to "join" a channel. A channel can be
* A security ticker (`AAPL`, `MSFT`, `GE`, etc)
* The security lobby (`$lobby`) where all price quotes for all securities are posted
* The security last price lobby (`$lobby_last_price`) where only last price quotes for all securities are posted

Special access is required for both lobby channeles. [Contact us](mailto:sales@intrinio.com) for more information.

## API Keys
You will receive your Intrinio API Username and Password after [creating an account](https://intrinio.com/signup). You will need a subscription to the [IEX Real-Time Stock Prices](https://intrinio.com/data/realtime-stock-prices) data feed as well.

## Documentation

### Methods

`constructor(options)` - Creates a new instance of the IntrinioRealtime client.
* **Parameter** `options`: An object with a `username` and `password` property corresponding to your Intrinio API Username and Password.
```javascript
var ir = new IntrinioRealtime({
  username: "INTRINIO_API_USERNAME",
  password: "INTRINIO_API_PASSWORD",
})
```

---------

`destroy()` - Closes the WebSocket, stops the self-healing and heartbeat intervals. You MUST call this to dispose of the client.

---------

`onError(callback)` - Invokes the given callback when a fatal error is encountered. If no callback has been registered and no `error` event listener has been registered, the error will be thrown.
* **Parameter** `callback` - The callback to invoke. The error will be passed as an argument to the callback.

---------

`onQuote(callback)` - Invokes the given callback when a quote has been received.
* **Parameter** `callback` - The callback to invoke. The quote will be passed as an argument to the callback.
```javascript
ir.onQuote(quote => {
  var { ticker, type, price, size, timestamp } = quote
  console.log("QUOTE: ", ticker, type, price, size, timestamp)
})
```

---------

`join(...channels)` - Joins the given channels. This can be called at any time. The client will automatically register joined channels and establish the proper subscriptions with the WebSocket connection.
* **Parameter** `channels` - An argument list or array of channels to join. See Channels section above for more details.
```javascript
ir.join("AAPL", "MSFT", "GE")
ir.join(["GOOG", "VG"])
ir.join("$lobby")
```

---------

`leave(...channels)` - Leaves the given channels.
* **Parameter** `channels` - An argument list or array of channels to leave.
```javascript
ir.leave("AAPL", "MSFT", "GE")
ir.leave(["GOOG", "VG"])
ir.leave("$lobby")
```

---------

`leaveAll()` - Leaves all joined channels.

---------

`listConnectedChannels()` - Returns the list of joined channels. Recently joined channels may not appear in this list immediately.

### Events

`connect` - Emitted when connected to the websocket successfully.

---------

`quote` - Emitted when a new quote has been received.

---------

`error` - Emitted when a fatal error is encountered. If no `onError` callback has been registered and no event listener has been registered, the error will be thrown.

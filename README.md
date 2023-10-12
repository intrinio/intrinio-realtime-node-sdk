# Intrinio Web and NodeJS SDK for Real-Time Stock Prices

SDK for working with Intrinio's realtime Multi-Exchange prices feed. Intrinio’s Multi-Exchange feed bridges the gap by merging real-time equity pricing from the IEX and MEMX exchanges. Get a comprehensive view with increased market volume and enjoy no exchange fees, no per-user requirements, no permissions or authorizations, and little to no paperwork.

[Intrinio](https://intrinio.com/) provides real-time stock prices via a two-way WebSocket connection. To get started, [subscribe to a real-time data feed](https://intrinio.com/marketplace/data/prices/realtime) and follow the instructions below.

## Requirements
- NodeJS 20.7.0 (for NodeJS usage), or
- A modern web browser and web server (for vanilla JS usage)

## Docker
Add your API key to the realtime.js file, then
```
docker compose build
docker compose run client
```

## Features

* Receive streaming, real-time price quotes (last trade, bid, ask)
* Subscribe to updates from individual securities
* Subscribe to updates for all securities
* Multiple sources of data - REALTIME or DELAYED_SIP or NASDAQ_BASIC

## Script
To use the Web SDK (non-NodeJS), include the `index.js` script (found in this repository) at the end of your `<body>` tag:

```html
<script src='index.js' type='text/javascript'></script>
```

## Example Usage (Web)
```javascript
const accessKey = ""

function onTrade(trade) {
  let tradeElement = $.parseHTML('<div>' + trade.Symbol + '(trade): $' + trade.Price + '</div>')
  $('.container').prepend(tradeElement)
}

function onQuote(quote) {
  let quoteType = null
  if (quote.Type === 1) quoteType = "ask"
  else if (quote.Type === 2) quoteType = "bid"
  let quoteElement = $.parseHTML('<div>' + quote.Symbol + '(' + quote.Type + '): $' + quote.Price + '</div>')
  $('.container').prepend(quoteElement)
}

let config = {
  isPublicKey: true
}

let client = new IntrinioRealtime(accessKey, onTrade, onQuote, config)

client.join("GOOG", true)
```

For another example, see the `/sample` folder. Make sure to substitute your own Public Access Key.

## Public Access Key
You can create a Public Access Key after [creating an account](https://intrinio.com/signup). On your Account page, scroll down to Access Keys, click Add New Key, name it, and specify Public. The key will appear on your Account page, which you will need for to use the SDK. You will also need a subscription to a [real-time data feed](https://intrinio.com/marketplace/data/prices/realtime) for one of the providers listed below.

## NodeJS Installation
```
npm install intrinio-realtime --save
```

## Example Usage (NodeJS)
```javascript
"use strict"
const Client = require("./index").RealtimeClient;
//const Client = require("./index").ReplayClient;
const accessKey = "";

const config = {
  provider: 'REALTIME', //REALTIME or DELAYED_SIP or NASDAQ_BASIC or MANUAL
  ipAddress: undefined,
  tradesOnly: false,
  isPublicKey: false
};

// const config = { //replay config
//     provider: 'REALTIME', //REALTIME or DELAYED_SIP or NASDAQ_BASIC or MANUAL
//     ipAddress: undefined,
//     tradesOnly: false,
//     isPublicKey: false,
//     replayDate: '2023-10-06',
//     replayAsIfLive: false,
//     replayDeleteFileWhenDone: true
// };

let trades = new Map();
let quotes = new Map();
let maxTradeCount = 0;
let maxCountTrade = null;
let maxQuoteCount = 0;
let maxCountQuote = null;

function onTrade(trade) {
  let key = trade.Symbol;
  if (trades.has(key)) {
    let value = trades.get(key);
    if (value + 1 > maxTradeCount) {
      trades.set(key, value + 1);
      maxTradeCount = value + 1;
      maxCountTrade = trade;
    }
  }
  else trades.set(key, 1);
}

function onQuote(quote) {
  let key = quote.Symbol + ":" + quote.Type;
  if (quotes.has(key)) {
    let value = quotes.get(key);
    if (value + 1 > maxQuoteCount) {
      quotes.set(key, value + 1);
      maxQuoteCount = value + 1;
      maxCountQuote = quote;
    }
  }
  else quotes.set(key, 1);
}

let client = new Client(accessKey, onTrade, onQuote, config);
await client.join("AAPL", false); //use $lobby for firehose.

setInterval(() => {
  if (maxTradeCount > 0) {
    console.log("Most active security (by trade frequency): %s (%i updates)", JSON.stringify(maxCountTrade, (key, value) =>
            typeof value === 'bigint'
                    ? value.toString()
                    : value // return everything else unchanged
    ), maxTradeCount);
  }
  if (maxQuoteCount > 0) {
    console.log("Most active security (by quote frequency): %s (%i updates)", JSON.stringify(maxCountQuote, (key, value) =>
            typeof value === 'bigint'
                    ? value.toString()
                    : value // return everything else unchanged
    ), maxQuoteCount);
  }
  let totalMsgCount = client.getTotalMsgCount();
  if (totalMsgCount > 0) {
    console.log("Total updates received = %i", totalMsgCount);
  }
  else {
    console.log("No updates");
  }
}, 10000);

```

Make sure to use your API key as the `accessKey` parameter.

## Example Replay Client Usage (NodeJS) 
Used to replay a specific day's data by downloading the replay file from the REST API and then playing it back.
```javascript
"use strict"
//const Client = require("./index").RealtimeClient;
const Client = require("./index").ReplayClient;
const accessKey = "";

// const config = {
//     provider: 'REALTIME', //REALTIME or DELAYED_SIP or NASDAQ_BASIC or MANUAL
//     ipAddress: undefined,
//     tradesOnly: false,
//     isPublicKey: false
// };

const config = { //replay config
  provider: 'REALTIME', //REALTIME or DELAYED_SIP or NASDAQ_BASIC or MANUAL
  ipAddress: undefined,
  tradesOnly: false,
  isPublicKey: false,
  replayDate: '2023-10-06',
  replayAsIfLive: false,
  replayDeleteFileWhenDone: true
};

let trades = new Map();
let quotes = new Map();
let maxTradeCount = 0;
let maxCountTrade = null;
let maxQuoteCount = 0;
let maxCountQuote = null;

function onTrade(trade) {
  let key = trade.Symbol;
  if (trades.has(key)) {
    let value = trades.get(key);
    if (value + 1 > maxTradeCount) {
      trades.set(key, value + 1);
      maxTradeCount = value + 1;
      maxCountTrade = trade;
    }
  }
  else trades.set(key, 1);
}

function onQuote(quote) {
  let key = quote.Symbol + ":" + quote.Type;
  if (quotes.has(key)) {
    let value = quotes.get(key);
    if (value + 1 > maxQuoteCount) {
      quotes.set(key, value + 1);
      maxQuoteCount = value + 1;
      maxCountQuote = quote;
    }
  }
  else quotes.set(key, 1);
}

let client = new Client(accessKey, onTrade, onQuote, config);
await client.join("AAPL", false); //use $lobby for firehose.

setInterval(() => {
  if (maxTradeCount > 0) {
    console.log("Most active security (by trade frequency): %s (%i updates)", JSON.stringify(maxCountTrade, (key, value) =>
            typeof value === 'bigint'
                    ? value.toString()
                    : value // return everything else unchanged
    ), maxTradeCount);
  }
  if (maxQuoteCount > 0) {
    console.log("Most active security (by quote frequency): %s (%i updates)", JSON.stringify(maxCountQuote, (key, value) =>
            typeof value === 'bigint'
                    ? value.toString()
                    : value // return everything else unchanged
    ), maxQuoteCount);
  }
  let totalMsgCount = client.getTotalMsgCount();
  if (totalMsgCount > 0) {
    console.log("Total updates received = %i", totalMsgCount);
  }
  else {
    console.log("No updates");
  }
}, 10000);

```

Make sure to use your API key as the `accessKey` parameter, and changing the `replayDate` parameter

## Handling Quotes

There are thousands of securities, each with their own feed of activity.  We highly encourage you to make your trade and quote handlers has short as possible and follow a queue pattern so your app can handle the volume of activity.

#### Trade Message

```javascript
{
  Symbol: "AAPL",
  Price: 150.99,
  Size: 20,
  Timestamp: 1637092835566268084,
  TotalVolume: 2728543, 
  SubProvider: "IEX",
  MarketCenter: "",
  Condition: ""
}
```

* **Symbol** - Stock 'ticker' symbol for the security
* **Price** - The price in USD
* **Size** - The number of shares exchanged on the last trade
* **TotalVolume** - The total number of shares traded so far today, for the given symbol
* **Timestamp** - A unix timestamp (the number of nanoseconds since the unix epoch)
* **SubProvider** - Denotes the detailed source within grouped sources.
  *    **`NONE`** - No subtype specified.
  *    **`CTA_A`** - CTA_A in the DELAYED_SIP provider.
  *    **`CTA_B`** - CTA_B in the DELAYED_SIP provider.
  *    **`UTP`** - UTP in the DELAYED_SIP provider.
  *    **`OTC`** - OTC in the DELAYED_SIP provider.
  *    **`NASDAQ_BASIC`** - NASDAQ Basic in the NASDAQ_BASIC provider.
  *    **`IEX`** - From the IEX exchange in the REALTIME provider.
* **MarketCenter** - Provides the market center
* **Condition** - Provides the condition

#### Quote Message

```javascript
{ 
  Symbol: "GOOG",
  Type: 1,
  Price: 2994.78,
  Size: 105,
  Timestamp: 1637092847907710010,
  SubProvider: "IEX",
  MarketCenter: "",
  Condition: ""
}
```

* **Symbol** - Stock 'ticker' symbol for the security
* **Type** - The quote type (either 'ask' or 'bid')
  * **1** - represents an 'ask' type
  * **2** - represents a 'bid' type
* **Price** - The price in USD
* **Size** - The size of the last ask or bid
* **Timestamp** - A unix timestamp (the number of nanoseconds since the unix epoch)
* **SubProvider** - Denotes the detailed source within grouped sources.
  *    **`NONE`** - No subtype specified.
  *    **`CTA_A`** - CTA_A in the DELAYED_SIP provider.
  *    **`CTA_B`** - CTA_B in the DELAYED_SIP provider.
  *    **`UTP`** - UTP in the DELAYED_SIP provider.
  *    **`OTC`** - OTC in the DELAYED_SIP provider.
  *    **`NASDAQ_BASIC`** - NASDAQ Basic in the NASDAQ_BASIC provider.
  *    **`IEX`** - From the IEX exchange in the REALTIME provider.
* **MarketCenter** - Provides the market center
* **Condition** - Provides the condition

## API Keys
You will receive your Intrinio API Key after [creating an account](https://intrinio.com/signup). You will need a subscription to the [Real-Time Data Feed](https://intrinio.com/real-time-multi-exchange) as well.

## Methods

`constructor(accessKey, onTrade, ?onQuote, ?config)` - Creates a new instance of the IntrinioRealtime client.
* **Parameter** `accessKey`: Your API key. See the section on API Keys, above.
* **Parameter** `onTrade`: A callback invoked when a 'trade' has been received. The trade will be passed as an argument to the callback.
* **Parameter** `onQuote`: Optional. A callback invoked when a 'quote' has been received. The quote will be passed as an argument to the callback. If 'onQuote' is not provided, the client will NOT request to receive quote updates from the server.
* **Parameter** `config`: Optional. An object with properties `provider`, `ipAddress`, and `tradesOnly` corresponding to a provider code ("REALTIME" (default) or "MANUAL"), the ipAddress of the websocket server (only necessay when `provider` = "MANUAL"), and a boolean value indicating whether the server should return trade data only (as opposed to trade and quote data).
```javascript
function onTrade(trade) {
  console.log("TRADE: ", trade)
}
function onQuote(quote) {
  console.log("QUOTE: ", quote)
}
const client = new IntrinioRealtimeClient("INTRINIO_API_KEY", onTrade, onQuote, { tradesOnly: true })
```
---------

`stop()` - Closes the WebSocket, stops the self-healing and heartbeat intervals. You MUST call this to dispose of the client. Called automatically on 'SIGINT'.

---------

`join(symbols, tradesOnly)` - Joins the given channels. This can be called at any time. The client will automatically register joined channels and establish the proper subscriptions with the WebSocket connection.
* **Parameter** `symbols` - A string representing a single ticker symbol (e.g. "AAPL") or an array of ticker symbols (e.g. ["AAPL", "MSFT", "GOOG"]) to join. You can also use the special symbol, "$lobby" to join the firehose channel and recieved updates for all ticker symbols. You must have a valid "firehose" subscription.
* **Parameter** `tradesOnly` - Optional (default: false). A boolean value indicating whether the server should return trade data only (as opposed to trade and quote data).
```javascript
client.join(["AAPL", "MSFT", "GOOG"])
client.join("GE", true)
client.join("$lobby") //must have a valid 'firehose' subscription
```

---------

`leave(symbols)` - Leaves the given channels.
* **Parameter** `symbols` - Optional (default = all channels). A string representing a single ticker symbol (e.g. "AAPL") or an array of ticker symbols (e.g. ["AAPL", "MSFT", "GOOG"]) to leave. If not provided, all subscribed channels will be unsubscribed.
```javascript
client.leave(["AAPL", "MSFT", "GOOG"])
client.leave("GE")
client.leave("$lobby")
client.leave()
```

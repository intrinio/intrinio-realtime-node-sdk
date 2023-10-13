"use strict"
const Client = require("./index").RealtimeClient;
//const Client = require("./index").ReplayClient;
const accessKey = "API_KEY_HERE";

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
//require("./index").replayToCsv("outputFilePath.csv", config, ["$lobby"], false, accessKey)

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
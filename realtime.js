"use strict"
const Client = require("./index")
const accessKey = "OjEyOTk1MzNkMjEwZmY4MmYxNTRiMzhhYmY3NWIwOGYy"
const provider = "REALTIME"
const symbols = ["GOOG"]
const config = {
    tradesOnly: true,
    symbols: symbols
}

function onTrade(trade) {
    console.log(trade)
}

function onQuote(quote) {
    console.log(quote)
}

let client = new Client(accessKey, provider, onTrade, onQuote, config)
client.join()
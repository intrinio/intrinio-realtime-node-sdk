"use strict"
const Client = require("./index")

function onTrade(trade) {
    console.log(trade)
}

function onQuote(quote) {
    console.log(quote)
}

let client = new Client(onTrade, onQuote)
client.join()
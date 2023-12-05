const WebSocket = require('ws');
const moment = require('moment');
const db = require('./db'); // Adjust the path as needed
const Redis = require('ioredis');

const redis = new Redis({
    host: '87.107.190.181',
    port: '6379',
    password: 'D@n!@l12098',
    enableCompression: true,
});

const axios = require('axios');


var pipeline = redis.pipeline();

const serverUrl = 'wss://data.tradingview.com/socket.io/websocket?from=chart';
// const serverUrl = 'wss://data-iln1.tradingview.com/socket.io/websocket?from=chart';

const headers = {
    Origin: 'https://www.tradingview.com',
};

const tableMap = {
    "1M": "one_month_forex_candles",
    "1W": "one_week_forex_candles",
    "1D": "one_day_forex_candles",
    "240": "four_hour_forex_candles",
    "60": "one_hour_forex_candles",
    "30": "thirty_minute_forex_candles",
    "15": "fifteen_minute_forex_candles",
    "5": "five_minute_forex_candles",
    "1": "one_minute_forex_candles",
    "1s": "one_second_forex_candles",
};


const resolverMap = {
    "1": 83,
    "5": 83,
    "15": 84,
    "30": 84,
    "60": 84,
    "240": 85,
    "1D": 84,
    "1D": 84,
    "1W": 84,
    "1M": 84,
}


const tokenMap = {
    'BINANCE': { token: "qs_NMWrtw0wr0l4", sdsSystem: "sds_sym_1", timeframe: 1 },
    'OANDA': { token: "qs_NMWrtw0wr0l4", sdsSystem: "sds_sym_1", timeframe: 1 },
    'TVC': { token: "cs_JTzTazd4Mtuu", sdsSystem: "sds_sym_1", timeframe: 1 },
    'CRYPTOCAP': { token: "cs_YoDPLuZuk1Nw", sdsSystem: "sds_sym_1", timeframe: 1 },
    'NASDAQ': { token: "cs_pP9zg3HoX6qW", sdsSystem: "sds_sym_1", timeframe: 1 },
    'VANTAGE': { token: "cs_yaxhnaXssyg8", sdsSystem: "sds_sym_1", timeframe: 1 },
    'CME_MINI': { token: "cs_LNpwvZmd6tVX", sdsSystem: "sds_sym_1", timeframe: 1 },
    'CBOT_MINI': { token: "cs_RFG4482JOp7r", sdsSystem: "sds_sym_1", timeframe: 1 },
    'CAPITALCOM': { token: "cs_tACzA83YVRf2", sdsSystem: "sds_sym_1", timeframe: 1 },
    'FOREXCOM': { token: "cs_AyBuzOJAb7kD", sdsSystem: "sds_sym_1", timeframe: 1 },
    'AMEX': { token: "cs_q1ESErItRlZa", sdsSystem: "sds_sym_1", timeframe: 1 },
    'COMEX': { token: "cs_5UowB3gE6eqz", sdsSystem: "sds_sym_1", timeframe: 1 },
    'MCX': { token: "cs_HPvg93fZ6wIB", sdsSystem: "sds_sym_1", timeframe: 1 },
    'FX': { token: "cs_Aoj8CvPqSsks", sdsSystem: "sds_sym_1", timeframe: 1 },
    'FXOPEN': { token: "cs_BNKGYOtXcFM2", sdsSystem: "sds_sym_1", timeframe: 1 },
    'ECONOMICS': { token: "cs_N7G5G2KqaVey", sdsSystem: "sds_sym_1", timeframe: 5 },
    'XETR': { token: "cs_hSUZOrtZkU8B", sdsSystem: "sds_sym_1", timeframe: 5 },
    'FRED': { token: "cs_X9FVjqNe69Df", sdsSystem: "sds_sym_1", timeframe: 5 },
    'NYMEX': { token: "cs_LUzIUSS31l1H", sdsSystem: "sds_sym_1", timeframe: 5 },
    'INTOTHEBLOCK': { token: "cs_VgyAnZkuYrSQ", sdsSystem: "sds_sym_1", timeframe: 5 },
}


const symbols = {
    // "INTOTHEBLOCK:BTC_RETAIL": { resolver: 152, shouldActive: true, active: true },
    // "INTOTHEBLOCK:BTC_HASHRATE": { resolver: 154, shouldActive: true, active: true },
    // "INTOTHEBLOCK:BTC_TRADERS": { resolver: 153, shouldActive: true, active: true },
    // "INTOTHEBLOCK:BTC_BEARSVOLUME": { resolver: 157, shouldActive: true, active: true },
    // "INTOTHEBLOCK:BTC_BULLSVOLUME": { resolver: 157, shouldActive: true, active: true },
    // "INTOTHEBLOCK:BTC_TXVOLUME": { resolver: 154, shouldActive: true, active: true },
    // "INTOTHEBLOCK:BTC_TXVOLUMEUSD": { resolver: 157, shouldActive: true, active: true },
    // "INTOTHEBLOCK:ETH_RETAIL": { resolver: 152, shouldActive: true, active: true },
    // "INTOTHEBLOCK:ETH_TRADERS": { resolver: 153, shouldActive: true, active: true },
    // "INTOTHEBLOCK:ETH_BEARSVOLUME": { resolver: 157, shouldActive: true, active: true },
    // "INTOTHEBLOCK:ETH_BULLSVOLUME": { resolver: 157, shouldActive: true, active: true },
    // "INTOTHEBLOCK:ETH_TXVOLUME": { resolver: 154, shouldActive: true, active: true },
    // "INTOTHEBLOCK:ETH_TXVOLUMEUSD": { resolver: 157, shouldActive: true, active: true },
    // "ECONOMICS:USINTR": { resolver: 145, shouldActive: true, active: true },
    // "ECONOMICS:USIRYY": { resolver: 145, shouldActive: true, active: true },
    // "FRED:UNRATE": { resolver: 140, shouldActive: true, active: true, times: 1 },
    // "FRED:GDP": { resolver: 137, shouldActive: true, active: true, times: 1 },
    // "FRED:T5YIE": { resolver: 139, shouldActive: true, active: true, times: 1 },
    // "FRED:T10YIE": { resolver: 140, shouldActive: true, active: true, times: 1 },//1 means every month
    // "FRED:BAMLH0A0HYM2": { resolver: 146, shouldActive: true, active: true },
    // "ECONOMICS:USNFP": { resolver: 144, shouldActive: true, active: true },
    // "NYMEX:PL1!": { resolver: 139, shouldActive: true, active: true },
    // "NYMEX:MBE1!": { resolver: 140, shouldActive: true, active: true },
    // //23
    // "XETR:DAX": { resolver: 137, shouldActive: true, active: true },
    // "CRYPTOCAP:BTC.D": { resolver: 144, shouldActive: true, active: true, times: 0 },
    // "CRYPTOCAP:ETH.D": { resolver: 144, shouldActive: true, active: true, times: 0 },
    // "CRYPTOCAP:USDT.D": { resolver: 145, shouldActive: true, active: true, times: 0 },
    // "CRYPTOCAP:OTHERS.D": { resolver: 147, shouldActive: true, active: true, times: 0 },
    // "CRYPTOCAP:Total": { resolver: 144, shouldActive: true, active: true, times: 0 },
    // "CRYPTOCAP:Total2": { resolver: 145, shouldActive: true, active: true, times: 0 },
    // "CRYPTOCAP:Total3": { resolver: 145, shouldActive: true, active: true, times: 0 },
    // "CRYPTOCAP:TOTALDEFI": { resolver: 148, shouldActive: true, active: true, times: 0 },//0 is every day
    // "NASDAQ:FSTOK300": { resolver: 144, shouldActive: true, active: true },
    // "NASDAQ:FSTOK10": { resolver: 143, shouldActive: true, active: true },
    // "NASDAQ:FSTOK40": { resolver: 143, shouldActive: true, active: true },
    // "NASDAQ:FSTOK250": { resolver: 144, shouldActive: true, active: true },
    // "NASDAQ:FSTOKAGG": { resolver: 144, shouldActive: true, active: true },
    // "TVC:US05Y": { resolver: 138, shouldActive: true, active: true },
    // "TVC:US10Y": { resolver: 138, shouldActive: true, active: true },
    // "CME_MINI:NQ1!": { resolver: 142, shouldActive: true, active: true },
    // "CME_MINI:ES1!": { resolver: 142, shouldActive: true, active: true },
    // "CBOT_MINI:YM1!": { resolver: 143, shouldActive: true, active: true },
    // "VANTAGE:DJ30FT": { resolver: 143, shouldActive: true, active: true },
    // "FOREXCOM:DJI": { resolver: 141, shouldActive: true, active: true },
    // "FXOPEN:NDQM": { resolver: 140, shouldActive: true, active: true },
    // // //22
    "FX:XAUUSD": { resolver: 138, shouldActive: true, active: true },
    "TVC:US20Y": { resolver: 138, shouldActive: true, active: true },
    "AMEX:GDX": { resolver: 137, shouldActive: true, active: true },
    "AMEX:GDXJ": { resolver: 138, shouldActive: true, active: true },
    "AMEX:GLD": { resolver: 137, shouldActive: true, active: true },
    "CAPITALCOM:US30": { resolver: 144, shouldActive: true, active: true },
    "NASDAQ:NDX": { resolver: 139, shouldActive: true, active: true },
    "CAPITALCOM:US500": { resolver: 145, shouldActive: true, active: true },
    "CAPITALCOM:EU50": { resolver: 144, shouldActive: true, active: true },
    "CAPITALCOM:CN50": { resolver: 144, shouldActive: true, active: true },
    "TVC:BXY": { resolver: 136, shouldActive: true, active: true },
    "TVC:EXY": { resolver: 136, shouldActive: true, active: true },
    "TVC:DXY": { resolver: 136, shouldActive: true, active: true },
    "TVC:SXY": { resolver: 136, shouldActive: true, active: true },
    "TVC:JXY": { resolver: 136, shouldActive: true, active: true },
    "TVC:CXY": { resolver: 136, shouldActive: true, active: true },
    "TVC:AXY": { resolver: 136, shouldActive: true, active: true },
    "TVC:ZXY": { resolver: 136, shouldActive: true, active: true },
    "CAPITALCOM:HK50": { resolver: 144, shouldActive: true, active: true },
    "CAPITALCOM:NATURALGAS": { resolver: 150, shouldActive: true, active: true },
    "COMEX:HRC1!": { resolver: 140, shouldActive: true, active: true },
    "COMEX:HG1!": { resolver: 139, shouldActive: true, active: true },
    "MCX:ZINC1!": { resolver: 139, shouldActive: true, active: true },
    "FX:USOILSPOT": { resolver: 141, shouldActive: true, active: true },
    "FX:UKOILSPOT": { resolver: 141, shouldActive: true, active: true },

    // "OANDA:USDTRY": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:XPTUSD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:EURCNH": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:USDCNH": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:SPX500USD": { resolver: 144, shouldActive: true, active: true },
    // "OANDA:EURUSD": { resolver: 141, shouldActive: true, active: true },
    // "OANDA:GBPUSD": { resolver: 141, shouldActive: true, active: true },
    // "OANDA:USDCHF": { resolver: 141, shouldActive: true, active: true },
    // "OANDA:USDCAD": { resolver: 141, shouldActive: true, active: true },
    // "OANDA:USDJPY": { resolver: 141, shouldActive: true, active: true },
    // "OANDA:XAUUSD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:AUDUSD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:NZDUSD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:EURJPY": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:EURCAD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:EURNZD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:EURAUD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:EURCHF": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:GBPJPY": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:GBPNZD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:GBPAUD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:GBPCAD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:GBPCHF": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:AUDCAD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:AUDNZD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:AUDJPY": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:AUDCHF": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:CHFJPY": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:XAGUSD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:NZDCAD": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:NZDCHF": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:NZDJPY": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:EURGBP": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:CADCHF": { resolver: 141, shouldActive: true, active: false },
    // "OANDA:CADJPY": { resolver: 141, shouldActive: true, active: false },

    //35 87.107.190.181
}


async function getSymbolIdByName(symbolName) {
    try {
        const query = 'SELECT id FROM forex_symbols WHERE name = $1';
        const symbol = await db.oneOrNone(query, symbolName);
        return symbol ? symbol.id : null;
    } catch (error) {
        console.error('Error:', error.message);
        throw error;
    }
}

function formatNumberWithTwoDecimals(number) {
    // Check if the number has a fractional part
    if (Number.isInteger(number)) {
        return number + ".00"; // Add ".00" when there's no fractional part
    } else {
        return number; // Convert to a string without changes
    }
}


async function saveLastTwoCandleToRedis(lastTwoCandleArray, timeframe, symbolName) {
    await pipeline.set(`${timeframe}-${symbolName.toLowerCase()}`, JSON.stringify(lastTwoCandleArray)).expire(`${timeframe}-${symbolName.toLowerCase()}`, 120).exec();

}


const insertCandlestickBatch = async (tableName, batch, symbol, timeFrame, ws) => {
    const symbolName = symbol.toUpperCase();

    const fetchedSymbolId = await getSymbolIdByName(symbolName);

    try {
        // Save last two candlesticks to Redis
        await saveLastTwoCandleToRedis([batch[batch.length - 1], batch[batch.length - 2]], timeFrame, symbolName);

        const chunkSize = 10000;
        const chunkedBatches = [];

        for (let i = 0; i < batch.length; i += chunkSize) {
            chunkedBatches.push(batch.slice(i, i + chunkSize));
        }

        await db.tx(async (t) => {
            for (const chunk of chunkedBatches) {
                const valuePlaceholders = chunk.map((_, index) => `($${index * 10 + 1}, $${index * 10 + 2}, $${index * 10 + 3}, $${index * 10 + 4}, $${index * 10 + 5}, $${index * 10 + 6}, $${index * 10 + 7}, $${index * 10 + 8}, $${index * 10 + 9}, $${index * 10 + 10})`).join(', ');
                const conflictQuery = `
                INSERT INTO ${tableName} (symbol_id, symbol_name, open_time, open_price, high_price, low_price, close_price, volumn, close_time, created_at)
                VALUES ${valuePlaceholders}
                ON CONFLICT (symbol_name, created_at)
                DO UPDATE
                SET 
                    symbol_id = EXCLUDED.symbol_id,
                    open_time = EXCLUDED.open_time,
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volumn = EXCLUDED.volumn,
                    close_time = EXCLUDED.close_time
                WHERE ${tableName}.created_at = EXCLUDED.created_at;
                `;

                const values = chunk.flatMap(record => [
                    fetchedSymbolId,
                    record.symbol_name.toUpperCase(),
                    record.open_time,
                    formatNumberWithTwoDecimals(record.open_price),
                    formatNumberWithTwoDecimals(record.high_price),
                    formatNumberWithTwoDecimals(record.low_price),
                    formatNumberWithTwoDecimals(record.close_price),
                    record.volumn != null ? record.volumn : 0,
                    record.close_time,
                    record.created_at
                ]);

                await t.none(conflictQuery, values);
            }
        });

        console.log(`Data inserted or updated into ${tableName} for ${batch.length} records`);

        ws.close();
    } catch (error) {
        console.error('Error:', error.message);
    }
};


const filterAndSaveData = (inputString, timeframe, symbolName, fullName, ws) => {
    const dataArray = inputString.split(/~m~\d+~m~{/);
    // Filter out any empty strings from the result
    const filteredDataArray = dataArray.filter(data => data.trim() !== '');

    // Add the "~m~{" back to the beginning of each element in the array
    const separatedDatas = filteredDataArray.map(data => `{${data}`);
    const combinedArray = [];
    const batchCandles = []
    const usedOpenTimes = []
    separatedDatas.forEach(separatedData => {
        if (!separatedData.includes("~~h")) {
            if (separatedData.includes('timescale_update')) {
                combinedArray.push(JSON.parse(separatedData));
            }
        }
    });

    if (symbolName == "XAUUSD") {
        symbolName = "FXAUUSD"
    }

    if (combinedArray.length != 0) {
        combinedArray.forEach(element => {
            if (Object.keys(element.p[1]).length != 0) {
                element.p[1].sds_1.s.forEach(candle => {
                    var timestampSeconds = candle.v[0]; // Unix timestamp in seconds
                    var formattedDateTime = moment.unix(timestampSeconds).utc().format('YYYY-MM-DD HH:mm:ss');

                    const found = usedOpenTimes.find(usedOpenTime => usedOpenTime == formattedDateTime);
                    usedOpenTimes.push(formattedDateTime)

                    if (found == undefined) {
                        batchCandles.push({
                            symbol_id: symbols[fullName].id, // Set your symbol ID
                            symbol_name: symbolName, // Set your symbol name
                            open_time: timestampSeconds,
                            open_price: formatNumberWithTwoDecimals(candle.v[1]),
                            high_price: formatNumberWithTwoDecimals(candle.v[2]),
                            low_price: formatNumberWithTwoDecimals(candle.v[3]),
                            close_price: formatNumberWithTwoDecimals(candle.v[4]),
                            volumn: candle.v[5],
                            close_time: timestampSeconds + 1000, // You might want to adjust this
                            created_at: formattedDateTime,
                        })
                    }
                });

            }
        });

        if (batchCandles.length != 0) {
            insertCandlestickBatch(tableMap[timeframe], batchCandles, symbolName, timeframe, ws)
        }

    } else {

        return null
    }
}



function startStream(exchange, symbolName, resolver, allCandles, historyResolver, timeFrame) {
    const ws = new WebSocket(serverUrl, {
        headers: headers
    });

    ws.on('open', () => {
        console.log(`Connected to WebSocket server ${exchange + ":" + symbolName}`);


        const message = '~m~36~m~{"m":"set_data_quality","p":["low"]}';
        const auth = '~m~636~m~{"m":"set_auth_token","p":["eyJhbGciOiJSUzUxMiIsImtpZCI6IkdaeFUiLCJ0eXAiOiJKV1QifQ.eyJ1c2VyX2lkIjo1MzM4MDMzNiwiZXhwIjoxNjk2ODQ1MzU5LCJpYXQiOjE2OTY4MzA5NTksInBsYW4iOiIiLCJleHRfaG91cnMiOjEsInBlcm0iOiIiLCJzdHVkeV9wZXJtIjoiIiwibWF4X3N0dWRpZXMiOjIsIm1heF9mdW5kYW1lbnRhbHMiOjAsIm1heF9jaGFydHMiOjEsIm1heF9hY3RpdmVfYWxlcnRzIjoxLCJtYXhfc3R1ZHlfb25fc3R1ZHkiOjEsIm1heF9hY3RpdmVfcHJpbWl0aXZlX2FsZXJ0cyI6NSwibWF4X2FjdGl2ZV9jb21wbGV4X2FsZXJ0cyI6MSwibWF4X2Nvbm5lY3Rpb25zIjoyfQ.dUdtu9SbavQt3c_3Pj_-YvZpnebeoqgkQH28HFwkuGkE3Z6eIXGGnOUqzKFjqCW8y9351ZlV0E3R70rAeSuf0-xToRDgdpTGzslX2i5WBJCmmoTirzmqPHvmgj8Yai57DmAEzX-9dx8oSGn6Vo-LxXQ701G-MTuwblbkRXVWDgk"]}'
        const session = `~m~55~m~{"m":"chart_create_session","p":["${tokenMap[exchange].token}",""]}`
        const timeZone = `~m~57~m~{"m":"switch_timezone","p":["${tokenMap[exchange].token}","Etc/UTC"]}`
        const symbol = `~m~${resolver}~m~{"m":"resolve_symbol","p":["${tokenMap[exchange].token}","sds_sym_1","={\\"adjustment\\":\\"splits\\",\\"session\\":\\"regular\\",\\"symbol\\":\\"${exchange.toUpperCase()}:${symbolName.toUpperCase()}\\"}"]}`
        const series = `~m~${historyResolver}~m~{"m":"create_series","p":["${tokenMap[exchange].token}","sds_1","s1","sds_sym_1","${timeFrame}",20000,""]}`
        // Send the JSON string as a message
        ws.send(auth);
        ws.send(session);
        ws.send(timeZone);
        ws.send(symbol);

        console.log("__________________________________________________________________")
        ws.send(series);
        console.log("__________________________________________________________________")


        // setTimeout(() => {
        //     ws.close(1000, "Connection closed by the client");
        // }, 30000);

    });



    ws.on('message', (data) => {
        const refactored = data.toString('utf-8');

        if (!isNaN(refactored[refactored.length - 1])) {
            ws.send(refactored);
            console.log(refactored + " sent")
        }




        filterAndSaveData(refactored, timeFrame, symbolName, exchange + ":" + symbolName, ws)




    });

    ws.on('close', (data) => {
        console.log(data);
        console.log(`WebSocket connection closed ${symbolName}`);
    });

    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
    });
}


async function startStreams(symbols) {

    for (const symbol in symbols) {
        var pairArray = symbol.split(":");
        const allCandles = { "1M": [], "1w": [], "1D": [], "240": [], "60": [], "30": [], "15": [], "5": [], "1": [] };

        await new Promise(async (resolve) => {
            var counter = 1;

            for (const timeFrame in resolverMap) {
                await new Promise((innerResolve) => {
                    setTimeout(() => {
                        startStream(pairArray[0], pairArray[1], symbols[symbol].resolver, allCandles, resolverMap[timeFrame], timeFrame);
                        innerResolve();
                    }, counter * 2000); // 2-second delay for each time frame
                });

                counter++;
            }


            resolve();
        });

        setTimeout(() => {
            // sending activation data
            axios.get(`https://historyfx2.chtx.ir/active/${symbol.toUpperCase()}`)
                .then(response => {
                    // Handle the response data here
                    console.log(`${symbol} live activated`);
                })
                .catch(error => {
                    // Handle any errors that occur during the request
                    console.error(error);
                });
        }, 10000);




        // 4-second delay between symbols
        await new Promise((symbolResolve) => setTimeout(symbolResolve, 10000)); // 4000 milliseconds = 4 seconds
    }

}


// Call the function with your 'symbols' object to start the loop
startStreams(symbols)
    .then(() => {
        console.log("Loop started and will continue forever.");
    })
    .catch((error) => {
        console.error("Error starting the loop:", error);
    });









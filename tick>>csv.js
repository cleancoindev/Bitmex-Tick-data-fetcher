const got = require('got');
const fs = require('fs');
const moment = require('moment');
const zlib = require('minizlib')
const stream = require('stream');
const { promisify } = require('util');
const cliProgress = require('cli-progress');
const dotenv = require("dotenv");

moment.suppressDeprecationWarnings = true;
dotenv.config();
const pipeline = promisify(stream.pipeline);
const multibar = new cliProgress.MultiBar({
    format: '{bar} {title} | {percentage}% | ~{eta_formatted} | {action}',
    hideCursor: true,
    autopadding: true
}, cliProgress.Presets.shades_grey);
const acceptedTickers = JSON.parse(process.env.ACCEPTED_TICKERS)

/**
 * Returns a deep copy of a given Moment object
 * @param {Moment} date Moment date to clone
 */
const deepCopyMoment = (date) => {
    return moment(date.format('YYYY-MM-DD'));
}

/**
 *  Returns a row object from a string of unvalidated tick data
 * @param {string} dirtyDataRow Row of unvalidated tick data
 * @param {string} header the header so we can escape it
 */
const validateRow = (dirtyDataRow, currentDate, header) => {
    //Check for Empty rows
    if (!dirtyDataRow || dirtyDataRow == "")
        throw { message: "Empty row" }

    //Common error : "," at start of row
    if (dirtyDataRow.startsWith(','))
        dirtyDataRow = dirtyDataRow.substr(1);

    //Check for Header rows
    if (dirtyDataRow == header)
        throw { message: "Header row", type: "header" }

    //Create a row object from the string
    const dirtyDataRowCells = dirtyDataRow.split(',')
    const cleanRow = {
        "timestamp": dirtyDataRowCells[0],      //Date to the millisecond (ex: 1443177265706)
        "symbol": dirtyDataRowCells[1],         //Ticker/contract/market (ex: XBTUSD)
        "side": dirtyDataRowCells[2],           //Side of the Taker (Buy or Sell)
        "size": Number(dirtyDataRowCells[3]),           //Contract Amount (ex: 1)
        "price": Number(dirtyDataRowCells[4]),          //Individual Contract Price (ex: 239.99)
        "tickDirection": dirtyDataRowCells[5],  //Movement since last trade (ZeroPlusTick or PlusTick or ZeroMinusTick or MinusTick)
        "trdMatchID": dirtyDataRowCells[6],     //Trade Id in the Bitmex database, uuid (ex: 7ff37f6c-c4f6-4226-20f8-460ec68d4b50)
        "grossValue": Number(dirtyDataRowCells[7]),     //Value of the trade in Bitcoin Satoshi (ex: 239990)
        "homeNotional": Number(dirtyDataRowCells[8]),   //Value of the trade in the first part fo the ticker (ex: 0.002399XBT)
        "foreignNotional": Number(dirtyDataRowCells[9]),//Value of the trade in the second part of the ticker (ex: 0.575952USD)
    };

    //Check for Empty fields
    for (let prop in cleanRow) {
        if (!cleanRow[prop] || cleanRow[prop] == "")
            throw { message: "Invalid " + prop + " : " + cleanRow[prop] };
    }

    //Check for Trade Id
    if (!cleanRow.trdMatchID.match(/[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}/))
        throw { message: "Invalide TradeId: " + cleanRow.trdMatchID };

    //Check for Trade side
    if (!['Buy', 'Sell'].includes(cleanRow.side))
        throw { message: "Invalid side: " + cleanRow.side };

    //Check for Tick Direction
    if (!['ZeroPlusTick', 'MinusTick', 'ZeroMinusTick', 'PlusTick'].includes(cleanRow.tickDirection))
        throw { message: "Invalid tickDirection: " + cleanRow.tickDirection };

    // Check if the Ticker is in the Given list of accepted tickers
    if (!acceptedTickers.includes(cleanRow.symbol) && !(acceptedTickers === undefined || acceptedTickers.length == 0))
        throw { message: "Invalid ticker: " + cleanRow.symbol };
    // const isFutures = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'].map(x => x + currentDate.format('YY')).includes(cleanRow.symbol.substr(cleanRow.symbol.length - 3, 3))
    const isPerp = cleanRow.symbol.substr(cleanRow.symbol.length - 3, 3) == 'USD'
    const isAlt = cleanRow.symbol.substr(0, 3) != 'XBT'

    //Check for Timestamp
    const timestamp = moment(cleanRow.timestamp.replace('D', 'T'));
    if (!timestamp.isValid())
        throw { message: "Invalid date: " + cleanRow.timestamp };
    cleanRow.timestamp = timestamp.format('x');


    //Check for valid amounts (10% tolerance)

    //Foreign Notional
    const calculatedForeignNotional = !isPerp ? cleanRow.size * cleanRow.price : cleanRow.size
    if (Math.abs(calculatedForeignNotional - cleanRow.foreignNotional) > 0.1 * cleanRow.foreignNotional)
        throw { message: "Invalid amounts: " + calculatedForeignNotional + " != " + cleanRow.foreignNotional };

    //Home Notional
    const calculatedHomeNotional = !isPerp ? cleanRow.size : cleanRow.size / cleanRow.price
    if (Math.abs(calculatedHomeNotional - cleanRow.homeNotional) > 0.1 * cleanRow.homeNotional)
        throw { message: "Invalid amounts: " + calculatedHomeNotional.toFixed(4) + " != " + cleanRow.homeNotional.toFixed(4) };

    //Gross Value
    const calculatedGrossValue = (!isPerp ? cleanRow.foreignNotional : cleanRow.homeNotional) * 100000000
    if (!(isAlt && isPerp) && Math.abs(calculatedGrossValue - cleanRow.grossValue) > 0.1 * cleanRow.grossValue)
        throw { message: "Invalid amounts: " + calculatedGrossValue + " != " + cleanRow.grossValue };

    return cleanRow
}
/**
 * inserts the given rows into the database
 * @param {[rows]} rows Array of the rows to insert
 */
const insertRows = async (rows, currentDate) => {
    try {
        //Insert the data in batches of 1000 rows
        fs.writeFile('data/' + currentDate.format('YYYYMMDD') + '.csv', data, () => { })
    } catch (e) {
        //Debug here
        console.error(
            "Error writing file :", e
        )
    }
}

/**
 * Downloads, unzips and starts the validation and insertion for a given day
 * @param {Moment} currentDate Day to process
 * @param {cliProgress.SingleBar} mainBar Main progress bar
 * @param {function} callback Callback function
 */
const getGunzipped = async (currentDate, mainBar, callback) => {
    const gunz = new zlib.Gunzip();
    let rawUnzippedData = [];

    const url = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/' + currentDate.format('YYYYMMDD') + '.csv.gz';

    // 
    gunz.on('data', data => {
        rawUnzippedData.push(data.toString());
    }).on('end', async () => {
        try {
            //progress bar
            const detailBar = multibar.create(rawUnzippedData.length, 0, { title: currentDate.format('yyyy-MM-DD'), action: "" });
            mainBar.update({ action: "Cleaning " + currentDate.format('yyyy-MM-DD') });
            let errorCount = 0;
            let dirtyDataRowsCount = 0;
            const cleanDataRows = [];
            let header;

            //split into chunks
            let i, j, dirtyData, chunk = 1000;
            for (i = 0, j = rawUnzippedData.length; i < j; i += chunk) {
                dirtyData = rawUnzippedData.slice(i, i + chunk);
                detailBar.increment(1);
                const dirtyDataRows = dirtyData.toString().split('\n');
                dirtyDataRowsCount = dirtyDataRowsCount + dirtyDataRows.length;

                for (dirtyDataRow of dirtyDataRows) {
                    //Save the header, will always be the very first line
                    if (header == undefined)
                        header = dirtyDataRow

                    //Validate and add to the "To be inserted rows"
                    try {
                        cleanDataRows.push(validateRow(dirtyDataRow, currentDate, header));
                    }
                    catch (err) {
                        //Debug here
                        // console.error("Line skipped, cause:  ", err.message)
                        errorCount = errorCount + 1;
                    }
                }

                mainBar.update({ action: "Inserting " + currentDate.format('yyyy-MM-DD') });

                detailBar.increment(chunk - 1, { action: errorCount + "/" + dirtyDataRowsCount + " rows skipped" });
            }
            detailBar.stop();
            await insertRows(cleanDataRows);
            callback();
        }
        catch (err) {
            console.error(err);
        }
    }).on("error", (e) => {
        console.error(e);
    });

    await pipeline(
        got.stream(url),
        gunz
    );
}

/**
 * Starts the processing of tick data file for the given date range
 * @param {Moment} currentDate Date to process
 * @param {cliProgress.SingleBar} mainBar Total progress bar
 */
const fetchDay = async (currentDate, endDate, mainBar) => {
    //If the current day is before the last day
    if (currentDate < endDate) {
        //Update the main bar's current action
        mainBar.update({ action: "Downloading " + currentDate.format('YYYY-MM-DD') });

        //Starts the processing of the given day
        await getGunzipped(currentDate, mainBar,
            //After a file has finished processing
            () => {
                //Update the main progress bar's %
                mainBar.increment(1);
                //Increment the current date
                currentDate.add(1, 'day');
                //Recursively call the function to the process again on this new day
                fetchDay(currentDate, endDate, mainBar);
            }
        );
    }
    //We have processed the entire date range provided
    else {
        //Stop the progress bars and exit the program
        multibar.stop();
        process.exit();
    }
}

(async () => {
    try {
        //Get the dates from the .Env file
        let startDate = moment(process.env.START_DATE);
        let endDate = moment(process.env.END_DATE);
        //If present in the call parameters, over ride .Env dates
        for (let j = 0; j < process.argv.length; j++) {
            if (process.argv[j] == "--start")
                startDate = moment(process.argv[j + 1])
            if (process.argv[j] == "--end")
                endDate = moment(process.argv[j + 1])
        }
        //Check if the dates are valid
        if (!startDate.isValid() || !endDate.isValid() || startDate >= endDate)
            throw "Invalid start or end date"
        //Check if the table is valid in postgres
        checkTable();

        // Create a total progress bar, counts the days processing over the total date range given
        const mainBar = multibar.create(endDate.diff(startDate, 'days'), 0, { title: "  Total   ", action: "Initializing" });

        let currentDate = deepCopyMoment(startDate);
        // Start processing the file from the start of the given date range
        fetchDay(currentDate, endDate, mainBar);
    } catch (error) {
        //Stop the progress bars and exit the program
        console.error(error);
        multibar.stop();
        process.exit();
    }
})();
const FS = require('fs')
const Request = require('request')
const Highland = require('highland')
const CSVWriter = require('csv-write-stream')

const lists = [
    { type: 'person', year: 2017, uri: 'forbes-400' },               // American richest 400
    { type: 'person', year: 2017, uri: 'billionaires' },             // world richest
    { type: 'person', year: 2017, uri: 'hong-kong-billionaires' },   // Hong Kong richest 50
    { type: 'person', year: 2017, uri: 'australia-billionaires' },   // Australia richest 50
    { type: 'person', year: 2017, uri: 'china-billionaires' },       // China richest 400
    { type: 'person', year: 2017, uri: 'taiwan-billionaires' },      // Taiwan richest 50
    { type: 'person', year: 2017, uri: 'india-billionaires' },       // India richest 100
    { type: 'person', year: 2017, uri: 'japan-billionaires' },       // Japan richest 50
    { type: 'person', year: 2017, uri: 'africa-billionaires' },      // Africa richest 50
    { type: 'person', year: 2017, uri: 'korea-billionaires' },       // Korea richest 50
    { type: 'person', year: 2017, uri: 'malaysia-billionaires' },    // Malaysia richest 50
    { type: 'person', year: 2017, uri: 'philippines-billionaires' }, // Philippines richest 50
    { type: 'person', year: 2017, uri: 'singapore-billionaires' },   // Singapore richest 50
    { type: 'person', year: 2017, uri: 'indonesia-billionaires' },   // Indonesia richest 50
    { type: 'person', year: 2017, uri: 'thailand-billionaires' },    // Thailand richest 50
    { type: 'person', year: 2017, uri: 'self-made-women' },          // American richest self-made women
    { type: 'person', year: 2017, uri: 'richest-in-tech' },          // tech richest
    { type: 'person', year: 2017, uri: 'hedge-fund-managers' },      // hedge fund highest-earning
    { type: 'person', year: 2016, uri: 'powerful-people' },          // world powerful
    { type: 'person', year: 2017, uri: 'power-women' },              // world powerful women
    { type: 'person', year: 0, uri: 'rtb' },                         // real-time world billionaires
    { type: 'person', year: 0, uri: 'rtrl' },                        // real-time American richest 400
]

const http = Highland.wrapCallback((location, callback) => {
    return Request(location, (error, response) => {
        const failure = error ? error : (response.statusCode >= 400) ? new Error(response.statusCode) : null
        callback(failure, response)
    })
})

function locate(list) {
    return {
        uri: 'http://www.forbes.com/ajax/list/data',
        qs: list
    }
}

function parse(response) {
    return JSON.parse(response.body)
}

lists.map(list => {
    Highland([list])
        .map(locate)
        .flatMap(http)
        .flatMap(parse)
        .through(CSVWriter())
        .pipe(FS.createWriteStream('forbes-' + list.uri + '.csv'))
})

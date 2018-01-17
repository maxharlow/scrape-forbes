const FS = require('fs')
const Request = require('request')
const Highland = require('highland')
const CSVWriter = require('csv-write-stream')

const lists = [
    { type: 'person', year: 2017, uri: 'billionaires' },
    { type: 'person', year: 2017, uri: 'hedge-fund-managers' },
    { type: 'person', year: 2017, uri: 'china-billionaires' },
    { type: 'person', year: 0, uri: 'rtb' }, // real-time billionaires
    { type: 'person', year: 0, uri: 'rtrl' }, // real-time rich list, aka. the 400
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

var request = require('request')
var highland = require('highland')
var csvWriter = require('csv-write-stream')
var fs = require('fs')

var lists = [
    {type: 'person', year: 0, uri: 'rtb'}, // real-time billionaires
    {type: 'person', year: 0, uri: 'rtrl'}, // real-time rich list, aka. the 400
    {type: 'person', year: 2015, uri: 'powerful-people'},
    {type: 'person', year: 2015, uri: 'hedge-fund-managers'},
    {type: 'person', year: 2015, uri: 'china-billionaires' }
]

function retrieve(query, callback) {
    var data = {
	uri: 'http://www.forbes.com/ajax/list/data',
	qs: query
    }
    request(data, function (error, response, body) {
	var errorStatus = (response.statusCode >= 400) ? new Error(response.statusCode) : null
	callback(error || errorStatus, body)
    })
}

lists.map(function (list) {
    highland([list])
	.flatMap(highland.wrapCallback(retrieve))
	.flatMap(JSON.parse)
	.through(csvWriter())
	.pipe(fs.createWriteStream('forbes-' + list.uri + '.csv'))
})

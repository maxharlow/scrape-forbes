import FSExtra from 'fs-extra'
import Axios from 'axios'
import Scramjet from 'scramjet'

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

async function request(location) {
    const url = typeof location === 'object' ? location.url : location
    const timeout = 30 * 1000
    const instance = Axios.create({ timeout })
    const response = await instance(location)
    return {
        url,
        data: response.data,
        passthrough: location.passthrough
    }
}

function locate(item) {
    return {
        url: 'http://www.forbes.com/ajax/list/data',
        params: item,
        passthrough: item
    }
}

function parse(response) {
    return response.data.map(item => {
        return {
            name: item.name,
            surname: item.lastName,
            position: item.position,
            rank: item.rank,
            worth: item.worth,
            age: item.age,
            source: item.source,
            industry: item.industry,
            gender: item.gender,
            country: item.country
        }
    })
}

async function run() {
    const processes = lists.map(list => {
        const process = Scramjet.DataStream.from([list])
            .map(locate)
            .map(request)
            .flatMap(parse)
            .CSVStringify()
        process.pipe(FSExtra.createWriteStream(`forbes-${list.uri}.csv`))
        return process.whenEnd()
    })
    await Promise.all(processes)
    console.log('Done!')
}

run()

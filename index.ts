import * as yargs from 'yargs'
import * as lib from './lib'
import LineByLine = require('n-readlines')

const argv = yargs.usage('$0')
    .option('url', {
        describe: 'es://.../_bulk',
        string: true,
        default: "",
    })
    .option('input', {
        alias: 'i',
        demandOption: true,
        describe: 'es-search log',
        string: true,
    })
    .option('size', {
        describe: 'bulk size',
        number: true,
        default: 200,
    })
    .option('update', {
        describe: 'update doc',
        boolean: true
    })
    .option('upsert', {
        describe: 'update: doc_as_upsert',
        boolean: true
    })
    .option('delete', {
        describe: 'delete doc',
        boolean: true
    })
    .option('head', {
        describe: 'head <n> of input',
        number: true,
    })
    .option('iter', {
        describe: 'row hook (e)=>{}, skip return false',
        string: true,
    })
    .help('h').alias('h', 'help')
    .argv

const iter = (function () {
    if (argv.iter) {
        const fn = eval(argv.iter)
        if (typeof fn === 'function') {
            return fn
        }
    }
    return () => { }
})()

async function hander(line: string, bulk: lib.Bulker) {
    if (!line) return
    let row = lib.jparse<lib.EsRow>(line)
    if (!row) return
    if (false === iter(row)) return
    if (!row._id) return
    if (argv.update) {
        let oo = { update: { _id: row._id, _type: row._type, _index: row._index } }
        let txt = { doc: row._source || {} }
        if (argv.upsert) {
            Object.assign(txt, { doc_as_upsert: true })
        }
        await bulk.push(JSON.stringify(oo), JSON.stringify(txt))
    }
    else if (argv.delete) {
        let oo = { delete: { _id: row._id, _type: row._type, _index: row._index } }
        await bulk.push(JSON.stringify(oo))
    }
}

process.nextTick(async () => {
    const liner = new LineByLine(argv.input)
    let line: Buffer = Buffer.alloc(0)
    let ii = 0
    const bulker = new lib.Bulker(argv.url, argv.size)
    while (line = liner.next()) {
        if (argv.head && argv.head < ii) break
        await hander(line.toString('utf8'), bulker)
        ii++
    }
    await bulker.submit()
})

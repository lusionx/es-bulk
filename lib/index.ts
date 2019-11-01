import axios from 'axios'

export function jparse<T>(ss: string): T | undefined {
    try {
        return JSON.parse(ss)
    } catch (error) { }
    return undefined
}

export interface EsRow {
    _id: string
    _type: string
    _index: string
    _source: any
}

export interface BulkResp {
    took: number
    errors: boolean
    items: {
        update: BulkItem
        delete: BulkItem
    }[]
}

interface BulkItem extends EsRow {
    status: number
    error: any
}

export class Bulker {
    bulk: string[] = []
    size: number = 0
    protected page: number = 0
    constructor(public url: string, public max: number) {
    }
    async submit() {
        if (!this.bulk.length) return
        const headers = {
            'Content-Type': 'application/x-ndjson'
        }
        this.bulk.push('')
        const txt = this.bulk.join('\n')
        this.bulk = []
        const cc = this.size
        this.size = 0
        console.log('submit %d * %d', this.page++, cc)
        if (!/^http.*_bulk$/.test(this.url)) {
            console.log('invalid', this.url)
            console.log(txt)
            return
        }
        let { data } = await axios.post<BulkResp>(this.url, txt, { headers })
        if (!data.errors) return
        for (let { update } of data.items) { // 异常
            if (update.error) {
                console.log('%j', update)
            }
        }
    }
    async push(...lines: string[]) {
        this.bulk.push(...lines)
        this.size++
        if (this.size >= this.max) {
            await this.submit()
        }
    }
}

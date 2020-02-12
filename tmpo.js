$.ajaxSetup({
    timeout: 10 * 60 * 1000, // msecs
    cache: false
})


class Tmpo {
    constructor(sid, token, progress_cb = (() => { return }),
            resample=true, debug=false) {
        this.sid = sid
        this.token = token
        this.resample = resample
        this.debug = debug
        this.dbPromise = idb.open("flukso", 3, (upgradeDB) => {
            switch (upgradeDB.oldVersion) {
            case 0: // called at DB creation time
                upgradeDB.createObjectStore("device")
                upgradeDB.createObjectStore("sensor")
                upgradeDB.createObjectStore("tmpo")
            case 1:
                upgradeDB.createObjectStore("cache")
            case 2:
                upgradeDB.deleteObjectStore("device")
                upgradeDB.deleteObjectStore("sensor")
            }
        })
        this.progress = this._progress_state_init()
        this.progress_cb = progress_cb
    }

    async sync() {
        this.progress = this._progress_state_init("waiting")
        const last = await this._last_block()
        try {
            await this._tmpo_sensor_sync(last)
            this.progress.sync.state == "completed"
            this.progress.clean.state == "completed"
            this._progress_cb_handler()
            if (this.resample) {
                await this._cache_update()
                this.progress.resample.state == "completed"
                this._progress_cb_handler()
            }
            return true
        } catch(err) {
            this.progress.sync.state == "error"
            this.progress.error = err
            this._progress_cb_handler()
            return false
        }
    }

    async _tmpo_sensor_sync(last = { rid: 0, lvl: 0, bid: 0 }) {
        let prom = Array()
        const url = `https://www.flukso.net/api/sensor/${this.sid}/tmpo/sync`
        const data = {
            version: "1.0",
            token: this.token,
            rid: last.rid,
            lvl: last.lvl,
            bid: last.bid
        }
        const blocks = await $.ajax({
            dataType: "json",
            url: url,
            data: data
        })
        this._log("tmpo", `sync call for sensor ${this.sid} successful`)
        for (const block of blocks) {
            this.progress.sync.state = "running";
            this.progress.sync.todo++;
            prom.push(this._tmpo_block_sync(block))
        }
        return Promise.all(prom)
    }

    async _tmpo_block_sync(block) {
        const rid = block.rid
        const lvl = block.lvl
        const bid = block.bid
        const key = this._bid2key(this.sid, rid, lvl, bid)
        const url = `https://www.flukso.net/api/sensor/${this.sid}/tmpo/${rid}/${lvl}/${bid}`
        const data = {
            version: "1.0",
            token: this.token
        }
        const response = await $.ajax({
            dataType: "json",
            url: url,
            data: data
        })
        this._log("tmpo", `sync call for block ${key} successful`)
        const db = await this.dbPromise
        const tx = db.transaction("tmpo", "readwrite")
        tx.objectStore("tmpo").put(response, key)
        await tx.complete
        this._log("tmpo", `block ${key} stored locally`)
        await this._tmpo_clean(block)
        this.progress.sync.todo--;
        this._progress_cb_handler()
    }

    async _tmpo_clean(last) {
        const range = IDBKeyRange.bound(this.sid, `${this.sid}~`)
        const that = this // for 'process' function scoping
        const db = await this.dbPromise
        const tx = db.transaction("tmpo", "readonly")
        const store = tx.objectStore("tmpo")
        const cursor = store.openKeyCursor(range)
        let keys2delete = Array()
        return await cursor.then(function process(cursor) {
            if (last.lvl == 8) { return }
            if (!cursor) { return }
            const block = that._key2bid(cursor.key)
            if (block.rid == last.rid &&
                block.lvl < last.lvl &&
                block.bid < last.bid + Math.pow(2, last.lvl)) {
                keys2delete.push(cursor.key)
            }
            return cursor.continue().then(process)
        })
        .then(async () => {
            let list2str = keys2delete.toString()
            this._log("clean", `cleaning sensor ${this.sid} blocks [${list2str}]`)
            for (const key of keys2delete) {
                this.progress.clean.state = "running";
                this.progress.clean.todo++;
                await this._tmpo_delete_block(key)
            }
        })
    }

    async _tmpo_delete_block(key) {
        const db = await this.dbPromise
        const tx = db.transaction("tmpo", "readwrite")
        tx.objectStore("tmpo").delete(key)
        await tx.complete
        this._log("clean", `block ${key} deleted`)
        this.progress.clean.todo--;
        this._progress_cb_handler()
    }

    async _cache_update(rid=null) {
        this.progress.resample.state == "running"
        this._progress_cb_handler()
        const day2secs = 86400
        const tz_offset = -7200
        if (rid == null) {
            ({ rid } = await this._last_block())
        }
        let cblock = null
        let head = 0
        const last = await this._cache_last_block()
        if (last.cid > 0 && last.rid == rid) {
            cblock = await this._cache_block_load(last.rid, last.cid)
            head = (Math.floor(cblock.state.last / 256) + 1) * 256
        }
        const serieslist = await this._serieslist(
                { rid: rid, head: head, subsample: 8 })
        for (const { t, v } of serieslist) {
            for (let [i, _] of t.entries()) {
                if (!cblock) {
                    cblock = new CachedBlock(t[i], tz_offset)
                }
                if (!cblock.push(t[i], v[i])) {
                    this._cache_block_store(rid, cblock)
                    cblock = new CachedBlock(t[i], tz_offset)
                    cblock.push(t[i], v[i])
                }
            }
        }
        if (cblock) {
            cblock.close()
            this._cache_block_store(rid, cblock)
        }
    }

    _cache_block_store(rid, cblock) {
        const key = this._cache2key(this.sid, rid, cblock.head)
        this.dbPromise.then((db) => {
            const tx = db.transaction("cache", "readwrite")
            tx.objectStore("cache").put(cblock, key)
            return tx.complete
        })
        .then(() => {
            this._log("cache", `cblock ${key} stored`)
        })
    }

    async _cache_block_load(rid, cid) {
        const db = await this.dbPromise
        const key = this._cache2key(this.sid, rid, cid)
        const tx = db.transaction("cache")
        const store = tx.objectStore("cache")
        let cblock = await store.get(key)
        Object.setPrototypeOf(cblock, CachedBlock.prototype)
        return cblock
    }

    async _cache_last_block() {
        // get all cache blocks of a single sensor
        const range = IDBKeyRange.bound(this.sid, `${this.sid}~`)
        const that = this
        let last = {
            rid: 0,
            cid: 0
        }
        const db = await this.dbPromise
        const tx = db.transaction("cache", "readonly")
        const store = tx.objectStore("cache")
        const cursor = store.openKeyCursor(range)
        return await cursor.then(function process(cursor) {
            if (!cursor) { return }
            const cache = that._key2cache(cursor.key)
            if (cache.cid > last.cid) {
                last = cache
            }
            return cursor.continue().then(process)
        })
        .then(() => {
            return last
        })
    }

    async _cache_serieslist(
            { rid=null, head=0, tail=Number.POSITIVE_INFINITY, resample=60,
              series="avg" } = { }) {
        if (rid == null) {
            ({ rid } = await this._cache_last_block())
        }
        const cblocklist = await this._cache_blocklist(rid, head, tail)
        const cblocklist2string = JSON.stringify(cblocklist)
        this._log("series", `sensor ${this.sid} using cache blocks: ${cblocklist2string}`)
        const serieslist = await Promise.all(
            cblocklist.map(async (cache) => {
                const cblock = await this._cache_block_load(cache.rid, cache.cid)
                return this._cache_block2series(cblock, head, tail, resample, series)
            })
        )
        return serieslist
    }

    async _cache_blocklist(rid, head, tail) {
        const that = this
        // get all cache blocks of a single sensor/rid
        const range = IDBKeyRange.bound(`${this.sid}-${rid}`, `${this.sid}-${rid}~`)
        const db = await this.dbPromise
        const tx = db.transaction("cache", "readonly")
        const store = tx.objectStore("cache")
        const cursor = store.openKeyCursor(range)
        let cblocklist = []
        return await cursor.then(function process(cursor) {
            if (!cursor) { return }
            const cache = that._key2cache(cursor.key)
            if (head < that._cblocktail(cache.cid) && tail > cache.cid) {
                cblocklist.push(cache)
            }
            return cursor.continue().then(process)
        })
        .then(() => {
            return cblocklist.sort((a, b) => a.cid - b.cid)
        })
    }

    _cache_block2series(cblock, head, tail, resample=60, series="avg") {
        const start = Math.max(cblock.head,
                Math.ceil((head / resample) * resample))
        const stop = Math.min(cblock.head + cblock.day2secs - resample,
                Math.floor((tail / resample) * resample))
        const i_start = (start - cblock.head) / resample
        const i_stop = (stop - cblock.head) / resample
        const size = i_stop - i_start + 1
        let t = Array(size)
        for (let i = 0; i < size; i++) {
            t[i] = start + i * resample
        }
        let v = cblock.series[series][resample].slice(i_start, i_stop + 1)
        return {t: t, v: v}
    }

    _cache2key(sid, rid, cid) {
        return `${sid}-${rid}-${cid}`
    }

    _key2cache(key) {
        const cache = key.split("-")
        return {
            rid: Number(cache[1]),
            cid: Number(cache[2])
        }
    }

    _cblocktail(cid) {
        return cid + 86400
    }

    async series(params, sync=false) {
        if (sync) {
            await this.sync()
        }
        let serieslist = []
        if (params.resample) {
            serieslist = await this._cache_serieslist(params)
        } else {
            serieslist = await this._serieslist(params)
        }
        return this._serieslist_concat(serieslist)
    }

    async _serieslist(
            { rid=null, head=0, tail=Number.POSITIVE_INFINITY, subsample=0 } = { }) {
        if (rid == null) {
            ({ rid } = await this._last_block())
        }
        const blocklist = await this._blocklist(rid, head, tail)
        const blocklist2string = JSON.stringify(blocklist)
        this._log("series", `sensor ${this.sid} using blocks: ${blocklist2string}`)
        const serieslist = await Promise.all(
            blocklist.map(async (block) => {
                const content = await this._block_content(block.rid, block.lvl, block.bid)
                const key = this._bid2key(this.sid, block.rid, block.lvl, block.bid)
                return this._block2series(key, content, head, tail, subsample)
            })
        )
        return serieslist
    }

    async _last_block() {
        // get all tmpo blocks of a single sensor
        const range = IDBKeyRange.bound(this.sid, `${this.sid}~`)
        const that = this
        let last = {
            rid: 0,
            lvl: 0,
            bid: 0
        }
        const db = await this.dbPromise
        const tx = db.transaction("tmpo", "readonly")
        const store = tx.objectStore("tmpo")
        const cursor = store.openKeyCursor(range)
        return await cursor.then(function process(cursor) {
            if (!cursor) { return }
            const block = that._key2bid(cursor.key)
            if (block.bid > last.bid) {
                last = block
            }
            return cursor.continue().then(process)
        })
        .then(() => {
            const last_key = that._bid2key(that.sid, last.rid, last.lvl, last.bid)
            that._log("last", `last block: ${last_key}`)
            return last
        })
    }

    async _blocklist(rid, head, tail) {
        const that = this
        // get all tmpo blocks of a single sensor/rid
        const range = IDBKeyRange.bound(`${this.sid}-${rid}`, `${this.sid}-${rid}~`)
        const db = await this.dbPromise
        const tx = db.transaction("tmpo", "readonly")
        const store = tx.objectStore("tmpo")
        const cursor = store.openKeyCursor(range)
        let blocklist = []
        return await cursor.then(function process(cursor) {
            if (!cursor) { return }
            const block = that._key2bid(cursor.key)
            if (head < that._blocktail(block.lvl, block.bid) && tail > block.bid) {
                blocklist.push(block)
            }
            return cursor.continue().then(process)
        })
        .then(() => {
            return blocklist.sort((a, b) => a.bid - b.bid)
        })
    }

    _block2series(key, content, head, tail, subsample=0) {
        if (content == null) {
            this._log("tmpo", `block2series for block ${key} failed: empty block`)
            return {t: [], v: []}
        }
        let [t_accu, v_accu] = content.h.head
        let t_accu_last = 0
        let t = []
        let v = []
        for (let i in content.t) {
            t_accu += content.t[i]
            v_accu += content.v[i]
            if (t_accu >= head && t_accu <= tail
                    && t_accu >= t_accu_last + subsample) {
                t.push(t_accu)
                v.push(v_accu)
                t_accu_last = t_accu
            }
        }
        return {t: t, v: v}
    }

    _serieslist_concat(list) {
        let accu = { t: [], v: [] }
        for (let { t, v } of list) {
            accu.t = accu.t.concat(t)
            accu.v = accu.v.concat(v)
        }
        return accu
    }

    async _block_content(rid, lvl, bid) {
        const db = await this.dbPromise
        const key = this._bid2key(this.sid, rid, lvl, bid)
        const tx = db.transaction("tmpo")
        const store = tx.objectStore("tmpo")
        return store.get(key)
    }

    _blocktail(lvl, bid) {
        return bid + Math.pow(2, lvl)
    }

    _bid2key(sid, rid, lvl, bid) {
        return `${sid}-${rid}-${lvl}-${bid}`
    }

    _key2bid(key) {
        const block = key.split("-")
        return {
            rid: Number(block[1]),
            lvl: Number(block[2]),
            bid: Number(block[3])
        }
    }

    _progress_state_init(init_state="waiting") {
        return {
            // possible states: waiting/running/completed/error
            sync: { state: init_state, todo: 0 },
            clean: { state: init_state, todo: 0 },
            resample: { state: init_state, todo: 0 },
            time: { start: Date.now(), runtime: 0 },
            error: null
        }
    }

    _progress_cb_handler() {
        this.progress.time.runtime =
                Date.now() - this.progress.time.start
        this.progress_cb(this.progress)
    }

    _log(topic, message) {
        if (this.debug) {
            const time = Date.now()
            console.log(`${time} [${topic}] ${message}`)
        }
    }
}


class CachedBlock {
    constructor(timestamp, tz_offset) {
        this.day2secs = 86400
        const x = Math.floor((timestamp - tz_offset) / this.day2secs)
        this.head = x * this.day2secs + tz_offset
        this.series = {
            "avg": [],
            "min": [],
            "max": []
        }
        for (const metric of ["avg", "min", "max"]) {
            this.series[metric][60] = Array(1440)
            this.series[metric][900] = Array(96)
            this.series[metric][3600] = Array(24)
            this.series[metric][86400] = Array(1)
        }
        this.state = {
            last: this.head,
            minute: null,
            samples: []
        }
    }

    push(t, v) {
        if (t >= this.head + this.day2secs) {
            this.gen_minute()
            this.close()
            return false
        }
        if (!this.state.minute) {
            this.state.minute = Math.floor(t / 60) * 60
        }
        if (t < this.state.minute + 60) {
            this.state.last = t
            this.state.samples.push(v)
        } else {
            this.gen_minute()
            this.push(t, v)
        }
        return true
    }

    gen_minute() {
        const i = (this.state.minute - this.head) / 60
        const len = this.state.samples.length
        this.series.avg[60][i] =
                this.state.samples.reduce((a, b) => a + b, 0) / len
        this.series.min[60][i] = Math.min(...this.state.samples)
        this.series.max[60][i] = Math.max(...this.state.samples)
        this.state.minute = null
        this.state.samples = []
    }

    close() {
        const resampling = [
            { entries: 96, samples: 15, source:   60, sink:   900 },
            { entries: 24, samples:  4, source:  900, sink:  3600 },
            { entries:  1, samples: 24, source: 3600, sink: 86400 }
        ]
        for (const r of resampling) {
            this.resample(r)
        }
    }

    resample({ entries, samples, source, sink }) {
        for (const i of Array(entries).keys()) {
            let slice = {
                "avg": null,
                "min": null,
                "max": null
            }
            slice.avg = this.series.avg[source]
                    .slice(i * samples, (i + 1) * samples)
            const len = Object.keys(slice.avg).length
            if (len == 0) {
                continue
            }
            this.series.avg[sink][i] =
                    slice.avg.reduce((a, b) => a + b, 0) / len
            slice.min = this.series.min[source]
                    .slice(i * samples, (i + 1) * samples)
            this.series.min[sink][i] =
                    Math.min(...slice.min.filter(x => x != undefined))
            slice.max = this.series.max[source]
                    .slice(i * samples, (i + 1) * samples)
            this.series.max[sink][i] =
                    Math.max(...slice.max.filter(x => x != undefined))
        }
    }
}


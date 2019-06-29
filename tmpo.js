$.ajaxSetup({
    timeout: 10 * 60 * 1000, // msecs
    cache: false
})


class Tmpo {
    constructor(uid, token, cache=true, debug=false) {
        this.uid = uid
        this.token = token
        this.cache = cache
        this.debug = debug
        this.dbPromise = idb.open("flukso", 2, (upgradeDB) => {
            switch (upgradeDB.oldVersion) {
            case 0: // called at DB creation time
                upgradeDB.createObjectStore("device")
                upgradeDB.createObjectStore("sensor")
                upgradeDB.createObjectStore("tmpo")
            case 1:
                upgradeDB.createObjectStore("cache")
            }
        })
        this.sync_completed = true
        this.cache_completed = true
        this.sensor_cache_update = { }
        this.progress = this._progress_state_init("completed")
        this.progress_cb = (() => { return })
    }

    sync(progress_cb = (() => { return })) {
        this.sync_completed = false
        if (this.cache) {
            this.cache_completed = false
        }
        this.progress = this._progress_state_init("waiting")
        this.progress.sync.all.state = "running"
        this.progress_cb = progress_cb
        this._device_sync()
    }

    _device_sync() {
        const url = `https://www.flukso.net/api/user/${this.uid}/device`
        const data = {
            version: "1.0",
            token: this.token
        }
        $.getJSON(url, data)
        .done((devices) => {
            this._log("device", `sync call for uid ${this.uid} successful`)
            this.dbPromise.then(async (db) => {
                const tx = db.transaction("device", "readwrite")
                tx.objectStore("device").put(devices, this.uid)
                await tx.complete
                for (let i in devices) {
                    this.progress.sync.device.state = "running"
                    this.progress.sync.device.todo++;
                    this._sensor_sync(devices[i].device)
                }
                if (this.progress.sync.device.state == "waiting") {
                    this.progress.sync.all.state = "completed"
                }
                this._progress_cb_handler()
            })
        })
        .fail(() => {
            this.progress.sync.all.http_error++
        })
    }

    _sensor_sync(device) {
        const url = `https://www.flukso.net/api/device/${device}/sensor`
        const data = {
            version: "1.0",
            token: this.token
        }
        $.getJSON(url, data)
        .done((sensors) => {
            this._log("sensor", `sync call for device ${device} successful`)
            if (--this.progress.sync.device.todo == 0) {
                this.progress.sync.device.state = "completed"
            }
            this.dbPromise.then(async (db) => {
                const tx = db.transaction("sensor", "readwrite")
                tx.objectStore("sensor").put(sensors, device)
                await tx.complete
                for (let i in sensors) {
                    this.progress.sync.sensor.state = "running";
                    this.progress.sync.sensor.todo++;
                    const last = await this._last_block(sensors[i].sensor)
                    this._tmpo_sensor_sync(sensors[i].sensor, last)
                }
                if (this.progress.sync.device.state == "completed" &&
                    this.progress.sync.sensor.state != "running") { // edge case
                    this.progress.sync.all.state = "completed"
                }
                this._progress_cb_handler()
            })
        })
        .fail(() => {
            this.progress.sync.all.http_error++
        })
    }

    _tmpo_sensor_sync(sensor, last = { rid: 0, lvl: 0, bid: 0 }) {
        const url = `https://www.flukso.net/api/sensor/${sensor}/tmpo/sync`
        const data = {
            version: "1.0",
            token: this.token,
            rid: last.rid,
            lvl: last.lvl,
            bid: last.bid
        }
        $.getJSON(url, data)
        .done((blocks) => {
            this._log("tmpo", `sync call for sensor ${sensor} successful`)
            if (this.cache) {
                this.sensor_cache_update[sensor] = true
            }
            if (--this.progress.sync.sensor.todo == 0 &&
                this.progress.sync.device.state == "completed") {
                this.progress.sync.sensor.state = "completed"
            }
            for (let i in blocks) {
                this.progress.sync.block.state = "running";
                this.progress.sync.block.todo++;
                this._tmpo_block_sync(sensor, blocks[i])
            }
            if (this.progress.sync.device.state == "completed" &&
                this.progress.sync.sensor.state == "completed" &&
                this.progress.sync.block.state != "running") { // edge case
                this.progress.sync.all.state = "completed"
            }
            this._progress_cb_handler()
        })
        .fail(() => {
            this.progress.sync.all.http_error++
        })
    }

    _tmpo_block_sync(sensor, block) {
        const rid = block.rid
        const lvl = block.lvl
        const bid = block.bid
        const key = this._bid2key(sensor, rid, lvl, bid)
        const url = `https://www.flukso.net/api/sensor/${sensor}/tmpo/${rid}/${lvl}/${bid}`
        const data = {
            version: "1.0",
            token: this.token
        }
        $.getJSON(url, data)
        .done((response) => {
            this._log("tmpo", `sync call for block ${key} successful`)
            if (--this.progress.sync.block.todo == 0 &&
                this.progress.sync.sensor.state == "completed") {
                this.progress.sync.block.state = "completed"
            }
            this.dbPromise.then(async (db) => {
                const tx = db.transaction("tmpo", "readwrite")
                tx.objectStore("tmpo").put(response, key)
                await tx.complete
                this._tmpo_clean(sensor, block)
            })
        })
        .fail(() => {
            this._log("tmpo", `sync call for block ${key} failed`)
            this.progress.sync.all.http_error++
        })
    }

    _tmpo_clean(sensor, last) {
        const range = IDBKeyRange.bound(sensor, `${sensor}~`)
        const that = this // for 'process' function scoping
        let keys2delete = []
        this.dbPromise.then((db) => {
            const tx = db.transaction("tmpo", "readonly")
            const store = tx.objectStore("tmpo")
            return store.openKeyCursor(range)
        })
        .then(function process(cursor) {
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
        .then(() => {
            let list2str = keys2delete.toString()
            this._log("clean", `cleaning sensor ${sensor} blocks [${list2str}]`)
            for (let i in keys2delete) {
                this.progress.sync.clean.state = "running";
                this.progress.sync.clean.todo++;
                this._tmpo_delete_block(keys2delete[i])
            }
            if (this.progress.sync.device.state == "completed" &&
                this.progress.sync.sensor.state == "completed" &&
                this.progress.sync.block.state == "completed" &&
                this.progress.sync.clean.state != "running") { // edge case
                this.progress.sync.all.state = "completed"
            }
            this._progress_cb_handler()
        })
    }

    async _tmpo_delete_block(key) {
        this.dbPromise.then((db) => {
            const tx = db.transaction("tmpo", "readwrite")
            const store = tx.objectStore("tmpo")
            store.delete(key)
            return tx.complete
        })
        .then(() => {
            this._log("clean", `block ${key} deleted`)
            if (--this.progress.sync.clean.todo == 0 &&
                this.progress.sync.block.state == "completed") {
                this.progress.sync.clean.state = "completed"
                this.progress.sync.all.state = "completed"
            }
            this._progress_cb_handler()
        })
    }

    async _cache_update(sid, rid=null) {
        const day2secs = 86400
        const tz_offset = -7200
        if (rid == null) {
            ({ rid } = await this._last_block(sid))
        }
        let cblock = null
        let head = 0
        const last = await this._cache_last_block(sid)
        if (last.cid > 0 && last.rid == rid) {
            cblock = await this._cache_block_load(sid, last.rid, last.cid)
            head = (Math.floor(cblock.state.last / 256) + 1) * 256
        }
        const serieslist = await this._serieslist(sid,
                { rid: rid, head: head, subsample: 8 })
        for (const { t, v } of serieslist) {
            for (let [i, _] of t.entries()) {
                if (!cblock) {
                    cblock = new CachedBlock(t[i], tz_offset)
                }
                if (!cblock.push(t[i], v[i])) {
                    this._cache_block_store(sid, rid, cblock)
                    cblock = new CachedBlock(t[i], tz_offset)
                    cblock.push(t[i], v[i])
                }
            }
        }
        if (cblock) {
            cblock.close()
            this._cache_block_store(sid, rid, cblock)
        }
        if (--this.progress.cache.todo == 0) {
            this.progress.cache.state = "completed"
            this.cache_completed = true
        }
        this.progress.cache.runtime = Date.now() - this.progress.cache.start
        this.progress_cb(this.progress)
    }

    _cache_block_store(sid, rid, cblock) {
        const key = this._cache2key(sid, rid, cblock.head)
        this.dbPromise.then((db) => {
            const tx = db.transaction("cache", "readwrite")
            tx.objectStore("cache").put(cblock, key)
            return tx.complete
        })
        .then(() => {
            this._log("cache", `cblock ${key} stored`)
        })
    }

    async _cache_block_load(sid, rid, cid) {
        const db = await this.dbPromise
        const key = this._cache2key(sid, rid, cid)
        const tx = db.transaction("cache")
        const store = tx.objectStore("cache")
        let cblock = await store.get(key)
        Object.setPrototypeOf(cblock, CachedBlock.prototype)
        return cblock
    }

    async _cache_last_block(sid) {
        // get all cache blocks of a single sensor
        const range = IDBKeyRange.bound(sid, `${sid}~`)
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

    async _cache_serieslist(sid,
            { rid=null, head=0, tail=Number.POSITIVE_INFINITY, resample=60,
              series="avg" } = { }) {
        if (rid == null) {
            ({ rid } = await this._cache_last_block(sid))
        }
        const cblocklist = await this._cache_blocklist(sid, rid, head, tail)
        const cblocklist2string = JSON.stringify(cblocklist)
        this._log("series", `sensor ${sid} using cache blocks: ${cblocklist2string}`)
        const serieslist = await Promise.all(
            cblocklist.map(async (cache) => {
                const cblock = await this._cache_block_load(sid,
                    cache.rid, cache.cid)
                return this._cache_block2series(cblock, head, tail, resample, series)
            })
        )
        return serieslist
    }

    async _cache_blocklist(sid, rid, head, tail) {
        const that = this
        // get all cache blocks of a single sensor/rid
        const range = IDBKeyRange.bound(`${sid}-${rid}`, `${sid}-${rid}~`)
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

    async series(sid, params) {
        let serieslist = []
        if (params.resample) {
            serieslist = await this._cache_serieslist(sid, params)
        } else {
            serieslist = await this._serieslist(sid, params)
        }
        return this._serieslist_concat(serieslist)
    }

    async _serieslist(sid,
            { rid=null, head=0, tail=Number.POSITIVE_INFINITY, subsample=0 } = { }) {
        if (rid == null) {
            ({ rid } = await this._last_block(sid))
        }
        const blocklist = await this._blocklist(sid, rid, head, tail)
        const blocklist2string = JSON.stringify(blocklist)
        this._log("series", `sensor ${sid} using blocks: ${blocklist2string}`)
        const serieslist = await Promise.all(
            blocklist.map(async (block) => {
                const content = await this._block_content(sid,
                    block.rid, block.lvl, block.bid)
                const key = this._bid2key(sid, block.rid, block.lvl, block.bid)
                return this._block2series(key, content, head, tail, subsample)
            })
        )
        return serieslist
    }

    async _last_block(sid) {
        // get all tmpo blocks of a single sensor
        const range = IDBKeyRange.bound(sid, `${sid}~`)
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
            const last_key = that._bid2key(sid, last.rid, last.lvl, last.bid)
            that._log("last", `last block: ${last_key}`)
            return last
        })
    }

    async _blocklist(sid, rid, head, tail) {
        const that = this
        // get all tmpo blocks of a single sensor/rid
        const range = IDBKeyRange.bound(`${sid}-${rid}`, `${sid}-${rid}~`)
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

    async _block_content(sid, rid, lvl, bid) {
        const db = await this.dbPromise
        const key = this._bid2key(sid, rid, lvl, bid)
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
            // possible states: waiting/running/completed
            sync: {
                device: { state: init_state, todo: 0 },
                sensor: { state: init_state, todo: 0 },
                block: { state: init_state, todo: 0 },
                clean: { state: init_state, todo: 0 },
                all: {
                    state: init_state,
                    http_error: 0,
                    start: Date.now(),
                    runtime: 0
                }
            },
            cache: {
                state: init_state,
                todo: 0,
                start: Date.now(),
                runtime: 0
            }
        }
    }

    _progress_cb_handler() {
        if (!this.sync_completed) {
            this.progress.sync.all.runtime =
                Date.now() - this.progress.sync.all.start
        }
        if (this.progress.sync.all.state == "completed") {
            this.progress.sync.device.state = "completed"
            this.progress.sync.sensor.state = "completed"
            this.progress.sync.block.state = "completed"
            this.progress.sync.clean.state = "completed"
            for (const sid in this.sensor_cache_update) {
                this.progress.cache.state = "running"
                this.progress.cache.start = Date.now()
                this.progress.cache.todo++;
                this._cache_update(sid)
            }
            if (this.progress.cache.state != "running") {
                this.progress.cache.state = "completed"
                this.cache_completed = true
            }
            this.sensor_cache_update = { }
            this.sync_completed = true
        }
        this.progress_cb(this.progress)
    }

    _log(topic, message) {
        if (this.debug) {
            let time = Date.now()
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


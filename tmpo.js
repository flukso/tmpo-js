$.ajaxSetup({
    timeout: 10 * 60 * 1000, // msecs
    cache: false
})


class Tmpo {
    constructor(uid, token, debug=false) {
        this.uid = uid
        this.token = token
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
        this.sync_completed = false
        this.progress = this._progress_state_init("completed")
        this.progress_cb = (() => { return })
    }

    sync(progress_cb = (() => { return })) {
        this.sync_completed = false
        this.progress = this._progress_state_init("waiting")
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
            this.dbPromise.then((db) => {
                const tx = db.transaction("device", "readwrite")
                tx.objectStore("device").put(devices, this.uid)
                for (let i in devices) {
                    this.progress.device.state = "running"
                    this.progress.device.todo++;
                    this._sensor_sync(devices[i].device)
                }
                if (this.progress.device.state == "waiting") {
                    this.progress.all.state = "completed"
                }
                this._progress_cb_handler()
            })
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
            if (--this.progress.device.todo == 0) {
                this.progress.device.state = "completed"
            }
            this.dbPromise.then(async (db) => {
                const tx = db.transaction("sensor", "readwrite")
                tx.objectStore("sensor").put(sensors, device)
                for (let i in sensors) {
                    this.progress.sensor.state = "running";
                    this.progress.sensor.todo++;
                    const last = await this._last_block(sensors[i].sensor)
                    this._tmpo_sensor_sync(sensors[i].sensor, last)
                }
                if (this.progress.device.state == "completed" &&
                    this.progress.sensor.state != "running") { // edge case
                    this.progress.all.state = "completed"
                }
                this._progress_cb_handler()
            })
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
            if (--this.progress.sensor.todo == 0) {
                this.progress.sensor.state = "completed"
            }
            for (let i in blocks) {
                this.progress.block.state = "running";
                this.progress.block.todo++;
                this._tmpo_block_sync(sensor, blocks[i])
            }
            if (this.progress.device.state == "completed" &&
                this.progress.sensor.state == "completed" &&
                this.progress.block.state != "running") { // edge case
                this.progress.all.state = "completed"
            }
            this._progress_cb_handler()
        })
    }

    _tmpo_block_sync(sensor, block) {
        const rid = block.rid
        const lvl = block.lvl
        const bid = block.bid
        const url = `https://www.flukso.net/api/sensor/${sensor}/tmpo/${rid}/${lvl}/${bid}`
        const data = {
            version: "1.0",
            token: this.token
        }
        $.getJSON(url, data)
        .done((response) => {
            const key = this._bid2key(sensor, rid, lvl, bid)
            this._log("tmpo", `sync call for block ${key} successful`)
            if (--this.progress.block.todo == 0) {
                this.progress.block.state = "completed"
            }
            this.dbPromise.then((db) => {
                const tx = db.transaction("tmpo", "readwrite")
                tx.objectStore("tmpo").put(response, key)
                this._tmpo_clean(sensor, block)
            })
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
                this.progress.clean.state = "running";
                this.progress.clean.todo++;
                this._tmpo_delete_block(keys2delete[i])
            }
            if (this.progress.device.state == "completed" &&
                this.progress.sensor.state == "completed" &&
                this.progress.block.state == "completed" &&
                this.progress.clean.state != "running") { // edge case
                this.progress.all.state = "completed"
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
            if (--this.progress.clean.todo == 0) {
                this.progress.clean.state = "completed"
                this.progress.all.state = "completed"
            }
            this._progress_cb_handler()
        })
    }

    async _cache_update(sid) {
        const day2secs = 86400
        const tz_offset = -7200
        let cblock = null
        let head = 0
        const last_cid = await this._cache_last_block(sid)
        if (last_cid) {
            console.log(last_cid)
            cblock = await this._cache_block_load(sid, last_cid)
            head = (Math.floor(cblock.state.last / 256) + 1) * 256
        }
        const serieslist = await this._serieslist(sid,
                { head: head, subsample: 8 })
        for (const { t, v } of serieslist) {
            console.log(t, v)
            for (let [i, _] of t.entries()) {
                if (!cblock) {
                    cblock = new CachedBlock(t[i], tz_offset)
                }
                if (!cblock.push(t[i], v[i])) {
                    this._cache_block_store(sid, cblock)
                    console.log(cblock)
                    cblock = new CachedBlock(t[i], tz_offset)
                    cblock.push(t[i], v[i])
                }
            }
        }
        cblock.close()
        this._cache_block_store(sid, cblock)
        console.log(cblock)
    }

    _cache_block_store(sid, cblock) {
        const key = this._cid2key(sid, cblock.head)
        this.dbPromise.then((db) => {
            const tx = db.transaction("cache", "readwrite")
            tx.objectStore("cache").put(cblock, key)
        })
    }

    async _cache_block_load(sid, cid) {
        const db = await this.dbPromise
        const key = this._cid2key(sid, cid)
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
        let last_cid = 0
        const db = await this.dbPromise
        const tx = db.transaction("cache", "readonly")
        const store = tx.objectStore("cache")
        const cursor = store.openKeyCursor(range)
        return await cursor.then(function process(cursor) {
            if (!cursor) { return }
            const cid = that._key2cid(cursor.key)
            if (cid > last_cid) {
                last_cid = cid
            }
            return cursor.continue().then(process)
        })
        .then(() => {
            return last_cid
        })
    }

    _cid2key(sid, cid) {
        return `${sid}-${cid}`
    }

    _key2cid(key) {
        const cblock_id = key.split("-")
        return Number(cblock_id[1])
    }

    async series(sid, params) {
        const serieslist = await this._serieslist(sid, params)
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
                return this._block2series(content, head, tail, subsample)
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

    _block2series(content, head, tail, subsample=0) {
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
            device: { state: init_state, todo: 0 },
            sensor: { state: init_state, todo: 0 },
            block: { state: init_state, todo: 0 },
            clean: { state: init_state, todo: 0 },
            all: { state: init_state, start: Date.now(), runtime: 0 }
        }
    }

    _progress_cb_handler() {
        if (!this.sync_completed) {
            this.progress.all.runtime = Date.now() - this.progress.all.start
            this.progress_cb(this.progress)
        }
        if (this.progress.all.state == "completed") {
            this.sync_completed = true
        }
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
        const subsampling = [
            { entries: 96, samples: 15, source:   60, sink:   900 },
            { entries: 24, samples:  4, source:  900, sink:  3600 },
            { entries:  1, samples: 24, source: 3600, sink: 86400 }
        ]
        for (const s of subsampling) {
            this.subsample(s)
        }
    }

    subsample({ entries, samples, source, sink }) {
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


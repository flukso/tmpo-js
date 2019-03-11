$.ajaxSetup({
    timeout: 10 * 60 * 1000, // msecs
    cache: false
})

class Tmpo {
    constructor(uid, token, debug=false) {
        this.uid = uid
        this.token = token
        this.debug = debug
        this.dbPromise = idb.open("flukso", 1, (upgradeDB) => {
            switch (upgradeDB.oldVersion) {
            case 0: // called at DB creation time
                upgradeDB.createObjectStore("device")
                upgradeDB.createObjectStore("sensor")
                upgradeDB.createObjectStore("tmpo")
            }
        })
        this.sync_completed = false
        this.progress = this._progress_state_init("completed")
        this.progress_cb = (() => { return })
    }

    sync(progress_cb) {
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
            this.dbPromise.then((db) => {
                const tx = db.transaction("sensor", "readwrite")
                tx.objectStore("sensor").put(sensors, device)
                for (let i in sensors) {
                    this.progress.sensor.state = "running";
                    this.progress.sensor.todo++;
                    this._last_block(sensors[i].sensor)
                }
                if (this.progress.device.state == "completed" &&
                    this.progress.sensor.state != "running") { // edge case
                    this.progress.all.state = "completed"
                }
                this._progress_cb_handler()
            })
        })
    }

    _last_block(sensor) {
        // get all tmpo blocks of a single sensor
        const range = IDBKeyRange.bound(sensor, `${sensor}~`)
        const that = this
        let last = {
            rid: 0,
            lvl: 0,
            bid: 0
        }
        this.dbPromise.then((db) => {
            const tx = db.transaction("tmpo", "readonly")
            const store = tx.objectStore("tmpo")
            return store.openKeyCursor(range)
        })
        .then(function process(cursor) {
            if (!cursor) { return }
//            console.log('cursor is at:', cursor.key)
            const block = that._key2bid(cursor.key)
            if (block.bid > last.bid) {
                last = block
            }
            return cursor.continue().then(process)
        })
        .then(() => {
            const last_key = that._bid2key(sensor, last.rid, last.lvl, last.bid)
            that._log("last", `last block: ${last_key}`)
            that._tmpo_sensor_sync(sensor, last)
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

    _tmpo_delete_block(key) {
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

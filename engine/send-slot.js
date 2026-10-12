/**
 * Batasi jumlah operasi kirim WA yang benar-benar jalan bersamaan (proses-wide).
 * Throttle by heap/RSS + batas antrean waiters → kurangi spiral q= / RSS “nempel” / proses terasa hank.
 */
import config from './config/config.js';

class GlobalSendSlot {
    constructor() {
        this.max = 256;
        this.inFlight = 0;
        /** @type {(() => void)[]} */
        this.waiters = [];
        this.configured = false;
    }

    _configure() {
        if (this.configured) {
            return;
        }
        const perf = config.performance || {};
        let max = Number(perf.httpMaxConcurrentSends);
        if (!Number.isFinite(max) || max < 1) {
            max = 256;
        }
        this.max = Math.max(4, Math.min(2048, Math.floor(max)));
        this.configured = true;
    }

    /**
     * Paralel efektif: min(config, throttle heap, throttle RSS).
     */
    _effectiveMax() {
        this._configure();
        let eff = this.max;
        const perf = config.performance || {};
        const mu = process.memoryUsage();
        const heapMb = mu.heapUsed / 1024 / 1024;
        const rssMb = mu.rss / 1024 / 1024;

        const heapSoft = Number(perf.sendSlotHeapSoftMb);
        const heapCap = Number(perf.sendSlotHeapPressureMax);
        if (Number.isFinite(heapSoft) && heapSoft > 0 && Number.isFinite(heapCap) && heapCap >= 4 && heapMb > heapSoft) {
            eff = Math.min(eff, Math.floor(heapCap));
        }

        const rssSoft = Number(perf.sendSlotRssSoftMb);
        const rssCap = Number(perf.sendSlotRssPressureMax);
        if (Number.isFinite(rssSoft) && rssSoft > 0 && Number.isFinite(rssCap) && rssCap >= 4 && rssMb > rssSoft) {
            eff = Math.min(eff, Math.floor(rssCap));
        }

        return Math.max(4, eff);
    }

    /**
     * @returns {Promise<void>}
     */
    acquire() {
        this._configure();
        const perf = config.performance || {};
        let timeoutMs = Number(perf.sendSlotAcquireTimeoutMs);
        if (!Number.isFinite(timeoutMs) || timeoutMs < 500) {
            timeoutMs = 26000;
        }

        return new Promise((resolve, reject) => {
            let timer = null;
            const clearTimer = () => {
                if (timer) {
                    clearTimeout(timer);
                    timer = null;
                }
            };

            const tryEnter = () => {
                const maxQ = Number(perf.sendSlotMaxQueuedWaiters);
                if (Number.isFinite(maxQ) && maxQ > 0 && this.waiters.length >= maxQ) {
                    clearTimer();
                    reject(
                        new Error(
                            `Send pipeline overloaded: send queue full (${maxQ} waiters). Reduce PHP concurrency or split Baileys workers.`
                        )
                    );
                    return;
                }

                const eff = this._effectiveMax();
                if (this.inFlight < eff) {
                    this.inFlight++;
                    clearTimer();
                    resolve();
                } else {
                    this.waiters.push(tryEnter);
                }
            };

            if (timeoutMs > 0) {
                timer = setTimeout(() => {
                    const i = this.waiters.indexOf(tryEnter);
                    if (i !== -1) {
                        this.waiters.splice(i, 1);
                    }
                    reject(
                        new Error(
                            'Send pipeline busy: global send slot timeout (reduce concurrency or raise HTTP_MAX_CONCURRENT_SENDS)'
                        )
                    );
                }, timeoutMs);
            }

            tryEnter();
        });
    }

    release() {
        this._configure();
        if (this.inFlight > 0) {
            this.inFlight--;
        }
        const next = this.waiters.shift();
        if (next) {
            next();
        }
    }

    getStats() {
        this._configure();
        const perf = config.performance || {};
        const mu = process.memoryUsage();
        const heapMb = mu.heapUsed / 1024 / 1024;
        const rssMb = mu.rss / 1024 / 1024;
        const heapSoft = Number(perf.sendSlotHeapSoftMb);
        const rssSoft = Number(perf.sendSlotRssSoftMb);
        const effectiveMax = this._effectiveMax();

        return {
            max: this.max,
            effectiveMax,
            heapPressure: Number.isFinite(heapSoft) && heapSoft > 0 && heapMb > heapSoft,
            rssPressure: Number.isFinite(rssSoft) && rssSoft > 0 && rssMb > rssSoft,
            heapUsedMb: Math.round(heapMb),
            rssMb: Math.round(rssMb),
            inFlight: this.inFlight,
            queuedWaiters: this.waiters.length,
            maxQueuedWaiters: Number(perf.sendSlotMaxQueuedWaiters) || 0
        };
    }
}

export const globalSendSlot = new GlobalSendSlot();

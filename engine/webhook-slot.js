/**
 * Batasi jumlah POST webhook ke BE yang jalan bersamaan (seluruh proses).
 * Sisanya antri — tidak "maksa" ratusan axios paralel (ringan di Node + BE).
 */
import config from './config/config.js';

class WebhookHttpSlot {
    constructor() {
        this.max = 8;
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
        let max = Number(perf.webhookHttpMaxConcurrent);
        if (!Number.isFinite(max) || max < 1) {
            max = 8;
        }
        this.max = Math.max(1, Math.min(64, Math.floor(max)));
        this.configured = true;
    }

    /**
     * @returns {Promise<void>}
     */
    acquire() {
        this._configure();
        const perf = config.performance || {};
        let timeoutMs = Number(perf.webhookSlotAcquireTimeoutMs);
        if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
            timeoutMs = 0;
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
                if (this.inFlight < this.max) {
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
                    clearTimer();
                    reject(
                        new Error(
                            'Webhook HTTP busy: slot timeout (naikkan WEBHOOK_HTTP_MAX_CONCURRENT atau WEBHOOK_SLOT_ACQUIRE_TIMEOUT_MS)'
                        )
                    );
                }, Math.min(600000, Math.floor(timeoutMs)));
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
        return {
            max: this.max,
            inFlight: this.inFlight,
            queuedWaiters: this.waiters.length
        };
    }
}

export const globalWebhookSlot = new WebhookHttpSlot();

/**
 * Baileys WhatsApp Wrapper - Enhanced
 * Support QR Code dan Pairing Code dengan WebSocket real-time
 * 
 * @version 3.0.0
 * @date 2025-10-08
 */
import './quiet-console.js';

/** Env truthy helper — shared untuk gate log LID (journal). */
function envTruthy(v) {
    if (v == null || v === '') return false;
    const s = String(v).trim().toLowerCase();
    return s === '1' || s === 'true' || s === 'yes' || s === 'on';
}

/**
 * F.3.1 log ke journald — default OFF (LID_SEND_LOG=0).
 * Nyalakan sementara saat triage: LID_SEND_LOG=1 lalu restart shard.
 * Metrik tanpa journal: LID_SEND_METRICS=1 (default).
 */
function lidJournalLogEnabled() {
    return envTruthy(process.env.LID_SEND_LOG);
}

function lidMetricsEnabled() {
    const v = process.env.LID_SEND_METRICS;
    if (v == null || String(v).trim() === '') return true;
    return envTruthy(v);
}

function lidJournalWarn(...args) {
    if (lidJournalLogEnabled()) console.warn(...args);
}
// ✅ BYEE-WAMOD: Menggunakan byee-wamod untuk support interactiveButtons (button URL - cta_url)
// Dokumentasi: https://github.com/bylinggha/byee-wamod
let makeWASocket, useMultiFileAuthState, DisconnectReason, makeCacheableSignalKeyStore, fetchLatestWaWebVersion, fetchLatestBaileysVersion, Browsers, extractImageThumb;
let isJidBroadcast, isJidStatusBroadcast, isJidNewsletter, isLidUser, isPnUser, jidNormalizedUser;
let CacheableNodeCache = null;
import config from './config/config.js';
import { Boom } from '@hapi/boom';
import pino from 'pino';
import qrcode from 'qrcode';
import fs from 'fs';
import path from 'path';

/** Race: cron/logout hapus folder session saat Baileys masih saveCreds → ENOENT. Jangan bubble ke unhandledRejection. */
function wrapSafeSaveCreds(sessionId, sessionDir, rawSaveCreds, onGone) {
    let inflight = Promise.resolve();
    async function safeSaveCreds() {
        const run = (async () => {
        try {
            if (!fs.existsSync(sessionDir)) {
                console.warn(`⚠️ [SAVE-CREDS] Session dir missing (${sessionId}) — skip save`);
                onGone?.();
                return;
            }
            await rawSaveCreds();
        } catch (e) {
            const code = e?.code;
            const msg = e?.message || '';
            if (code === 'ENOENT' && (msg.includes('creds.json') || msg.includes(sessionId) || msg.includes('/sessions/'))) {
                console.warn(`⚠️ [SAVE-CREDS] ENOENT (${sessionId}) — folder removed, skip save`);
                onGone?.();
                return;
            }
            throw e;
        }
        })();
        inflight = inflight.then(() => run, () => run);
        await run;
    }
    // Forum #2512: flush ke disk sebelum socket baru, jangan reconnect baca stub/lama.
    safeSaveCreds.flush = async function flushSaveCreds() {
        try { await inflight; } catch (_) {}
    };
    return safeSaveCreds;
}
import axios from 'axios';
import crypto from 'crypto';
import { createClient } from 'redis';
import { getBaileysWebSocketReadyState, isBaileysWebSocketOpen } from './ws-ready-state.js';

let latestVersionCache = null;
let latestVersionPromise = null;

/** Dipanggil setelah shared Redis siap; index.js mendaftarkan `global.maybeStartWsBroadcastRedisSubscriber` (hanya shard 7001). */
function scheduleWsBroadcastRedisSubscriber() {
    try {
        const fn = global.maybeStartWsBroadcastRedisSubscriber;
        if (typeof fn === 'function') {
            void fn().catch((e) => console.error('[WS-BROADCAST] subscriber:', e?.message || e));
        }
    } catch (_) {}
}

/**
 * Satu koneksi Redis dipakai bersama semua sesi di shard ini.
 *
 * Dipisah jadi fungsi modul (bukan lazy di dalam initRedis) supaya index.js bisa
 * memanggilnya saat boot. Sebelumnya Redis baru connect ketika sesi pertama masuk,
 * jadi shard sehat yang sedang sepi melaporkan redis.connected=false di /baileys/health
 * dan watchdog me-restart-nya tanpa sebab.
 */
export async function ensureSharedRedis() {
    if (global.sharedRedis?.isReady) return global.sharedRedis;
    if (!config.redis?.enabled) return null;

    let pending = global.sharedRedisPromise;
    if (!pending) {
        const redisUrl = config.redis?.url || 'redis://127.0.0.1:6379';
        pending = (async () => {
            try {
                const sharedRedis = createClient({
                    url: redisUrl,
                    socket: {
                        reconnectStrategy: (retries) => {
                            // Jangan pernah menyerah permanen: client mati permanen bikin
                            // /baileys/health lapor redis dead → watchdog restart shard sehat
                            // (root cause pairing putus "Gagal menautkan perangkat").
                            if (retries > 0 && retries % 20 === 0) {
                                console.error(`❌ [SHARED-REDIS] Still reconnecting (attempt ${retries})`);
                            }
                            return Math.min(200 * retries, 10000);
                        },
                        connectTimeout: 2000,
                        commandTimeout: 1500,
                        keepAlive: 30000,
                        noDelay: true,
                        keepAliveInitialDelay: 0
                    },
                    disableClientInfo: true,
                    enableOfflineQueue: false
                });

                sharedRedis.on('error', (err) => {
                    console.error(`❌ [SHARED-REDIS] Error: ${err.message}`);
                });
                sharedRedis.on('connect', () => {
                    console.log('🔄 [SHARED-REDIS] Connecting... (shared for all sessions)');
                });
                sharedRedis.on('ready', () => {
                    console.log('✅ [SHARED-REDIS] Ready for high traffic (shared for all sessions)');
                });
                sharedRedis.on('end', () => {
                    console.log('🔌 [SHARED-REDIS] Connection ended');
                });

                await sharedRedis.connect();
                global.sharedRedis = sharedRedis;
                scheduleWsBroadcastRedisSubscriber();
                console.log(`[SHARED-REDIS-CONNECTED] Shared Redis connected successfully`);
                return sharedRedis;
            } catch (error) {
                console.error('❌ [SHARED-REDIS] Failed to connect:', error.message);
                return null;
            }
        })();
        global.sharedRedisPromise = pending;
        // Reset hanya kalau promise ini masih yang terpasang, supaya percobaan connect
        // berikutnya tidak menghapus promise milik pemanggil lain yang sedang menunggu.
        pending.finally(() => {
            if (global.sharedRedisPromise === pending) global.sharedRedisPromise = null;
        });
    }

    return await pending;
}

/**
 * Bedakan creds hasil pairing SUNGGUHAN dari "stub" yang ditulis requestPairingCode.
 *
 * requestPairingCode() di byee-wamod menulis sendiri creds.me = { id: <nomor>, name: '~' }
 * lalu memancarkan creds.update SEBELUM user menempel kode apa pun. Kalau hanya me.id yang
 * dicek, setiap creds.update berikutnya (pre-key, app-state, dll) terlihat seperti auth
 * berhasil — padahal belum. Akibatnya jendela grace 401 dibuka palsu dan pembersih creds
 * sisa ikut mati, sehingga sesi terkunci: creds stub bikin Baileys mencoba resume,
 * WhatsApp menolak 401, dan kode pairing baru tidak pernah bisa dibuat lagi.
 *
 * configureSuccessfulPairing() hanya terpanggil saat stanza pair-success datang, dan di
 * situlah account/lid/platform terisi. Itu penanda yang tidak bisa dipalsukan.
 */
function credsLookAuthenticated(creds) {
    const id = creds?.me?.id;
    if (!id || typeof id !== 'string' || !id.includes('@') || id.includes('sess_')) return false;
    if (creds.account) return true;
    if (creds.me?.lid) return true;
    if (creds.platform) return true;
    // name '~' adalah placeholder hardcoded requestPairingCode; nama asli berarti pair beres.
    const name = creds.me?.name;
    return Boolean(name && name !== '~');
}

/** creds di disk yang punya me.id tapi belum pernah pair sukses — harus dibuang saat pair baru. */
function credsFileIsPairingStub(credsPath) {
    try {
        const raw = JSON.parse(fs.readFileSync(credsPath, 'utf8'));
        const id = raw?.me?.id ? String(raw.me.id) : '';
        if (!id.includes('@') || id.includes('sess_')) return false;
        return !credsLookAuthenticated(raw);
    } catch (_) {
        return false;
    }
}

/** Media IQ / 1006 — koneksi utama masih bisa hidup; fallback teks/tombol tanpa gambar. */
function isMediaConnFlakyError(err) {
    const raw = `${err?.message || ''} ${err?.toString?.() || ''} ${err?.output?.payload?.message || ''} ${err?.data?.reason || ''}`;
    const m = raw.toLowerCase();
    if (err?.code === 1006) return true;
    if (m.includes('1006')) return true;
    if (m.includes('failed to fetch media connection')) return true;
    if (m.includes('cannot send image') && m.includes('connection issue')) return true;
    if (m.includes('connection issue') && (m.includes('media') || m.includes('fetch'))) return true;
    return false;
}

/** Hasil sendMessageDirect fallback image→text: processSendMessageResult pakai tipe ini. */
const PROCESS_SEND_AS = Symbol.for('baileys.processAs');

/** Key queue webhook global (sama untuk semua sesmen). */
const WEBHOOK_OUTBOX_KEY = 'webhook:outbox';
const WEBHOOK_PROCESSING_KEY = 'webhook:processing';
const GET_MESSAGE_CACHE_TTL_SEC = Math.max(3600, Math.min(7 * 86400, Number(process.env.GET_MESSAGE_CACHE_TTL_SEC) || 7 * 86400));
/** Retry kirim (tetap tunggu msg_id asli) — default 1 retry = 2 attempt total */
const SEND_MESSAGE_RETRY_MAX = Math.max(0, Math.min(2, Number(process.env.SEND_MESSAGE_RETRY_MAX) || 1));

/** Forum #1879: filter broadcast/status/newsletter — kurangi beban event queue multi-session */
function shouldIgnoreJidForFleet(jid) {
    if (!jid || typeof jid !== 'string') {
        return false;
    }
    try {
        if (typeof isJidBroadcast === 'function' && isJidBroadcast(jid)) return true;
        if (typeof isJidStatusBroadcast === 'function' && isJidStatusBroadcast(jid)) return true;
        if (typeof isJidNewsletter === 'function' && isJidNewsletter(jid)) return true;
    } catch {
        /* fallback below */
    }
    if (jid === 'status@broadcast' || jid.endsWith('@broadcast')) return true;
    if (jid.includes('newsletter')) return true;
    return false;
}

function webhookEnvMs(name, def, min, max) {
    const n = Number(process.env[name]);
    if (!Number.isFinite(n)) return def;
    return Math.min(max, Math.max(min, Math.floor(n)));
}

/** Interval worker pop dari outbox (default lebih santai dari 200ms → hemat Redis). */
const WEBHOOK_OUTBOX_WORKER_MS = webhookEnvMs('WEBHOOK_OUTBOX_WORKER_MS', 400, 150, 10000);
/** Satu timer pemeliharaan per proses; override env untuk stuck recovery. */
const WEBHOOK_REDIS_MAINT_INTERVAL_MS = webhookEnvMs('WEBHOOK_REDIS_MAINT_INTERVAL_MS', 25000, 5000, 120000);
/** LLEN di hot path worker hanya tiap N siklus (bukan tiap tick). */
const WEBHOOK_WORKER_LLEN_EVERY = Math.max(1, Math.floor(Number(process.env.WEBHOOK_WORKER_LLEN_EVERY) || 12));

/**
 * Pemeliharaan queue webhook (stuck processing, cleanup) — satu interval per proses.
 * Dulu tiap sesmen punya timer sendiri → LLEN/LRANGE berulang N× untuk key yang sama.
 */
async function runSharedWebhookQueueMaintenance() {
    const redis = global.sharedRedis?.isReady ? global.sharedRedis : null;
    if (!redis) return;

    try {
        const queueLength = await redis.lLen(WEBHOOK_OUTBOX_KEY).catch(() => 0);
        const processingLength = await redis.lLen(WEBHOOK_PROCESSING_KEY).catch(() => 0);

        if (queueLength > 0 || processingLength > 0) {
            setImmediate(() =>
                console.log(`📊 [REDIS-CHECK] Queue: ${queueLength} pending, ${processingLength} processing`)
            );
        }

        if (processingLength > 0) {
            const LRANGE_CHUNK = 400;
            const MAX_ITEMS_PER_CHECK = 8000;
            const processingItems = [];
            for (let start = 0; processingItems.length < MAX_ITEMS_PER_CHECK; start += LRANGE_CHUNK) {
                const remain = MAX_ITEMS_PER_CHECK - processingItems.length;
                const end = start + Math.min(LRANGE_CHUNK, remain) - 1;
                const chunk = await redis.lRange(WEBHOOK_PROCESSING_KEY, start, end).catch(() => []);
                if (!chunk.length) break;
                processingItems.push(...chunk);
                if (chunk.length < end - start + 1) break;
            }
            const now = Date.now();
            let movedCount = 0;
            let cleanedCount = 0;
            const confirmedItemsToRemove = new Map();
            const itemsToProcess = [];

            for (const raw of processingItems) {
                try {
                    const item = JSON.parse(raw);
                    const processingTime = now - (item.processingStartedAt || item.timestamp || 0);
                    const itemAge = now - (item.createdAt || item.timestamp || now);
                    const ageDays = Math.round(itemAge / (24 * 60 * 60 * 1000));
                    const event = (item.event || item.status || '').toString().toUpperCase();
                    const isLifecycle = ['DEVICE_SENT', 'DELIVERED', 'READ', 'PLAYED', 'SERVER_ACK', 'MESSAGE_SENT'].includes(event);
                    const lastError = (item.lastError || '').toString().toLowerCase();
                    const isPermanentError =
                        lastError.includes('missing bot_id') ||
                        lastError.includes('missing bot') ||
                        lastError.includes('invalid bot');
                    const msgId = item.msg_id || item.msgId || item.messageId || 'unknown';

                    if ((isPermanentError && ageDays > 3) || ageDays > 10) {
                        itemsToProcess.push({ action: 'remove', raw, event, msgId, reason: 'old' });
                        continue;
                    }

                    if (isLifecycle && msgId !== 'unknown') {
                        const confirmKey = `webhook:confirmed:${msgId}:${event}`;
                        const key = `${msgId}:${event}`;

                        if (!confirmedItemsToRemove.has(key)) {
                            const alreadyConfirmed = await redis.get(confirmKey).catch(() => null);
                            if (alreadyConfirmed) {
                                confirmedItemsToRemove.set(key, { raw, event, msgId });
                            }
                        } else {
                            confirmedItemsToRemove.set(key, { raw, event, msgId });
                        }

                        if (confirmedItemsToRemove.has(key)) {
                            continue;
                        }
                    }

                    if (processingTime > 120000) {
                        itemsToProcess.push({ action: 'move', raw, event, msgId, isLifecycle, processingTime });
                    }
                } catch {
                    itemsToProcess.push({
                        action: 'remove',
                        raw,
                        event: 'UNKNOWN',
                        msgId: 'unknown',
                        reason: 'invalid'
                    });
                }
            }

            if (confirmedItemsToRemove.size > 0) {
                const batchRemovePromises = [];
                const confirmedByEvent = new Map();

                for (const { raw, event, msgId } of confirmedItemsToRemove.values()) {
                    batchRemovePromises.push(redis.lRem(WEBHOOK_PROCESSING_KEY, 1, raw).catch(() => {}));

                    if (!confirmedByEvent.has(event)) {
                        confirmedByEvent.set(event, new Set());
                    }
                    confirmedByEvent.get(event).add(msgId);
                }

                await Promise.all(batchRemovePromises);
                cleanedCount += confirmedItemsToRemove.size;

                const summary = Array.from(confirmedByEvent.entries())
                    .map(([event, msgIds]) => `${event}(${msgIds.size} items)`)
                    .join(', ');
                console.log(
                    `✅ [REDIS-CHECK] Removed ${confirmedItemsToRemove.size} confirmed items from processing: ${summary} (prevent spam)`
                );
            }

            for (const { action, raw, event, msgId, isLifecycle, processingTime, reason } of itemsToProcess) {
                if (action === 'remove') {
                    await redis.lRem(WEBHOOK_PROCESSING_KEY, 1, raw).catch(() => {});
                    cleanedCount++;
                    if (reason === 'old') {
                        let ageDays = 0;
                        try {
                            ageDays = Math.round(
                                (now - (JSON.parse(raw).createdAt || now)) / (24 * 60 * 60 * 1000)
                            );
                        } catch {
                            ageDays = 0;
                        }
                        console.warn(`🗑️ [AGGRESSIVE-CLEANUP] Removed old item: ${event} msgId: ${msgId} (${ageDays} days old)`);
                    }
                } else if (action === 'move') {
                    await redis.lRem(WEBHOOK_PROCESSING_KEY, 1, raw);

                    if (isLifecycle) {
                        await redis.lPush(WEBHOOK_OUTBOX_KEY, raw).catch(() => {});
                    } else {
                        await redis.rPush(WEBHOOK_OUTBOX_KEY, raw).catch(() => {});
                    }

                    movedCount++;
                    console.warn(
                        `⚠️ [REDIS-CHECK] Moved stuck item back to queue: ${event} msgId: ${msgId} (stuck for ${Math.round(processingTime / 1000)}s)`
                    );
                }
            }

            if (movedCount > 0) {
                console.log(`✅ [REDIS-CHECK] Moved ${movedCount} stuck items back to queue`);
            }
            if (cleanedCount > confirmedItemsToRemove.size) {
                console.log(
                    `🗑️ [REDIS-CHECK] Cleaned ${cleanedCount - confirmedItemsToRemove.size} old/permanent-error items from queue`
                );
            }
        }

        if (queueLength > 100) {
            const outboxItems = await redis.lRange(WEBHOOK_OUTBOX_KEY, 0, 50).catch(() => []);
            const now2 = Date.now();
            let cleanedOutboxCount = 0;

            for (const raw of outboxItems) {
                try {
                    const item = JSON.parse(raw);
                    const itemAge = now2 - (item.createdAt || item.timestamp || now2);
                    const ageDays = Math.round(itemAge / (24 * 60 * 60 * 1000));
                    const event = (item.event || item.status || '').toString().toUpperCase();
                    const lastError = (item.lastError || '').toString().toLowerCase();
                    const isPermanentError =
                        lastError.includes('missing bot_id') ||
                        lastError.includes('missing bot') ||
                        lastError.includes('invalid bot');

                    if ((isPermanentError && ageDays > 3) || ageDays > 10) {
                        await redis.lRem(WEBHOOK_OUTBOX_KEY, 1, raw);
                        cleanedOutboxCount++;
                    }
                } catch {
                    await redis.lRem(WEBHOOK_OUTBOX_KEY, 1, raw).catch(() => {});
                    cleanedOutboxCount++;
                }
            }

            if (cleanedOutboxCount > 0) {
                console.log(`🗑️ [REDIS-CHECK] Cleaned ${cleanedOutboxCount} old/permanent-error items from outbox queue`);
            }
        }
    } catch (err) {
        console.error(`❌ [REDIS-CHECK] Error checking Redis queue:`, err.message);
    }
}

function ensureSharedWebhookQueueMaintTimer() {
    if (global.__baileysWebhookRedisMaintInterval) return;
    global.__baileysWebhookRedisMaintInterval = setInterval(() => {
        void runSharedWebhookQueueMaintenance();
    }, WEBHOOK_REDIS_MAINT_INTERVAL_MS);
    console.log(
        `✅ [REDIS-CHECK] Shared queue maintenance every ${WEBHOOK_REDIS_MAINT_INTERVAL_MS}ms (one timer per process)`
    );
}

/** official = @whiskeysockets/baileys; default byee-wamod (button cta_url) */
function getBaileysLibEngine() {
    const e = String(process.env.BAILEYS_LIB_ENGINE || process.env.BAILEYS_USE || 'byee-wamod').trim().toLowerCase();
    if (['official', 'whiskeysockets', '@whiskeysockets/baileys', 'baileys'].includes(e)) return 'official';
    return 'byee-wamod';
}

function bindBaileysExports(mod) {
    makeWASocket = mod.default || mod.makeWASocket;
    useMultiFileAuthState = mod.useMultiFileAuthState;
    DisconnectReason = mod.DisconnectReason;
    makeCacheableSignalKeyStore = mod.makeCacheableSignalKeyStore;
    fetchLatestWaWebVersion = mod.fetchLatestWaWebVersion;
    fetchLatestBaileysVersion = mod.fetchLatestBaileysVersion;
    Browsers = mod.Browsers || { ubuntu: () => ["Ubuntu", "Chrome", "22.04.2"], windows: () => ["Windows", "Chrome", "10"], macOS: () => ["macOS", "Chrome", "10"] };
    extractImageThumb = mod.extractImageThumb;
    isJidBroadcast = mod.isJidBroadcast;
    isJidStatusBroadcast = mod.isJidStatusBroadcast;
    isJidNewsletter = mod.isJidNewsletter;
    isLidUser = mod.isLidUser;
    isPnUser = mod.isPnUser;
    jidNormalizedUser = mod.jidNormalizedUser;
}

/**
 * Load Baileys library (ES module) — engine via BAILEYS_LIB_ENGINE env
 */
async function loadBaileys() {
    if (!makeWASocket) {
        const engine = getBaileysLibEngine();
        const mod = engine === 'official'
            ? await (console.log("📦 Loading @whiskeysockets/baileys (official)..."), import("@whiskeysockets/baileys"))
            : await (console.log("📦 Loading byee-wamod library..."), import("byee-wamod"));

        bindBaileysExports(mod);

        try {
            const ncMod = await import('@cacheable/node-cache');
            CacheableNodeCache = ncMod.default;
        } catch (_) {
            console.warn('⚠️ @cacheable/node-cache unavailable — signal key store cap disabled');
        }

        if (typeof makeWASocket !== 'function') {
            console.error("❌ Failed to load makeWASocket!");
            throw new Error("makeWASocket is not a function. Check library structure.");
        }
        if (typeof useMultiFileAuthState !== 'function') {
            throw new Error("useMultiFileAuthState is not available");
        }
        if (!DisconnectReason) {
            throw new Error("DisconnectReason is not available");
        }

        console.log(engine === 'official'
            ? "✅ @whiskeysockets/baileys loaded successfully"
            : "✅ byee-wamod loaded successfully");
    }
}

/** B.1/B.2 — bounded signal key store cache; evict oldest instead of throw (Cache max keys exceeded). */
function createBoundedSignalKeyStoreCache() {
    if (!CacheableNodeCache) return undefined;
    const maxKeys = Math.max(500, Math.min(10000, Number(process.env.SIGNAL_KEY_STORE_CACHE_MAX) || 2500));
    const stdTTL = Math.max(60, Math.min(86400, Number(process.env.SIGNAL_KEY_STORE_CACHE_TTL_SEC) || 600));
    const raw = new CacheableNodeCache({ stdTTL, useClones: false, deleteOnExpire: true, maxKeys });
    return new Proxy(raw, {
        get(target, prop) {
            if (prop === 'set') {
                return (key, val, ttl) => {
                    try {
                        if (!target.has(key) && target.keys().length >= maxKeys) {
                            const oldest = target.keys()[0];
                            if (oldest) target.del(oldest);
                        }
                        return target.set(key, val, ttl);
                    } catch (e) {
                        if (String(e?.message || e).includes('max keys')) {
                            const oldest = target.keys()[0];
                            if (oldest) target.del(oldest);
                            return target.set(key, val, ttl);
                        }
                        throw e;
                    }
                };
            }
            const v = target[prop];
            return typeof v === 'function' ? v.bind(target) : v;
        },
    });
}

function waVersionFetchTimeoutMs() {
    return Math.max(5000, Math.min(30000, Number(process.env.WA_VERSION_FETCH_TIMEOUT_MS) || 12000));
}

/**
 * fetch() bawaan Node mengabaikan opsi { timeout }, jadi fetchLatestWaWebVersion bisa
 * menggantung selamanya. Karena latestVersionPromise dipakai bersama, satu request macet
 * membuat SEMUA pairing di shard ini ikut menunggu → "Gagal menautkan perangkat".
 * AbortController + Promise.race memaksa gagal cepat supaya promise-nya bisa direset.
 */
async function withAbortTimeout(fn, timeoutMs, label) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        return await Promise.race([
            fn(controller.signal),
            new Promise((_, reject) =>
                setTimeout(() => reject(new Error(`${label} timeout ${timeoutMs}ms`)), timeoutMs + 500)
            ),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

/**
 * Get WhatsApp Web version dengan validasi (menggunakan byee-wamod)
 */
async function getLatestBaileysVersionCached() {
    try {
        await loadBaileys();
        
        if (latestVersionCache) {
            return latestVersionCache;
        }

        // ENV pin per-server (.env): WA_WEB_USE_STATIC_VERSION + WA_WEB_VERSION (tanpa ubah config.js)
        const __envUseStatic = String(process.env.WA_WEB_USE_STATIC_VERSION || '').toLowerCase();
        if (['1','true','yes','on'].includes(__envUseStatic)) {
            const __mm = String(process.env.WA_WEB_VERSION || '').trim().match(/^(\d+)\.(\d+)\.(\d+)$/);
            if (__mm) {
                const __envVersion = [Number(__mm[1]), Number(__mm[2]), Number(__mm[3])];
                latestVersionCache = __envVersion;
                console.log('[OK] Using ENV-pinned WA Web version: ' + __envVersion.join('.'));
                return __envVersion;
            }
        }
        const configuredStaticVersion = config.whatsapp?.staticVersion;
        if (config.whatsapp?.useStaticVersion && Array.isArray(configuredStaticVersion) && configuredStaticVersion.length === 3) {
            latestVersionCache = configuredStaticVersion;
            console.log(`✅ Using configured static WA Web version: ${configuredStaticVersion.join('.')}`);
            return configuredStaticVersion;
        }
        
        if (!latestVersionPromise) {
            const versionTimeoutMs = waVersionFetchTimeoutMs();
            latestVersionPromise = withAbortTimeout(
                (signal) => fetchLatestWaWebVersion({ timeout: versionTimeoutMs, signal }),
                versionTimeoutMs,
                'fetchLatestWaWebVersion'
            )
                .then((result) => {
                    const { version, error } = result || {};
                    
                    if (error) {
                        throw new Error(error.message || 'unknown version error');
                    }
                    
                    if (version && Array.isArray(version) && version.length === 3) {
                        const versionNum = version[2];
                        console.log(`📊 Latest version fetched: ${version.join('.')}`);
                        
                        // Rentang dihitung RELATIF terhadap pin statik, bukan angka mati.
                        // Angka mati jadi bom waktu: revisi WA Web naik ~4,7 juta per bulan,
                        // jadi ceiling tetap pasti tertabrak dan sistem diam-diam mundur ke
                        // pin basi. Guard ini hanya untuk menolak nilai sampah, bukan membatasi
                        // pertumbuhan yang wajar.
                        const pinNum = (Array.isArray(configuredStaticVersion) && configuredStaticVersion.length === 3)
                            ? Number(configuredStaticVersion[2])
                            : 1043857760;
                        const minAllowed = Math.max(1, pinNum - 20000000);
                        const maxAllowed = pinNum + 500000000;
                        if (!Number.isFinite(versionNum) || versionNum < minAllowed || versionNum > maxAllowed) {
                            throw new Error(`version out of allowed range: ${versionNum} (izin ${minAllowed}-${maxAllowed})`);
                        }
                        
                        console.log(`✅ Using latest version: ${version.join('.')}`);
                        latestVersionCache = version;
                        latestVersionPromise = null;
                        return version;
                    }
                    
                    throw new Error("Invalid version format");
                })
                .catch(async (err) => {
                    // Forum #2679: fetchLatestBaileysVersion sering versi basi — jangan dipakai fallback.
                    console.warn(`⚠️ Failed to fetch latest WA web version: ${err.message}`);
                    if (latestVersionCache && Array.isArray(latestVersionCache) && latestVersionCache.length === 3) {
                        console.log(`♻️ Reusing last cached version: ${latestVersionCache.join('.')}`);
                        latestVersionPromise = null;
                        return latestVersionCache;
                    }
                    latestVersionPromise = null;
                    throw err;
                });
        }
        return latestVersionPromise;
    } catch (error) {
        console.warn(`⚠️ Failed to load byee-wamod/latest version: ${error.message}`);
        if (latestVersionCache && Array.isArray(latestVersionCache) && latestVersionCache.length === 3) {
            console.log(`♻️ Reusing last cached version: ${latestVersionCache.join('.')}`);
            return latestVersionCache;
        }
        throw error;
    }
}

// ✅ Global flag untuk prevent multiple heap cleanup intervals
let globalHeapCleanupInterval = null;

class BaileysWrapper {
    constructor(sessionId, sessionManager, callbacks = {}) {
        this.sessionId = sessionId;
        this.sessionManager = sessionManager;
        
        // Rumah sesi = port di sessionId (sess_USERID_PORT_TIMESTAMP). Webhook/check/restore
        // wajib ikut ini, bukan process.env.PORT (bisa kebetulan shard yang salah).
        const userIdMatch = String(sessionId || '').match(/^sess_(\d+)_(\d+)_/);
        this.userId = userIdMatch ? userIdMatch[1] : null;
        const rawHome = userIdMatch ? parseInt(userIdMatch[2], 10) : NaN;
        const foldHome = Number.isFinite(rawHome) && rawHome > 0
            ? (7001 + ((((rawHome - 7001) % 7) + 7) % 7))
            : 0;
        this.port = foldHome || Number(process.env.PORT) || null;
        
        this.sock = null;
        this.qr = null;
        this.qrImage = null;
        this.pairingCode = null;
        this.pairingCodeGeneratedTime = null; // ✅ Timestamp saat pairing code di-generate (untuk prevent premature CONNECTED)
        this.requestedPhoneNumber = null;
        this.pairingStartedAt = null; // awal pairing kode (tidak direset saat reconnect) — batas PAIRING_MAX_AGE_MS
        this.isPairingMode = false;
        /** Creds sudah auth sebelum kode tampil — block CONNECTED sampai paste ack */
        this.pairingLeftoverCredsBlocked = false;
        this.pairingPasteAcknowledged = false;
        this.isRestartingForPairing = false; // ✅ Flag untuk skip DISCONNECTED webhook saat restart pairing
        this._pairingStickWatchdog = null; // force CONNECTED jika paste sukses tapi webhook miss
        this.pairingAuthSucceededAt = null; // window resume blip pasca-pair (401/515)
        this._pairing401Remaps = 0; // 401 beruntun dalam grace; lewat cap = WA benar-benar logout
        this._pairing401Exhausted = false;
        this._pairingModeClearTimer = null;
        this._connectedRetryTimer = null;
        this._messageDeletedDedupe = new Map(); // msgId → expireMs
        this.isAuthenticated = false;
        this.messageCount = 0;
        // ✅ REDIS-BASED: Track messageId yang dikirim dari wrapper ini (untuk prevent "bot hantu")
        // ✅ GLOBAL DEDUPLICATION: Menggunakan Redis untuk prevent duplicate di semua server/instance
        // Key format: `sent_messages:{sessionId}:{messageId}` dengan TTL 7 hari
        this.sentMessageIds = new Set(); // In-memory cache untuk fast lookup (backup)
        this.sentMessagesRedisKey = `sent_messages:${sessionId}`; // Redis key untuk sent messages
        this.sentMessagesTTL = 7 * 24 * 60 * 60; // 7 hari TTL untuk sent messages
        this.lastMessagesUpdateAt = Date.now();
        this.lastEventAt = Date.now();
        /** Outbound deaf (#2491): kirim dapat msg_id tapi WA tidak emit status 2 (HP tampak konek). */
        this.lastOutboundAckAt = 0;
        this.lastOutboundSendAt = 0;
        this.outboundSendSinceLastAck = 0;
        this.lastOnWhatsAppProbeAt = 0;
        this.deafRecoveryCount = 0;
        this.reconnectRampUntil = 0;
        /** Echo verify gate (#2491): block kirim setelah SEND-VERIFY gagal sampai recovery/ACK. */
        this.echoVerifyFailCount = 0;
        this.sendGateBlockedUntil = 0;
        this.lastDegradedWebhookAt = 0;
        this.getMessageMemoryCache = new Map(); // msg cache key → { exp, data }
        this.getMessageMemoryCacheMax = Math.max(100, Math.min(500, Number(process.env.GET_MESSAGE_CACHE_MAX) || 200));
        // Format: TMP_{timestamp}_{random16} untuk guarantee unique dan tidak bentrok
        // Mapping: temp_msgid:{tempId} -> {realMsgId} dengan TTL 1 jam
        
        this.webhookFailCount = 0;
        this.webhookCircuitUntil = 0; // epoch ms; if now < this → skip webhook
        this.reconnectInFlight = false;
        // ✅ MEMORY LEAK FIX: Move dedupeCache ke Redis untuk prevent memory heap growth
        // ✅ OPTIMIZED: Gunakan Redis TTL instead of setTimeout untuk prevent timer accumulation
        this.dedupeCache = new Map(); // msgId -> expireAt (fallback jika Redis tidak ada)
        this.dedupeCacheMaxSize = 50; // ✅ LIGHTWEIGHT: Max 50 entries (reduced untuk lebih ringan)
        
        // ✅ IMAGE URL CACHE: Cache untuk URL images yang sama (prevent duplicate processing)
        // ✅ HYBRID CACHE: Memory cache (fast) + Redis cache (persist across restarts)
        // ✅ SPEED OPTIMIZATION: Store local file path untuk skip download (eliminate 1s delay)
        this.imageUrlCache = new Map(); // url -> { processed: true, timestamp: number, localPath: string (optional), thumbnail: base64 string (optional) }
        this.imageUrlCacheTTL = 60 * 60 * 1000; // 1 jam TTL untuk cache URL images
        this.imageUrlCacheMaxSize = 50; // B.2 — selaras cleanup interval (was 100)
        this.imageDownloadPromises = new Map(); // cacheFilePath -> Promise localPath, prevent concurrent .tmp races
        // ✅ REDIS CACHE: Persist image URL cache ke Redis untuk survive restarts
        // ✅ Key format: image_cache:{urlHash} -> {timestamp, processed, localPath}
        // ✅ TTL: 1 jam (sama dengan memory cache)
        
        // ✅ SPEED OPTIMIZATION: Local file cache untuk pre-downloaded images
        // ✅ Eliminate 1s delay dengan menggunakan local file instead of URL
        this.imageCacheDir = path.join(process.cwd(), 'cache', 'images');
        this.imageDownloadTimeout = Math.max(1000, Math.min(15000, Number(process.env.IMAGE_DOWNLOAD_TIMEOUT_MS) || 5000));
        this.imageMaxSize = Math.max(256 * 1024, Math.min(20 * 1024 * 1024, Number(process.env.IMAGE_MAX_BYTES) || (5 * 1024 * 1024)));
        // ✅ Ensure cache directory exists
        try {
            if (!fs.existsSync(this.imageCacheDir)) {
                fs.mkdirSync(this.imageCacheDir, { recursive: true });
            }
        } catch (err) {
            console.warn(`⚠️ Failed to create image cache directory: ${err.message}`);
        }
        
        // 🔧 ULTRA STABLE: Reconnection settings untuk session yang tidak mudah lepas
        // ✅ ANTI-BLOCK: Reconnect dengan exponential backoff untuk prevent WhatsApp block
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = Math.max(2, Math.min(5, Number(process.env.MAX_RECONNECT_ATTEMPTS) || 3));
        this.baseReconnectDelay = 3000; // ✅ FIXED: 3 detik (bukan 1 detik) - prevent too fast reconnect
        this.minReconnectDelay = 3000; // Minimum 3 detik
        this.maxReconnectDelay = 15000; // ✅ IMPROVED: Maximum 15 detik (dari 30s) - lebih responsive untuk reconnect
        this.connectionStreak = 0; // Track berapa kali berhasil connect berturut-turut
        this.lastStableConnection = 0; // Waktu terakhir connection stabil
        this.heartbeatInterval = null; // Interval untuk heartbeat monitoring
        this.authPollingInterval = null; // ✅ LIGHTWEIGHT: Interval untuk polling authentication (ringan, auto-cleanup)
        
        // ✅ CONNECTION STABILITY IMPROVEMENTS: Tracking untuk connection health & idle timeout
        this.connectionHealthInterval = null; // Interval untuk connection health check
        this.heartbeatFailureCount = 0; // Track berapa kali heartbeat gagal berturut-turut
        this.maxHeartbeatFailures = 3; // Max 3 kali heartbeat gagal sebelum trigger reconnect check
        this.lastConnectionHealthCheck = 0; // Timestamp last health check
        this.lastActivityForIdleCheck = Date.now(); // Track last activity untuk idle timeout prevention
        this.idleTimeoutWarningSent = false; // Flag apakah idle timeout warning sudah dikirim
        this.lastDisconnectTime = 0; // Track waktu disconnect terakhir untuk quick reconnect
        this.quickReconnectWindow = 5000; // 5 detik window untuk quick reconnect (temporary disconnect)
        
        // ✅ CONNECTION STABILITY: Tracking untuk connection health check
        this.connectionHealthInterval = null; // Interval untuk connection health check
        this.heartbeatFailureCount = 0; // Track berapa kali heartbeat gagal berturut-turut
        this.maxHeartbeatFailures = 3; // Max 3 kali heartbeat gagal sebelum trigger reconnect check
        this.lastConnectionHealthCheck = 0; // Timestamp last health check
        this.lastActivityForIdleCheck = Date.now(); // Track last activity untuk idle timeout
        this.idleTimeoutWarningSent = false; // Flag apakah idle timeout warning sudah dikirim
        this.lastNonOpenWsAt = 0; // Watchdog untuk socket stuck CLOSING/CLOSED tanpa reconnectInFlight
        
        // ✅ HIGH PERFORMANCE: Rate limiting per session (bukan global)
        // Setiap bot session bisa send dengan interval sendiri (1 detik per bot)
        // ✅ RINGAN: Circular buffer dengan limit kecil untuk prevent heap spike
        // ✅ AMAN: Protection untuk pastikan tidak ada webhook kelewat
        this.messageSendQueue = [];
        this.maxQueueSize = Math.max(1, Math.min(200, Number(config.performance?.maxMessageQueueSize) || 5));
        this.queueProtection = {
            processingItems: new Set(), // Track items yang sedang diproses (prevent dihapus)
            completedItems: new Set()    // Track items yang sudah selesai (prevent duplicate)
        };
        this.isProcessingMessageQueue = false;
        this.queueWasPausedForReconnect = false;
        this.lastMessageSendTime = 0;
        // ✅ OPTIMIZED: High throughput untuk 10 msg/sec per session (100 sessions × 10 msg/sec = 1000 msg/sec)
        // ✅ Server spec: 31GB RAM, 8 cores - SUPPORTED dengan parallel processing
        this.minMessageSendInterval = Math.max(0, Math.min(2000, Number(config.performance?.minMessageSendIntervalMs) || 100));
        this.baseMinMessageSendInterval = this.minMessageSendInterval;
        this.maxConcurrentSends = Math.max(1, Math.min(30, Number(config.performance?.maxConcurrentSendsPerSession) || 3));
        this.activeSends = 0; // Track active sends untuk better concurrency control
        this.terminalDisconnect = false;
        
        // ✅ MONITORING: Track queue metrics untuk detect bottleneck
        this.queueMetrics = {
            maxQueueLength: 0,
            totalProcessed: 0,
            avgWaitTime: 0,
            lastLogTime: Date.now()
        };
        
        // ✅ SUPER SMART: Track last activity untuk auto cleanup (ringan)
        this.lastActivityTime = Date.now(); // Track last activity untuk cleanup inactive sessions
        
        // Forum / skrip git lama: cooldown 30–60s, satu kode ~2 menit. Jangan 18s.
        this.lastPairingCodeRequest = 0;
        this.pairingCodeCooldown = Math.max(30000, Math.min(90000, Number(process.env.PAIRING_CODE_COOLDOWN_MS) || 45000));
        this._lastPairingQr = null;
        this._pairingNeedRefresh = false;
        this._pairingRefreshSoftCd = false;
        
        // ✅ ANTI-BLOCK: Limit untuk QR code regeneration
        this.qrGenerationCount = 0;
        this.qrGenerationResetTime = 0;
        this.maxQrGenerationsPerHour = 10; // Max 10 QR per jam per session
        
        // ✅ HIGH PERFORMANCE: Cache untuk WhatsApp number check (reduce API calls)
        // ✅ MEMORY LEAK FIX: Move ke Redis untuk prevent memory heap growth
        this.whatsappNumberCache = new Map(); // phoneNumber -> { exists: boolean, timestamp: number } (fallback jika Redis tidak ada)
        this.whatsappNumberCacheTTL = 5 * 60 * 1000; // 5 menit cache TTL
        this.whatsappNumberCacheMaxSize = 100; // ✅ LIGHTWEIGHT: Max 100 entries (reduced untuk lebih ringan)
        
        // ✅ WHATSAPP STANDARD FLOW: Flag untuk prevent duplicate lifecycle events
        this.lastLifecycleEvent = null; // Track last lifecycle event yang dikirim
        this.lastLifecycleEventTime = 0; // Timestamp last lifecycle event
        
        // ✅ FIXED: Track disconnect cycle untuk prevent multiple RECONNECTING per disconnect
        this.currentDisconnectCycle = null; // Track disconnect cycle ID (timestamp)
        this.reconnectingSent = false; // Flag apakah RECONNECTING sudah dikirim untuk disconnect cycle ini
        this.sessionDeadWebhookSent = false; // Flag apakah DISCONNECTED webhook sudah dikirim untuk session mati
        
        // Callbacks
        this.onQR = callbacks.onQR || (() => {});
        this.onPairingCode = callbacks.onPairingCode || (() => {});
        this.onConnected = callbacks.onConnected || (() => {});
        this.onDisconnected = callbacks.onDisconnected || (() => {});
        this.onBannedImmediate = callbacks.onBannedImmediate || (() => {});
        this.onMessageReceived = callbacks.onMessageReceived || (() => {});
        this.onMessageSent = callbacks.onMessageSent || (() => {});
        this.onTyping = callbacks.onTyping || (() => {});
        this.onPresenceUpdate = callbacks.onPresenceUpdate || (() => {});
        this.onMessageTimer = callbacks.onMessageTimer || (() => {});
        
        // Logger
        this.logger = pino({ level: 'silent' }); // silent untuk production

        // Redis (outbox) state
        this.redis = null;
        this.outboxKey = 'webhook:outbox';
        this.processingKey = 'webhook:processing';
        this.dedupeTTL = 20; // seconds
        this.workerTimer = null;
        this.reconnectTimer = null;
        this.redisCheckTimer = null; // legacy: pemeliharaan queue pakai timer proses-global (__baileysWebhookRedisMaintInterval)
        this.workerStopped = false; // ✅ SMART: Flag untuk prevent restart saat sudah di-stop manual
        this.workerErrorCount = 0; // ✅ SMART: Track error count untuk circuit breaker
        this.workerMaxErrors = 10; // ✅ SMART: Max errors sebelum auto-stop worker
        this.lastWorkerError = null; // ✅ SMART: Track last error untuk debug
        
        // ✅ CRITICAL: Cache bot_id per session untuk webhook payload
        this.cachedBotId = null; // bot_id yang sudah di-resolve dari webhook server (CONNECTED event)
        
        // ✅ AUTO-GENERATE: Track jumlah ACK yang sudah pernah diterima (untuk enable auto-generate)
        this.ackCount = 0; // In-memory counter (global)
        this.ackCountRedisKey = `ack_count:${sessionId}`; // Redis key untuk persistent tracking (global)
        
        // ✅ FIX: Flag untuk track apakah CONNECTED webhook sudah dikirim (prevent duplicate)
        this.connectedWebhookSent = false;
        // ✅ FIX: Flag untuk prevent multiple execution saat processing CONNECTED webhook
        this.processingConnectedWebhook = false;
        
        // ✅ NEW: Track ACK count per nomor (recipient) untuk validasi sebelum generate SERVER_ACK fallback
        this.ackCountPerNumber = new Map(); // In-memory: nomor -> count
        // msgId → recipientKey (hindari Redis GET beruntun saat recipient-flush)
        this._sentMsgRecipient = new Map();
        this._recipientFlushAt = new Map(); // recipientKey → last flush ms
        
        // ✅ MEMORY LEAK FIX: Periodic cache cleanup setiap 5 menit untuk prevent heap growth (optimized untuk reduce API time)
        this.cacheCleanupInterval = null; // ✅ FIXED: Initialize as null untuk prevent memory leak
        this.startCacheCleanup(); // ✅ Start cache cleanup interval
    }
    
    // ✅ CACHE CLEANUP: Start periodic cache cleanup untuk prevent memory leak
    startCacheCleanup() {
        // ✅ FIXED: Clear existing interval jika ada (prevent duplicate)
        if (this.cacheCleanupInterval) {
            clearInterval(this.cacheCleanupInterval);
        }
        
        // ✅ LIGHTWEIGHT: Cache cleanup setiap 10 menit (dari 5 menit) untuk reduce overhead
        this.cacheCleanupInterval = setInterval(() => {
            try {
                const now = Date.now();
                let cleaned = 0;
                
                // ✅ LIGHTWEIGHT: Kurangi size limit lebih agresif untuk turunkan heap
                // ✅ CRITICAL: Reduce limit untuk prevent OOM (keep last 200 - reduced dari 300)
                if (this.sentMessageIds.size > 200) {
                    const entries = Array.from(this.sentMessageIds);
                    const toKeep = entries.slice(-200);
                    this.sentMessageIds.clear();
                    toKeep.forEach(id => this.sentMessageIds.add(id));
                    cleaned += entries.length - 200;
                }
                
                // ✅ CRITICAL: Cleanup dedupeCache lebih agresif (prevent memory leak)
                if (this.dedupeCache.size > 50) {
                    const entries = Array.from(this.dedupeCache.entries());
                    const expired = entries.filter(([_, expireAt]) => expireAt < now);
                    expired.forEach(([key]) => this.dedupeCache.delete(key));
                    // Jika masih besar, hapus oldest 30%
                    if (this.dedupeCache.size > 50) {
                        const sorted = entries.filter(([k]) => !expired.some(([ek]) => ek === k))
                            .sort((a, b) => a[1] - b[1]);
                        const toDelete = Math.floor(50 * 0.3);
                        for (let i = 0; i < toDelete && i < sorted.length; i++) {
                            this.dedupeCache.delete(sorted[i][0]);
                        }
                        cleaned += toDelete;
                    }
                }
                
                // ✅ CRITICAL: Cleanup imageUrlCache lebih agresif (prevent memory leak)
                if (this.imageUrlCache.size > 50) {
                    const entries = Array.from(this.imageUrlCache.entries());
                    const expired = entries.filter(([_, data]) => !data.timestamp || (now - data.timestamp) > this.imageUrlCacheTTL);
                    expired.forEach(([key]) => this.imageUrlCache.delete(key));
                    // Jika masih besar, hapus oldest 30%
                    if (this.imageUrlCache.size > 50) {
                        const sorted = entries.filter(([k]) => !expired.some(([ek]) => ek === k))
                            .sort((a, b) => (a[1].timestamp || 0) - (b[1].timestamp || 0));
                        const toDelete = Math.floor(50 * 0.3);
                        for (let i = 0; i < toDelete && i < sorted.length; i++) {
                            this.imageUrlCache.delete(sorted[i][0]);
                        }
                        cleaned += toDelete;
                    }
                }
                
                // ✅ CRITICAL: Cleanup dedupeCache lebih agresif (prevent OOM)
                // ✅ Reduced limit: 100 (dari 300) untuk prevent memory leak
                if (this.dedupeCache.size > 100) {
                    for (const [key, expireAt] of this.dedupeCache.entries()) {
                        if (expireAt < now) {
                            this.dedupeCache.delete(key);
                            cleaned++;
                        }
                        if (cleaned > 50) break; // Limit per cleanup (reduced)
                    }
                    // Jika masih besar setelah remove expired, trim ke 100
                    if (this.dedupeCache.size > 100) {
                        const entries = Array.from(this.dedupeCache.entries());
                        const toKeep = entries.slice(-100);
                        this.dedupeCache.clear();
                        toKeep.forEach(([key, value]) => this.dedupeCache.set(key, value));
                        cleaned += entries.length - 100;
                    }
                }
                
                // Cleanup imageUrlCache (remove expired, limit 50 - reduced dari 100)
                if (this.imageUrlCache.size > 50) {
                    for (const [key, value] of this.imageUrlCache.entries()) {
                        if (value.timestamp && (now - value.timestamp) > this.imageUrlCacheTTL) {
                            this.imageUrlCache.delete(key);
                            cleaned++;
                        }
                        if (cleaned > 150) break; // Limit per cleanup
                    }
                    // Jika masih besar, trim ke 50
                    if (this.imageUrlCache.size > 50) {
                        const entries = Array.from(this.imageUrlCache.entries());
                        const toKeep = entries.slice(-50);
                        this.imageUrlCache.clear();
                        toKeep.forEach(([key, value]) => this.imageUrlCache.set(key, value));
                        cleaned += entries.length - 50;
                    }
                }
                
                // Cleanup whatsappNumberCache (remove expired, limit 100 - reduced dari 200)
                if (this.whatsappNumberCache.size > 100) {
                    for (const [key, value] of this.whatsappNumberCache.entries()) {
                        if (value.timestamp && (now - value.timestamp) > this.whatsappNumberCacheTTL) {
                            this.whatsappNumberCache.delete(key);
                            cleaned++;
                        }
                        if (cleaned > 150) break; // Limit per cleanup
                    }
                    // Jika masih besar, trim ke 100
                    if (this.whatsappNumberCache.size > 100) {
                        const entries = Array.from(this.whatsappNumberCache.entries());
                        const toKeep = entries.slice(-100);
                        this.whatsappNumberCache.clear();
                        toKeep.forEach(([key, value]) => this.whatsappNumberCache.set(key, value));
                        cleaned += entries.length - 100;
                    }
                }
                
                // Cleanup ackCountPerNumber (keep last 200 - reduced dari 500)
                if (this.ackCountPerNumber.size > 200) {
                    const entries = Array.from(this.ackCountPerNumber.entries());
                    const toKeep = entries.slice(-200);
                    this.ackCountPerNumber.clear();
                    toKeep.forEach(([key, value]) => this.ackCountPerNumber.set(key, value));
                    cleaned += entries.length - 200;
                }
                
                // Cleanup statusHistory (keep last 100 - reduced dari 150)
                if (this.statusHistory && this.statusHistory.size > 100) {
                    const entries = Array.from(this.statusHistory.entries());
                    const toKeep = entries.slice(-100);
                    this.statusHistory.clear();
                    toKeep.forEach(([key, value]) => this.statusHistory.set(key, value));
                    cleaned += entries.length - 100;
                }
                
                // ✅ FIX MEMORY SPIKE: Cleanup messageSendQueue jika terlalu besar
                // ✅ AMAN: Hanya hapus item yang TIDAK sedang diproses (prevent webhook kelewat)
                if (this.messageSendQueue && this.messageSendQueue.length > this.maxQueueSize) {
                    const queueLength = this.messageSendQueue.length;
                    // ✅ AMAN: Filter hanya item yang tidak sedang diproses
                    const nonProcessingItems = this.messageSendQueue.filter(item => 
                        !item.processing && 
                        item.itemId &&
                        !this.queueProtection.processingItems.has(item.itemId)
                    );
                    
                    if (nonProcessingItems.length > 0) {
                        // ✅ AMAN: Keep last 5 items yang tidak sedang diproses, reject yang dihapus
                        const toKeep = 5;
                        const toRemove = nonProcessingItems.slice(0, Math.max(0, nonProcessingItems.length - toKeep));
                        
                        toRemove.forEach(item => {
                            // ✅ CRITICAL: Reject item yang dihapus untuk prevent webhook kelewat
                            if (item.reject) {
                                item.reject(new Error('Queue cleanup, item removed'));
                            }
                            const index = this.messageSendQueue.indexOf(item);
                            if (index > -1) {
                                this.messageSendQueue.splice(index, 1);
                            }
                        });
                        
                        cleaned += toRemove.length;
                        const processingCount = this.queueProtection.processingItems.size;
                        console.log(`🧹 [CACHE CLEANUP] Trimmed messageSendQueue: ${queueLength} → ${this.messageSendQueue.length} (protected ${processingCount} processing items, removed ${toRemove.length} items)`);
                    } else {
                        // Semua sedang diproses, skip cleanup
                        console.log(`🧹 [CACHE CLEANUP] Skipped messageSendQueue cleanup - all ${queueLength} items are processing`);
                    }
                }
                
                // ✅ LEAK FIX: cap getMessageMemoryCache (sebelumnya tidak pernah di-trim -> sumber heap growth)
                if (this.getMessageMemoryCache && this.getMessageMemoryCache.size > 300) {
                    for (const [__k, __v] of this.getMessageMemoryCache.entries()) {
                        if (!__v || !__v.exp || __v.exp < now) { this.getMessageMemoryCache.delete(__k); cleaned++; }
                    }
                    if (this.getMessageMemoryCache.size > 300) {
                        const __keep = Array.from(this.getMessageMemoryCache.entries()).slice(-300);
                        const __removed = this.getMessageMemoryCache.size - __keep.length;
                        this.getMessageMemoryCache.clear();
                        __keep.forEach(([__kk, __vv]) => this.getMessageMemoryCache.set(__kk, __vv));
                        cleaned += __removed;
                    }
                }

                this._cacheLogTick = (this._cacheLogTick || 0) + 1;
                if (this._cacheLogTick % 6 === 0) {
                    console.warn(
                        `[CACHE-STATS] ${this.sessionId} sent=${this.sentMessageIds?.size ?? 0} dedupe=${this.dedupeCache?.size ?? 0} img=${this.imageUrlCache?.size ?? 0} status=${this.statusHistory?.size ?? 0} getMsg=${this.getMessageMemoryCache?.size ?? 0}`
                    );
                }

                if (cleaned > 0) {
                    const memUsage = process.memoryUsage();
                    const heapUsedMB = (memUsage.heapUsed / 1024 / 1024).toFixed(2);
                    const heapTotalMB = (memUsage.heapTotal / 1024 / 1024).toFixed(2);
                    console.log(`🧹 [CACHE CLEANUP] Cleaned ${cleaned} entries, Heap: ${heapUsedMB}MB / ${heapTotalMB}MB`);
                    
                    // ✅ AGGRESSIVE: Force GC setelah cleanup untuk immediate effect
                    if (typeof global.gc === 'function' && cleaned > 50) {
                        try {
                            global.gc();
                            const memAfterGC = process.memoryUsage();
                            const heapAfterMB = (memAfterGC.heapUsed / 1024 / 1024).toFixed(2);
                            const freedMB = (parseFloat(heapUsedMB) - parseFloat(heapAfterMB)).toFixed(2);
                            if (parseFloat(freedMB) > 5) {
                                console.log(`🧹 [CACHE CLEANUP] After GC: ${heapAfterMB}MB (freed: ${freedMB}MB)`);
                            }
                        } catch (gcErr) {
                            // Ignore GC error
                        }
                    }
                }
            } catch (err) {
                console.warn(`⚠️ [CACHE CLEANUP] Error: ${err.message}`);
            }
        }, 3 * 60 * 1000); // ✅ LIGHTWEIGHT: Every 10 minutes (reduced overhead, masih efektif untuk prevent spike)
    }
    
    // ✅ TEMPORARY MESSAGE ID: Generate temporary message ID untuk enable_callback=true
    // Format: TMP_{12karakter} - timestamp base36 (4 chars) + random hex (8 chars) = 12 chars setelah TMP_
    // ✅ Total: TMP_ (4) + 12 = 16 karakter total
    // ✅ GUARANTEE UNIQUE: Kombinasi timestamp + random tetap unique dan tidak bentrok
    generateTempMessageId() {
        const timestamp = Date.now();
        // Convert timestamp ke base36 untuk lebih pendek
        const timestampBase36 = timestamp.toString(36);
        const shortTimestamp = timestampBase36.slice(-4); // 4 karakter timestamp (unique per ~1000ms)
        const random = crypto.randomBytes(4).toString('hex'); // 8 karakter hex (4 bytes)
        // Total: TMP_ (4) + 4 (timestamp) + 8 (random) = 16 karakter total
        return `TMP_${shortTimestamp}${random}`;
    }
    
    // ✅ TEMPORARY MESSAGE ID: Simpan mapping temp ID -> real ID di Redis
    async saveTempMessageIdMapping(tempMsgId, realMsgId) {
        if (!this.redis) return;
        const key = `temp_msgid:${tempMsgId}`;
        // TTL 1 jam untuk temporary mapping (cukup untuk update webhook)
        await this.redis.set(key, realMsgId, { EX: 3600 }).catch(() => {});
        
        // ✅ CRITICAL: Check apakah ada pending MESSAGE_SENT context untuk tempMsgId ini
        // ✅ Smart queue: Trigger otomatis saat real messageId tersedia (lebih cepat daripada polling retry)
        // ✅ Context sudah disimpan lengkap di awal untuk prevent data loss
        const pendingContextKey = `pending_msgsent:${tempMsgId}`;
        const pendingContext = await this.redis.get(pendingContextKey).catch(() => null);
        if (pendingContext) {
            try {
                const context = JSON.parse(pendingContext);
                // ✅ GREP: [TEMP-MSGID-MAPPING-TRIGGER]
                console.log(`[TEMP-MSGID-MAPPING-TRIGGER] tempMsgId=${tempMsgId} realMsgId=${realMsgId}`);
                
                // ✅ CRITICAL: Trigger queue dengan real messageId menggunakan context lengkap
                // ✅ Semua data sudah tersimpan lengkap (to, message, remoteJid, messageType, dll)
                await this.queueMessageSentWebhook(realMsgId, context.to, context.message, context.remoteJid || context.to, tempMsgId);
                
                // ✅ Hapus pending context setelah di-queue (prevent duplicate)
                await this.redis.del(pendingContextKey).catch(() => {});
                // ✅ GREP: [TEMP-MSGID-MAPPING-SUCCESS]
                console.log(`[TEMP-MSGID-MAPPING-SUCCESS] tempMsgId=${tempMsgId} realMsgId=${realMsgId}`);
            } catch (err) {
                // ✅ GREP: [TEMP-MSGID-MAPPING-ERROR]
                console.error(`[TEMP-MSGID-MAPPING-ERROR] tempMsgId=${tempMsgId} realMsgId=${realMsgId} error=${err.message}`);
                // ✅ FALLBACK: Jangan hapus context jika error, biarkan retry nanti
            }
        }
    }
    
    // ✅ TEMPORARY MESSAGE ID: Ambil real message ID dari temporary ID
    async getRealMessageIdFromTemp(tempMsgId) {
        if (!this.redis) return null;
        const key = `temp_msgid:${tempMsgId}`;
        return await this.redis.get(key).catch(() => null);
    }
    
    // ✅ TEMPORARY MESSAGE ID: Ambil temporary message ID dari real ID (reverse lookup)
    async getTempMessageIdFromReal(realMsgId) {
        if (!this.redis) return null;
        const key = `real_to_temp:${realMsgId}`;
        return await this.redis.get(key).catch(() => null);
    }
    
    // ✅ HELPER: Queue MESSAGE_SENT webhook setelah messageId tersedia (untuk enableCallback=true)
    async queueMessageSentWebhook(messageId, to, message, remoteJid, tempMsgId = null) {
        // ✅ GREP: [QUEUE-MSG-SENT-START]
        console.log(`[QUEUE-MSG-SENT-START] messageId=${messageId || 'NONE'} tempMsgId=${tempMsgId || 'NONE'} redis=${!!this.redis}`);
        
        if (!this.redis || !messageId) {
            // ✅ GREP: [QUEUE-MSG-SENT-ERROR]
            console.log(`[QUEUE-MSG-SENT-ERROR] Cannot queue: redis=${!!this.redis} messageId=${messageId || 'MISSING'}`);
            return;
        }
        
        try {
            // ✅ Check apakah sudah pernah di-queue (prevent duplicate)
            const forceQueuedFlag = await this.redis.get(`force_queued:${messageId}:MESSAGE_SENT`).catch(() => null);
            if (forceQueuedFlag) {
                // ✅ GREP: [QUEUE-MSG-SENT-DUPLICATE]
                console.log(`[QUEUE-MSG-SENT-DUPLICATE] messageId=${messageId} tempMsgId=${tempMsgId || 'NONE'}`);
                return;
            }
            
            // ✅ FIX: Extract nomor WhatsApp yang clean dan bot_id dari cache
            const cleanBotNumber = this.getCleanBotNumber();
            const botIdInt = this.cachedBotId || 0;
            
            // ✅ CRITICAL: Simpan reverse mapping SEBELUM buat payload untuk memastikan temp_msg_id tersedia
            // ✅ Reverse mapping: real_msg_id -> temp_msg_id (untuk auto-inject di webhook lainnya)
            if (tempMsgId && this.redis) {
                await this.redis.set(`real_to_temp:${messageId}`, tempMsgId, { EX: 3600 }).catch(() => {});
            }
            
            const webhookPayload = {
                event: 'MESSAGE_SENT',
                bot: cleanBotNumber,
                bot_id: botIdInt,
                sessionId: this.sessionId,
                msg_id: messageId, // ✅ Real message ID dari WhatsApp
                status: 'message_sent',
                user_id: this.userId,
                port: this.port,
                recipient: remoteJid || (to.includes('@') ? to : `${to}@s.whatsapp.net`),
                message: message,
                timestamp: Date.now(),
                force_queued: true, // Flag bahwa ini force queue untuk guarantee delivery
                temp_msg_id: tempMsgId || undefined // ✅ CRITICAL: Include temp_msg_id di payload
            };
            
            // ✅ CRITICAL: Set flag force_queued di Redis SEBELUM queue untuk prevent race condition
            await this.redis.set(`force_queued:${messageId}:MESSAGE_SENT`, '1', { EX: 3600 }).catch(() => {});
            
            // ✅ CRITICAL: Force queue dengan priority tinggi
            const enqueueResult = await this.enqueueWebhook(webhookPayload, { forceDirect: false }).catch(err => {
                // ✅ GREP: [QUEUE-MSG-SENT-ERROR]
                console.error(`[QUEUE-MSG-SENT-ERROR] messageId=${messageId} tempMsgId=${tempMsgId || 'NONE'} error=${err.message}`);
                // ✅ FALLBACK: Jika queue gagal, coba direct send dengan retry
                this.sendToWebhookWithRetry(webhookPayload, 3).catch(() => {});
                return null;
            });
            
            // ✅ GREP: [QUEUE-MSG-SENT-SUCCESS]
            if (enqueueResult) {
                const resultType = enqueueResult.direct ? 'direct' : (enqueueResult.queued ? 'queued' : 'unknown');
                console.log(`[QUEUE-MSG-SENT-SUCCESS] messageId=${messageId} tempMsgId=${tempMsgId || 'NONE'} type=${resultType}`);
            } else {
                console.log(`[QUEUE-MSG-SENT-SUCCESS] messageId=${messageId} tempMsgId=${tempMsgId || 'NONE'} type=queued`);
            }
            
            // ✅ CYCLE START: MESSAGE_SENT adalah awal lifecycle events cycle
            // ✅ AUTO-GENERATE: Setelah MESSAGE_SENT — hanya bila session sudah warm (hindari webhook sia-sia + tidak credit)
            if (config.autoGenerate?.enabled) {
                setImmediate(async () => {
                    try {
                        await new Promise(resolve => setTimeout(resolve, config.autoGenerate?.delayMs || 100));
                        if (!(await this.canFallbackServerAck(''))) {
                            return;
                        }
                        
                        // ✅ DEDUPE CHECK: Check apakah SERVER_ACK sudah ada
                        const [serverAckOriginal, serverAckLifecycleBot] = await Promise.all([
                            this.redis ? this.redis.get(`webhook:confirmed:${messageId}:SERVER_ACK:original`).catch(() => null) : null,
                            (this.redis && this.cachedBotId > 0)
                                ? this.redis.get(`lifecycle:server_ack:${this.cachedBotId}:${messageId}`).catch(() => null)
                                : null,
                        ]);
                        
                        if (serverAckOriginal || serverAckLifecycleBot) {
                            console.log(`⏭️ [AUTO-GEN] SERVER_ACK already exists for msgId ${messageId}, skipping auto-generate`);
                            return;
                        }
                        
                        const cleanBotNumber = this.getCleanBotNumber();
                        const minAck = config.autoGenerate?.minAckCount || 5;
                        if ((this.ackCount || 0) < minAck) {
                            console.log(`⏭️ [AUTO-GEN] skip SERVER_ACK msgId=${messageId} ackCount=${this.ackCount} < ${minAck}`);
                            return;
                        }
                        console.log(`🔄 [AUTO-GEN] MESSAGE_SENT queued (ACK count: ${this.ackCount}), generating SERVER_ACK for msgId: ${messageId}`);
                        this.enqueueWebhook({
                            event: 'SERVER_ACK',
                            bot: cleanBotNumber,
                            bot_id: this.cachedBotId || 0,
                            sessionId: this.sessionId,
                            msg_id: messageId,
                            status: 'server_ack',
                            user_id: this.userId,
                            port: this.port,
                            auto_generated: true
                        }, { forceDirect: false }).catch(() => {});
                    } catch (e) {
                        // Ignore error, non-blocking
                    }
                });
            }
        } catch (err) {
            console.error(`❌ [QUEUE-MSG-SENT] Error queueing MESSAGE_SENT webhook for msgId ${messageId}:`, err.message);
        }
    }
    
    /**
     * Daftarkan msgId ke memory + Redis SEBELUM return API / setImmediate.
     * Cegah race: messages.update (SERVER_ACK) vs processSendMessageResult async.
     * Dipakai untuk text, image, button — semua tipe kirim.
     */
    registerOutboundMessageSync(result, to, enableCallback = false) {
        const messageId = result?.key?.id;
        if (!messageId) {
            return null;
        }
        const toStr = String(to || '');
        const jid = toStr.includes('@') ? toStr : `${toStr}@s.whatsapp.net`;
        const remoteJid = result?.key?.remoteJid || jid;
        const idKey = String(messageId).toUpperCase();
        // Idempotent: sendMessage + processSendMessageResult sering panggil 2× — jangan double counter/timer
        if (!this._outboundRegisteredIds) this._outboundRegisteredIds = new Set();
        const already = this._outboundRegisteredIds.has(idKey) || this.sentMessageIds.has(messageId) || this.sentMessageIds.has(idKey);

        this.sentMessageIds.add(messageId);
        if (idKey !== messageId) this.sentMessageIds.add(idKey);
        this._outboundRegisteredIds.add(idKey);
        if (this._outboundRegisteredIds.size > 800) {
            this._outboundRegisteredIds = new Set(Array.from(this._outboundRegisteredIds).slice(-600));
        }

        if (this.sentMessageIds.size > 500) {
            const entries = Array.from(this.sentMessageIds);
            for (const id of entries.slice(0, entries.length - 400)) {
                this.sentMessageIds.delete(id);
            }
        }

        const recipientKey = this.normalizeRecipientKey(jid);

        if (recipientKey) {
            if (!this._sentMsgRecipient) this._sentMsgRecipient = new Map();
            this._sentMsgRecipient.set(messageId, recipientKey);
            this._sentMsgRecipient.set(idKey, recipientKey);
            if (this._sentMsgRecipient.size > 1000) {
                const keys = Array.from(this._sentMsgRecipient.keys());
                for (const k of keys.slice(0, keys.length - 800)) {
                    this._sentMsgRecipient.delete(k);
                }
            }
        }

        if (this.redis) {
            const key = `${this.sentMessagesRedisKey}:${messageId}`;
            this.redis.set(key, '1', { EX: this.sentMessagesTTL }).catch(() => {});
            if (idKey !== messageId) {
                this.redis.set(`${this.sentMessagesRedisKey}:${idKey}`, '1', { EX: this.sentMessagesTTL }).catch(() => {});
            }
            if (remoteJid) {
                this.redis.set(`sent_message_jid:${messageId}`, remoteJid, { EX: this.sentMessagesTTL }).catch(() => {});
            }
            if (recipientKey) {
                this.redis.set(`sent_message_recipient:${messageId}`, recipientKey, { EX: this.sentMessagesTTL }).catch(() => {});
            }
            const callbackKey = `callback_enabled:${messageId}`;
            this.redis.set(callbackKey, enableCallback ? '1' : '0', { EX: this.sentMessagesTTL }).catch(() => {});
        }

        if (String(remoteJid).includes('@lid')) {
            lidJournalWarn(`⚠️ [LID] outbound msgId=${messageId} remoteJid=${remoteJid} recipientPN=${recipientKey || 'unresolved'}`);
            void this.recordLidSendObservation(jid, remoteJid, messageId).catch(() => {});
        }

        if (already) {
            // Tetap pastikan verify/guarantee terpasang (dedupe di dalamnya)
            this.scheduleServerAckGuarantee(messageId, remoteJid);
            this.scheduleSendAckVerify(messageId);
            return messageId;
        }

        this.lastOutboundSendAt = Date.now();
        this.outboundSendSinceLastAck = (this.outboundSendSinceLastAck || 0) + 1;
        this.scheduleServerAckGuarantee(messageId, remoteJid);
        this.scheduleSendAckVerify(messageId);
        return messageId;
    }

    /** SERVER_ACK (2) / DELIVERED (3) / READ / PLAYED = ACK setara untuk verify + deaf. */
    static isAckEquivalentStatusName(name) {
        const n = String(name || '').toUpperCase();
        return (
            n === 'SERVER_ACK' ||
            n === 'DELIVERY_ACK' ||
            n === 'DELIVERED' ||
            n === 'DEVICE_SENT' ||
            n === 'READ' ||
            n === 'PLAYED' ||
            n === 'SENT'
        );
    }

    /** Ambil daftar status history untuk msgId (case-insensitive). */
    getStatusHistoryForMsgId(messageId) {
        if (!this.statusHistory || !messageId) return [];
        const raw = String(messageId);
        const upper = raw.toUpperCase();
        const a = this.statusHistory.get(raw) || [];
        const b = raw === upper ? [] : this.statusHistory.get(upper) || [];
        if (!b.length) return a;
        if (!a.length) return b;
        return Array.from(new Set([...a, ...b]));
    }

    /**
     * 1× ACK / DELIVERED / READ = session dianggap “ACK full” (warm):
     * deaf reset + ackCount + session_ack_live Redis → guarantee/fallback boleh jalan.
     * (Bayar tetap per msg_id — ini hanya tandai session hidup.)
     */
    noteOutboundAckEquivalent(reason = 'ACK') {
        this.lastOutboundAckAt = Date.now();
        this.outboundSendSinceLastAck = 0;
        this.deafRecoveryCount = 0;
        this.echoVerifyFailCount = 0;
        this.sendGateBlockedUntil = 0;
        this.lastMessagesUpdateAt = Date.now();
        this.lastEventAt = Date.now();
        if ((this.ackCount || 0) <= 0) {
            this.ackCount = 1;
        }
        if (this.redis && this.ackCountRedisKey) {
            this.redis.set(this.ackCountRedisKey, String(this.ackCount || 1), { EX: 86400 * 7 }).catch(() => {});
        }
        this.markSessionAckLiveRedis().catch(() => {});
        if (reason && String(process.env.SESSION_ACK_LIVE_DEBUG || '') === '1') {
            console.log(`✅ [SESSION-ACK-FULL] ${this.sessionId} reason=${reason} ackCount=${this.ackCount}`);
        }
    }

    /** GW canary: ACK setara datang? DELIVERED (status 3) = ACK. */
    scheduleSendAckVerify(messageId) {
        const ms = Number(process.env.SEND_ACK_VERIFY_MS) || 0;
        if (ms <= 0 || !messageId) return;
        const id = String(messageId).trim();
        const idKey = id.toUpperCase();
        if (!this._sendAckVerifyTimers) this._sendAckVerifyTimers = new Map();
        if (this._sendAckVerifyTimers.has(idKey)) return;
        const sid = this.sessionId;
        const timer = setTimeout(() => {
            this._sendAckVerifyTimers.delete(idKey);
            if (this.terminalDisconnect) return;
            const statuses = this.getStatusHistoryForMsgId(id);
            const hasAck = statuses.some((s) => BaileysWrapper.isAckEquivalentStatusName(s));
            if (hasAck) {
                console.log(
                    `✅ [SEND-VERIFY] ${sid} msgId=${id} ACK OK (SERVER_ACK|DELIVERED|…) ≤${ms}ms`
                );
                this.noteOutboundAckEquivalent('SEND-VERIFY');
            } else {
                console.warn(
                    `⚠️ [SEND-VERIFY] ${sid} msgId=${id} NO ACK/DELIVERED in ${ms}ms — cek history HP bot / session deaf`
                );
                this.echoVerifyFailCount = (this.echoVerifyFailCount || 0) + 1;
                // ECHO_VERIFY_GATE_MS=0 → jangan blokir kirim (gas mode)
                const gateRaw = Number(process.env.ECHO_VERIFY_GATE_MS);
                const gateMs =
                    gateRaw === 0
                        ? 0
                        : Math.max(15000, Math.min(180000, Number.isFinite(gateRaw) ? gateRaw : 60000));
                if (gateMs > 0) {
                    this.sendGateBlockedUntil = Date.now() + gateMs;
                }
                if (this.deafKeepSending()) {
                    return;
                }
                void this.emitDegradedWebhook('SEND_VERIFY_NO_ACK', id);
                // ECHO_VERIFY_FAIL_THRESHOLD<=0 → jangan recycle (gas / anti putus-nyambung)
                const failRaw = Number(process.env.ECHO_VERIFY_FAIL_THRESHOLD);
                if (Number.isFinite(failRaw) && failRaw <= 0) {
                    return;
                }
                const failThreshold = Math.max(1, Math.min(50, Number.isFinite(failRaw) ? failRaw : 2));
                if (this.echoVerifyFailCount >= failThreshold) {
                    void this.triggerDeafRecovery('ECHO-VERIFY');
                }
            }
        }, ms);
        this._sendAckVerifyTimers.set(idKey, timer);
    }

    /** Nomor penerima bersih (10–15 digit) dari JID / nomor. */
    normalizeRecipientKey(remoteJidOrPhone) {
        const raw = String(remoteJidOrPhone || '').trim();
        if (!raw) return '';
        let phone = raw.includes('@') ? raw.split('@')[0] : raw;
        if (phone.includes(':')) phone = phone.split(':')[0];
        phone = phone.replace(/\D/g, '');
        return /^\d{10,15}$/.test(phone) ? phone : '';
    }

    /** F.3.4 — pastikan target kirim selalu PN JID (tolak @lid sebagai target). */
    ensurePnSendJid(to) {
        const raw = String(to || '').trim();
        if (!raw) {
            throw new Error('Empty send target');
        }
        if (raw.includes('@lid')) {
            lidJournalWarn(`⚠️ [LID] send target is @lid — must use PN: ${raw}`);
            throw new Error('Send target must be phone number (PN), not @lid');
        }
        if (raw.includes('@') && !raw.endsWith('@s.whatsapp.net') && !raw.endsWith('@hosted')) {
            throw new Error(`Unsupported JID for blast send: ${raw}`);
        }
        const phone = this.normalizeRecipientKey(raw);
        if (!phone) {
            throw new Error(`Invalid phone send target: ${raw}`);
        }
        return `${phone}@s.whatsapp.net`;
    }

    /** F.3.4 — resolve @lid → PN via signal store mapping atau sent_message_recipient. */
    async resolveJidToPn(jidOrPhone, msgId = '') {
        const raw = String(jidOrPhone || '').trim();
        if (!raw) {
            if (msgId) {
                const rk = await this.getSentMessageRecipientKey(msgId);
                return rk ? `${rk}@s.whatsapp.net` : '';
            }
            return '';
        }
        const isLid = raw.includes('@lid') || (typeof isLidUser === 'function' && isLidUser(raw));
        if (!isLid) {
            if (raw.includes('@')) {
                return typeof jidNormalizedUser === 'function' ? jidNormalizedUser(raw) : raw;
            }
            const phone = this.normalizeRecipientKey(raw);
            return phone ? `${phone}@s.whatsapp.net` : '';
        }
        try {
            const lidMapping = this.sock?.signalRepository?.lidMapping;
            if (lidMapping?.getPNForLID) {
                const pn = await lidMapping.getPNForLID(raw);
                if (pn) {
                    return typeof jidNormalizedUser === 'function' ? jidNormalizedUser(pn) : pn;
                }
            }
        } catch (err) {
            lidJournalWarn(`⚠️ [LID] getPNForLID failed: ${err?.message || err}`);
        }
        if (msgId) {
            const rk = await this.getSentMessageRecipientKey(msgId);
            if (rk) return `${rk}@s.whatsapp.net`;
        }
        return raw;
    }

    /** F.3.3 — normalisasi result.key.remoteJid ke PN setelah send. */
    async applyForcePnResultKey(result, inputTo) {
        if (!result?.key) return result;
        const forcePn = String(process.env.FORCE_PN_ADDRESSING ?? '1').toLowerCase();
        if (forcePn === '0' || forcePn === 'false' || forcePn === 'off') return result;
        const inputPnJid = this.ensurePnSendJid(inputTo);
        const before = result.key.remoteJid || '';
        let after = before;
        if (before.includes('@lid') || !before) {
            after = await this.resolveJidToPn(before || inputPnJid, result.key.id);
        }
        if (!after || after.includes('@lid')) {
            after = inputPnJid;
        }
        if (typeof jidNormalizedUser === 'function') {
            after = jidNormalizedUser(after);
        }
        if (before !== after) {
            await this.recordLidSendObservation(inputPnJid, before, result.key.id, after);
            result.key.remoteJid = after;
        } else if (before.includes('@lid')) {
            await this.recordLidSendObservation(inputPnJid, before, result.key.id, after);
        }
        return result;
    }

    /** F.3.1 — log + counter Redis saat result JID ≠ input PN atau @lid. */
    async recordLidSendObservation(inputJid, resultRemoteJid, msgId, resolvedJid = '') {
        const inputPn = this.normalizeRecipientKey(inputJid);
        const wasLid = String(resultRemoteJid || '').includes('@lid');
        const mismatch = wasLid || (inputPn && resultRemoteJid && !String(resultRemoteJid).includes(inputPn));
        if (lidJournalLogEnabled() && mismatch) {
            lidJournalWarn(
                `📊 [LID-SEND] session=${this.sessionId} msgId=${msgId || '?'} inputPN=${inputPn || '?'} result=${resultRemoteJid || '-'} resolved=${resolvedJid || '-'}`
            );
        }
        if (this.redis && wasLid && lidMetricsEnabled()) {
            const day = new Date().toISOString().slice(0, 10);
            this.redis.incr(`metrics:lid_send_total:${day}`).catch(() => {});
            if (this.sessionId) {
                this.redis.incr(`metrics:lid_send:${day}:${this.sessionId}`).catch(() => {});
            }
        }
        if (wasLid) {
            this.lidOutboundCount = (this.lidOutboundCount || 0) + 1;
        }
    }

    async finalizeSendResult(result, to) {
        if (!result) return result;
        await this.applyForcePnResultKey(result, to);
        return result;
    }

    ackCountPerRecipientRedisKey(recipientKey) {
        return `ack_count_recipient:${this.sessionId}:${recipientKey}`;
    }

    async getSentMessageRemoteJid(messageId) {
        const id = String(messageId || '').trim();
        if (!id || !this.redis) return '';
        try {
            return (await this.redis.get(`sent_message_jid:${id}`)) || '';
        } catch (_) {
            return '';
        }
    }

    /** PN tujuan dari input kirim (bukan @lid di result.key) — gate ACK fallback. */
    async getSentMessageRecipientKey(messageId) {
        const id = String(messageId || '').trim();
        if (!id || !this.redis) return '';
        try {
            const stored = (await this.redis.get(`sent_message_recipient:${id}`)) || '';
            if (stored) return stored;
        } catch (_) { /* ignore */ }
        return this.normalizeRecipientKey(await this.getSentMessageRemoteJid(id));
    }

    async getRecipientAckCount(remoteJidOrPhone) {
        const key = this.normalizeRecipientKey(remoteJidOrPhone);
        if (!key) return 0;
        if (this.ackCountPerNumber.has(key)) {
            return this.ackCountPerNumber.get(key) || 0;
        }
        if (this.redis) {
            try {
                const n = await this.redis.get(this.ackCountPerRecipientRedisKey(key));
                const c = n ? parseInt(n, 10) : 0;
                if (c > 0) this.ackCountPerNumber.set(key, c);
                return c;
            } catch (_) { /* ignore */ }
        }
        return 0;
    }

    async incrementRecipientAckCount(remoteJidOrPhone) {
        const key = this.normalizeRecipientKey(remoteJidOrPhone);
        if (!key) return;
        const next = (this.ackCountPerNumber.get(key) || 0) + 1;
        this.ackCountPerNumber.set(key, next);
        if (this.redis) {
            const rk = this.ackCountPerRecipientRedisKey(key);
            try {
                await this.redis.incr(rk);
                await this.redis.expire(rk, 86400 * 7);
            } catch (_) { /* ignore */ }
        }
    }

    async isRecipientAckLive(remoteJidOrPhone) {
        return (await this.getRecipientAckCount(remoteJidOrPhone)) > 0;
    }

    /**
     * Gate generate/fallback: WAJIB sudah ada ≥1 receipt WA asli (SERVER_ACK/DELIVERED/READ).
     * Tanpa itu → jangan generate. SESSION_ACK_LIVE_ENABLED=0 tidak boleh bypass.
     */
    async canFallbackServerAck(remoteJidOrPhone = '', msgId = '') {
        // Jangan pakai ackCount Redis 7 hari sendirian — 1 ACK lama menular ke semua kawan.
        // Wajib session_ack_live masih hidup (di-clear tiap initialize).
        if (!(await this.isSessionAckLiveForGuarantee())) return false;
        let key = this.normalizeRecipientKey(remoteJidOrPhone);
        if (!key && msgId) {
            key = await this.getSentMessageRecipientKey(msgId);
        }
        if (key && (await this.isRecipientAckLive(key))) return true;
        return (this.ackCount || 0) > 0;
    }

    /** ACK setara sudah settled? (SERVER_ACK / DELIVERED / DEVICE_SENT / READ di Redis) */
    async isLifecycleAckSettled(msgId) {
        if (await this.isServerAckSettled(msgId)) return true;
        const id = String(msgId || '').trim().toUpperCase();
        if (!id) return true;
        if (this.redis) {
            const [delLife, delConf, devConf, readConf] = await Promise.all([
                this.redis.get(`lifecycle:delivered:${id}`).catch(() => null),
                this.redis.get(`webhook:confirmed:${id}:DELIVERED`).catch(() => null),
                this.redis.get(`webhook:confirmed:${id}:DEVICE_SENT`).catch(() => null),
                this.redis.get(`webhook:confirmed:${id}:READ`).catch(() => null),
            ]);
            if (delLife || delConf || devConf || readConf) return true;
        }
        return false;
    }

    /** DELIVERED asli WA → warm nomor + session path (WA sering skip status 2). */
    async markDeliveredPathWarm(remoteJidOrPhone) {
        const key = this.normalizeRecipientKey(remoteJidOrPhone);
        if (key) await this.incrementRecipientAckCount(key);
        if ((this.ackCount || 0) > 0) return;
        this.ackCount = 1;
        if (this.redis) {
            try {
                const cur = await this.redis.get(this.ackCountRedisKey);
                if (!cur || parseInt(cur, 10) <= 0) {
                    await this.redis.set(this.ackCountRedisKey, '1', { EX: 86400 * 7 });
                }
            } catch (_) { /* ignore */ }
        }
        // DELIVERED asli = jalur WA hidup — sinkron lib Redis dengan gate processor/guarantee
        this.markSessionAckLiveRedis().catch(() => {});
    }

    /** DELIVERED / echo / guarantee — generate SERVER_ACK fallback bila belum settled. */
    async maybeEmitFallbackServerAck(msgId, remoteJid = '', source = 'FALLBACK') {
        const id = this.normalizeEchoUpsertMsgId(msgId);
        if (!id) return false;
        if (!(await this.hasSentMessageId(id))) return false;
        if (await this.isServerAckSettled(id)) return false;
        const jid = remoteJid || await this.getSentMessageRemoteJid(id);
        if (source !== 'DELIVERED' && !(await this.canFallbackServerAck(jid, id))) {
            return false;
        }
        const ok = await this.emitEchoUpsertServerAck(id);
        if (ok) {
            console.log(`✅ [${source}] fallback SERVER_ACK msgId=${id} recipient=${this.normalizeRecipientKey(jid) || '?'}`);
        }
        return ok;
    }

    serverAckGuaranteeEnabled() {
        // Hard OFF kecuali SERVER_ACK_GUARANTEE=force1.
        // Env=1 dari drop-in gas-mode (999/9999) dulu menular 1 ACK ke kawan — jangan ikut.
        const v = String(process.env.SERVER_ACK_GUARANTEE ?? '0').trim().toLowerCase();
        return ['force1', 'force', 'override'].includes(v);
    }

    /** messages.upsert fromMe → SERVER_ACK fallback, hanya jika session sudah 1× ACK asli. */
    echoUpsertAckEnabled() {
        const v = String(process.env.ECHO_UPSERT_ACK ?? '1').trim().toLowerCase();
        return !['0', 'false', 'off', 'no'].includes(v);
    }

    /** Satu echo fromMe valid → flush ACK pending lain di session (default OFF — fleet policy). */
    echoUpsertFlushAllEnabled() {
        const v = String(process.env.ECHO_UPSERT_ACK_FLUSH_ALL ?? '0').trim().toLowerCase();
        return !['0', 'false', 'off', 'no'].includes(v);
    }

    /** Tanpa ACK: tetap kirim sampai WA putus sendiri. Default ON. */
    deafKeepSending() {
        const v = String(process.env.DEAF_KEEP_SENDING ?? '1').trim().toLowerCase();
        return !['0', 'false', 'off', 'no'].includes(v);
    }

    normalizeEchoUpsertMsgId(msgId) {
        const id = String(msgId || '').trim();
        if (id.length < 10 || !/^[A-Z0-9]+$/i.test(id)) return '';
        return id;
    }

    async emitEchoUpsertServerAck(msgId) {
        const id = this.normalizeEchoUpsertMsgId(msgId);
        if (!id) return false;
        if (!(await this.hasSentMessageId(id))) return false;
        if (await this.isServerAckSettled(id)) return false;
        if (!(await this.tryClaimServerAck(id))) return false;

        const cleanBotNumber = this.getCleanBotNumber();
        const result = await this.enqueueWebhook({
            event: 'SERVER_ACK',
            bot: cleanBotNumber || this.sessionId,
            bot_id: this.cachedBotId || 0,
            sessionId: this.sessionId,
            msg_id: id,
            status: 'server_ack',
            user_id: this.userId,
            port: this.port,
            fallback_generated: true,
            session_ack_warm: true,
            force_send: true,
        }, { forceDirect: true }).catch((err) => {
            this.releaseServerAckClaim(id);
            console.warn(`⚠️ [ECHO-UPSERT] SERVER_ACK failed msgId=${id}: ${err?.message || err}`);
            return null;
        });
        if (!result || result.duplicate) {
            this.releaseServerAckClaim(id);
            return false;
        }
        console.log(`📲 [ECHO-UPSERT] fromMe in store msgId=${id} → SERVER_ACK (fallback_generated)`);
        return true;
    }

    /** Batch: 1+ fromMe echo valid → ACK msgId itu + semua pending outbound session. */
    async processEchoUpsertOutboundBatch(msgIds) {
        if (!this.echoUpsertAckEnabled() || !Array.isArray(msgIds) || msgIds.length === 0) return;

        const uniq = [...new Set(msgIds.map((id) => this.normalizeEchoUpsertMsgId(id)).filter(Boolean))];
        if (uniq.length === 0) return;

        let sawOurOutbound = false;
        let emitted = 0;
        for (const id of uniq) {
            if (await this.hasSentMessageId(id)) {
                sawOurOutbound = true;
            }
            const jid = await this.getSentMessageRemoteJid(id);
            if (!(await this.canFallbackServerAck(jid, id))) continue;
            if (await this.emitEchoUpsertServerAck(id)) {
                emitted++;
            }
        }
        if (emitted === 0 && uniq.length > 0) {
            await new Promise((r) => setTimeout(r, 450));
            for (const id of uniq) {
                if (await this.hasSentMessageId(id)) sawOurOutbound = true;
                const jid = await this.getSentMessageRemoteJid(id);
                if (!(await this.canFallbackServerAck(jid, id))) continue;
                if (await this.emitEchoUpsertServerAck(id)) emitted++;
            }
        }

        if (!sawOurOutbound || !this.echoUpsertFlushAllEnabled()) {
            return;
        }

        const skip = new Set(uniq.map((id) => id.toUpperCase()));
        const maxFlush = Math.max(1, Math.min(200, Number(process.env.ECHO_UPSERT_ACK_MAX_FLUSH) || 50));
        let flushed = 0;
        for (const rawId of Array.from(this.sentMessageIds)) {
            if (flushed >= maxFlush) break;
            const id = this.normalizeEchoUpsertMsgId(rawId);
            if (!id || skip.has(id.toUpperCase())) continue;
            if (!(await this.hasSentMessageId(id))) continue;
            if (await this.emitEchoUpsertServerAck(id)) {
                flushed++;
            }
        }
        if (flushed > 0) {
            console.log(`📲 [ECHO-UPSERT-FLUSH] trigger=${uniq[0]} emitted=${emitted} flushed=${flushed} pending SERVER_ACK`);
        }
    }

    async handleEchoUpsertOutbound(_msg, msgId) {
        if (!this.echoUpsertAckEnabled()) return;
        const id = this.normalizeEchoUpsertMsgId(msgId);
        if (!id) return;
        const jid = _msg?.key?.remoteJid || await this.getSentMessageRemoteJid(id);
        if (!(await this.canFallbackServerAck(jid, id))) return;
        if (!(await this.emitEchoUpsertServerAck(id))) {
            await new Promise((r) => setTimeout(r, 450));
            await this.emitEchoUpsertServerAck(id);
        }
    }

    /** Admin flush: kirim SERVER_ACK webhook untuk semua pending sentMessageIds (session sudah live). */
    async flushPendingServerAckViaWebhook(maxFlush = 50) {
        if (!this.echoUpsertAckEnabled()) {
            return { flushed: 0, skipped: 'echo_disabled' };
        }
        if (!(await this.canFallbackServerAck(''))) {
            return { flushed: 0, skipped: 'not_warm' };
        }
        const cap = Math.max(1, Math.min(200, Number(maxFlush) || 50));
        let flushed = 0;
        for (const rawId of Array.from(this.sentMessageIds)) {
            if (flushed >= cap) break;
            if (await this.emitEchoUpsertServerAck(rawId)) {
                flushed++;
            }
        }
        if (flushed > 0) {
            console.log(`📲 [WEBHOOK-FLUSH] session=${this.sessionId} flushed=${flushed} pending SERVER_ACK`);
        }
        return { flushed, skipped: null };
    }

    /** Flush SERVER_ACK pending ke nomor yang sama (setelah DELIVERED / echo pada recipient ack-live). */
    async flushPendingServerAckForRecipient(remoteJid, maxFlush = 30) {
        const flushOn = String(process.env.RECIPIENT_FLUSH_ACK ?? '0').trim().toLowerCase();
        if (!['1', 'true', 'yes', 'on'].includes(flushOn)) return 0;
        if (!this.echoUpsertAckEnabled()) return 0;
        const target = this.normalizeRecipientKey(remoteJid);
        if (!target) return 0;
        const cap = Math.max(1, Math.min(100, Number(maxFlush) || 30));
        // Throttle: tiap DELIVERED dulu scan Redis per semua sentMessageIds → berat di peak
        if (!this._recipientFlushAt) this._recipientFlushAt = new Map();
        const now = Date.now();
        const minGap = Math.max(250, Math.min(10000, Number(process.env.RECIPIENT_FLUSH_MIN_GAP_MS) || 2000));
        const last = this._recipientFlushAt.get(target) || 0;
        if (now - last < minGap) return 0;
        this._recipientFlushAt.set(target, now);
        if (this._recipientFlushAt.size > 500) {
            for (const [k, t] of this._recipientFlushAt) {
                if (now - t > 60000) this._recipientFlushAt.delete(k);
            }
        }

        const ids = Array.from(this.sentMessageIds);
        const seen = new Set();
        const matchIds = [];
        const needLookup = [];
        // Newest-first, batasi scan (hindari O(n) Redis GET)
        for (let i = ids.length - 1; i >= 0; i--) {
            const rawId = ids[i];
            const uk = String(rawId || '').toUpperCase();
            if (!uk || seen.has(uk)) continue;
            seen.add(uk);
            if (seen.size > 120) break;
            const mem = this._sentMsgRecipient?.get(rawId) || this._sentMsgRecipient?.get(uk);
            if (mem) {
                if (mem === target) matchIds.push(rawId);
                continue;
            }
            needLookup.push(rawId);
            if (needLookup.length >= Math.max(cap * 3, 40)) break;
        }

        if (needLookup.length > 0 && this.redis) {
            for (let off = 0; off < needLookup.length && matchIds.length < cap; off += 40) {
                const chunk = needLookup.slice(off, off + 40);
                const keys = chunk.map((id) => `sent_message_recipient:${id}`);
                let vals = null;
                try {
                    vals = await this.redis.mGet(keys);
                } catch (_) {
                    vals = null;
                }
                if (!Array.isArray(vals)) {
                    for (const rawId of chunk) {
                        if (matchIds.length >= cap) break;
                        const jid = await this.getSentMessageRemoteJid(rawId);
                        if (this.normalizeRecipientKey(jid) === target) matchIds.push(rawId);
                    }
                    continue;
                }
                for (let j = 0; j < chunk.length; j++) {
                    const stored = this.normalizeRecipientKey(vals[j] || '');
                    if (!stored) continue;
                    if (!this._sentMsgRecipient) this._sentMsgRecipient = new Map();
                    this._sentMsgRecipient.set(chunk[j], stored);
                    this._sentMsgRecipient.set(String(chunk[j]).toUpperCase(), stored);
                    if (stored === target) matchIds.push(chunk[j]);
                }
            }
        }

        let flushed = 0;
        for (const rawId of matchIds) {
            if (flushed >= cap) break;
            if (await this.emitEchoUpsertServerAck(rawId)) flushed++;
        }
        if (flushed > 0) {
            console.log(`📲 [RECIPIENT-FLUSH] recipient=${target} flushed=${flushed} pending SERVER_ACK`);
        }
        return flushed;
    }

    serverAckLifecycleKey(msgId) {
        return `lifecycle:server_ack:${String(msgId || '').trim().toUpperCase()}`;
    }

    serverAckClaimKey(msgId) {
        return `lifecycle:server_ack_claim:${String(msgId || '').trim().toUpperCase()}`;
    }

    async tryClaimServerAck(msgId) {
        const id = String(msgId || '').trim().toUpperCase();
        if (id.length < 10 || !/^[A-Z0-9]+$/i.test(id)) return false;
        if (this.redis) {
            const ok = await this.redis.set(this.serverAckClaimKey(id), '1', { NX: true, EX: 86400 }).catch(() => null);
            return ok === 'OK';
        }
        if (!this._serverAckMemClaims) this._serverAckMemClaims = new Set();
        if (this._serverAckMemClaims.has(id)) return false;
        this._serverAckMemClaims.add(id);
        if (this._serverAckMemClaims.size > 2000) {
            this._serverAckMemClaims = new Set(Array.from(this._serverAckMemClaims).slice(-1500));
        }
        return true;
    }

    releaseServerAckClaim(msgId) {
        const id = String(msgId || '').trim().toUpperCase();
        if (!id) return;
        if (this.redis) {
            this.redis.del(this.serverAckClaimKey(id)).catch(() => {});
        }
        this._serverAckMemClaims?.delete(id);
    }

    serverAckLifecycleBotKey(msgId, botId = null) {
        const id = String(msgId || '').trim().toUpperCase();
        const bid = botId || this.cachedBotId || 0;
        if (bid > 0) return `lifecycle:server_ack:${bid}:${id}`;
        return this.serverAckLifecycleKey(id);
    }

    /** HTTP ok tapi BE tidak credit (synthetic/duplicate skip). */
    isWebhookResponseAckCredited(result, data = {}) {
        if (!result || typeof result !== 'object') return false;
        if (result.synthetic_skipped === true || result.duplicate_skipped === true || result.ack_credited === false) {
            return false;
        }
        const msg = String(result.msg || result.message || '').toLowerCase();
        if (msg.includes('synthetic skipped') || msg.includes('skip-credit') || msg.includes('duplicate skipped')) {
            return false;
        }
        return result.error === '0' || result.error === 0 || result.error === null || result.status === 'success';
    }

    async hasOrganicServerAckMarker(msgId, botId = null) {
        const id = String(msgId || '').trim().toUpperCase();
        if (!id || !this.redis) return false;
        const bid = botId || this.cachedBotId || 0;
        const [orig, lifeBot] = await Promise.all([
            this.redis.get(`webhook:confirmed:${id}:SERVER_ACK:original`).catch(() => null),
            bid > 0 ? this.redis.get(this.serverAckLifecycleBotKey(id, bid)).catch(() => null) : null,
        ]);
        return !!(orig || lifeBot);
    }

    async maybeMarkWebhookConfirmed(msgId, event, data = {}, result = null) {
        if (!this.redis || !msgId || !event) return false;
        if (result && !this.isWebhookResponseAckCredited(result, data)) return false;
        const confirmKey = `webhook:confirmed:${msgId}:${event}`;
        const exists = await this.redis.get(confirmKey).catch(() => null);
        if (!exists) {
            await this.redis.set(confirmKey, '1', { EX: 86400 }).catch(() => {});
        }
        const isSyntheticPayload = !!(data.fallback_generated || data.backfill_generated || data.auto_generated || data.timeout_generated || data.reconcile_via_webhook);
        if (data.original_from_whatsapp === true || (event === 'SERVER_ACK' && !isSyntheticPayload)) {
            const originalConfirmKey = `${confirmKey}:original`;
            const origExists = await this.redis.get(originalConfirmKey).catch(() => null);
            if (!origExists) {
                await this.redis.set(originalConfirmKey, '1', { EX: 86400 }).catch(() => {});
            }
        }
        return true;
    }

    async isServerAckSettled(msgId) {
        const id = String(msgId || '').trim().toUpperCase();
        if (!id) return true;
        if (await this.hasOrganicServerAckMarker(id)) return true;
        const dedupeKey = `${id}_2`;
        return (this.dedupeCache.get(dedupeKey) || 0) > Date.now();
    }

    /** Fallback SERVER_ACK hanya setelah session pernah dapat ACK asli WA (min 1 original). */
    async isSessionAckLiveForGuarantee() {
        if ((this.ackCount || 0) > 0) {
            return true;
        }
        if (this.redis) {
            try {
                const n = await this.redis.get(this.ackCountRedisKey);
                if (n && parseInt(n, 10) > 0) {
                    return true;
                }
            } catch (_) { /* ignore */ }
        }
        if (!this.sessionAckLiveEnabled()) {
            return false;
        }
        let botId = this.cachedBotId || 0;
        if (!botId && this.redis) {
            try {
                const cached = await this.redis.get(`bot_id:${this.sessionId}`);
                if (cached) botId = parseInt(cached, 10) || 0;
            } catch (_) { /* ignore */ }
        }
        if (botId <= 0 || !this.sessionId) {
            return false;
        }
        if (!this.redis) {
            return (this.ackCount || 0) > 0;
        }
        const key = `session_ack_live:${botId}:${this.sessionId}`;
        try {
            const n = await this.redis.exists(key);
            return n === 1;
        } catch (_) {
            return false;
        }
    }

    /** Batalkan guarantee bila ACK setara (SERVER_ACK / DELIVERED / READ) sudah datang. */
    cancelServerAckGuarantee(msgId) {
        if (!this._serverAckGuaranteeTimers || !msgId) return;
        const id = String(msgId).trim().toUpperCase();
        const timer = this._serverAckGuaranteeTimers.get(id);
        if (!timer) return;
        clearTimeout(timer);
        this._serverAckGuaranteeTimers.delete(id);
    }

    scheduleServerAckGuarantee(msgId, remoteJid = '') {
        if (!this.serverAckGuaranteeEnabled() || !msgId) return;
        const id = String(msgId).trim().toUpperCase();
        if (!this._serverAckGuaranteeTimers) this._serverAckGuaranteeTimers = new Map();
        if (this._serverAckGuaranteeTimers.has(id)) return;
        const delayMs = Math.max(2000, Math.min(15000, Number(process.env.SERVER_ACK_GUARANTEE_MS) || 4000));
        const timer = setTimeout(async () => {
            this._serverAckGuaranteeTimers.delete(id);
            try {
                // DELIVERED / SERVER_ACK / READ di history = ACK sudah ada — jangan backfill
                const hist = this.getStatusHistoryForMsgId(id);
                if (hist.some((s) => BaileysWrapper.isAckEquivalentStatusName(s))) {
                    this.noteOutboundAckEquivalent('GUARANTEE-SKIP-HIST');
                    return;
                }
                if (await this.isLifecycleAckSettled(id)) return;
                const hasOutbound = await this.hasSentMessageId(id).catch(() => false);
                if (!hasOutbound) return;
                const jid = remoteJid || await this.getSentMessageRemoteJid(id);
                if (!(await this.canFallbackServerAck(jid, id))) {
                    console.log(`⏭️ [DELIVERED-GUARANTEE] skip msgId=${id} — belum ada 1× ACK asli WA`);
                    return;
                }
                if (!(await this.tryClaimServerAck(id))) return;
                const cleanBotNumber = this.getCleanBotNumber();
                // DELIVERED backfill hanya setelah session warm (1× receipt asli)
                const result = await this.enqueueWebhook({
                    event: 'DELIVERED',
                    bot: cleanBotNumber || this.sessionId,
                    bot_id: this.cachedBotId || 0,
                    sessionId: this.sessionId,
                    msg_id: id,
                    status: 'delivered',
                    user_id: this.userId,
                    port: this.port,
                    backfill_generated: true,
                    session_ack_warm: true,
                    force_send: true,
                }, { forceDirect: false }).catch(() => null);
                if (!result || result.duplicate) {
                    this.releaseServerAckClaim(id);
                    return;
                }
                if (this.redis) {
                    this.redis.set(`lifecycle:delivered:${id}`, '1', { EX: 86400 }).catch(() => {});
                }
                // Jangan noteOutboundAckEquivalent di sini — warm hanya dari receipt WA asli
                console.log(`🛡️ [DELIVERED-GUARANTEE] Backfill DELIVERED msgId=${id} after ${delayMs}ms (session already warm)`);
            } catch (e) {
                this.releaseServerAckClaim(id);
                console.warn(`⚠️ [DELIVERED-GUARANTEE] msgId=${id}: ${e?.message || e}`);
            }
        }, delayMs);
        this._serverAckGuaranteeTimers.set(id, timer);
    }

    /** Processor warm-up + lib gate; default ON (matikan via SESSION_ACK_LIVE_ENABLED=0). */
    sessionAckLiveEnabled() {
        const v = String(process.env.SESSION_ACK_LIVE_ENABLED ?? '1').trim().toLowerCase();
        return !['0', 'false', 'off', 'no'].includes(v);
    }

    async clearSessionAckLiveRedis() {
        if (!this.redis) return;
        let botId = this.cachedBotId || 0;
        if (!botId) {
            try {
                const cached = await this.redis.get(`bot_id:${this.sessionId}`);
                if (cached) botId = parseInt(cached, 10) || 0;
            } catch (_) {}
        }
        if (botId <= 0) return;
        const key = `session_ack_live:${botId}:${this.sessionId}`;
        try {
            await this.redis.del(key);
            console.log(`🔄 [SESSION_ACK_LIVE] Cleared ${key} (reconnect/close)`);
        } catch (_) {}
    }

    /** Selalu set setelah ACK asli (status 2) — dipakai guarantee/backfill gate + processor warm-up. */
    async markSessionAckLiveRedis() {
        if (!this.redis) return;
        let botId = this.cachedBotId || 0;
        if (!botId) {
            try {
                const cached = await this.redis.get(`bot_id:${this.sessionId}`);
                if (cached) botId = parseInt(cached, 10) || 0;
            } catch (_) {}
        }
        if (botId <= 0 || !this.sessionId) return;
        const key = `session_ack_live:${botId}:${this.sessionId}`;
        const ttl = Math.max(3600, Number(process.env.SESSION_ACK_LIVE_TTL_SEC) || 86400);
        try {
            await this.redis.set(key, String(Math.floor(Date.now() / 1000)), { EX: ttl });
        } catch (_) {}
    }

    /** Redis + memory cache untuk getMessage (forum #2115 / Baileys retry resync) */
    getMessageCacheKey(messageId, remoteJid = '') {
        const id = String(messageId || '').trim();
        const jid = String(remoteJid || 'unknown');
        return `msg_cache:${this.sessionId}:${jid}:${id}`;
    }

    trimGetMessageMemoryCache() {
        if (this.getMessageMemoryCache.size <= this.getMessageMemoryCacheMax) return;
        const now = Date.now();
        for (const [k, v] of this.getMessageMemoryCache.entries()) {
            if (!v || v.exp <= now) this.getMessageMemoryCache.delete(k);
        }
        while (this.getMessageMemoryCache.size > this.getMessageMemoryCacheMax) {
            const first = this.getMessageMemoryCache.keys().next().value;
            if (first === undefined) break;
            this.getMessageMemoryCache.delete(first);
        }
    }

    buildGetMessageProto(result, to, message, messageType = 'text', options = {}) {
        const remoteJid = result?.key?.remoteJid || (String(to || '').includes('@') ? to : `${to}@s.whatsapp.net`);
        const key = result?.key ? { ...result.key, remoteJid: result.key.remoteJid || remoteJid } : null;
        if (!key?.id) return null;
        let inner = {};
        if (messageType === 'text' || (messageType !== 'image' && typeof message === 'string')) {
            const text = String(message ?? '');
            if (!text.trim()) return null;
            // Baileys getMessage expects IMessage; extendedTextMessage lebih reliable untuk resync (#676, #832)
            inner = text.length <= 300 && !/[\n\r]/.test(text)
                ? { conversation: text }
                : { extendedTextMessage: { text } };
        } else if (messageType === 'image') {
            const caption = String(options?.caption ?? message ?? '').trim();
            if (caption) {
                inner = caption.length <= 300
                    ? { conversation: caption }
                    : { extendedTextMessage: { text: caption } };
            } else {
                inner = { imageMessage: { mimetype: 'image/jpeg', caption: '' } };
            }
        } else if (message && typeof message === 'object') {
            inner = message;
        } else {
            return null;
        }
        return { key, message: inner };
    }

    /** Baileys getMessage expects IMessage, not WebMessageInfo wrapper. */
    normalizeGetMessagePayload(data) {
        if (!data || typeof data !== 'object') return undefined;
        if (data.message && typeof data.message === 'object') {
            return data.message;
        }
        if (data.conversation != null || data.extendedTextMessage || data.imageMessage || data.buttonsMessage) {
            return data;
        }
        return undefined;
    }

    async cacheOutboundMessageForGetMessage(result, to, message, messageType = 'text', options = {}) {
        try {
            const proto = this.buildGetMessageProto(result, to, message, messageType, options);
            if (!proto?.key?.id || !proto?.message) return;
            const cacheKey = this.getMessageCacheKey(proto.key.id, proto.key.remoteJid);
            this.getMessageMemoryCache.set(cacheKey, { exp: Date.now() + GET_MESSAGE_CACHE_TTL_SEC * 1000, data: proto });
            this.trimGetMessageMemoryCache();
            if (this.redis) {
                await this.redis.set(cacheKey, JSON.stringify(proto), { EX: GET_MESSAGE_CACHE_TTL_SEC }).catch(() => {});
            }
        } catch {
            /* non-blocking */
        }
    }

    async getCachedMessage(waKey) {
        if (!waKey?.id) return undefined;
        const cacheKey = this.getMessageCacheKey(waKey.id, waKey.remoteJid || '');
        const now = Date.now();
        const mem = this.getMessageMemoryCache.get(cacheKey);
        if (mem && mem.exp > now && mem.data) {
            const normalized = this.normalizeGetMessagePayload(mem.data);
            if (normalized) return normalized;
        }
        if (this.redis) {
            const raw = await this.redis.get(cacheKey).catch(() => null);
            if (raw) {
                try {
                    const parsed = JSON.parse(raw);
                    const normalized = this.normalizeGetMessagePayload(parsed);
                    if (normalized) {
                        this.getMessageMemoryCache.set(cacheKey, { exp: now + GET_MESSAGE_CACHE_TTL_SEC * 1000, data: parsed });
                        this.trimGetMessageMemoryCache();
                        return normalized;
                    }
                } catch {
                    /* ignore */
                }
            }
        }
        return undefined;
    }

    // ✅ REDIS-BASED: Helper methods untuk sentMessageIds (global deduplication)
    async addSentMessageId(messageId) {
        // ✅ CRITICAL: Add ke in-memory cache FIRST (instant, untuk security check)
        // ✅ SECURITY: Memory cache digunakan untuk validasi webhook (instant lookup)
        this.sentMessageIds.add(messageId);
        
        // ✅ OPTIMIZED: Redis write di background (non-blocking)
        // ✅ SAFE: Memory cache sudah ada, validasi webhook akan check memory dulu (instant)
        // Redis write hanya untuk backup/multi-server, tidak critical untuk response
        if (this.redis) {
            const key = `${this.sentMessagesRedisKey}:${messageId}`;
            // ✅ FIXED: Fire-and-forget Redis write (non-blocking)
            this.redis.set(key, '1', { EX: this.sentMessagesTTL }).catch(() => {});
        }
        
        // ✅ FIXED: Cleanup old messageIds dari memory (keep last 500 untuk lebih efisien)
        // ✅ OPTIMIZED: Kurangi dari 1000 ke 500 untuk reduce memory footprint dan improve lookup speed
        if (this.sentMessageIds.size > 500) {
            // Delete oldest 100 entries (keep last 400)
            const entries = Array.from(this.sentMessageIds);
            const toDelete = entries.slice(0, entries.length - 400);
            for (const id of toDelete) {
                this.sentMessageIds.delete(id);
            }
        }
    }
    
    // ✅ SMART CACHE: Helper untuk check dan update image URL cache (Hybrid: Memory + Redis)
    // ✅ OPTIMIZED: Non-blocking, lightweight, tidak menghalangi pengiriman gambar
    // ✅ MEMORY LEAK FIX: Smart URL normalization dengan hash untuk prevent duplicate URLs
    normalizeImageUrl(imageUrl) {
        if (!imageUrl || typeof imageUrl !== 'string') return null;
        
        try {
            // ✅ SMART: Parse URL untuk extract base path dan relevant query params
            const url = new URL(imageUrl);
            const basePath = url.pathname;
            
            // ✅ SMART: Extract relevant query params (jika ada size, width, height, dll yang mempengaruhi image)
            // ✅ Jika query params tidak mempengaruhi image content, gunakan base path saja
            const relevantParams = [];
            const relevantQueryKeys = ['size', 'width', 'height', 'w', 'h', 'resize', 'crop'];
            
            for (const [key, value] of url.searchParams.entries()) {
                if (relevantQueryKeys.includes(key.toLowerCase())) {
                    relevantParams.push(`${key}=${value}`);
                }
            }
            
            // ✅ SMART: Jika tidak ada relevant params, gunakan base path saja (better cache hit)
            // ✅ Jika ada relevant params, include dalam hash
            const urlToHash = relevantParams.length > 0 
                ? `${basePath}?${relevantParams.sort().join('&')}` 
                : basePath;
            
            // ✅ Generate hash untuk consistent key (prevent memory leak dari URL variations)
            const urlHash = crypto.createHash('md5').update(urlToHash).digest('hex');
            return { normalizedUrl: urlToHash, urlHash, originalUrl: imageUrl };
        } catch (err) {
            // ✅ Fallback: Jika URL parsing gagal, gunakan simple split
            const normalizedUrl = imageUrl.split('?')[0];
            const urlHash = crypto.createHash('md5').update(normalizedUrl).digest('hex');
            return { normalizedUrl, urlHash, originalUrl: imageUrl };
        }
    }
    
    async getImageUrlCache(imageUrl) {
        if (!imageUrl || typeof imageUrl !== 'string') return null;
        
        // ✅ SMART: Normalize URL dengan hash untuk prevent duplicate URLs
        const urlInfo = this.normalizeImageUrl(imageUrl);
        if (!urlInfo) return null;
        
        const { normalizedUrl, urlHash } = urlInfo;
        
        // ✅ STEP 1: Check memory cache first (fastest, instant, non-blocking)
        // ✅ Use urlHash sebagai key untuk prevent memory leak dari URL variations
        const memoryCached = this.imageUrlCache.get(urlHash);
        if (memoryCached) {
            const now = Date.now();
            const age = now - memoryCached.timestamp;
            
            // ✅ Check if cache expired
            if (age > this.imageUrlCacheTTL) {
                this.imageUrlCache.delete(urlHash);
                // ✅ Also delete from Redis if expired (non-blocking, fire and forget)
                if (this.redis) {
                    this.redis.del(`image_cache:${urlHash}`).catch(() => {});
                }
                return null;
            }
            
            // ✅ Memory cache hit - return cached data (instant, no delay)
            return memoryCached;
        }
        
        // ✅ STEP 2: Check Redis cache (persist across restarts) - OPTIMIZED dengan timeout
        // ✅ CRITICAL: Redis check dengan timeout untuk prevent blocking jika Redis lambat
        if (this.redis) {
            try {
                const redisKey = `image_cache:${urlHash}`;
                // ✅ OPTIMIZED: Timeout 100ms untuk Redis check (jika lambat, skip Redis, tidak blocking)
                const redisCached = await Promise.race([
                    this.redis.get(redisKey),
                    new Promise((resolve) => setTimeout(() => resolve(null), 100)) // Timeout 100ms
                ]).catch(() => null);
                
                if (redisCached) {
                    try {
                        const cached = JSON.parse(redisCached);
                        const now = Date.now();
                        const age = now - cached.timestamp;
                        
                        // ✅ Check if cache expired
                        if (age > this.imageUrlCacheTTL) {
                            // ✅ Non-blocking delete
                            this.redis.del(redisKey).catch(() => {});
                            return null;
                        }
                        
                        // ✅ Redis cache hit - restore to memory cache and return (non-blocking)
                        this.imageUrlCache.set(urlHash, cached);
                        return cached;
                    } catch (parseErr) {
                        // Ignore parse error, fallback to no cache
                    }
                }
            } catch (err) {
                // Ignore Redis errors, fallback to no cache (tidak blocking)
            }
        }
        
        return null;
    }
    
    // ✅ SMART CACHE: Set image URL cache dengan auto cleanup
    setImageUrlCache(imageUrl, data = {}) {
        if (!imageUrl || typeof imageUrl !== 'string') return;
        
        // ✅ SMART: Normalize URL dengan hash untuk prevent duplicate URLs
        const urlInfo = this.normalizeImageUrl(imageUrl);
        if (!urlInfo) return;
        
        const { normalizedUrl, urlHash } = urlInfo;
        const now = Date.now();
        
        // ✅ Auto cleanup expired entries jika cache penuh
        if (this.imageUrlCache.size >= this.imageUrlCacheMaxSize) {
            const entries = Array.from(this.imageUrlCache.entries());
            const expired = entries.filter(([_, value]) => (now - value.timestamp) > this.imageUrlCacheTTL);
            
            // ✅ Delete expired entries first
            for (const [hash] of expired) {
                this.imageUrlCache.delete(hash);
            }
            
            // ✅ Jika masih penuh, delete oldest 20% entries
            if (this.imageUrlCache.size >= this.imageUrlCacheMaxSize) {
                const sorted = entries
                    .filter(([hash]) => !expired.some(([eHash]) => eHash === hash))
                    .sort((a, b) => a[1].timestamp - b[1].timestamp);
                
                const toDelete = Math.floor(this.imageUrlCacheMaxSize * 0.2);
                for (let i = 0; i < toDelete && i < sorted.length; i++) {
                    this.imageUrlCache.delete(sorted[i][0]);
                }
            }
        }
        
        // ✅ Set cache dengan timestamp; preserve localPath jika caller cuma update metadata.
        const existingCache = this.imageUrlCache.get(urlHash) || {};
        const cacheData = {
            ...existingCache,
            ...data,
            timestamp: now,
            url: normalizedUrl,
            originalUrl: imageUrl
        };
        
        // ✅ STEP 1: Set memory cache (fast) - use urlHash sebagai key
        this.imageUrlCache.set(urlHash, cacheData);
        
        // ✅ STEP 2: Set Redis cache (persist across restarts) - non-blocking, fire and forget
        // ✅ OPTIMIZED: Tidak blocking, tidak menghalangi pengiriman gambar
        if (this.redis) {
            const redisKey = `image_cache:${urlHash}`;
            const redisTTL = Math.floor(this.imageUrlCacheTTL / 1000); // Convert to seconds
            // ✅ Fire and forget - tidak blocking, tidak menghalangi pengiriman
            this.redis.set(redisKey, JSON.stringify(cacheData), { EX: redisTTL }).catch(() => {});
        }
    }
    
    // ✅ SMART IMAGE MANAGEMENT: Download dan cache image dengan smart deduplication
    // ✅ Features: URL normalization, smart caching, memory efficient, prevent duplicates
    async downloadAndCacheImage(imageUrl) {
        if (!imageUrl || typeof imageUrl !== 'string' || (!imageUrl.startsWith('http://') && !imageUrl.startsWith('https://'))) {
            return null;
        }
        
        // ✅ STEP 1: Normalize URL dengan smart hash (prevent duplicate downloads untuk URL yang sama)
        const urlInfo = this.normalizeImageUrl(imageUrl);
        if (!urlInfo) return null;
        
        const { urlHash, normalizedUrl } = urlInfo;
        
        // ✅ STEP 2: Check cache dulu (memory + Redis) - FAST PATH
        const cached = await this.getImageUrlCache(imageUrl);
        if (cached && cached.localPath) {
            // ✅ Check if local file still exists (verify file integrity)
            try {
                if (fs.existsSync(cached.localPath)) {
                    // ✅ Verify file is readable and not corrupted
                    const stats = fs.statSync(cached.localPath);
                    if (stats.size > 0 && stats.size <= this.imageMaxSize) {
                        return cached.localPath; // ✅ Cache hit - instant return
                    } else {
                        // ✅ File corrupted or invalid size, remove from cache
                        this.imageUrlCache.delete(urlHash);
                        if (this.redis) {
                            this.redis.del(`image_cache:${urlHash}`).catch(() => {});
                        }
                        try {
                            fs.unlinkSync(cached.localPath); // Delete corrupted file
                        } catch (unlinkErr) {
                            // Ignore unlink error
                        }
                    }
                } else {
                    // ✅ File tidak ada, hapus dari cache
                    this.imageUrlCache.delete(urlHash);
                    if (this.redis) {
                        this.redis.del(`image_cache:${urlHash}`).catch(() => {});
                    }
                }
            } catch (err) {
                // ✅ File check error, remove from cache and continue to download
                this.imageUrlCache.delete(urlHash);
                if (this.redis) {
                    this.redis.del(`image_cache:${urlHash}`).catch(() => {});
                }
            }
        }
        
        // ✅ STEP 3: Generate cache filename dari URL hash (consistent untuk URL yang sama)
        let urlExt = '.jpg';
        try {
            const urlPath = new URL(normalizedUrl).pathname;
            urlExt = path.extname(urlPath) || '.jpg';
            // ✅ Validate extension (prevent security issues)
            if (!['.jpg', '.jpeg', '.png', '.gif', '.webp'].includes(urlExt.toLowerCase())) {
                urlExt = '.jpg'; // Default to jpg for unknown extensions
            }
        } catch (err) {
            // Invalid URL, use default extension
        }
        const cacheFileName = `${urlHash}${urlExt}`;
        const cacheFilePath = path.join(this.imageCacheDir, cacheFileName);
        
        // ✅ STEP 4: Check if file already exists (another session might have downloaded it)
        if (fs.existsSync(cacheFilePath)) {
            try {
                const stats = fs.statSync(cacheFilePath);
                if (stats.size > 0 && stats.size <= this.imageMaxSize) {
                    // ✅ File exists and valid, update cache and return
                    this.setImageUrlCache(imageUrl, {
                        processed: true,
                        localPath: cacheFilePath,
                        size: stats.size
                    });
                    return cacheFilePath; // ✅ Instant return, no download needed
                }
            } catch (err) {
                // File exists but error reading, continue to download
            }
        }
        
        const existingDownload = this.imageDownloadPromises.get(cacheFilePath);
        if (existingDownload) {
            return await existingDownload;
        }

        // ✅ STEP 5: Download image streaming ke file agar heap tidak menahan buffer besar.
        const downloadPromise = (async () => {
            const tempFilePath = `${cacheFilePath}.tmp`;
            let downloadedBytes = 0;
            try {
                const response = await axios.get(imageUrl, {
                    responseType: 'stream',
                    timeout: this.imageDownloadTimeout,
                    maxContentLength: this.imageMaxSize,
                    maxBodyLength: this.imageMaxSize,
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept': 'image/*'
                    },
                    validateStatus: (status) => status >= 200 && status < 400
                });

                // ✅ Validate response
                if (!response || !response.data) {
                    throw new Error('Invalid response');
                }

                await new Promise((resolve, reject) => {
                    let done = false;
                    const input = response.data;
                    const output = fs.createWriteStream(tempFilePath, { flags: 'w' });
                    const finish = (err) => {
                        if (done) return;
                        done = true;
                        if (err) {
                            try { input.destroy?.(); } catch (_) {}
                            try { output.destroy?.(); } catch (_) {}
                            reject(err);
                        } else {
                            resolve();
                        }
                    };
                    input.on('data', (chunk) => {
                        downloadedBytes += chunk?.length || 0;
                        if (downloadedBytes > this.imageMaxSize) {
                            finish(new Error(`Image size exceeds limit: ${downloadedBytes} bytes`));
                        }
                    });
                    input.on('error', finish);
                    output.on('error', finish);
                    output.on('finish', () => finish());
                    input.pipe(output);
                });

                if (downloadedBytes === 0) {
                    throw new Error('Image size invalid: 0 bytes');
                }

                fs.renameSync(tempFilePath, cacheFilePath); // Atomic rename

                // ✅ STEP 7: Update cache dengan local path (memory + Redis)
                this.setImageUrlCache(imageUrl, {
                    processed: true,
                    localPath: cacheFilePath,
                    size: downloadedBytes
                });

                // ✅ STEP 8: Cleanup old cache files (non-blocking, fire and forget)
                setImmediate(() => this.cleanupOldCacheFiles());

                return cacheFilePath;
            } catch (error) {
                // ✅ Download failed, cleanup temp file if exists
                try {
                    if (fs.existsSync(tempFilePath)) {
                        fs.unlinkSync(tempFilePath);
                    }
                } catch (cleanupErr) {
                    // Ignore cleanup error
                }

                // ✅ Log warning but don't throw (fallback to URL)
                console.warn(`⚠️ [IMAGE-CACHE] Failed to download and cache image: ${error.message}`);
                return null;
            } finally {
                this.imageDownloadPromises.delete(cacheFilePath);
            }
        })();
        this.imageDownloadPromises.set(cacheFilePath, downloadPromise);
        return await downloadPromise;
    }
    
    // ✅ SMART CLEANUP: Remove old cache files dengan smart management
    // ✅ Features: TTL-based cleanup, size limit, prevent disk space issues
    cleanupOldCacheFiles() {
        try {
            if (!fs.existsSync(this.imageCacheDir)) {
                return;
            }
            
            const files = fs.readdirSync(this.imageCacheDir);
            const now = Date.now();
            const maxAge = this.imageUrlCacheTTL; // Same as cache TTL
            let deletedCount = 0;
            let totalSize = 0;
            const fileStats = [];
            
            // ✅ STEP 1: Collect file stats (non-blocking, efficient)
            for (const file of files) {
                try {
                    // ✅ Skip temp files
                    if (file.endsWith('.tmp')) {
                        try {
                            const tmpPath = path.join(this.imageCacheDir, file);
                            const tmpStats = fs.statSync(tmpPath);
                            if (now - tmpStats.mtimeMs > this.imageDownloadTimeout * 2) {
                                fs.unlinkSync(tmpPath);
                                deletedCount++;
                            }
                        } catch (err) {
                            // Ignore
                        }
                        continue;
                    }
                    
                    const filePath = path.join(this.imageCacheDir, file);
                    const stats = fs.statSync(filePath);
                    const age = now - stats.mtimeMs;
                    
                    fileStats.push({
                        path: filePath,
                        age: age,
                        size: stats.size,
                        mtime: stats.mtimeMs
                    });
                    
                    totalSize += stats.size;
                } catch (err) {
                    // Ignore errors for individual files
                }
            }
            
            // ✅ STEP 2: Delete expired files (older than TTL)
            for (const fileStat of fileStats) {
                if (fileStat.age > maxAge) {
                    try {
                        fs.unlinkSync(fileStat.path);
                        deletedCount++;
                        totalSize -= fileStat.size;
                    } catch (err) {
                        // Ignore errors
                    }
                }
            }
            
            // ✅ STEP 3: If still too large (>500MB), delete oldest 20% files
            const maxCacheSize = 500 * 1024 * 1024; // 500MB max cache size
            if (totalSize > maxCacheSize) {
                const sortedByAge = fileStats
                    .filter(f => fs.existsSync(f.path)) // Only existing files
                    .sort((a, b) => a.mtime - b.mtime); // Oldest first
                
                const toDelete = Math.ceil(sortedByAge.length * 0.2); // Delete oldest 20%
                for (let i = 0; i < toDelete && i < sortedByAge.length; i++) {
                    try {
                        if (fs.existsSync(sortedByAge[i].path)) {
                            fs.unlinkSync(sortedByAge[i].path);
                            deletedCount++;
                        }
                    } catch (err) {
                        // Ignore errors
                    }
                }
            }
            
            if (deletedCount > 0) {
                const sizeMB = (totalSize / 1024 / 1024).toFixed(2);
                console.log(`🧹 [IMAGE-CACHE] Cleaned up ${deletedCount} old cache files (remaining: ${sizeMB}MB)`);
            }
        } catch (err) {
            // Ignore cleanup errors (non-critical)
            console.warn(`⚠️ [IMAGE-CACHE] Cleanup error: ${err.message}`);
        }
    }
    
    async hasSentMessageId(messageId) {
        const id = String(messageId || '').trim();
        if (!id) return false;
        const idUpper = id.toUpperCase();
        // Check in-memory cache first (fast) — case-insensitive
        if (this.sentMessageIds.has(id) || this.sentMessageIds.has(idUpper)) {
            return true;
        }
        
        // Check Redis untuk global deduplication (multi-server)
        if (this.redis) {
            const key = `${this.sentMessagesRedisKey}:${id}`;
            const keyUpper = `${this.sentMessagesRedisKey}:${idUpper}`;
            const [exists, existsUpper] = await Promise.all([
                this.redis.exists(key).catch(() => 0),
                id === idUpper ? Promise.resolve(0) : this.redis.exists(keyUpper).catch(() => 0),
            ]);
            if (exists || existsUpper) {
                this.sentMessageIds.add(id);
                this.sentMessageIds.add(idUpper);
                return true;
            }
        }
        
        return false;
    }
    
    // ✅ HELPER: Extract nomor WhatsApp yang clean (10-15 digits)
    getCleanBotNumber() {
        const botJid = this.sock?.user?.id || '';
        if (!botJid) return '';
        
        let botNumber = botJid.split('@')[0];
        if (botNumber.includes(':')) {
            botNumber = botNumber.split(':')[0];
        }
        
        // Validasi: harus 10-15 digits
        if (/^\d{10,15}$/.test(botNumber)) {
            return botNumber;
        }
        
        return ''; // Return empty jika tidak valid
    }

    normalizePhoneNumber(rawPhoneNumber, options = {}) {
        const defaults = {
            defaultCountryCode: config.whatsapp?.defaultCountryCode || '62',
            minLength: config.whatsapp?.minPhoneLength || 10,
            maxLength: config.whatsapp?.maxPhoneLength || 15
        };

        const {
            defaultCountryCode = defaults.defaultCountryCode,
            minLength = defaults.minLength,
            maxLength = defaults.maxLength
        } = { ...defaults, ...options };

        const sanitizedDefaultCountryCode = String(defaultCountryCode || '62').trim().replace(/[^0-9]/g, '') || '62';
        
        if (rawPhoneNumber === undefined || rawPhoneNumber === null) {
            return {
                raw: rawPhoneNumber,
                normalized: null,
                appliedDefaultCountryCode: false,
                defaultCountryCode: sanitizedDefaultCountryCode,
                isValid: false,
                reason: 'EMPTY_INPUT'
            };
        }
        
        let rawString;
        if (typeof rawPhoneNumber === 'string') {
            rawString = rawPhoneNumber.trim();
        } else if (typeof rawPhoneNumber === 'number') {
            rawString = String(Math.trunc(rawPhoneNumber));
        } else {
            rawString = String(rawPhoneNumber);
        }
        
        let normalized = rawString.replace(/[^0-9]/g, '');
        let appliedDefaultCountryCode = false;
        let appliedCode = null;
        
        if (normalized.startsWith('00')) {
            normalized = normalized.substring(2);
        }
        
        if (normalized.startsWith('0')) {
            normalized = `${sanitizedDefaultCountryCode}${normalized.substring(1)}`;
            appliedDefaultCountryCode = true;
            appliedCode = sanitizedDefaultCountryCode;
        }

        // 08xxx / 8xxxxxxxxxx tanpa 62 → WhatsApp menolak taut ("periksa nomor / dapatkan kode baru")
        if (
            sanitizedDefaultCountryCode === '62' &&
            /^8\d{8,12}$/.test(normalized)
        ) {
            normalized = `62${normalized}`;
            appliedDefaultCountryCode = true;
            appliedCode = sanitizedDefaultCountryCode;
        }
        
        const isDigitsOnly = /^\d+$/.test(normalized);
        const isValidLength = normalized.length >= minLength && normalized.length <= maxLength;
        const isValid = Boolean(normalized) && isDigitsOnly && isValidLength;
        
        return {
            raw: rawString,
            normalized,
            appliedDefaultCountryCode,
            defaultCountryCode: sanitizedDefaultCountryCode,
            defaultCodeUsed: appliedCode,
            isValid,
            reason: isValid ? null : (isDigitsOnly ? `INVALID_LENGTH_${normalized.length}` : 'NON_NUMERIC')
        };
    }

    /**
     * Tunggu this.sock siap sebelum requestPairingCode — cegah null sock race.
     */
    async waitForPairingSocketReady() {
        const maxWaitMs = Math.max(
            5000,
            Math.min(20000, Number(process.env.PAIRING_SOCKET_WAIT_MS) || 12000)
        );
        const stepMs = 400;
        let waited = 0;
        while (waited < maxWaitMs) {
            if (this.sock && isBaileysWebSocketOpen(this.sock)) {
                return { ok: true, waitedMs: waited };
            }
            await new Promise((resolve) => setTimeout(resolve, stepMs));
            waited += stepMs;
        }
        const wsState = getBaileysWebSocketReadyState(this.sock);
        return {
            ok: false,
            waitedMs: waited,
            wsState,
            reason: !this.sock ? 'SOCK_NULL' : `WS_NOT_OPEN_${wsState}`
        };
    }
    
    // ✅ ANOMALI DETECTION: Deteksi anomali yang mencurigakan (bot hantu)
    // ✅ COMPREHENSIVE CHECK: Check berbagai indikator anomali
    async detectAnomaly(messageId, remoteJid = null) {
        const anomalies = [];
        
        try {
            // ✅ ANOMALI 1: Check apakah messageId ada di sentMessageIds
            // ✅ Store di variable untuk digunakan di bagian lain
            const hasInSentList = await this.hasSentMessageId(messageId);
            if (!hasInSentList) {
                anomalies.push({
                    type: 'MISSING_IN_SENT_LIST',
                    severity: 'HIGH',
                    message: `MessageId ${messageId} NOT found in sentMessageIds/Redis - BOT HANTU SUSPECTED`
                });
            }
            
            // ✅ Store untuk digunakan di bagian loadMessage error handling
            let hasInSentListForError = hasInSentList;
            
            // ✅ ANOMALI 2: Check apakah ada remoteJid
            if (!remoteJid && this.redis) {
                const cachedJid = await this.redis.get(`sent_message_jid:${messageId}`).catch(() => null);
                if (!cachedJid) {
                    anomalies.push({
                        type: 'MISSING_REMOTE_JID',
                        severity: 'MEDIUM',
                        message: `No remoteJid found for messageId ${messageId} - Cannot validate destination`
                    });
                } else {
                    remoteJid = cachedJid;
                }
            }
            
            // ✅ ANOMALI 3: Check apakah pesan ada di WhatsApp
            if (this.sock && this.isAuthenticated && remoteJid) {
                try {
                    // ✅ FIX: Check apakah loadMessage method tersedia
                    if (!this.sock.loadMessage || typeof this.sock.loadMessage !== 'function') {
                        // ✅ Error teknis - bukan bot hantu! Cek indikator lain dulu
                        // ✅ Jika ada di sentMessageIds atau lifecycle indicators, downgrade severity
                        if (hasInSentList) {
                            // Pesan ada di sentMessageIds, error teknis bukan bot hantu
                            anomalies.push({
                                type: 'WHATSAPP_LOAD_ERROR_TECHNICAL',
                                severity: 'LOW',
                                message: `loadMessage method not available (technical error) - Message validated via sentMessageIds`
                            });
                        } else if (this.redis) {
                            // Cek lifecycle indicators
                            const [serverAck, delivered, deviceSent] = await Promise.all([
                                this.redis.get(`lifecycle:server_ack:${messageId}`),
                                this.redis.get(`lifecycle:delivered:${messageId}`),
                                this.redis.get(`lifecycle:device_sent:${messageId}`)
                            ]).catch(() => [null, null, null]);
                            
                            if (serverAck || delivered || deviceSent) {
                                // Ada lifecycle indicators, error teknis bukan bot hantu
                                anomalies.push({
                                    type: 'WHATSAPP_LOAD_ERROR_TECHNICAL',
                                    severity: 'LOW',
                                    message: `loadMessage method not available (technical error) - Message validated via lifecycle indicators`
                                });
                            } else {
                                // Tidak ada indikator positif, tapi tetap LOW karena error teknis
                                anomalies.push({
                                    type: 'WHATSAPP_LOAD_ERROR_TECHNICAL',
                                    severity: 'LOW',
                                    message: `loadMessage method not available (technical error) - Cannot fully validate`
                                });
                            }
                        } else {
                            // Tidak ada Redis, tapi error teknis tetap LOW
                            anomalies.push({
                                type: 'WHATSAPP_LOAD_ERROR_TECHNICAL',
                                severity: 'LOW',
                                message: `loadMessage method not available (technical error) - Cannot fully validate`
                            });
                        }
                    } else {
                        // Method tersedia, coba load message
                        const messageKey = {
                            remoteJid: remoteJid,
                            id: messageId,
                            fromMe: true
                        };
                        const message = await this.sock.loadMessage(messageKey);
                        
                        if (!message || !message.message) {
                            anomalies.push({
                                type: 'MESSAGE_NOT_IN_WHATSAPP',
                                severity: 'CRITICAL',
                                message: `Message ${messageId} NOT found in WhatsApp device - BOT HANTU CONFIRMED`
                            });
                        } else {
                            // ✅ ANOMALI 4: Check timestamp consistency
                            const messageTimestamp = message.messageTimestamp || message.message?.timestamp;
                            const now = Math.floor(Date.now() / 1000);
                            if (messageTimestamp) {
                                const age = now - messageTimestamp;
                                if (age < 0) {
                                    anomalies.push({
                                        type: 'FUTURE_TIMESTAMP',
                                        severity: 'HIGH',
                                        message: `Message ${messageId} has future timestamp - Anomaly detected`
                                    });
                                } else if (age > 3600) {
                                    anomalies.push({
                                        type: 'OLD_MESSAGE',
                                        severity: 'MEDIUM',
                                        message: `Message ${messageId} is very old (${age}s) - May be cached message`
                                    });
                                }
                            }
                        }
                    }
                } catch (loadError) {
                    // ✅ FIX: Handle error "is not a function" dengan lebih baik
                    if (loadError.message && loadError.message.includes('is not a function')) {
                        // Error teknis - bukan bot hantu! Cek indikator lain dulu
                        if (hasInSentList) {
                            // Pesan ada di sentMessageIds, error teknis bukan bot hantu
                            anomalies.push({
                                type: 'WHATSAPP_LOAD_ERROR_TECHNICAL',
                                severity: 'LOW',
                                message: `Cannot load message (technical error: ${loadError.message}) - Message validated via sentMessageIds`
                            });
                        } else if (this.redis) {
                            // Cek lifecycle indicators
                            const [serverAck, delivered, deviceSent] = await Promise.all([
                                this.redis.get(`lifecycle:server_ack:${messageId}`),
                                this.redis.get(`lifecycle:delivered:${messageId}`),
                                this.redis.get(`lifecycle:device_sent:${messageId}`)
                            ]).catch(() => [null, null, null]);
                            
                            if (serverAck || delivered || deviceSent) {
                                // Ada lifecycle indicators, error teknis bukan bot hantu
                                anomalies.push({
                                    type: 'WHATSAPP_LOAD_ERROR_TECHNICAL',
                                    severity: 'LOW',
                                    message: `Cannot load message (technical error: ${loadError.message}) - Message validated via lifecycle indicators`
                                });
                            } else {
                                // Tidak ada indikator positif, tapi tetap MEDIUM karena error teknis (bukan HIGH)
                                anomalies.push({
                                    type: 'WHATSAPP_LOAD_ERROR',
                                    severity: 'MEDIUM',
                                    message: `Cannot load message ${messageId} from WhatsApp - Error: ${loadError.message} (technical error, cannot fully validate)`
                                });
                            }
                        } else {
                            // Tidak ada Redis, tapi error teknis tetap MEDIUM
                            anomalies.push({
                                type: 'WHATSAPP_LOAD_ERROR',
                                severity: 'MEDIUM',
                                message: `Cannot load message ${messageId} from WhatsApp - Error: ${loadError.message} (technical error)`
                            });
                        }
                    } else {
                        // Error lain (bukan teknis), tetap HIGH tapi check dulu
                        if (!hasInSentList) {
                            // Tidak ada di sentMessageIds, bisa jadi bot hantu
                            anomalies.push({
                                type: 'WHATSAPP_LOAD_ERROR',
                                severity: 'HIGH',
                                message: `Cannot load message ${messageId} from WhatsApp - Error: ${loadError.message}`
                            });
                        } else {
                            // Ada di sentMessageIds, downgrade ke MEDIUM
                            anomalies.push({
                                type: 'WHATSAPP_LOAD_ERROR',
                                severity: 'MEDIUM',
                                message: `Cannot load message ${messageId} from WhatsApp - Error: ${loadError.message} (but message exists in sentMessageIds)`
                            });
                        }
                    }
                }
            }
            
            // ✅ ANOMALI 5: Check duplicate messageId pattern
            if (this.redis) {
                const duplicateCheck = await this.redis.get(`anomaly:duplicate:${messageId}`).catch(() => null);
                if (duplicateCheck) {
                    anomalies.push({
                        type: 'DUPLICATE_MESSAGE_ID',
                        severity: 'HIGH',
                        message: `MessageId ${messageId} appears multiple times - Possible duplicate`
                    });
                }
            }
            
        } catch (error) {
            anomalies.push({
                type: 'ANOMALY_CHECK_ERROR',
                severity: 'MEDIUM',
                message: `Error during anomaly detection: ${error.message}`
            });
        }
        
        // Log semua anomali yang ditemukan
        if (anomalies.length > 0) {
            const criticalAnomalies = anomalies.filter(a => a.severity === 'CRITICAL');
            const highAnomalies = anomalies.filter(a => a.severity === 'HIGH');
            
            if (criticalAnomalies.length > 0) {
                console.error(`🚨 [ANOMALY-DETECTED] CRITICAL anomalies for msgId ${messageId}:`, criticalAnomalies);
            } else if (highAnomalies.length > 0) {
                console.warn(`⚠️ [ANOMALY-DETECTED] HIGH severity anomalies for msgId ${messageId}:`, highAnomalies);
            } else {
                console.log(`🔍 [ANOMALY-DETECTED] Anomalies for msgId ${messageId}:`, anomalies);
            }
        }
        
        const isBotHantu = anomalies.some(a => a.severity === 'CRITICAL' || (a.severity === 'HIGH' && a.type === 'MESSAGE_NOT_IN_WHATSAPP'));
        
        // Mark Redis hanya jika BOT_HANTU_BLOCK=1 — default OFF: hantu tetap boleh kirim/reconnect tanpa ACK
        if (isBotHantu && this.redis && this.botHantuBlockEnabled()) {
            const botHantuKey = `bot_hantu:${this.sessionId}`;
            const botHantuTTL = 24 * 60 * 60; // 24 jam - prevent reconnect selama 24 jam
            await this.redis.set(botHantuKey, JSON.stringify({
                detected_at: new Date().toISOString(),
                messageId: messageId,
                anomalies: anomalies.filter(a => a.severity === 'CRITICAL' || a.severity === 'HIGH'),
                reason: 'CRITICAL_ANOMALY_DETECTED'
            }), { EX: botHantuTTL }).catch(() => {});
            console.error(`🚨 [BOT-HANTU] Session ${this.sessionId} marked as BOT HANTU in Redis - reconnect will be BLOCKED for 24 hours`);
        } else if (isBotHantu && !this.botHantuBlockEnabled()) {
            console.warn(`👻 [BOT-HANTU] detect msgId=${messageId} session=${this.sessionId} — BLOCK OFF, biarkan kirim tanpa ACK`);
        }
        
        return {
            hasAnomaly: anomalies.length > 0,
            anomalies: anomalies,
            isBotHantu: isBotHantu
        };
    }

    /** BOT_HANTU_BLOCK=1 → blok init/reconnect/webhook. Default 0: hantu tetap kirim (tanpa ACK pun). */
    botHantuBlockEnabled() {
        const v = String(process.env.BOT_HANTU_BLOCK ?? '0').trim().toLowerCase();
        return ['1', 'true', 'on', 'yes'].includes(v);
    }
    
    // ✅ CHECK BOT HANTU: Efficient check apakah session terdeteksi sebagai bot hantu
    async isSessionBotHantu() {
        if (!this.botHantuBlockEnabled()) {
            return false; // gas: jangan blokir kirim/reconnect
        }
        if (!this.redis) {
            return false; // Tanpa Redis, tidak bisa check (conservative: allow reconnect)
        }
        
        try {
            const botHantuKey = `bot_hantu:${this.sessionId}`;
            const botHantuData = await this.redis.get(botHantuKey);
            return botHantuData !== null;
        } catch (error) {
            console.error(`❌ [BOT-HANTU] Error checking bot hantu flag for ${this.sessionId}:`, error.message);
            return false; // Conservative: jika error, allow reconnect
        }
    }
    
    // ✅ MARK BOT HANTU: Manual mark (untuk critical cases)
    async markAsBotHantu(reason = 'MANUAL_MARK', details = {}) {
        if (!this.botHantuBlockEnabled()) {
            console.warn(`👻 [BOT-HANTU] mark skipped (BLOCK OFF) session=${this.sessionId} reason=${reason}`);
            return;
        }
        if (!this.redis) {
            console.warn(`⚠️ [BOT-HANTU] Cannot mark ${this.sessionId} as bot hantu - Redis not available`);
            return;
        }
        
        try {
            const botHantuKey = `bot_hantu:${this.sessionId}`;
            const botHantuTTL = 24 * 60 * 60; // 24 jam
            await this.redis.set(botHantuKey, JSON.stringify({
                detected_at: new Date().toISOString(),
                reason: reason,
                details: details,
                marked_by: 'SYSTEM'
            }), { EX: botHantuTTL });
            console.error(`🚨 [BOT-HANTU] Session ${this.sessionId} manually marked as BOT HANTU - reconnect will be BLOCKED for 24 hours`);
        } catch (error) {
            console.error(`❌ [BOT-HANTU] Error marking ${this.sessionId} as bot hantu:`, error.message);
        }
    }
    
    // ✅ VALIDASI: Check apakah pesan dengan msgId benar-benar ada di WhatsApp
    // ✅ PREVENT BOT HANTU: Jika pesan tidak ada di WhatsApp, berarti bot hantu
    // ✅ RETRY MECHANISM: Retry dengan delay untuk lebih reliable (1 detik delay, 3 retry)
    // ✅ ANOMALY DETECTION: Deteksi anomali sebelum validasi
    async validateMessageExistsInWhatsApp(messageId, remoteJid = null, retryCount = 0, maxRetries = 3) {
        try {
            // ✅ ANOMALY DETECTION: Check anomali sebelum validasi
            if (retryCount === 0) {
                const anomalyCheck = await this.detectAnomaly(messageId, remoteJid);
                if (anomalyCheck.isBotHantu) {
                    console.error(`🚨 [BOT-HANTU] CRITICAL anomaly detected for msgId ${messageId} - BOT HANTU CONFIRMED, skipping validation`);
                    return false;
                }
            }
            
            // ✅ FIXED: Hapus check isAuthenticated - biarkan WhatsApp yang handle
            if (!this.sock) {
                console.warn(`⚠️ [VALIDATE-MESSAGE] Cannot validate - socket not ready for msgId: ${messageId}`);
                return false; // Conservative: jika tidak bisa validate, return false
            }
            
            // Try to load message from WhatsApp
            // Jika remoteJid tidak ada, kita perlu cari dari cache atau skip validation
            if (!remoteJid) {
                // Coba ambil dari Redis jika ada
                if (this.redis) {
                    const cachedJid = await this.redis.get(`sent_message_jid:${messageId}`).catch(() => null);
                    if (cachedJid) {
                        remoteJid = cachedJid;
                    }
                }
                
                // Jika masih tidak ada, kita tidak bisa validate dengan pasti
                // Tapi kita bisa coba check dari sentMessageIds (setidaknya confirm dikirim dari wrapper ini)
                if (!remoteJid) {
                    console.warn(`⚠️ [VALIDATE-MESSAGE] No remoteJid for msgId ${messageId}, cannot fully validate but checking sentMessageIds`);
                    return await this.hasSentMessageId(messageId);
                }
            }
            
            // Try to load message menggunakan Baileys loadMessage
            // Format key: { remoteJid, id, fromMe }
            try {
                // ✅ FIX: Check apakah loadMessage method tersedia
                if (!this.sock.loadMessage || typeof this.sock.loadMessage !== 'function') {
                    // ✅ Method tidak tersedia - ini error teknis, bukan bot hantu!
                    // ✅ FALLBACK: Cek indikator lain untuk validasi
                    console.warn(`⚠️ [VALIDATE-MESSAGE] loadMessage method not available for msgId: ${messageId} - checking alternative indicators`);
                    
                    // ✅ INDICATOR 1: Check apakah ada SERVER_ACK atau DELIVERED dari WhatsApp (original)
                    if (this.redis) {
                        const [serverAck, delivered, deviceSent] = await Promise.all([
                            this.redis.get(`lifecycle:server_ack:${messageId}`),
                            this.redis.get(`lifecycle:delivered:${messageId}`),
                            this.redis.get(`lifecycle:device_sent:${messageId}`)
                        ]).catch(() => [null, null, null]);
                        
                        // Jika ada SERVER_ACK atau DELIVERED dari WhatsApp, berarti pesan valid
                        if (serverAck || delivered || deviceSent) {
                            console.log(`✅ [VALIDATE-MESSAGE] Message validated via lifecycle indicators (SERVER_ACK/DELIVERED) for msgId: ${messageId}`);
                            return true;
                        }
                    }
                    
                    // ✅ INDICATOR 2: Check apakah ada di sentMessageIds
                    const hasInSentList = await this.hasSentMessageId(messageId);
                    if (hasInSentList) {
                        console.log(`✅ [VALIDATE-MESSAGE] Message validated via sentMessageIds for msgId: ${messageId} (loadMessage unavailable)`);
                        return true;
                    }
                    
                    // ✅ Jika semua indikator negatif, baru deteksi sebagai bot hantu
                    console.warn(`⚠️ [VALIDATE-MESSAGE] Cannot validate msgId ${messageId} - loadMessage unavailable and no positive indicators found`);
                    return false; // Conservative: jika tidak bisa validate, return false
                }
                
                const messageKey = {
                    remoteJid: remoteJid,
                    id: messageId,
                    fromMe: true
                };
                
                // Try load message - jika berhasil, berarti pesan ada
                const message = await this.sock.loadMessage(messageKey);
                
                if (message && message.message) {
                    console.log(`✅ [VALIDATE-MESSAGE] Message exists in WhatsApp for msgId: ${messageId}${retryCount > 0 ? ` (retry ${retryCount})` : ''}`);
                    return true;
                } else {
                    // Retry jika belum mencapai max retries
                    if (retryCount < maxRetries) {
                        console.log(`🔄 [VALIDATE-MESSAGE] Message not found yet for msgId: ${messageId}, retrying in 1s (attempt ${retryCount + 1}/${maxRetries})`);
                        await new Promise(resolve => setTimeout(resolve, 1000)); // Delay 1 detik
                        return await this.validateMessageExistsInWhatsApp(messageId, remoteJid, retryCount + 1, maxRetries);
                    }
                    console.error(`🚨 [BOT-HANTU] Message NOT found in WhatsApp for msgId: ${messageId} after ${maxRetries} retries - BOT HANTU DETECTED`);
                    return false;
                }
            } catch (loadError) {
                // ✅ FIX: Handle error "is not a function" dengan lebih baik
                if (loadError.message && loadError.message.includes('is not a function')) {
                    // Error teknis - bukan bot hantu! Cek indikator lain
                    console.warn(`⚠️ [VALIDATE-MESSAGE] loadMessage error (technical) for msgId: ${messageId} - checking alternative indicators`);
                    
                    // ✅ FALLBACK: Cek indikator lain
                    if (this.redis) {
                        const [serverAck, delivered, deviceSent] = await Promise.all([
                            this.redis.get(`lifecycle:server_ack:${messageId}`),
                            this.redis.get(`lifecycle:delivered:${messageId}`),
                            this.redis.get(`lifecycle:device_sent:${messageId}`)
                        ]).catch(() => [null, null, null]);
                        
                        if (serverAck || delivered || deviceSent) {
                            console.log(`✅ [VALIDATE-MESSAGE] Message validated via lifecycle indicators despite loadMessage error for msgId: ${messageId}`);
                            return true;
                        }
                    }
                    
                    const hasInSentList = await this.hasSentMessageId(messageId);
                    if (hasInSentList) {
                        console.log(`✅ [VALIDATE-MESSAGE] Message validated via sentMessageIds despite loadMessage error for msgId: ${messageId}`);
                        return true;
                    }
                    
                    // Jika semua indikator negatif, return false (tapi jangan log sebagai bot hantu)
                    console.warn(`⚠️ [VALIDATE-MESSAGE] Cannot validate msgId ${messageId} - loadMessage error and no positive indicators`);
                    return false;
                }
                
                // Retry jika belum mencapai max retries
                if (retryCount < maxRetries) {
                    console.warn(`🔄 [VALIDATE-MESSAGE] Error loading message for msgId: ${messageId}, retrying in 1s (attempt ${retryCount + 1}/${maxRetries}): ${loadError.message}`);
                    await new Promise(resolve => setTimeout(resolve, 1000)); // Delay 1 detik
                    return await this.validateMessageExistsInWhatsApp(messageId, remoteJid, retryCount + 1, maxRetries);
                }
                // Jika error load message setelah retry, kemungkinan pesan tidak ada (bot hantu)
                console.error(`🚨 [BOT-HANTU] Cannot load message from WhatsApp for msgId: ${messageId} after ${maxRetries} retries - Error: ${loadError.message} - BOT HANTU DETECTED`);
                return false;
            }
        } catch (error) {
            // Retry jika belum mencapai max retries
            if (retryCount < maxRetries) {
                console.warn(`🔄 [VALIDATE-MESSAGE] Error validating message for msgId ${messageId}, retrying in 1s (attempt ${retryCount + 1}/${maxRetries}): ${error.message}`);
                await new Promise(resolve => setTimeout(resolve, 1000)); // Delay 1 detik
                return await this.validateMessageExistsInWhatsApp(messageId, remoteJid, retryCount + 1, maxRetries);
            }
            console.error(`❌ [VALIDATE-MESSAGE] Error validating message for msgId ${messageId} after ${maxRetries} retries:`, error.message);
            // Conservative: jika error setelah retry, return false (tidak generate fallback)
            return false;
        }
    }
    
    async initRedis() {
        try {
            if (!config.redis?.enabled) {
                if (!this.redis) {
                    console.log('⚠️  Redis disabled via config, skipping Redis initialization');
                }
                this.redis = null;
                return;
            }

            // ✅ SUPER SMART: Gunakan shared Redis connection jika tersedia (untuk 1 port - ringan)
            if (global.sharedRedis && global.sharedRedis.isReady) {
                this.redis = global.sharedRedis;
                // ✅ CRITICAL: Start worker setelah shared Redis ready (existing connection)
                // ✅ GREP: [REDIS-CONNECTED] - Track Redis connection success (existing shared)
                console.log(`[REDIS-CONNECTED] Using existing shared Redis, starting worker session=${this.sessionId}`);
                this.startOutboxWorker();
                console.log('✅ [REDIS] Existing shared Redis ready for webhook outbox (high traffic optimized)');
                scheduleWsBroadcastRedisSubscriber();
                return;
            }

            if (this.redis) return;

            // ✅ SUPER SMART: Shared Redis connection untuk semua session di 1 port
            const sharedRedis = await ensureSharedRedis();
            if (sharedRedis) {
                this.redis = sharedRedis;
                // ✅ CRITICAL: Start worker setelah shared Redis ready
                // ✅ GREP: [REDIS-CONNECTED] - Track Redis connection success (shared)
                console.log(`[REDIS-CONNECTED] Shared Redis connected successfully, starting worker session=${this.sessionId}`);
                this.startOutboxWorker();
                console.log('✅ [REDIS] Shared Redis ready for webhook outbox (high traffic optimized)');
                return;
            }
            
            // ✅ Fallback: Create per-session Redis jika shared tidak tersedia
            this.redis = createClient({ 
                url: redisUrl,
                socket: {
                    reconnectStrategy: (retries) => {
                        // Retry selamanya (lihat catatan di shared client di atas).
                        if (retries > 0 && retries % 20 === 0) {
                            console.error(`❌ [REDIS] Still reconnecting (attempt ${retries})`);
                        }
                        return Math.min(200 * retries, 10000);
                    },
                    connectTimeout: 2000,
                    commandTimeout: 1500,
                    keepAlive: 30000,
                    noDelay: true,
                    keepAliveInitialDelay: 0
                },
                disableClientInfo: true,
                enableOfflineQueue: false
            });
            
            this.redis.on('error', (e) => {
                console.error('❌ [REDIS] Error:', e.message);
            });
            
            this.redis.on('connect', () => {
                console.log('🔄 [REDIS] Connecting...');
            });
            
            this.redis.on('ready', () => {
                console.log('✅ [REDIS] Ready for high traffic');
                // ✅ SMART: Auto-restart worker saat Redis reconnect (jika session masih aktif)
                if (!this.workerStopped && !this.workerTimer && this.isAuthenticated) {
                    console.log(`🔄 [WORKER-AUTO-RECOVERY] Redis reconnected, restarting worker session=${this.sessionId}`);
                    this.startOutboxWorker();
                }
            });
            
            this.redis.on('reconnecting', () => {
                console.log('🔄 [REDIS] Reconnecting...');
            });
            
            // ✅ SMART: Handle Redis disconnect - stop worker sementara
            this.redis.on('end', () => {
                console.log('🔌 [REDIS] Connection ended');
                // Worker akan auto-stop saat detect Redis tidak healthy
            });
            
            // ✅ SMART: Handle Redis error - increment error count
            this.redis.on('error', (err) => {
                console.error(`❌ [REDIS] Error: ${err.message}`);
                // Worker akan handle error dengan circuit breaker
            });
            
            await this.redis.connect();
            
            // ✅ HIGH TRAFFIC: Test connection dengan ping
            await this.redis.ping().catch(() => {
                throw new Error('Redis ping failed');
            });
            
            // start worker once connected
            // ✅ GREP: [REDIS-CONNECTED] - Track Redis connection success
            console.log(`[REDIS-CONNECTED] Redis connected successfully, starting worker session=${this.sessionId}`);
            this.startOutboxWorker();
            console.log('✅ [REDIS] Connected and ready for webhook outbox (high traffic optimized)');
        } catch (e) {
            // ✅ GREP: [REDIS-ERROR] - Track Redis connection error
            console.error(`[REDIS-ERROR] Not available, fallback to direct webhook only: ${e.message} session=${this.sessionId}`);
            this.redis = null;
        }
    }

    async enqueueWebhook(data, options = {}) {
        try {
            const event = (data.event || data.status || '').toString().toUpperCase();
            const webhookMsgId = data.msg_id || data.msgId || data.messageId || '';

            // Defense-in-depth: MESSAGE_DELETED hanya boleh validated bot_revoke_sent
            if (event === 'MESSAGE_DELETED') {
                if (data?.validated !== true || data?.deletedBy !== 'bot_revoke_sent') {
                    console.warn(
                        `⏭️ [SKIP MESSAGE_DELETED] enqueue blocked session=${this.sessionId} ` +
                        `deletedBy=${data?.deletedBy || 'n/a'} validated=${data?.validated === true}`
                    );
                    return { queued: false, direct: false, skipped: true, reason: 'MESSAGE_DELETED_UNVALIDATED' };
                }
            }
            
            // ✅ TEMPORARY MESSAGE ID: Auto-inject temp_msg_id jika ada mapping (hanya untuk enable_callback=true)
            // ✅ Hanya lookup jika msg_id ada dan belum ada temp_msg_id di payload
            if (webhookMsgId && !data.temp_msg_id && this.redis) {
                const tempMsgId = await this.getTempMessageIdFromReal(webhookMsgId).catch(() => null);
                if (tempMsgId) {
                    // ✅ Ada mapping temp ID - inject ke webhook payload
                    data.temp_msg_id = tempMsgId;
                }
            }
            
            // ✅ WHATSAPP STANDARD FLOW: Deduplication dan validasi urutan untuk lifecycle events
            // Flow yang benar: DISCONNECTED → RECONNECTING → CONNECTED
            const lifecycleEvents = ['DISCONNECTED', 'RECONNECTING', 'CONNECTED', 'QR_CODE', 'PAIRING_CODE'];
            if (lifecycleEvents.includes(event)) {
                const dedupeKey = `${event.toLowerCase()}:${this.sessionId}`;
                const now = Date.now();
                const lastEventTime = this.dedupeCache.get(dedupeKey) || 0;
                
                // ✅ CRITICAL FIX: CONNECTED tidak pakai cooldown - selalu kirim jika belum pernah dikirim
                // ✅ CONNECTED adalah event penting yang harus selalu terkirim, terutama saat pairing
                if (event === 'CONNECTED') {
                    // ✅ CRITICAL: Log dulu untuk debugging
                    console.log(`🔍 [ENQUEUE] CONNECTED event detected for ${this.sessionId}, checking flags...`);
                    console.log(`🔍 [ENQUEUE] connectedWebhookSent: ${this.connectedWebhookSent}, processingConnectedWebhook: ${this.processingConnectedWebhook}`);
                    
                    if (this.redis) {
                        const redisDedupeKey = `lifecycle:connected:${this.sessionId}`;
                        const redisLastTime = await this.redis.get(redisDedupeKey).catch(() => null);
                        if (redisLastTime) {
                            this.connectedWebhookSent = true;
                            console.log(`⏭️ [DEDUPE-REDIS] CONNECTED for ${this.sessionId} already sent, skipping duplicate`);
                            return { queued: false, direct: false, skipped: true, reason: 'CONNECTED_ALREADY_SENT_REDIS' };
                        }
                    }
                    
                    // ✅ Check flag connectedWebhookSent - jika sudah true, skip (sudah pernah dikirim)
                    if (this.connectedWebhookSent) {
                        console.log(`⏭️ [DEDUPE] CONNECTED for ${this.sessionId} already sent (connectedWebhookSent: true), skipping duplicate`);
                        return { queued: false, direct: false, skipped: true, reason: 'CONNECTED_ALREADY_SENT' };
                    }
                    
                    // ✅ CRITICAL: Check processing flag juga
                    // ✅ FIX: Untuk pairing mode, jika processing terlalu lama (> 5 detik), allow retry
                    if (this.processingConnectedWebhook) {
                        const processingStartTime = this.processingConnectedWebhookStartTime || Date.now();
                        const processingDuration = Date.now() - processingStartTime;
                        const processingStaleMs = this.isPairingMode ? 5000 : 3000;

                        if (processingDuration > processingStaleMs) {
                            console.warn(`⚠️ [DEDUPE] CONNECTED processing stale (${processingDuration}ms), allowing retry`);
                            this.processingConnectedWebhook = false;
                            this.processingConnectedWebhookStartTime = null;
                        } else {
                            console.log(`⏭️ [DEDUPE] CONNECTED for ${this.sessionId} sedang diproses, skipping duplicate`);
                            return { queued: false, direct: false, skipped: true, reason: 'CONNECTED_PROCESSING' };
                        }
                    }
                    
                    // ✅ Set processing start time
                    this.processingConnectedWebhookStartTime = Date.now();
                    
                    // ✅ CONNECTED belum pernah dikirim, lanjutkan proses (tidak pakai cooldown)
                    console.log(`✅ [CONNECTED] CONNECTED event detected, will send (no cooldown check, flags OK)`);
                } else {
                    // ✅ FIXED: Cooldown lebih lama untuk RECONNECTING (15s) untuk prevent spam
                    // ✅ RECONNECTING hanya perlu dikirim sekali per disconnect cycle, tidak perlu berulang
                    const lifecycleCooldown = event === 'PAIRING_CODE' ? 0 :
                                             event === 'QR_CODE' ? 5000 : 
                                             event === 'RECONNECTING' ? 15000 : // ✅ 15s untuk RECONNECTING (prevent spam)
                                             event === 'DISCONNECTED' ? 10000 : // ✅ 10s untuk DISCONNECTED
                                             3000; // 3s untuk event lainnya
                    
                    // ✅ CRITICAL FIX: DISCONNECTED perlu check Redis juga untuk prevent spam dari multiple triggers
                    // ✅ Memory cache bisa terlewat jika ada multiple request bersamaan atau instance berbeda
                    if (event === 'DISCONNECTED' && this.redis) {
                        const redisDedupeKey = `lifecycle:disconnected:${this.sessionId}`;
                        const redisLastTime = await this.redis.get(redisDedupeKey).catch(() => null);
                        if (redisLastTime) {
                            const redisTimeDiff = now - parseInt(redisLastTime);
                            if (redisTimeDiff < lifecycleCooldown) {
                                console.log(`⏭️ [DEDUPE-REDIS] ${event} for ${this.sessionId} sent recently in Redis (${Math.floor(redisTimeDiff/1000)}s ago), skipping duplicate`);
                                return { queued: false, direct: false, skipped: true, reason: 'DUPLICATE_LIFECYCLE_EVENT_REDIS' };
                            }
                        }
                    }
                    
                    // ✅ WHATSAPP STANDARD: Prevent duplicate dengan cooldown (kecuali CONNECTED)
                    if (now - lastEventTime < lifecycleCooldown) {
                        console.log(`⏭️ [DEDUPE] ${event} for ${this.sessionId} sent recently (${Math.floor((now - lastEventTime)/1000)}s ago), skipping duplicate`);
                        return { queued: false, direct: false, skipped: true, reason: 'DUPLICATE_LIFECYCLE_EVENT' };
                    }
                }
                
                // ✅ CRITICAL FIX: Untuk RECONNECTING, check juga flag reconnectingSent
                // ✅ Prevent multiple RECONNECTING dalam waktu singkat meskipun cooldown sudah lewat
                if (event === 'RECONNECTING' && this.reconnectingSent) {
                    // ✅ Check apakah masih dalam disconnect cycle yang sama (5 detik window)
                    const cycleAge = this.currentDisconnectCycle ? (now - this.currentDisconnectCycle) : Infinity;
                    if (cycleAge < 5000) {
                        setImmediate(() => console.log(`⏭️ [DEDUPE] RECONNECTING for ${this.sessionId} already sent in this disconnect cycle (${Math.floor(cycleAge/1000)}s ago), skipping duplicate`));
                        return { queued: false, direct: false, skipped: true, reason: 'RECONNECTING_ALREADY_SENT_IN_CYCLE' };
                    }
                }
                
                // ✅ WHATSAPP STANDARD: Validasi urutan event (prevent RECONNECTING sebelum DISCONNECTED)
                // RECONNECTING HARUS dikirim setelah DISCONNECTED
                if (event === 'RECONNECTING') {
                    // Check apakah DISCONNECTED sudah dikirim sebelumnya (dalam 5 detik terakhir)
                    const disconnectKey = `disconnected:${this.sessionId}`;
                    const lastDisconnectTime = this.dedupeCache.get(disconnectKey) || 0;
                    const timeSinceDisconnect = now - lastDisconnectTime;
                    
                    if (lastDisconnectTime === 0 || timeSinceDisconnect > 5000) {
                        // DISCONNECTED belum dikirim atau sudah lama (>5s), skip RECONNECTING
                        setImmediate(() => console.warn(`⚠️ [FLOW] ${event} skipped for ${this.sessionId} - DISCONNECTED not sent first (last disconnect: ${lastDisconnectTime === 0 ? 'never' : Math.floor(timeSinceDisconnect/1000) + 's ago'})`));
                        return { queued: false, direct: false, skipped: true, reason: 'INVALID_EVENT_ORDER' };
                    }
                }
                
                // Update dedupe cache dan track last lifecycle event
                // ✅ CONNECTED tidak perlu update cache (tidak pakai cooldown)
                if (event !== 'CONNECTED') {
                    const lifecycleCooldown = event === 'PAIRING_CODE' ? 0 :
                                             event === 'QR_CODE' ? 5000 : 
                                             event === 'RECONNECTING' ? 15000 :
                                             event === 'DISCONNECTED' ? 10000 :
                                             3000;
                    // ✅ MEMORY LEAK FIX: Use Redis TTL instead of setTimeout untuk prevent timer accumulation
                    this.dedupeCache.set(dedupeKey, now + lifecycleCooldown);
                    // ✅ Use Redis untuk persistent dedupe (prevent memory leak)
                    if (this.redis) {
                        const redisKey = `dedupe:${this.sessionId}:${dedupeKey}`;
                        this.redis.set(redisKey, now.toString(), { EX: Math.ceil(lifecycleCooldown / 1000) }).catch(() => {});
                    }
                    // ✅ Cleanup memory cache jika terlalu besar (fallback)
                    if (this.dedupeCache.size > this.dedupeCacheMaxSize) {
                        const entries = Array.from(this.dedupeCache.entries());
                        const expired = entries.filter(([_, expireAt]) => expireAt < now);
                        expired.forEach(([key]) => this.dedupeCache.delete(key));
                        // Jika masih besar, hapus oldest 20%
                        if (this.dedupeCache.size > this.dedupeCacheMaxSize) {
                            const sorted = entries.filter(([k]) => !expired.some(([ek]) => ek === k))
                                .sort((a, b) => a[1] - b[1]);
                            const toDelete = Math.floor(this.dedupeCacheMaxSize * 0.2);
                            for (let i = 0; i < toDelete && i < sorted.length; i++) {
                                this.dedupeCache.delete(sorted[i][0]);
                            }
                        }
                    }
                    
                    // ✅ CRITICAL FIX: Set Redis flag untuk DISCONNECTED untuk prevent spam dari multiple triggers/instances
                    if (event === 'DISCONNECTED' && this.redis) {
                        const redisDedupeKey = `lifecycle:disconnected:${this.sessionId}`;
                        this.redis.set(redisDedupeKey, now.toString(), { EX: Math.ceil(lifecycleCooldown / 1000) }).catch(() => {});
                    }
                }
                this.lastLifecycleEvent = event;
                this.lastLifecycleEventTime = now;
            }
            
            // ✅ CRITICAL: MESSAGE_SENT sebagai lifecycle event (awal cycle)
            // ✅ Logic:
            //   - enable_callback=true: Force queue MESSAGE_SENT sebagai awal cycle, deduplicate jika WhatsApp juga kirim
            //   - enable_callback=false: SKIP MESSAGE_SENT dari WhatsApp events (tidak perlu, sudah dapat original ID di response)
            if (webhookMsgId && this.redis && event === 'MESSAGE_SENT') {
                const callbackKey = `callback_enabled:${webhookMsgId}`;
                const callbackEnabled = await this.redis.get(callbackKey).catch(() => null);
                const isForceQueued = data.force_queued === true; // ✅ Flag dari force queue
                
                // ✅ enable_callback=false: SKIP MESSAGE_SENT dari WhatsApp events (tidak perlu webhook, sudah dapat original ID di response)
                if (callbackEnabled === '0' && !isForceQueued) {
                    console.log(`⏭️ [SKIP] MESSAGE_SENT from WhatsApp for msgId ${webhookMsgId} skipped (enable_callback=false, already got original ID in response, no webhook needed)`);
                    return { queued: false, direct: false, skipped: true, reason: 'ENABLE_CALLBACK_FALSE_NO_WEBHOOK' };
                }
                
                // ✅ DEDUPLICATION: Jika enable_callback=true (force queue), check apakah force_queued sudah di-queue/dikirim
                // ✅ Jika ini MESSAGE_SENT dari WhatsApp events dan enable_callback=true, skip untuk prevent duplicate
                if (callbackEnabled === '1' && !isForceQueued) {
                    // ✅ Check apakah force_queued MESSAGE_SENT sudah di-queue atau dikirim (check flag di Redis)
                    // ✅ Gunakan flag yang lebih reliable: check apakah ada force_queued flag atau sudah di-queue
                    const forceQueuedFlag = await this.redis.get(`force_queued:${webhookMsgId}:MESSAGE_SENT`).catch(() => null);
                    const [forceQueuedConfirmed, forceQueuedOriginal] = await Promise.all([
                        this.redis.get(`webhook:confirmed:${webhookMsgId}:MESSAGE_SENT`).catch(() => null),
                        this.redis.get(`webhook:confirmed:${webhookMsgId}:MESSAGE_SENT:force_queued`).catch(() => null)
                    ]);
                    
                    // ✅ Jika force_queued flag ada atau sudah confirmed, skip WhatsApp MESSAGE_SENT untuk prevent duplicate
                    if (forceQueuedFlag === '1' || forceQueuedConfirmed === '1' || forceQueuedOriginal === '1') {
                        console.log(`⏭️ [DEDUPE] MESSAGE_SENT from WhatsApp for msgId ${webhookMsgId} skipped (force_queued already queued/sent as cycle start, prevent duplicate)`);
                        return { queued: false, direct: false, skipped: true, reason: 'DUPLICATE_MESSAGE_SENT' };
                    }
                }
                
                // ✅ enable_callback=true: Terima MESSAGE_SENT dari WhatsApp events jika belum ada force_queued (fallback)
                // ✅ MESSAGE_SENT selalu valid sebagai awal lifecycle events cycle
            }
            
            if (event === 'QR_CODE' && this.isPairingMode && this.requestedPhoneNumber) {
                console.warn(`⚠️ [PAIRING-MODE] Skipping QR_CODE webhook for ${this.sessionId} (pairing mode active)`);
                return { queued: false, direct: false, skipped: true, reason: 'PAIRING_MODE_ACTIVE' };
            }
            
            // ✅ CRITICAL: Validasi bot hantu SEBELUM kirim webhook (untuk lifecycle events)
            // ✅ SKIP validasi jika:
            //   - original_from_whatsapp = true (webhook langsung dari WhatsApp, terpercaya)
            //   - validated_bot_hantu = true (sudah divalidasi sebelumnya)
            //   - backfill_generated = true (backfill hanya untuk DELIVERED/SERVER_ACK/READ, sudah terpercaya)
            //   - force_send = true (SERVER_ACK yang dipaksa kirim, tidak boleh skip)
            // ✅ VALIDASI untuk:
            //   - timeout_generated (fallback timeout untuk DELIVERED/SERVER_ACK, perlu validasi)
            //   - fallback_generated (fallback untuk DELIVERED/SERVER_ACK/READ, perlu validasi)
            //   - webhook tanpa flag (webhook dari source lain, perlu validasi)
            // ✅ NOTE: Hanya DELIVERED, SERVER_ACK, READ yang punya generated/fallback mechanism
            //   DEVICE_SENT, PLAYED, FAILED hanya dari original WhatsApp (tidak ada generated)
            const shouldValidateBotHantu = this.botHantuBlockEnabled() &&
                webhookMsgId && 
                !data.validated_bot_hantu && 
                !data.original_from_whatsapp && 
                !data.backfill_generated &&
                !data.force_send; // ✅ CRITICAL: SERVER_ACK dengan force_send tidak boleh di-validasi (wajib kirim)
            
            if (shouldValidateBotHantu) {
                const isLifecycleEvent = ['DEVICE_SENT', 'DELIVERED', 'READ', 'PLAYED', 'SERVER_ACK'].includes(event);
                if (isLifecycleEvent) {
                    // ✅ STEP 1: Check apakah pesan ada di sentMessageIds
                    const hasMessage = await this.hasSentMessageId(webhookMsgId);
                    if (!hasMessage) {
                        console.error(`🚨 [BOT-HANTU] msgId ${webhookMsgId} not in sentMessageIds/Redis - BOT HANTU DETECTED, blocking ${event} webhook`);
                        const remoteJid = await (this.redis?.get(`sent_message_jid:${webhookMsgId}`).catch(() => null) || Promise.resolve(null));
                        await this.detectAnomaly(webhookMsgId, remoteJid);
                        return { queued: false, direct: false, blocked: true, reason: 'BOT_HANTU_DETECTED' };
                    }
                    
                    // ✅ STEP 2: Check anomaly detection
                    const remoteJid = await (this.redis?.get(`sent_message_jid:${webhookMsgId}`).catch(() => null) || Promise.resolve(null));
                    const anomalyCheck = await this.detectAnomaly(webhookMsgId, remoteJid);
                    if (anomalyCheck.isBotHantu) {
                        console.error(`🚨 [BOT-HANTU] Anomaly detected for msgId ${webhookMsgId} - BOT HANTU CONFIRMED, blocking ${event} webhook`);
                        return { queued: false, direct: false, blocked: true, reason: 'BOT_HANTU_DETECTED' };
                    }
                    
                    // ✅ STEP 3: Validate message exists in WhatsApp (untuk DELIVERED/DEVICE_SENT/SERVER_ACK)
                    if (['DELIVERED', 'DEVICE_SENT', 'SERVER_ACK'].includes(event)) {
                        const messageExists = await this.validateMessageExistsInWhatsApp(webhookMsgId, remoteJid);
                        if (!messageExists) {
                            console.error(`🚨 [BOT-HANTU] msgId ${webhookMsgId} - Message NOT found in WhatsApp - BOT HANTU DETECTED, blocking ${event} webhook`);
                            return { queued: false, direct: false, blocked: true, reason: 'BOT_HANTU_DETECTED' };
                        }
                    }
                }
            }
            
            // ✅ CRITICAL: Lifecycle events tanpa message ID (DISCONNECTED, RECONNECTING, CONNECTED, QR_CODE, PAIRING_CODE)
            // ✅ Semua lifecycle events ini selalu direct send tanpa perlu message ID
            const lifecycleEventsNoMsgId = ['DISCONNECTED', 'RECONNECTING', 'CONNECTED', 'QR_CODE', 'PAIRING_CODE'];
            const isLifecycleEventNoMsgId = lifecycleEventsNoMsgId.includes(event);
            
            // ✅ CYCLE: Lifecycle events dengan message ID
            // ✅ MESSAGE_SENT = awal cycle (enable_callback=true: force queue, enable_callback=false: dari WhatsApp)
            // ✅ Setelah MESSAGE_SENT: SERVER_ACK → DEVICE_SENT → DELIVERED → READ/PLAYED (mengikuti)
            // ✅ Urutan: MESSAGE_SENT (cycle start) → SERVER_ACK → DEVICE_SENT → DELIVERED → READ → PLAYED
            const isLifecycleEventType = ['MESSAGE_SENT', 'SERVER_ACK', 'DEVICE_SENT', 'DELIVERED', 'READ', 'PLAYED'].includes(event);
            // ✅ CRITICAL FIX: Jika forceDirect explicitly false, respect itu (untuk MESSAGE_SENT dari queueMessageSentWebhook)
            // ✅ Jika forceDirect explicitly true, langsung direct send
            // ✅ Jika forceDirect undefined, default: lifecycle events direct send, non-lifecycle queue
            const forceDirect = options.forceDirect === true ? true : 
                               options.forceDirect === false ? false : 
                               (isLifecycleEventType || isLifecycleEventNoMsgId);
            
            // ✅ CRITICAL: Lifecycle events tanpa message ID selalu direct send dengan retry
            if (isLifecycleEventNoMsgId && forceDirect) {
                console.log(`🚀 [${event}] ${event} event - forcing direct send with retry (no message ID required)`);
                
                // Direct send untuk lifecycle events tanpa message ID dengan retry mechanism
                let attempts = 0;
                const maxAttempts = 3;
                
                while (attempts < maxAttempts) {
                    attempts++;
                    try {
                        console.log(`📤 [${event}] Attempt ${attempts}/${maxAttempts}: Sending ${event} webhook...`);
                        const result = await Promise.race([
                            this.sendToWebhook(data),
                            new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout')), 8000)) // ✅ Increased to 8s untuk prevent webhook loss
                        ]);
                        
                        const httpSuccess = result && typeof result === 'object';
                        const webhookSuccess = httpSuccess && (result.error === '0' || result.error === 0 || result.error === null || result.status === 'success');
                        
                        if (webhookSuccess) {
                            console.log(`✅ [${event}] ${event} webhook sent successfully (direct send, attempt ${attempts})`);
                            if (event === 'CONNECTED') {
                                await this.markConnectedWebhookSent();
                            }
                            return { queued: false, direct: true, success: true, response: result };
                        } else {
                            const errorMsg = result?.msg || result?.error || result?.message || 'Unknown error';
                            if (attempts < maxAttempts) {
                                console.warn(`⚠️ [${event}] Webhook server returned error: ${errorMsg}, retrying in ${attempts * 100}ms... (attempt ${attempts}/${maxAttempts})`);
                                await new Promise(r => setTimeout(r, attempts * 100));
                                continue;
                            } else {
                                console.warn(`⚠️ [${event}] Webhook server returned error after ${maxAttempts} attempts: ${errorMsg}, will queue for retry`);
                                // Fallback to queue
                                break;
                            }
                        }
                    } catch (err) {
                        if (attempts < maxAttempts) {
                            console.warn(`⚠️ [${event}] Direct send failed (attempt ${attempts}/${maxAttempts}): ${err.message}, retrying in ${attempts * 100}ms...`);
                            await new Promise(r => setTimeout(r, attempts * 100));
                            continue;
                        } else {
                            console.error(`❌ [${event}] Direct send failed after ${maxAttempts} attempts: ${err.message}, will queue for retry`);
                            // Fallback to queue
                            break;
                        }
                    }
                }
                
                // ✅ Fallback to queue jika semua direct send gagal
                console.log(`🔄 [${event}] All direct send attempts failed, falling back to queue...`);
            }
            
            // ✅ DEBUG: Log untuk MESSAGE_SENT dengan forceDirect=false
            if (event === 'MESSAGE_SENT') {
                const tempMsgId = data.temp_msg_id || 'NONE';
                console.log(`[ENQUEUE-DEBUG] MESSAGE_SENT msgId=${webhookMsgId || 'NONE'} tempMsgId=${tempMsgId} forceDirect=${forceDirect} options.forceDirect=${options.forceDirect}`);
            }
            
            if (forceDirect && webhookMsgId) {
                // ✅ STRATEGY: Direct send dengan Redis confirmation tracking
                // 1. Cek dulu apakah sudah pernah terkirim (prevent duplicate)
                // 2. Direct send dengan retry
                // 3. Set Redis marker sebagai konfirmasi terkirim
                // 4. Fallback ke queue jika semua gagal
                
                const confirmKey = `webhook:confirmed:${webhookMsgId}:${event}`;
                let alreadyConfirmed = false;
                
                // Cek Redis apakah sudah terkirim sebelumnya
                if (this.redis) {
                    try {
                        alreadyConfirmed = !!(await this.redis.get(confirmKey));
                    } catch (e) {
                        // Redis error, continue anyway
                    }
                }
                
                if (alreadyConfirmed) {
                    const isOriginalFromWhatsApp = data.original_from_whatsapp === true;
                    const isAutoGenerated = data.auto_generated === true;
                    const isFallbackGenerated = data.fallback_generated === true;
                    const isBackfillGenerated = data.backfill_generated === true;

                    if (event === 'SERVER_ACK') {
                        const hasOrganic = await this.hasOrganicServerAckMarker(webhookMsgId);
                        if (hasOrganic) {
                            console.log(`⏭️ [DEDUPE] SERVER_ACK for msgId ${webhookMsgId} already confirmed, skipping duplicate`);
                            return { queued: false, direct: true, confirmed: true, duplicate: true };
                        }
                        console.log(`♻️ [DEDUPE] SERVER_ACK ghost confirm for msgId ${webhookMsgId}, retrying send`);
                    } else if (isOriginalFromWhatsApp && !isAutoGenerated && !isFallbackGenerated && !isBackfillGenerated) {
                        const originalConfirmKey = `webhook:confirmed:${webhookMsgId}:${event}:original`;
                        const alreadySentOriginal = this.redis ? !!(await this.redis.get(originalConfirmKey).catch(() => null)) : false;

                        if (alreadySentOriginal) {
                            console.log(`⏭️ [DEDUPE] Original ${event} from WhatsApp for msgId ${webhookMsgId} already sent, skipping duplicate`);
                            return { queued: false, direct: true, confirmed: true, duplicate: true };
                        }

                        if (this.redis) {
                            try {
                                await this.redis.set(originalConfirmKey, '1', { EX: 86400 });
                            } catch (e) {
                                // Redis error, continue anyway
                            }
                        }
                        console.log(`✅ [ORIGINAL] Original ${event} from WhatsApp for msgId ${webhookMsgId}, sending (overriding fallback/auto-generated)`);
                    } else {
                        const isBillingEvent = ['DEVICE_SENT', 'DELIVERED', 'READ'].includes(event);
                        const sourceType = isAutoGenerated ? 'auto-generated' : (isFallbackGenerated ? 'fallback' : (isBackfillGenerated ? 'backfill' : 'unknown'));
                        if (isBillingEvent) {
                            console.log(`⏭️ [DEDUPE] ${event} (${sourceType}) for msgId ${webhookMsgId} already confirmed, skipping duplicate (prevent double billing)`);
                        } else {
                            console.log(`⏭️ [DEDUPE] ${event} (${sourceType}) for msgId ${webhookMsgId} already confirmed, skipping duplicate`);
                        }
                        return { queued: false, direct: true, confirmed: true, duplicate: true };
                    }
                }
                
                // Track attempt di memory untuk prevent infinite loop
                const attemptKey = `attempt:${webhookMsgId}:${event}:${Date.now()}`;
                if (!this.webhookAttempts) {
                    this.webhookAttempts = new Set();
                }
                
                // Direct send dengan retry yang lebih agresif
                let attempts = 0;
                const maxDirectAttempts = 3; // Increased to 3 attempts
                
                const tryDirectSend = async () => {
                    attempts++;
                    try {
                        // ✅ OPTIMIZED: Timeout lebih cepat (2s) untuk langsung masuk queue jika server lambat
                        const result = await Promise.race([
                            this.sendToWebhook(data),
                            new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout')), 2000))
                        ]);
                        
                        // ✅ CRITICAL: Verify benar-benar terkirim ke server - cek response error code
                        const httpSuccess = result && typeof result === 'object';
                        const webhookSuccess = httpSuccess && this.isWebhookResponseAckCredited(result, data);
                        
                        if (webhookSuccess) {
                            // ✅ SUCCESS: Set Redis confirmation marker HANYA setelah benar-benar terkirim & credited
                            if (this.redis) {
                                try {
                                    await this.maybeMarkWebhookConfirmed(webhookMsgId, event, data, result);
                                    const responsePreview = JSON.stringify(result).substring(0, 150);
                                    console.log(`✅ [LIFECYCLE] ${event} confirmed in Redis for msgId: ${webhookMsgId} (server response: ${responsePreview})`);
                                } catch (e) {
                                    // Redis error, but webhook succeeded
                                }
                            }
                            console.log(`✅ [LIFECYCLE] Direct webhook sent & confirmed for ${event} (attempt ${attempts}) msgId: ${webhookMsgId}`);
                            return { queued: false, direct: true, success: true, confirmed: true, response: result };
                        } else if (httpSuccess && !webhookSuccess) {
                            console.log(`⏭️ [LIFECYCLE] ${event} skipped by webhook (no credit) msgId: ${webhookMsgId}`);
                            return { queued: false, direct: true, success: true, confirmed: false, skipped: true, response: result };
                        } else {
                            // ✅ CRITICAL: Jika response tidak success, jangan set confirmed, masukkan ke queue untuk retry
                            const errorMsg = result?.msg || result?.error || result?.message || 'Unknown error';
                            throw new Error(`Webhook server returned error: ${errorMsg} (will retry via queue)`);
                        }
                    } catch (err) {
                        if (attempts < maxDirectAttempts) {
                            const backoff = Math.min(50 * attempts, 200); // ✅ OPTIMIZED: 50ms, 100ms, 200ms (lebih cepat)
                            console.warn(`⚠️ [LIFECYCLE] Direct webhook failed for ${event} (attempt ${attempts}/${maxDirectAttempts}) msgId: ${webhookMsgId}, retrying in ${backoff}ms...`);
                            await new Promise(r => setTimeout(r, backoff));
                            return tryDirectSend();
                        } else {
                            console.error(`❌ [LIFECYCLE] Direct webhook failed for ${event} after ${maxDirectAttempts} attempts msgId: ${webhookMsgId}, fallback to queue`);
                            // ✅ CRITICAL: Fallback to queue - RETRY sampai berhasil masuk queue
                            let queueAttempts = 0;
                            const maxQueueAttempts = 5;
                            
                            while (queueAttempts < maxQueueAttempts) {
                                try {
                                    const queueResult = await this.enqueueWebhook(data, { forceDirect: false });
                                    if (queueResult?.queued) {
                                        console.log(`✅ [LIFECYCLE] ${event} enqueued successfully after direct failure (msgId: ${webhookMsgId})`);
                                        return { queued: true, direct: false, fallback: true };
                                    }
                                    // Queue returned but not queued (maybe dedupe?), try again
                                    queueAttempts++;
                                    if (queueAttempts < maxQueueAttempts) {
                                        await new Promise(r => setTimeout(r, 200 * queueAttempts));
                                        continue;
                                    }
                                } catch (queueErr) {
                                    queueAttempts++;
                                    if (queueAttempts < maxQueueAttempts) {
                                        console.warn(`⚠️ [LIFECYCLE] Queue attempt ${queueAttempts}/${maxQueueAttempts} failed for ${event} msgId: ${webhookMsgId}, retrying...`);
                                        await new Promise(r => setTimeout(r, 200 * queueAttempts));
                                        continue;
                                    } else {
                                        console.error(`💀 [CRITICAL] All queue attempts failed for ${event} msgId: ${webhookMsgId}`);
                                        // Last resort: async enqueue tanpa await
                                        setTimeout(() => {
                                            this.enqueueWebhook(data, { forceDirect: false }).catch(() => {
                                                console.error(`💀 [CRITICAL] Final async queue also failed for ${event} msgId: ${webhookMsgId}`);
                                            });
                                        }, 1000);
                                        return { queued: false, direct: false, failed: true, willRetryAsync: true };
                                    }
                                }
                            }
                            
                            return { queued: false, direct: false, failed: true };
                        }
                    }
                };
                
                // Execute dengan error handling yang kuat
                tryDirectSend().catch(async (err) => {
                    console.error(`💀 [CRITICAL] Unexpected error sending ${event} for msgId ${webhookMsgId}:`, err.message);
                    // ✅ CRITICAL FIX: Last resort - pasti masuk queue meskipun semua gagal
                    try {
                        const queueResult = await this.enqueueWebhook(data, { forceDirect: false });
                        if (queueResult?.queued) {
                            console.log(`✅ [CRITICAL] ${event} for msgId ${webhookMsgId} enqueued as final fallback`);
                        } else {
                            console.error(`❌ [CRITICAL] Queue also failed for ${event} msgId ${webhookMsgId}, retrying in 2s...`);
                            // One more try after 2 seconds
                            setTimeout(async () => {
                                try {
                                    await this.enqueueWebhook(data, { forceDirect: false });
                                    console.log(`✅ [CRITICAL-RETRY] ${event} for msgId ${webhookMsgId} enqueued after retry`);
                                } catch (retryErr) {
                                    // ✅ FIXED: Catch sudah ada, tapi pastikan tidak throw
                                    console.error(`💀 [CRITICAL] Final retry failed for ${event} msgId ${webhookMsgId}:`, retryErr.message);
                                    // Jangan throw, biarkan continue
                                }
                            }, 2000);
                        }
                    } catch (finalErr) {
                        console.error(`💀 [CRITICAL] Final fallback failed for ${event} msgId ${webhookMsgId}, will retry in 5s:`, finalErr.message);
                        // Last last resort: retry after 5 seconds
                        setTimeout(async () => {
                            try {
                                await this.enqueueWebhook(data, { forceDirect: false });
                                console.log(`✅ [CRITICAL-FINAL] ${event} for msgId ${webhookMsgId} enqueued after 5s delay`);
                            } catch (finalRetryErr) {
                                // ✅ FIXED: Catch sudah ada, tapi pastikan tidak throw
                                console.error(`💀 [CRITICAL] Final 5s retry also failed for ${event} msgId ${webhookMsgId}`);
                                // Jangan throw, biarkan continue
                            }
                        }, 5000);
                    }
                });
                
                return { queued: false, direct: true, async: true };
            }
            
            // Ensure Redis client
            if (!this.redis) await this.initRedis();

            // If still no redis, fallback to direct dengan retry
            if (!this.redis) {
                console.warn(`⚠️ [WEBHOOK] Redis not available, using direct send with retry for ${event}`);
                return await this.sendToWebhookWithRetry(data, 3); // 3 retries
            }

            // Dedupe Redis NX — SERVER_ACK ikut claim lifecycle (tryClaimServerAck) di entry point
            if (webhookMsgId && event) {
                const dedupeKey = `dedupe:event:${webhookMsgId}:${event}`;
                const isBillingEvent = ['DEVICE_SENT', 'DELIVERED'].includes(event);
                const isLifecycleDedup = ['DEVICE_SENT', 'DELIVERED', 'READ', 'PLAYED', 'SERVER_ACK'].includes(event);
                const dedupeTTL = isBillingEvent ? 30 : (isLifecycleDedup ? 86400 : this.dedupeTTL);

                try {
                    const set = await this.redis.set(dedupeKey, '1', { NX: true, EX: dedupeTTL });
                    if (!set) {
                        if (isBillingEvent) {
                            const existingTTL = await this.redis.ttl(dedupeKey).catch(() => null);
                            if (existingTTL && existingTTL < 20) {
                                console.warn(`⚠️ [BILLING] Recent duplicate ${event} for msgId ${webhookMsgId}, skipping`);
                                return null;
                            }
                        } else {
                            console.log(`⏭️ [WEBHOOK] Duplicate ${event} for msgId ${webhookMsgId}, skipping`);
                            return { queued: false, direct: true, confirmed: true, duplicate: true };
                        }
                    }
                } catch (dedupeErr) {
                    console.error(`⚠️ [WEBHOOK] Redis dedupe error for ${event}:`, dedupeErr.message);
                }
            }

            // ✅ PRIORITY QUEUE: Lifecycle events dapat priority di queue
            // ✅ PERSISTENT: Tambahkan metadata untuk tracking lama tersimpan
            // ✅ CRITICAL: Simpan lifecycle events selama 7 hari untuk terus retry sampai server bisa terima
            const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000; // 7 hari dalam milliseconds
            const ONE_HOUR_MS = 60 * 60 * 1000; // 1 jam untuk non-lifecycle events
            
            // ✅ FIX: Pastikan bot dan bot_id field selalu ada (required untuk webhook server)
            // ✅ CRITICAL: bot harus nomor WhatsApp (10-15 digits), bot_id harus integer > 0 jika tersedia
            let botNumber = data.bot || '';
            // Extract nomor WhatsApp dari JID jika ada
            if (botNumber.includes('@')) {
                botNumber = botNumber.split('@')[0];
            }
            if (botNumber.includes(':')) {
                botNumber = botNumber.split(':')[0];
            }
            // Jika botNumber tidak valid (bukan 10-15 digits), gunakan dari sock
            if (!botNumber || !/^\d{10,15}$/.test(botNumber)) {
                const sockBot = (this.sock?.user?.id||'').split('@')[0];
                if (sockBot && /^\d{10,15}$/.test(sockBot)) {
                    botNumber = sockBot;
                } else {
                    botNumber = ''; // Biarkan webhook server yang resolve dari sessionId
                }
            }
            
            // ✅ CRITICAL: Coba ambil bot_id dari data, cache instance, atau Redis
            let botIdInt = 0;
            const botIdValue = data.bot_id || data.botId;
            if (botIdValue) {
                botIdInt = typeof botIdValue === 'number' ? botIdValue : (typeof botIdValue === 'string' && /^\d+$/.test(botIdValue) ? parseInt(botIdValue, 10) : 0);
            }
            
            // ✅ FIX: Jika bot_id masih 0, coba ambil dari cache instance atau Redis
            if (botIdInt === 0) {
                // Cek cache instance dulu (paling cepat)
                if (this.cachedBotId && this.cachedBotId > 0) {
                    botIdInt = this.cachedBotId;
                } else if (this.redis) {
                    // Cek Redis (async, tapi kita perlu await)
                    try {
                        const cachedBotId = await this.redis.get(`bot_id:${this.sessionId}`).catch(() => null);
                        if (cachedBotId && /^\d+$/.test(cachedBotId)) {
                            botIdInt = parseInt(cachedBotId, 10);
                            // Update cache instance untuk akses cepat berikutnya
                            this.cachedBotId = botIdInt;
                        }
                    } catch (redisErr) {
                        // Redis error, skip (akan tetap 0)
                    }
                }
            }
            
            const payload = {
                ...data,
                bot: botNumber || data.bot || '', // ✅ FIX: Always include bot field (nomor WhatsApp atau empty)
                bot_id: botIdInt, // ✅ FIX: Always include bot_id field as integer (0 jika belum tersedia, webhook server akan resolve)
                attempts: 0,
                nextAttemptAt: 0,
                priority: (isLifecycleEventType || isLifecycleEventNoMsgId) ? 1 : 0, // 1 = high priority (semua lifecycle events high priority)
                timestamp: Date.now(), // Timestamp pertama kali di-queue
                createdAt: Date.now(), // Timestamp creation untuk tracking lama tersimpan
                maxAge: (isLifecycleEventType || isLifecycleEventNoMsgId) ? SEVEN_DAYS_MS : ONE_HOUR_MS // Lifecycle: 7 hari, Others: 1h
            };
            
            // For lifecycle events, push to front (left) of queue for faster processing
            const tempMsgId = payload.temp_msg_id || 'NONE';
            if (isLifecycleEventType || isLifecycleEventNoMsgId) {
                await this.redis.lPush(this.outboxKey, JSON.stringify(payload));
                // ✅ GREP: [QUEUE-REDIS] - Simple format untuk mudah di-grep
                console.log(`[QUEUE-REDIS] event=${event} msgId=${webhookMsgId || 'NONE'} tempMsgId=${tempMsgId}`);
            } else {
                await this.redis.rPush(this.outboxKey, JSON.stringify(payload));
                // ✅ GREP: [QUEUE-REDIS] - Simple format untuk mudah di-grep
                console.log(`[QUEUE-REDIS] event=${event} msgId=${webhookMsgId || 'NONE'} tempMsgId=${tempMsgId}`);
            }
            
            return { queued: true, priority: isLifecycleEventType };
        } catch (e) {
            console.error('enqueueWebhook error:', e.message);
            // fallback to direct
            return await this.sendToWebhookWithRetry(data);
        }
    }

    /**
     * ✅ SMART: Check Redis connection health
     */
    isRedisHealthy() {
        if (!this.redis) return false;
        // Check various Redis client states
        if (this.redis.isReady === false) return false;
        if (this.redis.isOpen === false) return false;
        // Check if client is in error state
        if (this.redis.status === 'end' || this.redis.status === 'error') return false;
        return true;
    }

    /**
     * ✅ SMART: Stop worker dengan proper cleanup
     */
    stopOutboxWorker(reason = 'unknown') {
        if (this.workerTimer) {
            clearInterval(this.workerTimer);
            this.workerTimer = null;
            this.workerStopped = true;
            this.workerErrorCount = 0;
            this.lastWorkerError = null;
            console.log(`🛑 [WORKER-STOPPED] Stopped outbox worker for session=${this.sessionId} reason=${reason}`);
        }
    }

    /**
     * ✅ SMART: Start worker dengan health check dan auto-recovery
     */
    startOutboxWorker() {
        // ✅ SMART: Prevent restart jika sudah di-stop manual
        if (this.workerStopped) {
            console.log(`[WORKER-INFO] Worker stopped manually, not restarting session=${this.sessionId}`);
            return;
        }
        
        if (this.workerTimer) {
            console.log(`[WORKER-INFO] Worker already started for session=${this.sessionId}`);
            return;
        }
        
        if (!this.redis) {
            console.error(`[WORKER-ERROR] Cannot start worker: Redis not available session=${this.sessionId}`);
            return;
        }
        
        // ✅ SMART: Check Redis health sebelum start
        if (!this.isRedisHealthy()) {
            console.error(`[WORKER-ERROR] Cannot start worker: Redis not healthy session=${this.sessionId}`);
            return;
        }
        
        // ✅ SMART: Reset error count saat start baru
        this.workerErrorCount = 0;
        this.lastWorkerError = null;
        
        // ✅ GREP: [WORKER-STARTED] - Track worker start
        console.log(
            `[WORKER-STARTED] Starting outbox worker for session=${this.sessionId} outboxKey=${this.outboxKey} redisReady=${this.isRedisHealthy()} tickMs=${WEBHOOK_OUTBOX_WORKER_MS} llenEvery=${WEBHOOK_WORKER_LLEN_EVERY}`
        );
        // Interval & sampling LLEN: env WEBHOOK_OUTBOX_WORKER_MS / WEBHOOK_WORKER_LLEN_EVERY
        // ✅ BATCH PROCESSING: Process multiple items sekaligus untuk efisiensi
        // ✅ FIXED: Wrap setInterval dengan error handler untuk prevent crash
        // ✅ CRITICAL: Track worker cycle counter untuk debug
        let workerCycleCount = 0;
        this.workerTimer = setInterval(async () => {
            workerCycleCount++;
            try {
                // ✅ SMART: Check Redis health dengan comprehensive check
                if (!this.redis || !this.isRedisHealthy()) {
                    // ✅ SMART: Increment error count untuk circuit breaker
                    this.workerErrorCount++;
                    this.lastWorkerError = 'Redis not available or not healthy';
                    
                    // ✅ SMART: Auto-stop worker jika terlalu banyak error (circuit breaker)
                    if (this.workerErrorCount >= this.workerMaxErrors) {
                        console.error(`🛑 [WORKER-CIRCUIT-BREAKER] Too many errors (${this.workerErrorCount}), stopping worker session=${this.sessionId}`);
                        this.stopOutboxWorker('circuit_breaker');
                        return;
                    }
                    
                    // ✅ SMART: Log error setiap 10 cycles untuk reduce spam
                    if (this.workerErrorCount % 10 === 0) {
                        console.warn(`⚠️ [WORKER-WARNING] Redis not healthy, error count: ${this.workerErrorCount}/${this.workerMaxErrors} session=${this.sessionId}`);
                    }
                    return;
                }
                
                // ✅ SMART: Reset error count jika Redis healthy
                if (this.workerErrorCount > 0) {
                    this.workerErrorCount = 0;
                    this.lastWorkerError = null;
                }
                
                // ✅ OPTIMIZED: Batch size ditingkatkan ke 30 untuk faster processing dengan parallel
                const batchSize = 30;
                
                // ✅ PARALLEL PROCESSING: Ambil semua items sekaligus untuk parallel processing
                const itemsToProcess = [];
                
                // ✅ BATCH REDIS OPERATIONS: Ambil multiple items sekaligus untuk reduce round trips
                try {
                    // Snapshot — hindari race: interval lain bisa set this.redis=null saat await di atas
                    const redis = this.redis;
                    if (!redis) {
                        this.workerErrorCount++;
                        this.lastWorkerError = 'redis_snapshot_null';
                        if (this.workerErrorCount >= this.workerMaxErrors) {
                            this.stopOutboxWorker('redis_null');
                        }
                        return;
                    }
                    // LLEN tiap tick × banyak sesmen = storm ke Redis pada key outbox global — sample tiap N siklus
                    const sampleQueueLen = workerCycleCount % WEBHOOK_WORKER_LLEN_EVERY === 1;
                    let queueLength = 0;
                    if (sampleQueueLen) {
                        queueLength = await redis.lLen(this.outboxKey).catch((err) => {
                            this.workerErrorCount++;
                            this.lastWorkerError = err.message;

                            if (err.message && (err.message.includes('closed') || err.message.includes('Connection is closed'))) {
                                this.stopOutboxWorker('redis_closed');
                                return 0;
                            }

                            if (this.workerErrorCount < this.workerMaxErrors) {
                                console.error(
                                    `[WORKER-REDIS-ERROR] lLen failed: ${err.message} outboxKey=${this.outboxKey} session=${this.sessionId} cycle=${workerCycleCount} errorCount=${this.workerErrorCount}`
                                );
                            }
                            return 0;
                        });
                        if (queueLength > 0) {
                            console.log(
                                `[WORKER-QUEUE-SIZE] queue_length=${queueLength} outboxKey=${this.outboxKey} session=${this.sessionId} cycle=${workerCycleCount}`
                            );
                        }
                    }
                    
                    // Ambil priority items (lifecycle events) dulu
                    const priorityItems = await Promise.all(
                        Array.from({ length: Math.min(batchSize, 15) }, () => 
                            redis.lPop(this.outboxKey).catch((err) => {
                                // ✅ SMART: Handle Redis errors dengan circuit breaker
                                this.workerErrorCount++;
                                this.lastWorkerError = err.message;
                                
                                // ✅ SMART: Suppress error jika Redis sudah ditutup (tidak log spam)
                                if (err.message && (err.message.includes('closed') || err.message.includes('Connection is closed'))) {
                                    // ✅ SMART: Stop worker jika Redis ditutup
                                    this.stopOutboxWorker('redis_closed');
                                    return null;
                                }
                                
                                // ✅ SMART: Log error hanya jika belum mencapai max errors
                                if (this.workerErrorCount < this.workerMaxErrors) {
                                    // ✅ GREP: [WORKER-REDIS-ERROR] - Track Redis error (hanya untuk error selain closed)
                                    console.error(`[WORKER-REDIS-ERROR] lPop failed: ${err.message} outboxKey=${this.outboxKey} session=${this.sessionId} cycle=${workerCycleCount} errorCount=${this.workerErrorCount}`);
                                }
                                return null;
                            })
                        )
                    );
                    
                    // ✅ GREP: [WORKER-PRIORITY-FETCH] - Track priority items (ALWAYS log jika ada atau queue > 0)
                    const priorityCount = priorityItems.filter(Boolean).length;
                    if (priorityCount > 0) {
                        // ✅ CRITICAL: Log detail untuk MESSAGE_SENT
                        const messageSentCount = priorityItems.filter(Boolean).filter(raw => {
                            try {
                                const item = JSON.parse(raw);
                                return (item.event || item.status || '').toString().toUpperCase() === 'MESSAGE_SENT';
                            } catch { return false; }
                        }).length;
                        if (messageSentCount > 0) {
                            console.log(`[WORKER-PRIORITY-FETCH] fetched=${priorityCount} priority_items MESSAGE_SENT=${messageSentCount} outboxKey=${this.outboxKey} session=${this.sessionId} cycle=${workerCycleCount}`);
                        } else {
                            console.log(`[WORKER-PRIORITY-FETCH] fetched=${priorityCount} priority_items outboxKey=${this.outboxKey} session=${this.sessionId} cycle=${workerCycleCount}`);
                        }
                    } else if (sampleQueueLen && queueLength > 0) {
                        // Hanya saat siklus LLEN: indikasi bug jika queue > 0 tapi lPop tidak dapat apa-apa
                        console.log(
                            `[WORKER-PRIORITY-FETCH] fetched=0 but queue_length=${queueLength} outboxKey=${this.outboxKey} session=${this.sessionId} cycle=${workerCycleCount}`
                        );
                    }
                    
                    // Ambil normal items untuk mengisi sisa batch
                    const remainingSlots = batchSize - priorityItems.filter(Boolean).length;
                    const normalItems = await Promise.all(
                        Array.from({ length: remainingSlots }, () => 
                            redis.rPopLPush(this.outboxKey, this.processingKey).catch(() => null)
                        )
                    );
                    
                    // ✅ GREP: [WORKER-NORMAL-FETCH] - Track normal items
                    const normalCount = normalItems.filter(Boolean).length;
                    if (normalCount > 0) {
                        console.log(`[WORKER-NORMAL-FETCH] fetched=${normalCount} normal_items`);
                    }
                    
                    // Combine dan filter null
                    const allItems = [...priorityItems, ...normalItems].filter(Boolean);
                    
                    // ✅ GREP: [WORKER-FETCH] - Track items yang di-fetch dari queue
                    if (allItems.length > 0) {
                        const eventCounts = {};
                        allItems.forEach(raw => {
                            try {
                                const parsed = JSON.parse(raw);
                                const evt = (parsed.event || parsed.status || 'UNKNOWN').toString().toUpperCase();
                                eventCounts[evt] = (eventCounts[evt] || 0) + 1;
                            } catch (e) {
                                // Ignore parse error
                            }
                        });
                        const eventSummary = Object.entries(eventCounts).map(([e, c]) => `${e}:${c}`).join(' ');
                        // ✅ LIGHTWEIGHT: Log fetch hanya jika ada MESSAGE_SENT atau banyak items (reduce log spam)
                        const hasMessageSent = eventCounts['MESSAGE_SENT'] > 0;
                        if (hasMessageSent || allItems.length > 5) {
                            console.log(`[WORKER-FETCH] fetched=${allItems.length} events=[${eventSummary}]`);
                        }
                    } else {
                        // ✅ Log empty untuk track worker jalan (tapi reduce frequency)
                        // console.log(`[WORKER-FETCH] fetched=0 (empty queue)`); // Commented untuk reduce log spam
                    }
                    
                    // Move priority items ke processing queue
                    if (priorityItems.filter(Boolean).length > 0) {
                        await Promise.all(
                            priorityItems.filter(Boolean).map(raw => 
                                redis.lPush(this.processingKey, raw).catch(() => {})
                            )
                        );
                    }
                    
                    // Parse semua items sekaligus
                    for (const raw of allItems) {
                        try {
                            const item = JSON.parse(raw);
                        item.processingStartedAt = Date.now();
                            item._raw = raw;
                            item._fromPriority = priorityItems.includes(raw);
                            itemsToProcess.push(item);
                    } catch (e) {
                            // Malformed, remove from processing
                            await redis.lRem(this.processingKey, 1, raw).catch(() => {});
                            // ✅ GREP: [WORKER-PARSE-ERROR]
                            console.error(`[WORKER-PARSE-ERROR] Failed to parse item: ${e.message}`);
                        }
                    }
                        } catch (redisError) {
                    // ✅ SMART: Handle batch operation errors dengan circuit breaker
                    this.workerErrorCount++;
                    this.lastWorkerError = redisError.message;
                    
                    // ✅ SMART: Suppress error jika Redis sudah ditutup
                    if (redisError.message && (redisError.message.includes('closed') || redisError.message.includes('Connection is closed'))) {
                        this.stopOutboxWorker('redis_closed');
                        return;
                    }
                    
                    // ✅ SMART: Log error hanya jika belum mencapai max errors
                    if (this.workerErrorCount < this.workerMaxErrors) {
                        console.error(`❌ [WORKER] Redis batch operation error: ${redisError.message} errorCount=${this.workerErrorCount}`);
                    } else if (this.workerErrorCount === this.workerMaxErrors) {
                        console.error(`🛑 [WORKER-CIRCUIT-BREAKER] Max errors reached, stopping worker session=${this.sessionId}`);
                        this.stopOutboxWorker('circuit_breaker');
                    }
                    return; // Skip this cycle
                }
                
                if (itemsToProcess.length === 0) return; // Nothing to process
                
                // ✅ GREP: [WORKER-BATCH] - Track batch processing
                const eventCounts = {};
                const messageSentItems = [];
                itemsToProcess.forEach(item => {
                    const evt = (item.event || item.status || 'UNKNOWN').toString().toUpperCase();
                    eventCounts[evt] = (eventCounts[evt] || 0) + 1;
                    // ✅ CRITICAL: Track MESSAGE_SENT items untuk debug
                    if (evt === 'MESSAGE_SENT') {
                        messageSentItems.push({
                            msgId: item.msg_id || item.msgId || 'unknown',
                            tempMsgId: item.temp_msg_id || 'NONE',
                            bot: item.bot || item.bot_id || 'NONE'
                        });
                        // ✅ CRITICAL: Log setiap MESSAGE_SENT item yang akan diproses
                        console.log(`[WORKER-BATCH-MESSAGE-SENT] Found MESSAGE_SENT item msgId=${item.msg_id || item.msgId || 'unknown'} tempMsgId=${item.temp_msg_id || 'NONE'} bot=${item.bot || item.bot_id || 'NONE'}`);
                    }
                });
                const eventSummary = Object.entries(eventCounts).map(([e, c]) => `${e}:${c}`).join(' ');
                if (messageSentItems.length > 0) {
                    console.log(`[WORKER-BATCH] processing=${itemsToProcess.length} events=[${eventSummary}] MESSAGE_SENT_items=${messageSentItems.length} details=${JSON.stringify(messageSentItems).substring(0, 200)}`);
                } else {
                    console.log(`[WORKER-BATCH] processing=${itemsToProcess.length} events=[${eventSummary}]`);
                }
                
                // ✅ PARALLEL PROCESSING: Process semua items secara parallel (bukan sequential)
                const processItem = async (item) => {
                    const raw = item._raw;
                    const fromPriority = item._fromPriority;
                    delete item._raw;
                    delete item._fromPriority;
                    
                        // Update item di processing queue dengan processingStartedAt
                        if (fromPriority) {
                            await this.redis.lSet(this.processingKey, -1, JSON.stringify(item)).catch(() => {});
                        } else {
                            await this.redis.lSet(this.processingKey, -1, JSON.stringify(item)).catch(() => {});
                    }

                    const now = Date.now();
                    const itemAge = now - (item.createdAt || item.timestamp || now);
                    const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000; // 7 hari
                    const ONE_HOUR_MS = 60 * 60 * 1000; // 1 jam
                    const maxAge = item.maxAge || (item.priority ? SEVEN_DAYS_MS : ONE_HOUR_MS);
                    
                    const event = (item.event || item.status || '').toString().toUpperCase();
                    const isLifecycleEvent = ['DEVICE_SENT', 'DELIVERED', 'READ', 'PLAYED', 'SERVER_ACK', 'MESSAGE_SENT'].includes(event);
                    const msgId = item.msg_id || item.msgId || item.messageId || 'unknown';
                    
                    // ✅ GREP: [WORKER-ITEM-START] - Track setiap item yang di-process
                    const tempMsgId = item.temp_msg_id || 'NONE';
                    console.log(`[WORKER-ITEM-START] event=${event} msgId=${msgId} tempMsgId=${tempMsgId}`);
                    
                    // ✅ FIX: Cek permanent error SEBELUM process
                    if (item.lastError) {
                        const lastError = item.lastError.toString().toLowerCase();
                        const isPermanentError = lastError.includes('missing bot_id') || lastError.includes('missing bot') || lastError.includes('invalid bot');
                        if (isPermanentError) {
                            await this.redis.lRem(this.processingKey, 1, raw);
                            const ageDays = Math.round(itemAge / (24 * 60 * 60 * 1000));
                            console.error(`🗑️ [PERMANENT-ERROR] ${event} for msgId ${msgId} dengan error permanent (${lastError}), langsung hapus dari queue (tidak akan pernah terkirim, umur: ${ageDays} hari)`);
                            return; // Skip this item
                        }
                    }
                    
                    // ✅ AGGRESSIVE CLEANUP: Hapus lifecycle events yang sudah >7 hari dengan permanent error
                    if (itemAge > maxAge && !isLifecycleEvent) {
                        await this.redis.lRem(this.processingKey, 1, raw);
                        const ageHours = Math.round(itemAge / 3600000);
                        console.warn(`⏰ [AGED] ${event} for msgId ${msgId} too old (${ageHours}h), dropping from queue`);
                        return; // Skip this item
                    } else if (itemAge > maxAge && isLifecycleEvent) {
                        const ageDays = Math.round(itemAge / (24 * 60 * 60 * 1000));
                        const lastError = (item.lastError || '').toString().toLowerCase();
                        const isPermanentError = lastError.includes('missing bot_id') || lastError.includes('missing bot') || lastError.includes('invalid bot');
                        
                        if (isPermanentError || ageDays > 10) {
                            await this.redis.lRem(this.processingKey, 1, raw);
                            console.error(`🗑️ [AGGRESSIVE-CLEANUP] ${event} for msgId ${msgId} sudah ${ageDays} hari ${isPermanentError ? `dengan error permanent (${lastError})` : '(>10 hari)'}, dropping from queue (tidak akan pernah terkirim)`);
                            return; // Skip this item
                        }
                    }
                
                    // backoff check
                    if (item.nextAttemptAt && now < item.nextAttemptAt) {
                        await this.redis.rPush(this.outboxKey, JSON.stringify(item));
                        await this.redis.lRem(this.processingKey, 1, raw);
                        return; // Skip this item
                    }

                    // ✅ FIX: Pastikan item lama punya bot_id sebelum dikirim
                    if (!item.bot && !item.bot_id && !item.botId) {
                        const cleanBotNumber = this.getCleanBotNumber();
                        item.bot = cleanBotNumber;
                        item.bot_id = this.cachedBotId || 0;
                    }

                    // ✅ CRITICAL: Validasi bot hantu di worker (extra safety untuk item lama yang belum divalidasi)
                    // ✅ SKIP validasi jika sudah divalidasi sebelumnya atau dari WhatsApp original
                    // ✅ CRITICAL FIX: MESSAGE_SENT tidak perlu bot hantu check karena sudah dari queueMessageSentWebhook (valid)
                    const shouldValidateInWorker = this.botHantuBlockEnabled() &&
                        msgId && msgId !== 'unknown' &&
                        event !== 'MESSAGE_SENT' && // ✅ FIX: Skip bot hantu check untuk MESSAGE_SENT
                        !item.validated_bot_hantu &&
                        !item.original_from_whatsapp &&
                        !item.backfill_generated &&
                        !item.force_send &&
                        isLifecycleEvent;
                    
                    if (shouldValidateInWorker) {
                        // ✅ STEP 1: Check apakah pesan ada di sentMessageIds
                        const hasMessage = await this.hasSentMessageId(msgId);
                        if (!hasMessage) {
                            console.error(`🚨 [BOT-HANTU-WORKER] msgId ${msgId} not in sentMessageIds/Redis - BOT HANTU DETECTED, blocking ${event} webhook from queue`);
                            const remoteJid = await (this.redis?.get(`sent_message_jid:${msgId}`).catch(() => null) || Promise.resolve(null));
                            await this.detectAnomaly(msgId, remoteJid);
                            await this.redis.lRem(this.processingKey, 1, raw);
                            return; // Skip this item
                        }
                        
                        // ✅ STEP 2: Check anomaly detection (fast check)
                        const remoteJid = await (this.redis?.get(`sent_message_jid:${msgId}`).catch(() => null) || Promise.resolve(null));
                        const anomalyCheck = await this.detectAnomaly(msgId, remoteJid);
                        if (anomalyCheck.isBotHantu) {
                            console.error(`🚨 [BOT-HANTU-WORKER] Anomaly detected for msgId ${msgId} - BOT HANTU CONFIRMED, blocking ${event} webhook from queue`);
                            await this.redis.lRem(this.processingKey, 1, raw);
                            return; // Skip this item
                        }
                    } else if (event === 'MESSAGE_SENT') {
                        // ✅ CRITICAL: Log untuk MESSAGE_SENT yang skip bot hantu check
                        console.log(`[WORKER-MESSAGE-SENT] Skipping bot hantu check for MESSAGE_SENT msgId=${msgId} tempMsgId=${tempMsgId} (valid from queueMessageSentWebhook)`);
                    }

                    // ✅ CRITICAL: Check confirmation marker SEBELUM send webhook (prevent duplicate/spam)
                    // ✅ CRITICAL FIX: MESSAGE_SENT tidak perlu check already_confirmed karena selalu baru dari queueMessageSentWebhook
                    if (isLifecycleEvent && msgId && msgId !== 'unknown' && event !== 'MESSAGE_SENT') {
                        let alreadyConfirmed = false;
                        if (event === 'SERVER_ACK') {
                            alreadyConfirmed = await this.hasOrganicServerAckMarker(msgId);
                        } else {
                            alreadyConfirmed = !!(await this.redis.get(`webhook:confirmed:${msgId}:${event}`).catch(() => null));
                        }
                        if (alreadyConfirmed) {
                            // ✅ Already confirmed, remove from processing dan skip send
                            await this.redis.lRem(this.processingKey, 1, raw);
                            const tempMsgId = item.temp_msg_id || 'NONE';
                            // ✅ GREP: [WORKER-SKIP] - Simple format
                            console.log(`[WORKER-SKIP] event=${event} msgId=${msgId} tempMsgId=${tempMsgId} reason=already_confirmed`);
                            return; // Skip this item
                        }
                    } else if (event === 'MESSAGE_SENT') {
                        // ✅ CRITICAL: Log untuk MESSAGE_SENT yang skip already_confirmed check
                        console.log(`[WORKER-MESSAGE-SENT] Skipping already_confirmed check for MESSAGE_SENT msgId=${msgId} tempMsgId=${tempMsgId} (always new from queueMessageSentWebhook)`);
                    }
                    
                    // ✅ GREP: [WORKER-START] - Simple format
                    console.log(`[WORKER-START] event=${event} msgId=${msgId} tempMsgId=${tempMsgId}`);
                    
                    // ✅ CRITICAL: Log sebelum send webhook untuk track MESSAGE_SENT
                    if (event === 'MESSAGE_SENT') {
                        console.log(`[WORKER-SEND-START] event=${event} msgId=${msgId} tempMsgId=${tempMsgId} bot=${item.bot || item.bot_id || 'NONE'}`);
                    }
                    
                    // ✅ PARALLEL WEBHOOK SENDING: Send webhook tanpa blocking
                    const result = await Promise.race([
                        this.sendToWebhook(item).catch(err => {
                            // ✅ GREP: [WORKER-ERROR] - Simple format
                            console.error(`[WORKER-ERROR] event=${event} msgId=${msgId} tempMsgId=${tempMsgId} error=${err.message}`);
                            return { error: '1', msg: err.message };
                        }),
                        new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout')), 2000))
                    ]).catch((timeoutErr) => {
                        // ✅ GREP: [WORKER-TIMEOUT] - Track timeout
                        console.error(`[WORKER-TIMEOUT] event=${event} msgId=${msgId} tempMsgId=${tempMsgId} error=Timeout after 2s`);
                        return null;
                    });
                    
                    const httpOk = result && typeof result === 'object' && (result.error === '0' || result.error === 0);
                    const credited = httpOk && this.isWebhookResponseAckCredited(result, item);
                    
                    // ✅ GREP: [WORKER-SUCCESS] atau [WORKER-FAILED] - Simple format
                    if (credited) {
                        const responseDetail = result ? JSON.stringify(result).substring(0, 100) : 'null';
                        console.log(`[WORKER-SUCCESS] event=${event} msgId=${msgId} tempMsgId=${tempMsgId} response=${responseDetail}`);
                    } else {
                        const errorDetail = result ? JSON.stringify(result).substring(0, 200) : 'null result';
                        console.log(`[WORKER-FAILED] event=${event} msgId=${msgId} tempMsgId=${tempMsgId} response=${errorDetail}`);
                    }
                    
                    // ✅ FIX: Cek permanent error dari response
                    if (!httpOk && result) {
                        const errorMsg = (result?.msg || result?.error || '').toString().toLowerCase();
                        const isPermanentError = errorMsg.includes('missing bot_id') || errorMsg.includes('missing bot') || errorMsg.includes('invalid bot');
                        if (isPermanentError) {
                            await this.redis.lRem(this.processingKey, 1, raw);
                            const ageDays = Math.round(itemAge / (24 * 60 * 60 * 1000));
                            console.error(`🗑️ [PERMANENT-ERROR] ${event} for msgId ${msgId} dengan error permanent dari response (${errorMsg}), langsung hapus dari queue (tidak akan pernah terkirim, umur: ${ageDays} hari)`);
                            return; // Skip this item
                        }
                    }
                    
                    if (httpOk) {
                        // ✅ SUCCESS: Remove from processing; confirm hanya jika credited
                        await this.redis.lRem(this.processingKey, 1, raw);
                        
                        if (isLifecycleEvent && msgId && credited) {
                            await this.maybeMarkWebhookConfirmed(msgId, event, item, result);
                            const tempMsgId = item.temp_msg_id || 'NONE';
                            // ✅ GREP: [WORKER-CONFIRMED] - Simple format
                            console.log(`[WORKER-CONFIRMED] event=${event} msgId=${msgId} tempMsgId=${tempMsgId}`);
                        } else if (isLifecycleEvent && msgId && !credited) {
                            const tempMsgId = item.temp_msg_id || 'NONE';
                            console.log(`[WORKER-SKIP-CREDIT] event=${event} msgId=${msgId} tempMsgId=${tempMsgId} (webhook no credit)`);
                        }
                        return; // Success (termasuk synthetic skip — jangan retry selamanya)
                    } else {
                        // ✅ PERSISTENT RETRY: Increment attempts, set exponential backoff, requeue
                        // ✅ CRITICAL: Untuk lifecycle events (billing + SERVER_ACK), TIDAK pernah drop dari queue (retry sampai server up)
                        const attempts = (item.attempts || 0) + 1;
                        const isBillingEvent = ['DEVICE_SENT', 'DELIVERED', 'READ'].includes(event);
                        const isLifecycleEvent = ['DEVICE_SENT', 'DELIVERED', 'READ', 'PLAYED', 'SERVER_ACK', 'MESSAGE_SENT'].includes(event);
                        const maxAttempts = item.priority ? 5 : 3; // Lifecycle events: 5 retries, others: 3
                        
                        // ✅ FIX: Cek apakah error permanent (Missing bot_id) - tidak akan pernah terkirim
                        const errorMsg = (result?.msg || result?.error || item.lastError || '').toString().toLowerCase();
                        const isPermanentError = errorMsg.includes('missing bot_id') || errorMsg.includes('missing bot') || errorMsg.includes('invalid bot');
                        
                        // ✅ FIX: Jika error permanent, hapus dari queue (tidak akan pernah terkirim)
                        if (isPermanentError) {
                            await this.redis.lRem(this.processingKey, 1, raw);
                            console.error(`🗑️ [PERMANENT-ERROR] ${event} for msgId ${msgId} dengan error permanent (${errorMsg}), dropping from queue (tidak akan pernah terkirim)`);
                            return; // Skip this item
                        }
                    
                        // ✅ PERSISTENT: Lifecycle events (billing + SERVER_ACK) tidak pernah di-drop, terus retry sampai server up
                        // SERVER_ACK juga harus persistent karena penting untuk tracking
                        if (attempts >= maxAttempts && !isLifecycleEvent) {
                            // Max attempts reached, remove from queue (hanya untuk non-lifecycle events)
                            await this.redis.lRem(this.processingKey, 1, raw);
                            console.error(`❌ [WORKER] Max attempts (${maxAttempts}) reached for ${event} msgId: ${msgId}, dropping from queue`);
                            return; // Skip this item
                        } else {
                        // ✅ EXPONENTIAL BACKOFF: Semakin lama server down, semakin lama interval retry
                        // ✅ OPTIMIZED: Untuk server down lama, retry interval lebih panjang tapi tetap reasonable
                        // ✅ CRITICAL: Lifecycle events (termasuk SERVER_ACK) juga pakai persistent retry
                        let backoffMs;
                        const maxBackoffForLifecycle = 300000; // Max 5 menit untuk lifecycle events (jangan terlalu lama)
                        
                        if (isLifecycleEvent && attempts > 20) {
                            // After 20 attempts (>20 menit): retry every 5 minutes (server down lama, save resources)
                            backoffMs = maxBackoffForLifecycle;
                            console.warn(`🔄 [PERSISTENT] ${event} for msgId ${msgId} server down lama, retrying every 5min (attempt ${attempts})`);
                        } else if (isLifecycleEvent && attempts > 10) {
                            // After 10 attempts: retry every 60 seconds (server probably down)
                            backoffMs = 60000;
                            console.warn(`🔄 [PERSISTENT] ${event} for msgId ${msgId} server seems down, retrying every 60s (attempt ${attempts})`);
                        } else if (isLifecycleEvent && attempts > 5) {
                            // After 5 attempts: retry every 10 seconds
                            backoffMs = 10000;
                        } else if (item.priority || isLifecycleEvent) {
                            // Priority/Lifecycle: 100ms, 200ms, 300ms, 400ms, 500ms, then 1s, 2s, 5s, 10s
                            backoffMs = attempts <= 5 
                                ? Math.min(100 * attempts, 500)
                                : Math.min(1000 * Math.pow(2, attempts - 6), 10000);
                        } else {
                            // Normal: 300ms, 700ms, then 2s, 5s
                            backoffMs = attempts === 1 ? 300 : (attempts === 2 ? 700 : Math.min(2000 * Math.pow(2, attempts - 3), 10000));
                        }
                        
                        item.attempts = attempts;
                        item.nextAttemptAt = Date.now() + backoffMs;
                        // ✅ FIX: Simpan error message yang lebih detail untuk permanent error detection
                        // ✅ CRITICAL: Simpan result?.msg (bukan result?.error) karena "Missing bot_id" ada di msg
                        item.lastError = result?.msg || result?.error || item.lastError || 'Server unreachable';
                        item.lastAttemptAt = Date.now();
                        // ✅ TRACKING: Simpan total waktu yang sudah di-retry
                        item.totalRetryTime = (item.totalRetryTime || 0) + backoffMs;
                        item.firstFailureAt = item.firstFailureAt || Date.now();
                        const updated = JSON.stringify(item);
                        
                        // Requeue based on priority
                        if (item.priority) {
                            await this.redis.lPush(this.outboxKey, updated); // High priority to front
                        } else {
                            await this.redis.rPush(this.outboxKey, updated); // Normal to back
                        }
                        await this.redis.lRem(this.processingKey, 1, raw);
                        
                        const retryTimeMinutes = Math.round((Date.now() - (item.firstFailureAt || Date.now())) / 60000);
                        const itemAgeDays = Math.round(itemAge / (24 * 60 * 60 * 1000)); // Umur item dalam hari
                        const maxRetryDays = 7;
                        const remainingDays = Math.max(0, maxRetryDays - itemAgeDays); // Sisa hari retry (min 0)
                        
                        if (isLifecycleEvent && attempts > 20) {
                            console.log(`💾 [PERSISTENT-7DAYS] ${event} stored in Redis, retrying every 5min (msgId: ${msgId}, attempt: ${attempts}, retry time: ${retryTimeMinutes}min, remaining: ${remainingDays} days)`);
                        } else if (isLifecycleEvent && attempts > 10) {
                            console.log(`💾 [PERSISTENT-7DAYS] ${event} stored in Redis, will retry every 60s until server up (msgId: ${msgId}, attempt: ${attempts}, retry time: ${retryTimeMinutes}min, remaining: ${remainingDays} days)`);
                        } else if (isLifecycleEvent) {
                            console.log(`🔄 [WORKER-7DAYS] ${event} requeued (attempt ${attempts} - persistent for 7 days) msgId: ${msgId}, next retry in ${Math.round(backoffMs/1000)}s, retry time: ${retryTimeMinutes}min, remaining: ${remainingDays} days`);
                            } else {
                                console.log(`🔄 [WORKER] ${event} requeued (attempt ${attempts}/${maxAttempts}) msgId: ${msgId}, next retry in ${Math.round(backoffMs/1000)}s, total retry time: ${retryTimeMinutes}min`);
                            }
                        }
                        return; // Done processing this item
                    }
                };
                
                // ✅ PARALLEL PROCESSING: Process semua items secara parallel (bukan sequential)
                // ✅ CONCURRENT LIMIT: Max 10 concurrent webhook sends untuk prevent overload
                const concurrentLimit = 10;
                const chunks = [];
                for (let i = 0; i < itemsToProcess.length; i += concurrentLimit) {
                    chunks.push(itemsToProcess.slice(i, i + concurrentLimit));
                }
                
                // Process chunks secara parallel, tapi items dalam chunk juga parallel
                for (const chunk of chunks) {
                    await Promise.all(chunk.map(item => processItem(item).catch(err => {
                        console.error(`❌ [WORKER] Error processing item:`, err.message);
                    })));
                }
            } catch (e) {
                // worker error → log and continue
                // ✅ GREP: [WORKER-ERROR] - Track worker error
                console.error(`[WORKER-ERROR] Error processing webhook batch: ${e.message} session=${this.sessionId}`);
            }
        }, WEBHOOK_OUTBOX_WORKER_MS);

        // Satu timer pemeliharaan queue per proses (bukan per sesmen) — lihat runSharedWebhookQueueMaintenance
        ensureSharedWebhookQueueMaintTimer();
    }

    normalizeConnectedBotNumber(botJid) {
        let botNumber = botJid;
        if (botNumber && botNumber.includes('@')) {
            botNumber = botNumber.split('@')[0];
        }
        if (botNumber && botNumber.includes(':')) {
            botNumber = botNumber.split(':')[0];
        }
        if (!botNumber || !/^\d{10,15}$/.test(botNumber)) {
            botNumber = this.sessionId;
        }
        return botNumber;
    }

    async cacheConnectedBotIdFromResponse(webhookResponse) {
        if (!webhookResponse) return;
        let resolvedBotId = null;
        if (typeof webhookResponse === 'object') {
            resolvedBotId = webhookResponse.bot_id || webhookResponse.botId;
        } else if (typeof webhookResponse === 'string') {
            try {
                const parsed = JSON.parse(webhookResponse);
                resolvedBotId = parsed.bot_id || parsed.botId;
            } catch (e) {
                // ignore
            }
        }
        if (resolvedBotId && parseInt(resolvedBotId, 10) > 0) {
            const botIdInt = parseInt(resolvedBotId, 10);
            if (this.redis) {
                await this.redis.set(`bot_id:${this.sessionId}`, String(botIdInt), { EX: 7 * 24 * 60 * 60 }).catch(() => {});
            }
            this.cachedBotId = botIdInt;
        }
    }

    async markConnectedWebhookSent() {
        this.connectedWebhookSent = true;
        this.processingConnectedWebhook = false;
        this.processingConnectedWebhookStartTime = null;
        if (this.redis) {
            // TTL pendek supaya setelah crash/restart tidak "mengunci" CONNECTED terlalu lama.
            // DISCONNECTED handler akan clear key ini untuk siklus berikutnya.
            const ttl = Math.max(30, Math.min(3600, Number(process.env.CONNECTED_DEDUPE_TTL_SEC) || 180));
            await this.redis.set(`lifecycle:connected:${this.sessionId}`, String(Date.now()), { EX: ttl }).catch(() => {});
        }
    }

    async clearConnectedWebhookDedupe() {
        this.connectedWebhookSent = false;
        this.processingConnectedWebhook = false;
        this.processingConnectedWebhookStartTime = null;
        if (this.redis) {
            await this.redis.del(`lifecycle:connected:${this.sessionId}`).catch(() => {});
        }
    }

    /**
     * MESSAGE_DELETED ketat: hanya revoke pesan yang benar-benar dikirim API session ini,
     * skip history-sync / delete longgar (false-positive blacklist).
     */
    shouldReportMessageDeleted(messageId) {
        if (!messageId || messageId === 'unknown') {
            console.log(`⏭️ [SKIP MESSAGE_DELETED] missing messageId session=${this.sessionId}`);
            return false;
        }
        const idKey = String(messageId).toUpperCase();
        const known =
            (this._sentMsgRecipient &&
                (this._sentMsgRecipient.has(messageId) || this._sentMsgRecipient.has(idKey))) ||
            (this.sentMessageIds &&
                (this.sentMessageIds.has(messageId) || this.sentMessageIds.has(idKey)));
        if (!known) {
            console.log(
                `⏭️ [SKIP MESSAGE_DELETED] msgId not from API send session=${this.sessionId} msgId=${messageId}`
            );
            return false;
        }
        const connectedAt = this.connectedAt || 0;
        const graceMs = Math.max(0, Number(process.env.MESSAGE_DELETED_GRACE_MS) || 90000);
        if (!connectedAt || Date.now() - connectedAt < graceMs) {
            console.log(
                `⏭️ [SKIP MESSAGE_DELETED] history-sync grace session=${this.sessionId} ageMs=${
                    connectedAt ? Date.now() - connectedAt : -1
                }`
            );
            return false;
        }
        if (!this._messageDeletedDedupe) this._messageDeletedDedupe = new Map();
        const now = Date.now();
        const prev = this._messageDeletedDedupe.get(idKey) || 0;
        if (prev > now) {
            console.log(
                `⏭️ [SKIP MESSAGE_DELETED] dedupe session=${this.sessionId} msgId=${messageId}`
            );
            return false;
        }
        this._messageDeletedDedupe.set(idKey, now + 120000);
        if (this._messageDeletedDedupe.size > 500) {
            for (const [k, exp] of this._messageDeletedDedupe) {
                if (exp <= now) this._messageDeletedDedupe.delete(k);
            }
        }
        return true;
    }

    markPairingPasteAcknowledged(source = 'unknown') {
        if (!this.pairingPasteAcknowledged) {
            this.pairingPasteAcknowledged = true;
            console.log(
                `✅ [PAIRING-PASTE] acknowledged via=${source} session=${this.sessionId} leftoverBlocked=${this.pairingLeftoverCredsBlocked}`
            );
            this.enqueueWebhook({
                event: 'PAIRING_PASTE_ACK',
                sessionId: this.sessionId,
                bot: this.sessionId,
                user_id: this.userId,
                port: this.port,
                source,
                timestamp: Date.now(),
            }).catch(() => {});
        }
        // Scan/tempel nyata — kirim CONNECTED yang tertahan gate, bukan CONNECTED palsu.
        this.scheduleConnectedRetry('paste_ack');
    }

    scheduleConnectedRetry(reason = 'retry') {
        if (this.connectedWebhookSent || this._connectedRetryTimer) return;
        this._connectedRetryTimer = setTimeout(() => {
            this._connectedRetryTimer = null;
            if (this.connectedWebhookSent) return;
            const jid = this.resolveAuthenticatedJid();
            if (!jid || !isBaileysWebSocketOpen(this.sock)) return;
            const botNumber = this.normalizeConnectedBotNumber(jid);
            this.sendConnectedOnce(`retry.${reason}`, botNumber, { forceDirect: true }).catch((err) => {
                console.warn(`⚠️ [CONNECTED-RETRY] ${this.sessionId} ${err?.message || err}`);
            });
        }, 2500);
    }

    /**
     * CONNECTED hanya dari creds.update / connection.open setelah WA identity nyata.
     */
    startPairingStickWatchdog(_delayMs = 10000) {
        if (this._pairingStickWatchdog) {
            clearTimeout(this._pairingStickWatchdog);
            this._pairingStickWatchdog = null;
        }
        console.log(
            `⏭️ [PAIRING-STICK] disabled (no force-CONNECTED) session=${this.sessionId}`
        );
    }

    async runPairingStickWatchdog() {
        this._pairingStickWatchdog = null;
        console.log(`⏭️ [PAIRING-STICK] force path disabled session=${this.sessionId}`);
        return;
    }

    /**
     * Pairing belum benar-benar taut (identity WA tidak ada) — termasuk CONNECTED palsu.
     * Setelah 2 menit: kill, jangan nempel RAM/DB.
     */
    shouldKillIncompletePairing() {
        if (!this.isPairingMode) return false;
        // Sudah CONNECTED sukses — jangan timeout-kill (cegah race socket blip → AUTH_TIMEOUT).
        if (this.connectedWebhookSent || this.pairingAuthSucceededAt) return false;
        return !this.assertPairingConnectedAllowed('pairing.timeout').ok;
    }

    /**
     * Gate CONNECTED pairing: jangan publish sebelum kode pairing ada + socket open + identity WA nyata.
     */
    assertPairingConnectedAllowed(detectedVia = '') {
        if (!this.isPairingMode) return { ok: true };
        // Sesi QR (tanpa nomor yang diminta): setelah scan, isPairingMode ikut true hanya karena jendela
        // grace 401 (inPostPairGrace). Gerbang kode pairing/tempel tidak berlaku — tanpa pengecualian ini
        // CONNECTED ditolak PAIRING_CODE_NOT_READY dan 85 % scan QR yang sukses tidak pernah sampai ke BE.
        if (!this.requestedPhoneNumber) return { ok: true };
        if (String(detectedVia) === 'pairing.stick_watchdog') {
            return { ok: false, reason: 'PAIRING_STICK_DISABLED' };
        }
        if (!this.pairingCode || !this.pairingCodeGeneratedTime) {
            return { ok: false, reason: 'PAIRING_CODE_NOT_READY' };
        }
        const id = this.resolveAuthenticatedJid();
        if (!id) {
            return { ok: false, reason: 'PAIRING_NO_WA_IDENTITY' };
        }
        if (!isBaileysWebSocketOpen(this.sock)) {
            return { ok: false, reason: 'PAIRING_SOCKET_NOT_OPEN' };
        }
        const wantInfo = this.normalizePhoneNumber(this.requestedPhoneNumber);
        const gotNum = this.getCleanBotNumber();
        if (wantInfo.isValid && wantInfo.normalized && gotNum) {
            const wantDigits = String(wantInfo.normalized).replace(/\D/g, '');
            if (wantDigits && gotNum !== wantDigits) {
                return { ok: false, reason: 'PAIRING_NUMBER_MISMATCH' };
            }
        }
        if (this.pairingLeftoverCredsBlocked && !this.pairingPasteAcknowledged) {
            return { ok: false, reason: 'PAIRING_NO_PASTE_ACK' };
        }
        // Floor hanya selama belum scan/tempel sungguhan. Stub requestPairingCode ≠ pair sukses.
        const realPair = this.pairingPasteAcknowledged || credsLookAuthenticated(this.authState?.creds);
        const minMs = Math.max(12000, Number(process.env.PAIRING_CONNECTED_MIN_AGE_MS) || 12000);
        if (!realPair && Date.now() < this.pairingCodeGeneratedTime + minMs) {
            return { ok: false, reason: 'PAIRING_CONNECTED_TOO_EARLY' };
        }
        return { ok: true };
    }

    getPostPairGraceMs() {
        const n = Number(process.env.PAIRING_401_RESUME_MS);
        if (Number.isFinite(n) && n > 0) {
            return Math.max(120000, Math.min(Math.floor(n), 300000));
        }
        // Web modal 2 menit = kesempatan taut. Blip 401/515 forum sering pas di ~120s.
        // Grace 180s setelah AUTH supaya 401 menit ke-2 di-remap, bukan LOGGED_OUT.
        return 180000;
    }

    inPostPairGrace() {
        if (!this.pairingAuthSucceededAt) return false;
        // 401 beruntun menembus cap = bukan blip. Tutup grace supaya remap, penahanan webhook
        // DISCONNECTED, dan pembersih creds semuanya berhenti lewat satu pintu yang sama.
        if (this._pairing401Exhausted) return false;
        return (Date.now() - this.pairingAuthSucceededAt) < this.getPostPairGraceMs();
    }

    /**
     * JID bot yang benar-benar terautentikasi, atau null.
     *
     * sock.user.id hanya terisi setelah login sukses, jadi itu tepercaya. Sebaliknya
     * authState.creds.me.id ikut terisi oleh requestPairingCode sebelum user menempel kode,
     * jadi cadangan itu hanya boleh dipakai kalau creds-nya punya penanda pair-success.
     */
    resolveAuthenticatedJid() {
        const fromSock = this.sock?.user?.id;
        if (fromSock && typeof fromSock === 'string' && fromSock.includes('@') && !fromSock.includes('sess_')) {
            return fromSock;
        }
        const creds = this.authState?.creds;
        if (credsLookAuthenticated(creds)) return creds.me.id;
        return null;
    }

    /** Mulai jendela remap 401→resume sedini auth nyata (QR dan pairing code). */
    markPairingAuthSucceeded(source = 'unknown') {
        const first = !this.pairingAuthSucceededAt;
        this.pairingAuthSucceededAt = this.pairingAuthSucceededAt || Date.now();
        if (first) {
            this._pairing401Remaps = 0;
            this._pairing401Exhausted = false;
            console.log(
                `📱 [PAIR-401] auth window start via=${source} session=${this.sessionId} graceMs=${this.getPostPairGraceMs()} qr=${!this.pairingCode && !this.isPairingMode}`
            );
            this.schedulePairingModeClear();
        }
        return first;
    }

    schedulePairingModeClear() {
        const grace = this.getPostPairGraceMs();
        if (this._pairingModeClearTimer) {
            clearTimeout(this._pairingModeClearTimer);
        }
        this._pairingModeClearTimer = setTimeout(() => {
            this._pairingModeClearTimer = null;
            if (!this.isPairingMode) return;
            this.isPairingMode = false;
            this.requestedPhoneNumber = null;
            console.log(
                `📱 [PAIR-401] pairing mode cleared after ${Math.round(grace / 1000)}s grace session=${this.sessionId}`
            );
        }, grace);
    }

    /**
     * Satu pintu kirim CONNECTED — dedupe memory + Redis, normalize bot number.
     */
    async sendConnectedOnce(detectedVia, botNumber, options = {}) {
        if (this.connectedWebhookSent) {
            return { skipped: true, reason: 'CONNECTED_ALREADY_SENT' };
        }
        if (this.redis) {
            const redisSent = await this.redis.get(`lifecycle:connected:${this.sessionId}`).catch(() => null);
            if (redisSent) {
                this.connectedWebhookSent = true;
                return { skipped: true, reason: 'CONNECTED_ALREADY_SENT_REDIS' };
            }
        }
        if (this.processingConnectedWebhook) {
            return { skipped: true, reason: 'CONNECTED_PROCESSING' };
        }

        const pairingGate = this.assertPairingConnectedAllowed(detectedVia);
        if (!pairingGate.ok) {
            console.warn(
                `⏭️ [CONNECTED-GATE] skip session=${this.sessionId} via=${detectedVia} reason=${pairingGate.reason}`
            );
            if (pairingGate.reason === 'PAIRING_CONNECTED_TOO_EARLY' && this.pairingPasteAcknowledged) {
                this.scheduleConnectedRetry('gate_too_early');
            }
            return { skipped: true, reason: pairingGate.reason };
        }

        try {
            const webhookResult = await this.enqueueWebhook(
                this.buildCredsUpdateConnectedPayload(botNumber, detectedVia),
                options
            );
            if (webhookResult?.skipped) {
                return { skipped: true, reason: webhookResult.reason, raw: webhookResult };
            }
            await this.markConnectedWebhookSent();
            this.markPairingAuthSucceeded('connected_webhook');
            if (this.pairingTimeout) {
                clearTimeout(this.pairingTimeout);
                this.pairingTimeout = null;
            }
            return {
                skipped: false,
                raw: webhookResult,
                response: webhookResult?.response || webhookResult
            };
        } catch (err) {
            this.connectedWebhookSent = false;
            this.processingConnectedWebhook = false;
            this.processingConnectedWebhookStartTime = null;
            throw err;
        }
    }

    /**
     * Payload CONNECTED (alur creds.update) + _debug — satu sumber untuk primary/retry/final (tanpa tiga kali copy obyek 40+ baris).
     */
    buildCredsUpdateConnectedPayload(botNumber, detectedVia) {
        return {
            event: 'CONNECTED',
            sessionId: this.sessionId,
            bot: botNumber,
            bot_id: 0,
            user_id: this.userId,
            port: this.port,
            proxyId: this.proxyInfo?.proxyId || null,
            proxyAddress: this.proxyInfo?.address ? this.proxyInfo.address.replace(/:[^:@]*@/, ':***@') : null,
            _debug: {
                timestamp: new Date().toISOString(),
                connectionState: {
                    isAuthenticated: this.isAuthenticated,
                    reconnectInFlight: this.reconnectInFlight,
                    connectionStreak: this.connectionStreak,
                    lastStableConnection: this.lastStableConnection
                        ? new Date(this.lastStableConnection).toISOString()
                        : null,
                    reconnectAttempts: this.reconnectAttempts
                },
                flags: {
                    reconnectingSent: this.reconnectingSent,
                    disconnectCycleId: this.currentDisconnectCycle
                },
                lastLifecycleEvent: this.lastLifecycleEvent,
                lastLifecycleEventTime: this.lastLifecycleEventTime
                    ? new Date(this.lastLifecycleEventTime).toISOString()
                    : null,
                detected_via: detectedVia
            }
        };
    }

    /**
     * Restart koneksi setelah CONNECTED webhook (pairing) — dipanggil dari beberapa setTimeout; satu implementasi = lebih mudah di-audit.
     */
    scheduleCredsUpdatePairingRestart(delayMs = 2000) {
        // Default OFF (0): post-CONNECTED socket recycle sering amplify 401 setelah taut.
        // Set PAIRING_POST_CONNECTED_RESTART=1 hanya bila perlu alur 515 recreate creds.
        const allow = String(process.env.PAIRING_POST_CONNECTED_RESTART ?? '0').toLowerCase().trim();
        if (allow === '0' || allow === 'false' || allow === 'off' || allow === 'no') {
            console.log(
                `⏭️ [CREDS.UPDATE] Pairing post-CONNECTED socket restart skipped (PAIRING_POST_CONNECTED_RESTART=${allow})`
            );
            return;
        }
        const delay = Math.max(0, Number(delayMs) || 2000);
        setTimeout(async () => {
            try {
                this.isRestartingForPairing = true;
                console.log(`🔄 [CREDS.UPDATE] Restarting connection for pairing mode (resume creds, no re-pair)...`);
                if (this.sock) {
                    await this.sock.end();
                }
                // null = keluar pairing mode, pakai creds.me yang sudah tersimpan (alur 515 resmi)
                if (typeof this.saveCreds?.flush === 'function') {
                    await this.saveCreds.flush();
                }
                await this.initialize(null);
                console.log(`✅ [CREDS.UPDATE] Connection restarted successfully for pairing mode`);
                this.isRestartingForPairing = false;
            } catch (restartError) {
                console.error(
                    `❌ [CREDS.UPDATE] Error restarting connection for pairing mode:`,
                    restartError.message
                );
                this.isRestartingForPairing = false;
            }
        }, delay);
    }
    
    async initialize(phoneNumber = null) {
        try {
            if (typeof this.saveCreds?.flush === 'function') {
                await this.saveCreds.flush();
            }
            // ✅ CRITICAL: Load byee-wamod library FIRST sebelum menggunakan useMultiFileAuthState
            await loadBaileys();
            await this.clearSessionAckLiveRedis();
            this.ackCount = 0;
            if (this.redis && this.ackCountRedisKey) {
                this.redis.del(this.ackCountRedisKey).catch(() => {});
            }
            
            // ✅ Set pairing mode SEBELUM bot hantu check (untuk allow pairing meskipun bot hantu)
            if (phoneNumber !== undefined && phoneNumber !== null) {
                this.requestedPhoneNumber = typeof phoneNumber === 'string'
                    ? phoneNumber.trim()
                    : String(phoneNumber).trim();
                if (this.requestedPhoneNumber === '') {
                    this.requestedPhoneNumber = null;
                }
            } else if (!this.inPostPairGrace()) {
                this.requestedPhoneNumber = null;
            }
            this.isPairingMode = Boolean(this.requestedPhoneNumber) || this.inPostPairGrace();
            if (this.isPairingMode && this.requestedPhoneNumber && !this.isAuthenticated && !this.pairingAuthSucceededAt) {
                if (!this.pairingStartedAt) this.pairingStartedAt = Date.now();
            } else if (!this.isPairingMode) {
                this.pairingStartedAt = null;
            }

            if (this.isPairingMode) {
                const keepHoldMs = Math.max(60000, Math.min(180000, Number(process.env.PAIRING_CODE_HOLD_MS) || 180000));
                const keepAge = this.pairingCodeGeneratedTime ? (Date.now() - this.pairingCodeGeneratedTime) : Number.POSITIVE_INFINITY;
                const keepSocketOpen = isBaileysWebSocketOpen(this.sock);
                if (this.pairingCode && (keepAge < keepHoldMs || keepSocketOpen)) {
                    console.log(`⏭️ [PAIRING] initialize keep code age=${Math.round(keepAge / 1000)}s socket=${keepSocketOpen ? 'open' : 'down'} — jangan requestPairingCode ulang`);
                } else {
                    this.pairingCode = null;
                    this.pairingCodeGeneratedTime = null;
                    this.pairingLeftoverCredsBlocked = false;
                    this.pairingPasteAcknowledged = false;
                    this._lastPairingQr = null;
                    this._pairingNeedRefresh = false;
                    this._pairingRefreshSoftCd = false;
                }
                // Pair baru: reset window 401. Reconnect creds (CONNECTED sudah pernah) jangan dihapus.
                if (!this.connectedWebhookSent && !this.pairingAuthSucceededAt) {
                    if (this._pairingModeClearTimer) {
                        clearTimeout(this._pairingModeClearTimer);
                        this._pairingModeClearTimer = null;
                    }
                }
            }
            if (this.pairingTimeout) {
                clearTimeout(this.pairingTimeout);
                this.pairingTimeout = null;
            }
            
            // ✅ DEBUG: Log pairing mode status
            if (this.isPairingMode) {
                console.log(`📱 [PAIRING] Pairing mode ENABLED for session ${this.sessionId}, phoneNumber: ${this.requestedPhoneNumber}`);
            } else {
                console.log(`📱 [PAIRING] Pairing mode DISABLED for session ${this.sessionId}, phoneNumber: ${this.requestedPhoneNumber || 'null'}`);
            }
            
            // ✅ BOT HANTU CHECK: Prevent initialize jika session terdeteksi sebagai bot hantu
            // ✅ CRITICAL: Jangan block pairing mode (pairing perlu bisa connect)
            const isBotHantu = await this.isSessionBotHantu();
            if (isBotHantu && !this.isPairingMode) {
                console.error(`🚨 [BOT-HANTU] Initialize BLOCKED for session ${this.sessionId} - Session marked as BOT HANTU`);
                
                // Send webhook untuk inform bahwa initialize diblokir
                await this.enqueueWebhook({
                    event: 'INITIALIZE_BLOCKED',
                    sessionId: this.sessionId,
                    bot: this.sessionId,
                    reason: 'BOT_HANTU_DETECTED',
                    message: 'Session terdeteksi sebagai bot hantu, initialize diblokir untuk 24 jam',
                    user_id: this.userId,
                    port: this.port
                }).catch(() => {});
                
                throw new Error(`Session ${this.sessionId} is marked as BOT HANTU - Initialize blocked for 24 hours`);
            } else if (isBotHantu && this.isPairingMode) {
                console.log(`⏭️ [BOT-HANTU] Skipping bot hantu check - pairing mode active (must complete until connect): ${this.sessionId}`);
            }
            
            // ✅ V2: Use process.cwd() directly (guardian runs from parent /opt/baileys)
            // ✅ process.cwd() will be /opt/baileys when run from parent directory
            const sessionDir = path.join(process.cwd(), 'sessions', this.sessionId);
            
            // ✅ CRITICAL: Create session directory if not exists (dengan recursive untuk ensure parent dir juga dibuat)
            try {
                if (!fs.existsSync(sessionDir)) {
                    fs.mkdirSync(sessionDir, { recursive: true });
                    console.log(`📁 [INIT] Created session directory: ${sessionDir}`);
                }
            } catch (mkdirError) {
                console.error(`❌ [INIT] Error creating session directory: ${mkdirError.message}`);
                throw mkdirError; // Re-throw untuk prevent continue dengan invalid session dir
            }

            // Pair baru: folder ini jangan bawa creds lama. Kalau me.id sudah ada, socket langsung
            // OPEN → CONNECTED tanpa paste kode (user nunggu di bots, tiba-tiba terhubung).
            if (Boolean(this.requestedPhoneNumber)) {
                const credsFile = path.join(sessionDir, 'creds.json');
                if (fs.existsSync(credsFile)) {
                    try {
                        const leftover = JSON.parse(fs.readFileSync(credsFile, 'utf8'));
                        const meId = leftover?.me?.id ? String(leftover.me.id) : '';
                        const isStub = credsFileIsPairingStub(credsFile);
                        // Stub WAJIB dibuang berapa pun umur grace-nya: selama me.id ada tapi
                        // pair belum sukses, Baileys mencoba resume, tidak pernah memancarkan QR,
                        // dan kode pairing baru mustahil dibuat — sesi terkunci 401 selamanya.
                        const freshPair = !this.inPostPairGrace() && !this.pairingAuthSucceededAt;
                        if (meId.includes('@') && !meId.includes('sess_') && (isStub || freshPair)) {
                            console.warn(
                                `⚠️ [PAIRING] wipe leftover creds session=${this.sessionId} me=${meId} stub=${isStub}`
                            );
                            fs.rmSync(sessionDir, { recursive: true, force: true });
                            fs.mkdirSync(sessionDir, { recursive: true });
                            // Stempel auth palsu dari stub ikut dibuang, kalau tidak grace 401
                            // tetap aktif dan menutupi kegagalan pair berikutnya.
                            if (isStub) {
                                this.pairingAuthSucceededAt = null;
                                if (this._pairingModeClearTimer) {
                                    clearTimeout(this._pairingModeClearTimer);
                                    this._pairingModeClearTimer = null;
                                }
                            }
                        }
                    } catch (wipeErr) {
                        console.warn(`⚠️ [PAIRING] leftover creds wipe skipped: ${wipeErr.message}`);
                    }
                }
            }
            
            // ⏰ TIMEOUT SETUP: auto-close pairing cepat supaya session gagal tidak nempel di RAM.
            // pairingTimeout sudah di-clear di atas saat initialize ulang.
            // 180s = sama dengan getPostPairGraceMs(). Batas 120s lama lebih pendek dari
            // grace 401 dan dari umur kode WA (~3 menit) → sesi mati saat user masih menempel kode.
            const PAIRING_TIMEOUT_DURATION = Math.max(
                60000,
                Math.min(180000, Number(process.env.PAIRING_INCOMPLETE_TTL_MS || config.performance?.pairingIncompleteTtlMs) || 180000)
            );
            
            // 🔧 PAIRING CODE: Skip proxy untuk pairing method (lebih stabil & cepat)
            const skipProxy = this.isPairingMode || !config.proxy.enabled;
            
            const proxySetupPromise = (async () => {
                let proxyAgent = null;
                let proxyInfo = null;
                
                if (skipProxy) {
                    console.log(`?? PAIRING MODE: Skipping proxy for better stability & faster connect`);
                    return { proxyAgent, proxyInfo };
                }

                if (!config.proxy.enabled) {
                    console.log('?? Proxy disabled via config - connecting directly to WhatsApp');
                    return { proxyAgent, proxyInfo };
                }
                
                try {
                    const proxyUrl = config.proxy.primaryUrl;
                    const proxyApiKey = config.proxy.apiKey;
                    
                    console.log(`?? Fetching unique proxy for session ${this.sessionId}...`);
                    
                    const proxyResponse = await axios.post(proxyUrl, {
                        passwordnya: proxyApiKey
                    }, {
                        timeout: config.proxy.timeoutMs,
                        headers: {
                            'Content-Type': 'application/json'
                        }
                    });
                    
                    if (proxyResponse.data.error === "0" && proxyResponse.data.proxy) {
                        const proxyAddress = proxyResponse.data.proxy;
                        proxyInfo = {
                            address: proxyAddress,
                            proxyId: proxyResponse.data.proxyId || null
                        };
                        
                        const { SocksProxyAgent } = await import('socks-proxy-agent');
                        proxyAgent = new SocksProxyAgent(proxyAddress);
                        
                        console.log(`? Proxy assigned: ${proxyAddress.replace(/:[^:@]*@/, ':***@')}`);
                        
                        await this.enqueueWebhook({
                            event: 'PROXY_ASSIGNED',
                            sessionId: this.sessionId,
                            proxyId: proxyInfo.proxyId,
                            proxyAddress: proxyAddress.replace(/:[^:@]*@/, ':***@'),
                            user_id: this.userId
                        });
                    } else {
                        console.warn('?? No proxy available, connecting without proxy');
                    }
                } catch (proxyError) {
                    console.error('? Error fetching proxy:', proxyError.message);
                    console.warn('?? Continuing without proxy');
                }
                
                return { proxyAgent, proxyInfo };
            })();
            
            const versionPromise = (async () => {
                const configStaticVersion = (
                    Array.isArray(config.whatsapp?.staticVersion) &&
                    config.whatsapp.staticVersion.length === 3
                ) ? config.whatsapp.staticVersion : null;

                let staticVersion = configStaticVersion;
                let version;
                let isLatest = false;

                try {
                    const latest = await getLatestBaileysVersionCached();
                    if (Array.isArray(latest?.version) && latest.version.length === 3) {
                        version = latest.version;
                        isLatest = latest.isLatest === true;
                    } else if (Array.isArray(latest) && latest.length === 3) {
                        version = latest;
                        isLatest = true;
                    } else {
                        throw new Error('latest version format invalid');
                    }
                    // fleet-quiet 2026-09-07: per proses, catat hanya bila versi berubah atau >10 menit sejak catatan terakhir.
                    {
                        const vKey = JSON.stringify(version);
                        const nowV = Date.now();
                        if (globalThis.__fleetLastVersionKey !== vKey || nowV - (globalThis.__fleetLastVersionLogAt || 0) > 600000) {
                            globalThis.__fleetLastVersionKey = vKey;
                            globalThis.__fleetLastVersionLogAt = nowV;
                            console.log(`Using dynamic Baileys version: ${vKey} (isLatest=${isLatest})`);
                        }
                    }
                } catch (e) {
                    const allowStaticFallback = String(process.env.ALLOW_STATIC_VERSION_FALLBACK || '').toLowerCase();
                    const canFallbackStatic = ['1', 'true', 'yes', 'on'].includes(allowStaticFallback);
                    if (canFallbackStatic && staticVersion) {
                        version = staticVersion;
                        console.warn(`⚠️ Dynamic version fetch failed (${e.message}), fallback static enabled via env: ${JSON.stringify(version)}`);
                    } else {
                        throw e;
                    }
                }

                return { version, staticVersion };
            })();
            
            const authStatePromise = useMultiFileAuthState(sessionDir);
            const redisInitPromise = this.initRedis();
            
            const [{ version, staticVersion }, { state, saveCreds: rawSaveCreds }, { proxyAgent, proxyInfo }] = await Promise.all([
                versionPromise,
                authStatePromise,
                proxySetupPromise
            ]);
            const saveCreds = wrapSafeSaveCreds(this.sessionId, sessionDir, rawSaveCreds, () => {
                this._sessionDirGone = true;
            });
            this.saveCreds = saveCreds;
            
            await redisInitPromise;
            
            // Store state for later access
            this.authState = state;
            this.proxyInfo = proxyInfo;
            this.sessionDir = sessionDir;
            
            // ✅ FIX RESTORE: Check apakah session sudah connected sebelum create socket baru
            // ✅ Jika creds sudah punya me.id yang valid, berarti session sudah connected sebelumnya
            // ✅ Jangan force create socket baru yang bisa trigger disconnect
            // ✅ CRITICAL: Untuk pairing mode, jangan gunakan existing credentials (harus fresh pairing)
            const hasValidCreds = state.creds && state.creds.me && state.creds.me.id && 
                                  state.creds.me.id.includes('@') && !state.creds.me.id.includes('sess_');
            
            // Create WhatsApp socket with proxy
            const socketOptions = {
                version,
                logger: this.logger,
                printQRInTerminal: false, // Kita handle QR sendiri
                auth: {
                    creds: state.creds,
                    keys: makeCacheableSignalKeyStore(state.keys, this.logger, createBoundedSignalKeyStoreCache())
                },
                // Forum Baileys 2026: Chrome (bukan Desktop); pairing paling stabil dengan Mac OS Chrome canonical.
                // Baca WA_BROWSER=OS,Browser,Version dari env (sebelumnya env diabaikan → selalu Windows/122).
                browser: (() => {
                    let picked = null;
                    const envBrowser = String(process.env.WA_BROWSER || '').trim();
                    if (envBrowser) {
                        const parts = envBrowser.split(',').map((s) => s.trim()).filter(Boolean);
                        if (parts.length >= 3) picked = [parts[0], parts[1], parts.slice(2).join(',')];
                        else if (parts.length === 2) picked = [parts[0], parts[1], '14.4.1'];
                    }
                    if (!picked && Array.isArray(config.whatsapp?.browser) && config.whatsapp.browser.length === 3) {
                        picked = config.whatsapp.browser;
                    }
                    if (!picked) picked = ['Mac OS', 'Chrome', '14.4.1'];
                    setImmediate(() => console.log(`🌐 [BROWSER] Session ${this.sessionId} → ${JSON.stringify(picked)}`));
                    return picked;
                })(),
                connectTimeoutMs: 120000,
                // Pairing: jangan timeout query prematur (forum OpenClaw/Baileys pairing tips)
                defaultQueryTimeoutMs: (() => {
                    if (this.isPairingMode) {
                        const pairRaw = process.env.BAILEYS_PAIRING_QUERY_TIMEOUT_MS;
                        if (pairRaw !== undefined && String(pairRaw).trim() !== '') {
                            const pn = Number(pairRaw);
                            if (pn === 0) return undefined;
                            if (Number.isFinite(pn) && pn > 0) return Math.max(30000, Math.min(180000, pn));
                        }
                        return undefined;
                    }
                    const raw = process.env.BAILEYS_QUERY_TIMEOUT_MS;
                    const n = raw !== undefined && raw !== '' ? Number(raw) : 60000;
                    if (n === 0) return 0;
                    return Math.max(30000, Math.min(120000, Number.isFinite(n) ? n : 60000));
                })(),
                keepAliveIntervalMs: Math.max(20000, Math.min(45000, Number(process.env.BAILEYS_KEEPALIVE_MS) || 25000)),
                fireInitQueries: true,
                transactionOpts: { maxCommitRetries: 3, delayBetweenTriesMs: 500 },
                markOnlineOnConnect: false,
                syncFullHistory: false,
                shouldIgnoreJid: (jid) => shouldIgnoreJidForFleet(jid),
                generateHighQualityLinkPreview: true,
                getMessage: async (key) => this.getCachedMessage(key),
            };
            
            // ? Add proxy agent if available
            if (proxyAgent) {
                socketOptions.agent = proxyAgent;
                console.log(`?? Socket will use proxy`);
            }
            
            // ✅ OPTIMIZED: Init Redis dengan timeout untuk prevent blocking terlalu lama
            // ✅ CRITICAL: Jika Redis lambat, jangan block initialize - continue tanpa Redis
            try {
                await Promise.race([
                    this.initRedis(),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('Redis init timeout')), 2000)) // 2 detik max
                ]);
            } catch (e) {
                console.warn(`⚠️ [INIT] Redis init timeout or failed (${e.message}), continuing without Redis (webhook will use direct send only)`);
                this.redis = null; // Set null untuk fallback ke direct send
            }
            
            if (hasValidCreds && !this.isPairingMode) {
                console.log(`✅ [RESTORE] Session ${this.sessionId} sudah punya credentials valid (me.id: ${state.creds.me.id}), akan create socket untuk restore connection`);
            }
            
            try {
                this.sock = makeWASocket(socketOptions);
            } catch (e) {
                // Retry dengan static version yang sama — jangan fetch latest (pin fleet)
                if (staticVersion && Array.isArray(staticVersion) && staticVersion.length === 3) {
                    console.warn(`Socket creation failed (${e.message}), retrying with static version ${JSON.stringify(staticVersion)}...`);
                    socketOptions.version = staticVersion;
                    this.sock = makeWASocket(socketOptions);
                } else {
                    throw e;
                }
            }
            
            // ✅ CRITICAL FIX: Register ALL event listeners IMMEDIATELY after socket creation
            // BEFORE connection.update handler to prevent race condition
            // If ACK arrives before listener is ready, it will be lost!
            // ============================================================================
            // ?? MESSAGE HANDLERS - MUST BE REGISTERED FIRST (BEFORE connection.update)
            // ============================================================================
            
            // Save credentials on update
            this.sock.ev.on('creds.update', saveCreds);
            // fork-fix 2026-09-07: batas pesan chat baru dari WhatsApp (dipancarkan messages-recv.js yang ditambal)
            this.sock.ev.on('message-capping.update', (payload) => {
                this.messageCapping = { payload, at: new Date().toISOString() };
                console.warn(`🧢 [MESSAGE-CAPPING] ${this.sessionId} ${JSON.stringify(payload || {}).slice(0, 300)}`);
            });
            
            // ✅ CRITICAL: Listen creds.update untuk detect authentication complete
            // ✅ creds.update akan fire saat authentication selesai (setelah QR scan atau pairing code input)
            // ✅ Ini lebih reliable daripada hanya check sock.user.id
            this.sock.ev.on('creds.update', (creds) => {
                // me.id saja tidak cukup: requestPairingCode menulisnya sendiri sebelum user
                // menempel kode. Wajib ada penanda pair-success (account/lid/platform/nama asli).
                if (creds?.me?.id && !credsLookAuthenticated(creds)) {
                    // creds.update terpancar tiap perubahan pre-key/app-state — cukup catat sesekali.
                    const nowStub = Date.now();
                    if (!this._lastStubCredsLogAt || nowStub - this._lastStubCredsLogAt > 15000) {
                        this._lastStubCredsLogAt = nowStub;
                        setImmediate(() => console.log(
                            `⏭️ [CREDS.UPDATE] stub pairing (belum pair-success) — abaikan: ${creds.me.id}`
                        ));
                    }
                    return;
                }
                if (credsLookAuthenticated(creds)) {
                    // ✅ Authentication complete via creds.update!
                    setImmediate(() => console.log(`✅ [CREDS.UPDATE] Authentication detected via creds.update: ${creds.me.id}`));
                    
                    // Kode di-generate ≠ user sudah paste. Jangan CONNECTED dari creds sisa.
                    // Hanya untuk pairing kode (requestedPhoneNumber). Sesi QR pasca-scan juga isPairingMode (grace 401)
                    // tetapi tidak punya kode — dulu ikut di-skip di sini dan di connection.open → CONNECTED hilang.
                    if (this.isPairingMode && this.requestedPhoneNumber) {
                        if (!this.pairingCode) {
                            console.warn(`⚠️ [CREDS.UPDATE] Pairing mode active but pairing code not generated yet - skipping CONNECTED webhook (waiting for pairing code input)`);
                            console.warn(`⚠️ [CREDS.UPDATE] This might be cached credentials from previous session - ignoring until pairing code is input`);
                            return;
                        }
                        
                        const pairingCodeGeneratedTime = this.pairingCodeGeneratedTime;
                        if (!pairingCodeGeneratedTime || pairingCodeGeneratedTime === null || pairingCodeGeneratedTime === undefined) {
                            console.warn(`⚠️ [CREDS.UPDATE] Pairing code generated time is null/undefined - skipping CONNECTED webhook (pairing code not generated yet)`);
                            return;
                        }
                        
                        const credsUpdateTime = Date.now();
                        const minPairAge = Math.max(12000, Number(process.env.PAIRING_CONNECTED_MIN_AGE_MS) || 12000);
                        if (credsUpdateTime < (pairingCodeGeneratedTime + minPairAge)) {
                            console.warn(`⚠️ [CREDS.UPDATE] too early for CONNECTED (${credsUpdateTime - pairingCodeGeneratedTime}ms after code, need ${minPairAge}ms) — leftover creds, skip`);
                            return;
                        }
                        
                        console.log(`✅ [CREDS.UPDATE] Pairing code check passed - pairing code generated ${credsUpdateTime - pairingCodeGeneratedTime}ms ago`);
                        this.markPairingPasteAcknowledged('creds.update');
                        // Stamp grace 401 SEKARANG (sebelum gate socket/CONNECTED) supaya blip 401 tidak LOGGED_OUT
                        this.markPairingAuthSucceeded('creds.update');
                    } else {
                        this.markPairingAuthSucceeded('creds.update.qr');
                    }
                    
                    // ✅ CRITICAL FIX: Kirim CONNECTED webhook jika belum pernah dikirim (check flag)
                    // ✅ Untuk pairing mode, CONNECTED webhook HARUS dikirim di sini (karena connection.open mungkin tidak fire lagi)
                    // ✅ Jangan check `!this.isAuthenticated` karena bisa sudah di-set oleh connection.open handler sebelumnya
                    // ✅ FIX: Jangan set flag di sini, set SETELAH webhook benar-benar berhasil dikirim (tidak di-skip)
                    if (this.connectedWebhookSent) {
                        console.log(`⏭️ [CREDS.UPDATE] CONNECTED webhook already sent, skipping duplicate (connectedWebhookSent: ${this.connectedWebhookSent})`);
                        return; // Exit early untuk prevent duplicate processing
                    }
                    
                    // ✅ CRITICAL: Prevent multiple execution jika sedang processing
                    // ✅ FIX: Untuk pairing mode, jika processing terlalu lama (> 10 detik), allow new call (mungkin hang)
                    if (this.processingConnectedWebhook) {
                        const processingStartTime = this.processingConnectedWebhookStartTime || Date.now();
                        const processingDuration = Date.now() - processingStartTime;
                        
                        // ✅ Untuk pairing mode, jika processing > 10 detik, reset flag dan allow new call
                        if (this.isPairingMode && processingDuration > 10000) {
                            console.warn(`⚠️ [CREDS.UPDATE] Processing too long (${processingDuration}ms) for pairing mode, resetting flag and allowing new call`);
                            this.processingConnectedWebhook = false;
                            this.processingConnectedWebhookStartTime = null;
                        } else {
                            console.log(`⏭️ [CREDS.UPDATE] CONNECTED webhook sedang diproses, skipping duplicate call`);
                            return; // Exit early untuk prevent duplicate processing
                        }
                    }
                    
                    // Baileys best practice: creds.update means credentials changed, not send-ready.
                    // Only connection.open + valid WA identity is allowed to publish CONNECTED.
                    const socketOpenForConnected = isBaileysWebSocketOpen(this.sock);
                    const connectedIdentity = this.resolveAuthenticatedJid();
                    if (!socketOpenForConnected || !connectedIdentity) {
                        setImmediate(() => console.log(
                            `⏳ [CREDS.UPDATE] Auth detected but socket not send-ready yet; waiting for connection.open ` +
                            `(socketOpen=${socketOpenForConnected}, wsState=${getBaileysWebSocketReadyState(this.sock)})`
                        ));
                        return;
                    }

                    const pairingGate = this.assertPairingConnectedAllowed('creds.update');
                    if (!pairingGate.ok) {
                        console.warn(
                            `⏭️ [CREDS.UPDATE] CONNECTED gated session=${this.sessionId} reason=${pairingGate.reason}`
                        );
                        return;
                    }

                    // ✅ Set flag untuk prevent multiple execution
                    this.processingConnectedWebhook = true;
                    this.processingConnectedWebhookStartTime = Date.now();
                    
                    console.log(`🚀 [CREDS.UPDATE] Starting CONNECTED webhook flow for pairing mode...`);
                    
                    // ✅ Process CONNECTED webhook
                    // ✅ CRITICAL: Untuk pairing mode, tetap set isAuthenticated dan kirim CONNECTED webhook
                    // ✅ creds.update akan fire saat authentication selesai (setelah pairing code input)
                    // ✅ CONNECTED webhook HARUS dikirim di sini untuk pairing mode (karena connection.open mungkin tidak fire lagi)
                    if (this.isPairingMode) {
                        setImmediate(() => console.log(`✅ [CREDS.UPDATE] Pairing mode: Authentication detected, will send CONNECTED webhook`));
                    }
                    
                    // ✅ Set authenticated jika belum
                    if (!this.isAuthenticated) {
                        // ✅ Stop polling jika masih jalan
                        if (this.authPollingInterval) {
                            clearInterval(this.authPollingInterval);
                            this.authPollingInterval = null;
                            setImmediate(() => console.log(`🔄 [CREDS.UPDATE] Stopped polling - creds.update detected authentication`));
                        }
                        
                        // ✅ Set authenticated
                        this.isAuthenticated = true;
                    }
                    
                    // ✅ Extract clean bot number
                    let botNumber = creds.me.id;
                    if (botNumber && botNumber.includes('@')) {
                        botNumber = botNumber.split('@')[0];
                    }
                    if (botNumber && botNumber.includes(':')) {
                        botNumber = botNumber.split(':')[0];
                    }
                    
                    // ✅ Validate bot number
                    if (!botNumber || !botNumber.match(/^\d{10,15}$/)) {
                        botNumber = this.sessionId;
                    }
                    
                    // ✅ Cancel AUTH timeout
                    if (this.pairingTimeout) {
                        clearTimeout(this.pairingTimeout);
                        this.pairingTimeout = null;
                        setImmediate(() => console.log(`⏰ AUTH timeout cancelled - authentication completed via creds.update`));
                    }
                    
                    // ✅ Trigger CONNECTED flow (non-blocking)
                    (async () => {
                        try {
                            console.log(`🔍 [CREDS.UPDATE] Inside CONNECTED webhook flow for session ${this.sessionId}`);
                            
                            // Reset connection tracking
                            this.reconnectAttempts = 0;
                            this.reconnectInFlight = false;
                            this.connectionStreak++;
                            this.lastStableConnection = Date.now();
                            
                            // ✅ FIXED: Reset disconnect cycle flags saat CONNECTED (new cycle, prevent spam webhook)
                            this.currentDisconnectCycle = null;
                            this.reconnectingSent = false;
                            this.sessionDeadWebhookSent = false; // Reset flag saat CONNECTED
                            
                            // Start heartbeat
                            this.startHeartbeatMonitoring();
                            this.startConnectionHealthCheck(); // ✅ CONNECTION STABILITY: Start health check
                            
                            // Update session status
                            await this.sessionManager.updateSessionLogin(this.sessionId, true);
                            
                            // Callback
                            this.onConnected();
                            
                            // ✅ FIXED: Gunakan enqueueWebhook untuk CONNECTED (dengan deduplication)
                            console.log(`📤 [CREDS.UPDATE] Calling enqueueWebhook for CONNECTED event, botNumber: ${botNumber}`);
                            
                            let webhookResponse = null;
                            try {
                                console.log(`🔍 [CREDS.UPDATE] Before calling enqueueWebhook, checking dedupeCache...`);
                                
                                // ✅ CRITICAL: Add timeout untuk prevent hang (30 detik max)
                                const webhookPromise = this.enqueueWebhook(
                                    this.buildCredsUpdateConnectedPayload(botNumber, 'creds.update'),
                                    { forceDirect: true }
                                );
                                
                                // ✅ CRITICAL: Add timeout untuk prevent hang (30 detik max)
                                const timeoutPromise = new Promise((_, reject) => {
                                    setTimeout(() => reject(new Error('enqueueWebhook timeout after 30 seconds')), 30000);
                                });
                                
                                console.log(`⏳ [CREDS.UPDATE] Waiting for enqueueWebhook to return (with 30s timeout)...`);
                                const webhookResult = await Promise.race([webhookPromise, timeoutPromise]);
                                console.log(`✅ [CREDS.UPDATE] enqueueWebhook promise resolved, result type: ${typeof webhookResult}, skipped: ${webhookResult?.skipped}, queued: ${webhookResult?.queued}, direct: ${webhookResult?.direct}`);
                                
                                // ✅ Check apakah webhook di-skip oleh deduplication
                                if (webhookResult?.skipped) {
                                    console.warn(`⚠️ [CREDS.UPDATE] CONNECTED webhook was SKIPPED by deduplication, reason: ${webhookResult.reason || 'unknown'}`);
                                    // ✅ CRITICAL: Reset processing flag jika di-skip, biar connection.open bisa kirim
                                    this.processingConnectedWebhook = false;
                                    // ✅ Untuk pairing mode, jika webhook di-skip, tunggu sebentar lalu retry (karena connection.open mungkin tidak fire)
                                    if (this.isPairingMode) {
                                        console.log(`🔄 [CREDS.UPDATE] Webhook skipped for pairing mode, will retry in 2 seconds...`);
                                        // ✅ CRITICAL: Reset processing flag SEBELUM retry untuk pairing mode
                                        this.processingConnectedWebhook = false;
                                        // ✅ Retry setelah 2 detik untuk pairing mode
                                        setTimeout(async () => {
                                            if (!this.connectedWebhookSent) {
                                                // ✅ Check lagi apakah masih processing (mungkin ada call lain)
                                                if (this.processingConnectedWebhook) {
                                                    console.warn(`⚠️ [CREDS.UPDATE] Still processing, skipping retry`);
                                                    return;
                                                }
                                                console.log(`🔄 [CREDS.UPDATE] Retrying CONNECTED webhook for pairing mode...`);
                                                this.processingConnectedWebhook = true;
                                                this.processingConnectedWebhookStartTime = Date.now();
                                                try {
                                                    const retryResult = await this.enqueueWebhook(
                                                        this.buildCredsUpdateConnectedPayload(
                                                            botNumber,
                                                            'creds.update_retry'
                                                        ),
                                                        { forceDirect: true }
                                                    );
                                                    
                                                    if (retryResult?.skipped) {
                                                        console.warn(`⚠️ [CREDS.UPDATE] Retry also skipped, reason: ${retryResult.reason || 'unknown'}`);
                                                        // ✅ Reset flag untuk allow retry lagi jika perlu
                                                        this.processingConnectedWebhook = false;
                                                        this.processingConnectedWebhookStartTime = null;
                                                        // ✅ Jika masih di-skip karena processing, tunggu lebih lama lalu retry lagi
                                                        if (retryResult.reason === 'CONNECTED_PROCESSING') {
                                                            console.log(`🔄 [CREDS.UPDATE] Retry skipped due to processing, will retry again in 3 seconds...`);
                                                            setTimeout(async () => {
                                                                if (!this.connectedWebhookSent && !this.processingConnectedWebhook) {
                                                                    console.log(`🔄 [CREDS.UPDATE] Final retry for pairing mode (bypassing processing check)...`);
                                                                    this.processingConnectedWebhook = true;
                                                                    this.processingConnectedWebhookStartTime = Date.now();
                                                                    try {
                                                                        // ✅ Force bypass dengan set flag processing ke false sementara
                                                                        const tempProcessing = this.processingConnectedWebhook;
                                                                        this.processingConnectedWebhook = false;
                                                                        const finalRetryResult = await this.enqueueWebhook(
                                                                            this.buildCredsUpdateConnectedPayload(
                                                                                botNumber,
                                                                                'creds.update_final_retry'
                                                                            ),
                                                                            { forceDirect: true }
                                                                        );
                                                                        this.processingConnectedWebhook = tempProcessing;
                                                                        
                                                                        if (!finalRetryResult?.skipped) {
                                                                            this.connectedWebhookSent = true;
                                                                            this.processingConnectedWebhook = false;
                                                                            this.processingConnectedWebhookStartTime = null;
                                                                            console.log(`✅ [CREDS.UPDATE] CONNECTED webhook sent successfully on final retry`);
                                                                            
                                                                            if (this.isPairingMode) {
                                                                                console.log(
                                                                                    `🔄 [CREDS.UPDATE] Pairing mode: Restarting connection after CONNECTED webhook (final retry) to fully establish connection...`
                                                                                );
                                                                                this.scheduleCredsUpdatePairingRestart(2000);
                                                                            }
                                                                        } else {
                                                                            console.error(`❌ [CREDS.UPDATE] Final retry also skipped, reason: ${finalRetryResult.reason || 'unknown'}`);
                                                                            this.processingConnectedWebhook = false;
                                                                            this.processingConnectedWebhookStartTime = null;
                                                                        }
                                                                    } catch (finalRetryError) {
                                                                        console.error(`❌ [CREDS.UPDATE] Final retry error:`, finalRetryError.message);
                                                                        this.processingConnectedWebhook = false;
                                                                        this.processingConnectedWebhookStartTime = null;
                                                                    }
                                                                }
                                                            }, 3000);
                                                        }
                                                    } else {
                                                        this.connectedWebhookSent = true;
                                                        this.processingConnectedWebhook = false;
                                                        this.processingConnectedWebhookStartTime = null;
                                                        console.log(`✅ [CREDS.UPDATE] CONNECTED webhook sent successfully on retry`);
                                                    }
                                                } catch (retryError) {
                                                    console.error(`❌ [CREDS.UPDATE] Retry error:`, retryError.message);
                                                    this.processingConnectedWebhook = false;
                                                }
                                            }
                                        }, 2000);
                                    } else {
                                        // ✅ QR/non-pairing: retry CONNECTED jika race dengan creds.update
                                        console.log(`🔄 [CREDS.UPDATE] Webhook skipped (${webhookResult.reason}), will retry CONNECTED in 1.5s...`);
                                        setTimeout(async () => {
                                            if (this.connectedWebhookSent || this.processingConnectedWebhook) return;
                                            try {
                                                const retry = await this.sendConnectedOnce('creds.update_retry_qr', botNumber, { forceDirect: true });
                                                if (!retry?.skipped) {
                                                    console.log(`✅ [CREDS.UPDATE] CONNECTED webhook sent on QR retry`);
                                                }
                                            } catch (e) {
                                                console.error(`❌ [CREDS.UPDATE] QR retry error:`, e.message);
                                            }
                                        }, 1500);
                                    }
                                    return; // Exit early, jangan lanjut process response
                                }
                                
                                // ✅ CRITICAL: Set flag SETELAH webhook benar-benar berhasil dikirim (tidak di-skip)
                                await this.markConnectedWebhookSent();
                                
                                console.log(`✅ [CREDS.UPDATE] Flag connectedWebhookSent set to TRUE (webhook successfully sent)`);
                                
                                // Extract webhook response dari result jika ada
                                webhookResponse = webhookResult?.response || webhookResult;
                                console.log(`✅ [CREDS.UPDATE] enqueueWebhook returned successfully, webhookResponse type: ${typeof webhookResponse}, has response: ${!!webhookResponse}`);
                                
                                // ✅ CRITICAL FIX: Untuk pairing mode, restart connection setelah CONNECTED webhook dikirim
                                // ✅ Ini penting karena setelah pairing code input, connection perlu di-restart untuk fully establish
                                // ✅ Tanpa restart, HP akan menunjukkan "couldn't link" meskipun sudah terhubung
                                if (this.isPairingMode) {
                                    console.log(
                                        `🔄 [CREDS.UPDATE] Pairing mode: Restarting connection after CONNECTED webhook to fully establish connection...`
                                    );
                                    this.scheduleCredsUpdatePairingRestart(2000);
                                }
                            } catch (webhookError) {
                                console.error(`❌ [CREDS.UPDATE] Error calling enqueueWebhook:`, webhookError.message, webhookError.stack);
                                // ✅ Reset flags jika error, biar bisa retry
                                this.connectedWebhookSent = false;
                                this.processingConnectedWebhook = false;
                                throw webhookError; // Re-throw untuk di-catch di outer try-catch
                            }
                            
                            console.log(`🔍 [CREDS.UPDATE] Processing webhook response, webhookResponse: ${webhookResponse ? 'exists' : 'null'}`);
                            
                            // Handle webhook response (bot_id resolution, etc.)
                            if (webhookResponse) {
                                let resolvedBotId = null;
                                if (typeof webhookResponse === 'object') {
                                    resolvedBotId = webhookResponse.bot_id || webhookResponse.botId;
                                } else if (typeof webhookResponse === 'string') {
                                    try {
                                        const parsed = JSON.parse(webhookResponse);
                                        resolvedBotId = parsed.bot_id || parsed.botId;
                                    } catch (e) {}
                                }
                                
                                if (resolvedBotId && parseInt(resolvedBotId, 10) > 0) {
                                    const botIdInt = parseInt(resolvedBotId, 10);
                                    if (this.redis) {
                                        this.redis.set(`bot_id:${this.sessionId}`, String(botIdInt), { EX: 7 * 24 * 60 * 60 }).catch(() => {});
                                    }
                                    this.cachedBotId = botIdInt;
                                }
                            }
                            
                            // Check for logout request - HANYA disconnect, bukan logout
                            // Logout hanya boleh dipanggil dari API endpoint, bukan dari webhook
                            if (webhookResponse && webhookResponse.error === '1' && webhookResponse.action === 'logout') {
                                console.log(`🔌 Webhook requested disconnect (not logout) for: ${this.sessionId}`);
                                await this.smartDisconnect({ 
                                    force: false, 
                                    reason: `Webhook requested: ${webhookResponse.msg || 'Session rejected'}` 
                                });
                                return;
                            }
                            
                            // ✅ Flag sudah di-set setelah webhook berhasil dikirim (tidak di-skip)
                            console.log(`✅ [CONNECTED] CONNECTED webhook sent for session ${this.sessionId} (via creds.update${this.isPairingMode ? ' - pairing mode' : ''}), webhookResponse: ${webhookResponse ? JSON.stringify(webhookResponse).substring(0, 100) : 'null'}`);
                            console.log(`🎉 [CONNECTED] CONNECTED webhook flow COMPLETED for session ${this.sessionId}`);
                        } catch (error) {
                            console.error(`❌ [CONNECTED] Error sending CONNECTED webhook (creds.update):`, error.message);
                            console.error(`❌ [CONNECTED] Error stack:`, error.stack);
                            // ✅ Reset flags jika error, biar bisa retry
                            this.connectedWebhookSent = false;
                            this.processingConnectedWebhook = false;
                        }
                    })();
                }
            });
            
            console.log(`🔧 [INIT] Registering event listeners IMMEDIATELY for session ${this.sessionId} (BEFORE connection.open)`);
            
            // ✅ FIXED: Message received dengan comprehensive error handling
            this.sock.ev.on('messages.upsert', async ({ messages, type }) => {
                try {
                this.lastEventAt = Date.now();
                if (process.env.MESSAGES_UPSERT_LOG === '1') {
                    console.log(`?? [MESSAGES.UPSERT] type: ${type}, count: ${messages.length}`);
                }
                
                if (type !== 'notify' && type !== 'append') return;

                const echoUpsertMsgIds = [];
                
                for (const msg of messages) {
                    if (!msg.message) continue;
                    
                    // DETECT REVOKE via protocolMessage (type 0 = REVOKE) — ketat, anti false-positive
                    if (msg.key?.fromMe && msg.message?.protocolMessage?.type === 0) {
                        const revokedMessageId = msg.message?.protocolMessage?.key?.id;
                        const remoteJid = msg.message?.protocolMessage?.key?.remoteJid || msg.key?.remoteJid;
                        
                        console.log(`[REVOKE via UPSERT] session=${this.sessionId} remoteJid=${remoteJid} msgId=${revokedMessageId}`);
                        
                        if (!this.shouldReportMessageDeleted(revokedMessageId)) {
                            continue;
                        }

                        let botNumber = this.sock?.user?.id || this.sessionId;
                        if (botNumber && botNumber.includes('@')) {
                            botNumber = botNumber.split('@')[0];
                        }
                        if (botNumber && botNumber.includes(':')) {
                            botNumber = botNumber.split(':')[0];
                        }
                        
                        if (/^\d{10,15}$/.test(botNumber)) {
                            console.log(`[VIOLATION] Confirmed API-sent message revoked by bot msgId=${revokedMessageId}`);
                            await this.enqueueWebhook({
                                event: 'MESSAGE_DELETED',
                                sessionId: this.sessionId,
                                bot: botNumber,
                                remoteJid: remoteJid,
                                messageId: revokedMessageId,
                                deletedBy: 'bot_revoke_sent',
                                validated: true,
                                user_id: this.userId,
                                port: this.port,
                                timestamp: Date.now()
                            });
                        }
                        
                        continue;
                    }
                    
                    // fromMe = pesan keluar di store (API blast / echo HP) — sinyal sekunder, bukan incoming
                    if (msg.key.fromMe) {
                        // Hanya konten outbound nyata — bukan protocol-only (revoke sudah di-handle di atas)
                        const hasOutboundContent = !!(
                            msg.message?.conversation || msg.message?.extendedTextMessage
                            || msg.message?.imageMessage || msg.message?.videoMessage
                            || msg.message?.documentMessage || msg.message?.audioMessage
                            || msg.message?.stickerMessage || msg.message?.buttonsMessage
                            || msg.message?.templateMessage || msg.message?.listMessage
                        );
                        if (hasOutboundContent && msg.key?.id) {
                            echoUpsertMsgIds.push(msg.key.id);
                        }
                        continue;
                    }
                    
                    // 🔍 DETECT MESSAGE TIMER (Ephemeral/Disappearing Messages)
                    let hasTimer = false;
                    let timerDuration = 0;
                    if (msg.message?.ephemeralMessage) {
                        // Pesan punya ephemeral timer
                        hasTimer = true;
                        // Cek expiration dari message context atau ephemeral setting
                        timerDuration = msg.message?.messageContextInfo?.expiration || 0;
                        console.log(`⏱️ [TIMER DETECTED] Incoming message has ephemeral timer: ${timerDuration}s (msgId: ${msg.key.id})`);
                    } else if (msg.message?.messageContextInfo?.expiration) {
                        hasTimer = true;
                        timerDuration = msg.message.messageContextInfo.expiration;
                        console.log(`⏱️ [TIMER DETECTED] Incoming message has timer from context: ${timerDuration}s (msgId: ${msg.key.id})`);
                    }
                    
                    console.log(`?? Message received in session ${this.sessionId}${hasTimer ? ` (with ${timerDuration}s timer)` : ''}`);
                    
                    // ✅ WEBHOOK: Kirim webhook jika pesan punya timer via callback (sendWebhookWithLogging)
                    if (hasTimer && timerDuration > 0) {
                        const payload = {
                            event: 'MESSAGE_TIMER',
                            sessionId: this.sessionId,
                            bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                            msg_id: msg.key.id,
                            remoteJid: msg.key.remoteJid,
                            fromMe: msg.key.fromMe,
                            timerDuration: timerDuration,
                            timerDurationSeconds: timerDuration,
                            timerDurationMinutes: Math.round(timerDuration / 60),
                            user_id: this.userId,
                            port: this.port,
                            timestamp: new Date().toISOString(),
                            messageType: 'INCOMING'
                        };
                        this.onMessageTimer(payload);
                    }
                    
                    this.onMessageReceived(msg);
                }

                if (echoUpsertMsgIds.length > 0) {
                    this.processEchoUpsertOutboundBatch(echoUpsertMsgIds).catch((err) => {
                        console.warn(`⚠️ [ECHO-UPSERT] batch error: ${err?.message || err}`);
                    });
                }
                } catch (error) {
                    // ✅ FIXED: Comprehensive error handling untuk prevent crash
                    console.error(`❌ [MESSAGES.UPSERT] Error processing message in session ${this.sessionId}:`, error.message);
                    console.error(`❌ [MESSAGES.UPSERT] Error stack:`, error.stack);
                    // Jangan throw, biarkan process continue
                }
            });
            
            // ✅ CRITICAL: Message sent/updated - MUST BE REGISTERED BEFORE connection.update
            // This prevents race condition where ACK arrives before listener is ready
            // ✅ FIXED: Comprehensive error handling untuk prevent crash
            this.sock.ev.on('messages.update', async (updates) => {
                try {
                console.log(`?? [MESSAGES.UPDATE] Received ${updates.length} updates in session ${this.sessionId}`);
                
                // 🔍 DEBUG: Log struktur array dari Baileys
                if (updates.length > 1) {
                    console.log(`📋 [BATCH UPDATE] Multiple updates in one batch:`, updates.map(u => ({
                        msgId: u.key?.id,
                        status: u.update?.status
                    })));
                }
                
                // ✅ PROCESS EACH UPDATE SEPARATELY - One webhook per msgId per status
                // Baileys bisa kirim multiple updates dalam satu batch, tapi kita kirim webhook satu-satu
                for (const upd of updates) {
                    const st = upd.update?.status;
                    const key = upd.key || upd.update?.key || {};
                    const msgId = key.id;
                    const fromMe = key.fromMe;
                    
                    // Log semua update untuk debugging
                    console.log(`?? [MESSAGES.UPDATE] Update detail:`, JSON.stringify({
                        status: st,
                        statusName: st === 2 ? 'SERVER_ACK' : st === 3 ? 'DELIVERY_ACK' : st === 4 ? 'READ' : st === 5 ? 'PLAYED' : `UNKNOWN(${st})`,
                        msgId,
                        fromMe,
                        remoteJid: key.remoteJid,
                        update: upd.update
                    }));
                    
                    if (!msgId) {
                        console.warn(`⚠️ [MESSAGES.UPDATE] Skipping update without msgId`);
                        continue;
                    }

                    // F.3.4 — ACK/webhook pakai PN, bukan @lid dari WA key
                    if (key.remoteJid && String(key.remoteJid).includes('@lid')) {
                        const resolvedPn = await this.resolveJidToPn(key.remoteJid, msgId);
                        if (resolvedPn && !String(resolvedPn).includes('@lid')) {
                            const lidBefore = key.remoteJid;
                            lidJournalWarn(`📊 [LID-ACK] msgId=${msgId} ${lidBefore} → ${resolvedPn}`);
                            key.remoteJid = resolvedPn;
                            if (upd.key) upd.key.remoteJid = resolvedPn;
                        }
                    }
                    
                    // ✅ CRITICAL: Dedupe per msgId+status untuk prevent duplicate webhook
                    // Tapi untuk lifecycle events (SERVER_ACK, DELIVERED, READ), lebih longgar untuk pastikan terkirim
                    const dedupeKey = `${msgId}_${st}`;
                    const isLifecycleStatus = st === 2 || st === 3 || st === 4; // SERVER_ACK, DELIVERED, atau READ
                    const now = Date.now();
                    
                    // ✅ FIXED: Cleanup expired entries dari dedupeCache setiap 500 entries (prevent memory leak)
                    // ✅ OPTIMIZED: Cleanup lebih sering untuk prevent slowdown
                    if (this.dedupeCache.size > 500) {
                        // ✅ OPTIMIZED: Cleanup expired entries (non-blocking dengan setImmediate)
                        setImmediate(() => {
                            const now = Date.now();
                            let deleted = 0;
                            for (const [key, expireAt] of this.dedupeCache.entries()) {
                                if (expireAt < now) {
                                    this.dedupeCache.delete(key);
                                    deleted++;
                                }
                                // Limit cleanup untuk prevent blocking (max 200 entries per cleanup)
                                if (deleted > 200) break;
                            }
                        });
                    }
                    
                    const exp = this.dedupeCache.get(dedupeKey) || 0;
                    
                    // Lifecycle + non-lifecycle: dedupe 30s (thin pipe — billing idempotent di webhook)
                    const dedupeWindow = 30000;

                    if (exp > now) {
                        console.log(`⏭️ [MESSAGES.UPDATE] Skipping duplicate status ${st} for msgId ${msgId} (seen ${Math.floor((exp - now) / 1000)}s ago)`);
                        continue;
                    }
                    // ✅ MEMORY LEAK FIX: Set dedupe dengan Redis TTL (prevent timer accumulation)
                    this.dedupeCache.set(dedupeKey, now + dedupeWindow);
                    // ✅ Use Redis untuk persistent dedupe (prevent memory leak)
                    if (this.redis) {
                        const redisKey = `dedupe:${this.sessionId}:${dedupeKey}`;
                        this.redis.set(redisKey, (now + dedupeWindow).toString(), { EX: Math.ceil(dedupeWindow / 1000) }).catch(() => {});
                    }

                    // Map Baileys status → our lifecycle
                    // Proto Status: 
                    //   1 = PENDING (internal, tidak diterima via messages.update)
                    //   2 = SERVER_ACK (Server WhatsApp menerima pesan - belum sampai ke user)
                    //   3 = DELIVERY_ACK = DELIVERED (Pesan sampai ke penerima - ini yang penting untuk billing!)
                    //   4 = READ (Pesan dibaca)
                    //   5 = PLAYED (Pesan diputar - audio/video)
                    // messages.update status yang diterima: 2=SERVER_ACK, 3=DELIVERY_ACK (DELIVERED), 4=READ, 5=PLAYED
                    console.log(`✅ [MESSAGES.UPDATE] Processing status ${st} (${st === 2 ? 'SERVER_ACK' : st === 3 ? 'DELIVERY_ACK=DELIVERED' : st === 4 ? 'READ' : st === 5 ? 'PLAYED' : 'UNKNOWN'}) for msgId: ${msgId}, fromMe: ${fromMe}`);
                    
                    // 📊 TRACK STATUS ARRAY untuk logging
                    // ✅ MEMORY LEAK FIX: Move statusHistory ke Redis untuk prevent memory heap growth
                    if (!this.statusHistory) {
                        this.statusHistory = new Map(); // msgId -> array of statuses (fallback jika Redis tidak ada)
                    }
                    
                    // ✅ FIXED: Cleanup old entries dari statusHistory setiap 200 entries (prevent memory leak)
                    // ✅ OPTIMIZED: Reduced threshold untuk prevent memory heap growth
                    if (this.statusHistory.size > 200) {
                        // ✅ OPTIMIZED: Cleanup old entries (non-blocking dengan setImmediate)
                        setImmediate(() => {
                            // Keep only last 150 entries (remove oldest 50)
                            const entries = Array.from(this.statusHistory.entries());
                            const toDelete = entries.slice(0, entries.length - 150);
                            for (const [key] of toDelete) {
                                this.statusHistory.delete(key);
                            }
                        });
                    }
                    
                    if (!this.statusHistory.has(msgId)) {
                        this.statusHistory.set(msgId, []);
                    }
                    const statusArray = this.statusHistory.get(msgId);
                    // Mirror key uppercase — SEND-VERIFY lookup case-insensitive
                    const msgIdUpper = String(msgId).toUpperCase();
                    if (msgIdUpper !== msgId) {
                        if (!this.statusHistory.has(msgIdUpper)) {
                            this.statusHistory.set(msgIdUpper, statusArray);
                        }
                    }
                    
                    // Tambahkan status ke array — DELIVERY_ACK (3) = DELIVERED = ACK
                    const statusName = st === 2 ? 'SERVER_ACK' : st === 3 ? 'DELIVERY_ACK' : st === 4 ? 'READ' : st === 5 ? 'PLAYED' : `UNKNOWN(${st})`;
                    if (!statusArray.includes(statusName)) {
                        statusArray.push(statusName);
                    }
                    if (st === 3 && !statusArray.includes('DELIVERED')) {
                        statusArray.push('DELIVERED');
                    }
                    
                    // Log array status untuk debugging
                    console.log(`📊 [STATUS ARRAY] msgId: ${msgId}, statuses: [${statusArray.join(', ')}]`);
                    console.log(`📊 [STATUS ARRAY JSON]`, JSON.stringify({
                        msgId: msgId,
                        statusArray: statusArray,
                        currentStatus: statusName,
                        statusNumber: st,
                        allStatuses: statusArray
                    }));
                    
                    try {
                        // SERVER_ACK dari WA (fromMe): jangan block di sini — race dengan registerOutboundMessageSync
                        // sudah ditangani sync register; enqueueWebhook pakai original_from_whatsapp + skip bot-hantu.
                        this.lastMessagesUpdateAt = Date.now();
                        this.lastEventAt = Date.now();

                        if (st === 2) {
                            // Thin pipe: forward SERVER_ACK asli WA → webhook (dedupe 30s di atas)
                            if (!msgId || typeof msgId !== 'string') continue;
                            const msgIdTrimmed = msgId.trim();
                            if (msgIdTrimmed.length < 10 || !/^[A-Z0-9]+$/i.test(msgIdTrimmed)) continue;
                            // Deaf-reset hanya bila fromMe + outbound API (bukan history sync)
                            if (fromMe) {
                                const trackedAck = await this.hasSentMessageId(msgIdTrimmed).catch(() => false);
                                if (trackedAck) {
                                    this.noteOutboundAckEquivalent('SERVER_ACK');
                                    this.cancelServerAckGuarantee(msgIdTrimmed);
                                }
                            }
                            const cleanBotNumber = this.getCleanBotNumber();
                            const claimed = await this.tryClaimServerAck(msgIdTrimmed);
                            if (!claimed) {
                                console.log(`⏭️ [SERVER_ACK] Already claimed, skip WA duplicate msgId=${msgIdTrimmed}`);
                                continue;
                            }
                            this.markSessionAckLiveRedis().catch(() => {});
                            this.enqueueWebhook({
                                event: 'SERVER_ACK',
                                bot: cleanBotNumber || this.sessionId,
                                bot_id: this.cachedBotId || 0,
                                sessionId: this.sessionId,
                                msg_id: msgIdTrimmed,
                                status: 'server_ack',
                                user_id: this.userId,
                                port: this.port,
                                original_from_whatsapp: true,
                            }, { forceDirect: true }).then((r) => {
                                if (!r || r.duplicate || r.skipped || (r.success && r.confirmed === false)) {
                                    this.releaseServerAckClaim(msgIdTrimmed);
                                }
                            }).catch((err) => {
                                this.releaseServerAckClaim(msgIdTrimmed);
                                console.error(`❌ [SERVER_ACK] enqueue failed for msgId ${msgIdTrimmed}:`, err?.message || err);
                            });
                            setImmediate(async () => {
                                try {
                                    this.ackCount = (this.ackCount || 0) + 1;
                                    const recipForAck = await this.getSentMessageRecipientKey(msgIdTrimmed);
                                    if (recipForAck) {
                                        await this.incrementRecipientAckCount(recipForAck);
                                    } else if (fromMe && key.remoteJid) {
                                        if (String(key.remoteJid).includes('@lid')) {
                                            lidJournalWarn(`⚠️ [LID] SERVER_ACK msgId=${msgIdTrimmed} remoteJid=${key.remoteJid} — recipient PN unresolved`);
                                        }
                                        await this.incrementRecipientAckCount(key.remoteJid);
                                    }
                                    if (this.redis) {
                                        const currentCount = await this.redis.get(this.ackCountRedisKey).catch(() => null);
                                        const newCount = (currentCount ? parseInt(currentCount, 10) : 0) + 1;
                                        await this.redis.set(this.ackCountRedisKey, newCount.toString(), { EX: 86400 * 7 }).catch(() => {});
                                        this.ackCount = newCount;
                                    }
                                } catch (_) { /* ignore */ }
                            });
                            if (config.autoGenerate?.enabled) {
                                setImmediate(async () => {
                                    try {
                                        await new Promise((resolve) => setTimeout(resolve, config.autoGenerate?.delayMs || 100));
                                        const [messageSentConfirmed, messageSentOriginal] = await Promise.all([
                                            this.redis ? this.redis.get(`webhook:confirmed:${msgId}:MESSAGE_SENT`).catch(() => null) : null,
                                            this.redis ? this.redis.get(`webhook:confirmed:${msgId}:MESSAGE_SENT:original`).catch(() => null) : null,
                                        ]);
                                        if (messageSentConfirmed || messageSentOriginal) return;
                                        this.enqueueWebhook({
                                            event: 'MESSAGE_SENT',
                                            bot: cleanBotNumber,
                                            bot_id: this.cachedBotId || 0,
                                            sessionId: this.sessionId,
                                            msg_id: msgId,
                                            status: 'message_sent',
                                            user_id: this.userId,
                                            port: this.port,
                                            auto_generated: true,
                                        }, { forceDirect: false }).catch(() => {});
                                    } catch (_) { /* ignore */ }
                                });
                            }
                            // Timeout auto-DELIVERED OFF — billing via DELIVERED/DEVICE_SENT asli WA saja
                        } else if (st === 3) {
                            // ✅ DELIVERY_ACK (status 3) = DELIVERED (pesan sampai ke penerima)
                            // Ini adalah status yang paling penting untuk billing!
                            // Ketika status 3 diterima, artinya pesan sudah sampai ke device penerima
                            // ✅ CRITICAL: Kirim dengan guarantee delivery mechanism
                            console.log(`\n📤 [DELIVERED] ===========================================`);
                            console.log(`📤 [DELIVERED] Processing DELIVERY_ACK (status 3) → DELIVERED webhook`);
                            console.log(`📤 [DELIVERED] msgId: ${msgId}`);
                            console.log(`📤 [DELIVERED] SessionId: ${this.sessionId}`);
                            console.log(`📤 [DELIVERED] fromMe: ${fromMe}`);
                            console.log(`📤 [DELIVERED] Timestamp: ${new Date().toISOString()}`);
                            console.log(`📤 [DELIVERED] ===========================================\n`);
                            
                            // ✅ SECURITY CHECK: Hanya process status update untuk pesan yang dikirim dari bot ini
                            // Prevent "bot hantu" yang kirim pesan tapi tidak ter-track
                            // ✅ NOTE: Kita lebih permissive untuk DELIVERED - jika fromMe = true, berarti dari bot ini
                            // (bisa dari WhatsApp Web/Desktop, jadi tidak selalu ada di sentMessageIds)
                            
                            // Skip WA history sync — hanya billing untuk outbound API send (ada di sentMessageIds).
                            if (!fromMe) {
                                continue;
                            }
                            const hasTrackedOutbound = await this.hasSentMessageId(msgId).catch(() => false);
                            if (!hasTrackedOutbound) {
                                console.log(`⏭️ [SYNC-SKIP] Skip DELIVERED/DEVICE_SENT msgId=${msgId} — WA history sync, bukan kiriman API`);
                                continue;
                            }

                            // Pokok: DELIVERED (status 3) = ACK — reset deaf + batalkan guarantee
                            this.noteOutboundAckEquivalent('DELIVERED');
                            this.cancelServerAckGuarantee(msgId);

                            const deliveredRemoteJid = key.remoteJid || await this.getSentMessageRemoteJid(msgId);
                            this.markDeliveredPathWarm(deliveredRemoteJid).then(() => {
                                this.maybeEmitFallbackServerAck(msgId, deliveredRemoteJid, 'DELIVERED').catch(() => {});
                                this.flushPendingServerAckForRecipient(deliveredRemoteJid, 30).catch(() => {});
                            }).catch(() => {});
                            
                            // ✅ CRITICAL BILLING: Parallel send untuk DELIVERED dan DEVICE_SENT
                            // Kedua webhook ini WAJIB terkirim untuk billing!
                            // Pakai forceDirect dengan guarantee delivery mechanism
                            // ✅ FIX: Status 3 (DELIVERY_ACK) datang langsung dari WhatsApp, jadi terpercaya
                            // Tambahkan flag original_from_whatsapp untuk skip validasi bot hantu
                            console.log(`💰 [BILLING] Sending DELIVERED + DEVICE_SENT for msgId: ${msgId} (original from WhatsApp)`);
                            
                            // ✅ CRITICAL: Wrap dengan timeout dan ensure queue fallback
                            const cleanBotNumber = this.getCleanBotNumber();
                            
                            // ✅ CRITICAL: Pastikan DELIVERED dan DEVICE_SENT selalu dikirim dengan guarantee delivery
                            // ✅ FIX: Gunakan bot_id dari cache jika tersedia
                            const deliverPromise = Promise.race([
                                this.enqueueWebhook({
                                    event: 'DELIVERED', 
                                    bot: cleanBotNumber, // ✅ FIX: Hanya nomor WhatsApp (10-15 digits)
                                    bot_id: this.cachedBotId || 0, // ✅ FIX: Integer (cached atau 0)
                                    sessionId: this.sessionId, 
                                    msg_id: msgId, 
                                    status: 'delivered', 
                                    user_id: this.userId, 
                                    port: this.port,
                                    original_from_whatsapp: true // ✅ FIX: Status 3 dari WhatsApp, terpercaya - skip validasi bot hantu
                                }, { forceDirect: true }),
                                new Promise(resolve => setTimeout(() => resolve({ timeout: true, failed: true }), 10000))
                            ]).then((result) => {
                                if (result && result.failed) {
                                    // Timeout atau error, queue fallback
                                    console.warn(`⚠️ [BILLING] DELIVERED direct send timeout/error for msgId ${msgId}, using queue fallback`);
                                    return this.enqueueWebhook({
                                        event: 'DELIVERED', 
                                        bot: cleanBotNumber,
                                        bot_id: this.cachedBotId || 0,
                                        sessionId: this.sessionId, 
                                        msg_id: msgId, 
                                        status: 'delivered', 
                                        user_id: this.userId, 
                                        port: this.port,
                                        original_from_whatsapp: true
                                    }, { forceDirect: false }).catch(e => {
                                        console.error(`❌ [BILLING] Queue fallback also failed for DELIVERED:`, e.message);
                                        return { failed: true };
                                    });
                                }
                                return result;
                            }).catch(err => {
                                console.error(`❌ [BILLING] DELIVERED webhook FAILED for msgId ${msgId}:`, err.message);
                                // ✅ CRITICAL: Jika error, langsung queue fallback
                                return this.enqueueWebhook({
                                    event: 'DELIVERED', 
                                    bot: cleanBotNumber,
                                    bot_id: this.cachedBotId || 0,
                                    sessionId: this.sessionId, 
                                    msg_id: msgId, 
                                    status: 'delivered', 
                                    user_id: this.userId, 
                                    port: this.port,
                                    original_from_whatsapp: true
                                }, { forceDirect: false }).catch(e => {
                                    console.error(`❌ [BILLING] Queue fallback also failed for DELIVERED:`, e.message);
                                    return { failed: true };
                                });
                            });
                            
                            const deviceSentPromise = Promise.race([
                                this.enqueueWebhook({
                                    event: 'DEVICE_SENT', 
                                    bot: cleanBotNumber, // ✅ FIX: Hanya nomor WhatsApp (10-15 digits)
                                    bot_id: this.cachedBotId || 0, // ✅ FIX: Integer (cached atau 0)
                                    sessionId: this.sessionId, 
                                    msg_id: msgId, 
                                    status: 'device_sent', 
                                    user_id: this.userId, 
                                    port: this.port,
                                    original_from_whatsapp: true // ✅ FIX: Status 3 dari WhatsApp, terpercaya - skip validasi bot hantu
                                }, { forceDirect: true }),
                                new Promise(resolve => setTimeout(() => resolve({ timeout: true, failed: true }), 10000))
                            ]).then((result) => {
                                if (result && result.failed) {
                                    // Timeout atau error, queue fallback
                                    console.warn(`⚠️ [BILLING] DEVICE_SENT direct send timeout/error for msgId ${msgId}, using queue fallback`);
                                    return this.enqueueWebhook({
                                        event: 'DEVICE_SENT', 
                                        bot: cleanBotNumber,
                                        bot_id: this.cachedBotId || 0,
                                        sessionId: this.sessionId, 
                                        msg_id: msgId, 
                                        status: 'device_sent', 
                                        user_id: this.userId, 
                                        port: this.port,
                                        original_from_whatsapp: true
                                    }, { forceDirect: false }).catch(e => {
                                        console.error(`❌ [BILLING] Queue fallback also failed for DEVICE_SENT:`, e.message);
                                        return { failed: true };
                                    });
                                }
                                return result;
                            }).catch(err => {
                                console.error(`❌ [BILLING] DEVICE_SENT webhook FAILED for msgId ${msgId}:`, err.message);
                                // ✅ CRITICAL: Jika error, langsung queue fallback
                                return this.enqueueWebhook({
                                    event: 'DEVICE_SENT', 
                                    bot: cleanBotNumber,
                                    bot_id: this.cachedBotId || 0,
                                    sessionId: this.sessionId, 
                                    msg_id: msgId, 
                                    status: 'device_sent', 
                                    user_id: this.userId, 
                                    port: this.port,
                                    original_from_whatsapp: true
                                }, { forceDirect: false }).catch(e => {
                                    console.error(`❌ [BILLING] Queue fallback also failed for DEVICE_SENT:`, e.message);
                                    return { failed: true };
                                });
                            });
                            
                            // ✅ CRITICAL: Wait untuk hasil dan pastikan kedua webhook terkirim
                            // Timeout lebih pendek untuk detect cepat jika gagal
                            Promise.all([
                                Promise.race([deliverPromise, new Promise(r => setTimeout(() => r({ timeout: true, failed: true }), 20000))]),
                                Promise.race([deviceSentPromise, new Promise(r => setTimeout(() => r({ timeout: true, failed: true }), 20000))])
                            ]).then(async ([delivered, deviceSent]) => {
                                // Check timeout
                                if (delivered?.timeout || deviceSent?.timeout) {
                                    console.error(`⏱️ [TIMEOUT] Webhook timeout for msgId ${msgId} - DELIVERED: ${delivered?.timeout ? 'TIMEOUT' : 'OK'}, DEVICE_SENT: ${deviceSent?.timeout ? 'TIMEOUT' : 'OK'}`);
                                }
                                
                                if (delivered?.failed || delivered?.timeout) {
                                    console.error(`❌ [BILLING] DELIVERED webhook FAILED/TIMEOUT for msgId ${msgId} - ensuring queue fallback`);
                                    // ✅ CRITICAL: Pastikan masuk queue jika direct gagal
                                    const cleanBotNumber = this.getCleanBotNumber();
                                    this.enqueueWebhook({
                                        event: 'DELIVERED', 
                                        bot: cleanBotNumber, // ✅ FIX: Hanya nomor WhatsApp (10-15 digits)
                                        bot_id: 0, // ✅ FIX: Integer 0
                                        sessionId: this.sessionId, 
                                        msg_id: msgId, 
                                        status: 'delivered', 
                                        user_id: this.userId, 
                                        port: this.port
                                    }, { forceDirect: false }).then(r => {
                                        if (r?.queued) {
                                            console.log(`✅ [BILLING-FALLBACK] DELIVERED queued for msgId: ${msgId}`);
                                        }
                                    }).catch(e => console.error(`❌ [BILLING] Queue fallback failed for DELIVERED msgId ${msgId}:`, e.message));
                                } else if (delivered?.confirmed || delivered?.success) {
                                    console.log(`✅ [DELIVERED] DELIVERED webhook confirmed for msgId: ${msgId}`);
                                    
                                    // ✅ AUTO-GENERATE: Jika dapat DELIVERED, cek SERVER_ACK dulu, generate jika belum ada
                                    // ✅ FIX: Selalu generate (tidak perlu tunggu 5 ACK) untuk memastikan semua pesan dapat lifecycle events
                                    if (config.autoGenerate?.enabled) {
                                        setImmediate(async () => {
                                            try {
                                                await new Promise(resolve => setTimeout(resolve, config.autoGenerate?.delayMs || 100));
                                                if (this.redis) {
                                                    const cleanBotNumber = this.getCleanBotNumber();
                                                    
                                                    // ✅ DEDUPE CHECK: Check apakah SERVER_ACK sudah ada (original atau auto-generated) - prevent bentrok
                                                    const hasOrganicAck = await this.hasOrganicServerAckMarker(msgId);
                                                    
                                                    // ✅ CRITICAL: Generate SERVER_ACK dulu jika belum ada (cek ACK dulu sebelum generate DELIVERED)
                                                    if (!hasOrganicAck) {
                                                        const minAck = config.autoGenerate?.minAckCount || 5;
                                                        if ((this.ackCount || 0) < minAck) {
                                                            console.log(`⏭️ [AUTO-GEN] skip fill SERVER_ACK msgId=${msgId} ackCount=${this.ackCount} < ${minAck}`);
                                                        } else {
                                                            console.log(`🔄 [AUTO-GEN] DELIVERED received but SERVER_ACK missing (ACK count: ${this.ackCount}), generating SERVER_ACK first for msgId: ${msgId}`);
                                                            this.enqueueWebhook({
                                                                event: 'SERVER_ACK',
                                                                bot: cleanBotNumber,
                                                                bot_id: this.cachedBotId || 0,
                                                                sessionId: this.sessionId,
                                                                msg_id: msgId,
                                                                status: 'server_ack',
                                                                user_id: this.userId,
                                                                port: this.port,
                                                                auto_generated: true
                                                            }, { forceDirect: false }).catch(() => {});
                                                        }
                                                    } else {
                                                        console.log(`⏭️ [AUTO-GEN] SERVER_ACK already exists for msgId ${msgId}, skipping auto-generate (prevent bentrok)`);
                                                    }
                                                    
                                                    // ✅ DEDUPE CHECK: Check apakah DEVICE_SENT sudah ada - prevent bentrok
                                                    const [deviceSentConfirmed, deviceSentOriginal] = await Promise.all([
                                                        this.redis.get(`webhook:confirmed:${msgId}:DEVICE_SENT`).catch(() => null),
                                                        this.redis.get(`webhook:confirmed:${msgId}:DEVICE_SENT:original`).catch(() => null)
                                                    ]);
                                                    
                                                    // Generate DEVICE_SENT jika belum ada
                                                    if (!deviceSentConfirmed && !deviceSentOriginal && (deviceSent?.failed || deviceSent?.timeout)) {
                                                        console.log(`🔄 [AUTO-GEN] DELIVERED received, generating DEVICE_SENT for msgId: ${msgId}`);
                                                        this.enqueueWebhook({
                                                            event: 'DEVICE_SENT',
                                                            bot: cleanBotNumber,
                                                            bot_id: this.cachedBotId || 0,
                                                            sessionId: this.sessionId,
                                                            msg_id: msgId,
                                                            status: 'device_sent',
                                                            user_id: this.userId,
                                                            port: this.port,
                                                            auto_generated: true
                                                        }, { forceDirect: false }).catch(() => {});
                                                    } else if (deviceSentConfirmed || deviceSentOriginal) {
                                                        console.log(`⏭️ [AUTO-GEN] DEVICE_SENT already exists for msgId ${msgId}, skipping auto-generate (prevent bentrok)`);
                                                    }
                                                }
                                            } catch (e) {
                                                // Ignore error, non-blocking
                                            }
                                        });
                                    }
                                } else {
                                    console.log(`📤 [DELIVERED] DELIVERED webhook queued/processing for msgId: ${msgId}`);
                                }
                                
                                if (deviceSent?.failed || deviceSent?.timeout) {
                                    console.error(`❌ [BILLING] DEVICE_SENT webhook FAILED/TIMEOUT for msgId ${msgId} - ensuring queue fallback`);
                                    // ✅ CRITICAL: Pastikan masuk queue jika direct gagal
                                    const cleanBotNumber = this.getCleanBotNumber();
                                    this.enqueueWebhook({
                                        event: 'DEVICE_SENT', 
                                        bot: cleanBotNumber, // ✅ FIX: Hanya nomor WhatsApp (10-15 digits)
                                        bot_id: 0, // ✅ FIX: Integer 0
                                        sessionId: this.sessionId, 
                                        msg_id: msgId, 
                                        status: 'device_sent', 
                                        user_id: this.userId, 
                                        port: this.port
                                    }, { forceDirect: false }).then(r => {
                                        if (r?.queued) {
                                            console.log(`✅ [BILLING-FALLBACK] DEVICE_SENT queued for msgId: ${msgId}`);
                                        }
                                    }).catch(e => console.error(`❌ [BILLING] Queue fallback failed for DEVICE_SENT msgId ${msgId}:`, e.message));
                                } else if (deviceSent?.confirmed || deviceSent?.success) {
                                    console.log(`✅ [DEVICE_SENT] DEVICE_SENT webhook confirmed for msgId: ${msgId}`);
                                } else {
                                    console.log(`📤 [DEVICE_SENT] DEVICE_SENT webhook queued/processing for msgId: ${msgId}`);
                                }
                                
                                // ✅ CRITICAL: Log summary untuk tracking billing
                                // Pastikan kita cek semua kemungkinan status
                                const deliveredConfirmed = delivered?.confirmed || delivered?.success;
                                const deviceSentConfirmed = deviceSent?.confirmed || deviceSent?.success;
                                const deliveredQueued = delivered?.queued || (!delivered?.failed && !delivered?.timeout && !deliveredConfirmed);
                                const deviceSentQueued = deviceSent?.queued || (!deviceSent?.failed && !deviceSent?.timeout && !deviceSentConfirmed);
                                
                                const deliveredOk = deliveredConfirmed || deliveredQueued;
                                const deviceSentOk = deviceSentConfirmed || deviceSentQueued;
                                
                                if (deliveredConfirmed && deviceSentConfirmed) {
                                    console.log(`💰 [BILLING] ✅ Both DELIVERED + DEVICE_SENT CONFIRMED for billing msgId: ${msgId}`);
                                } else if (deliveredOk && deviceSentOk) {
                                    console.warn(`💰 [BILLING] ⚠️ Both DELIVERED + DEVICE_SENT queued/processing for billing msgId: ${msgId} (waiting for confirmation)`);
                                } else {
                                    console.error(`⚠️ [BILLING] ⚠️ MISSING webhook for billing msgId: ${msgId} - DELIVERED: ${deliveredConfirmed ? 'CONFIRMED' : deliveredQueued ? 'QUEUED' : (delivered?.failed ? 'FAILED' : delivered?.timeout ? 'TIMEOUT' : 'UNKNOWN')}, DEVICE_SENT: ${deviceSentConfirmed ? 'CONFIRMED' : deviceSentQueued ? 'QUEUED' : (deviceSent?.failed ? 'FAILED' : deviceSent?.timeout ? 'TIMEOUT' : 'UNKNOWN')}`);
                                    
                                    // ✅ CRITICAL: Final fallback - jika ada yang tidak OK, PASTIKAN queue keduanya
                                    // Double-check dengan Redis confirmation
                                    if (!deliveredConfirmed && !deliveredQueued) {
                                        console.error(`💀 [CRITICAL-FINAL] DELIVERED not confirmed/queued for msgId ${msgId}, FORCING queue with retry`);
                                        // Retry sampai berhasil masuk queue
                                        let queueRetries = 0;
                                        const tryQueueDelivered = async () => {
                                            queueRetries++;
                                            try {
                                                const cleanBotNumber = this.getCleanBotNumber();
                                                const r = await this.enqueueWebhook({
                                                    event: 'DELIVERED', 
                                                    bot: cleanBotNumber, // ✅ FIX: Hanya nomor WhatsApp (10-15 digits)
                                                    bot_id: 0, // ✅ FIX: Integer 0
                                                    sessionId: this.sessionId, 
                                                    msg_id: msgId, 
                                                    status: 'delivered', 
                                                    user_id: this.userId, 
                                                    port: this.port
                                                }, { forceDirect: false });
                                                if (r?.queued) {
                                                    console.log(`✅ [CRITICAL-FINAL] DELIVERED forced to queue for msgId: ${msgId} (attempt ${queueRetries})`);
                                                    return true;
                                                } else if (queueRetries < 3) {
                                                    await new Promise(res => setTimeout(res, 500 * queueRetries));
                                                    return tryQueueDelivered();
                                                }
                                            } catch (e) {
                                                if (queueRetries < 3) {
                                                    await new Promise(res => setTimeout(res, 500 * queueRetries));
                                                    return tryQueueDelivered();
                                                } else {
                                                    console.error(`💀 [CRITICAL] All queue retries failed for DELIVERED msgId ${msgId}`);
                                                }
                                            }
                                        };
                                        tryQueueDelivered();
                                    }
                                    
                                    if (!deviceSentConfirmed && !deviceSentQueued) {
                                        console.error(`💀 [CRITICAL-FINAL] DEVICE_SENT not confirmed/queued for msgId ${msgId}, FORCING queue with retry`);
                                        // Retry sampai berhasil masuk queue
                                        let queueRetries = 0;
                                        const tryQueueDeviceSent = async () => {
                                            queueRetries++;
                                            try {
                                                const cleanBotNumber = this.getCleanBotNumber();
                                                const r = await this.enqueueWebhook({
                                                    event: 'DEVICE_SENT', 
                                                    bot: cleanBotNumber, // ✅ FIX: Hanya nomor WhatsApp (10-15 digits)
                                                    bot_id: 0, // ✅ FIX: Integer 0
                                                    sessionId: this.sessionId, 
                                                    msg_id: msgId, 
                                                    status: 'device_sent', 
                                                    user_id: this.userId, 
                                                    port: this.port
                                                }, { forceDirect: false });
                                                if (r?.queued) {
                                                    console.log(`✅ [CRITICAL-FINAL] DEVICE_SENT forced to queue for msgId: ${msgId} (attempt ${queueRetries})`);
                                                    return true;
                                                } else if (queueRetries < 3) {
                                                    await new Promise(res => setTimeout(res, 500 * queueRetries));
                                                    return tryQueueDeviceSent();
                                                }
                                            } catch (e) {
                                                if (queueRetries < 3) {
                                                    await new Promise(res => setTimeout(res, 500 * queueRetries));
                                                    return tryQueueDeviceSent();
                                                } else {
                                                    console.error(`💀 [CRITICAL] All queue retries failed for DEVICE_SENT msgId ${msgId}`);
                                                }
                                            }
                                        };
                                        tryQueueDeviceSent();
                                    }
                                }
                            }).catch(err => {
                                console.error(`💀 [CRITICAL] Error tracking webhook results for msgId ${msgId}:`, err.message);
                                // ✅ CRITICAL: Fallback queue jika tracking juga error
                                console.error(`💀 [CRITICAL] Enqueueing both webhooks to queue as fallback for msgId ${msgId}`);
                                this.enqueueWebhook({
                                    event: 'DELIVERED', bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                                    sessionId: this.sessionId, msg_id: msgId, status: 'delivered', user_id: this.userId, port: this.port
                                }, { forceDirect: false }).catch(() => {});
                                this.enqueueWebhook({
                                    event: 'DEVICE_SENT', bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                                    sessionId: this.sessionId, msg_id: msgId, status: 'device_sent', user_id: this.userId, port: this.port
                                }, { forceDirect: false }).catch(() => {});
                            });
                            
                            // ✅ CRITICAL: Double-check confirmation setelah 2 detik untuk pastikan tidak ada yang terlewat
                            // Jika setelah 2 detik belum confirmed, paksa masuk queue
                            setTimeout(async () => {
                                try {
                                if (this.redis) {
                                    try {
                                        const [deliveredConfirmed, deviceSentConfirmed] = await Promise.all([
                                            this.redis.get(`webhook:confirmed:${msgId}:DELIVERED`),
                                            this.redis.get(`webhook:confirmed:${msgId}:DEVICE_SENT`)
                                            ]).catch(() => [null, null]);
                                        
                                        if (!deliveredConfirmed) {
                                            console.warn(`⚠️ [DELAYED-CHECK] DELIVERED not confirmed after 2s for msgId ${msgId}, forcing to queue`);
                                            this.enqueueWebhook({
                                                event: 'DELIVERED', bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                                                sessionId: this.sessionId, msg_id: msgId, status: 'delivered', user_id: this.userId, port: this.port
                                            }, { forceDirect: false }).catch(() => {});
                                        }
                                        
                                        if (!deviceSentConfirmed) {
                                            console.warn(`⚠️ [DELAYED-CHECK] DEVICE_SENT not confirmed after 2s for msgId ${msgId}, forcing to queue`);
                                            this.enqueueWebhook({
                                                event: 'DEVICE_SENT', bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                                                sessionId: this.sessionId, msg_id: msgId, status: 'device_sent', user_id: this.userId, port: this.port
                                            }, { forceDirect: false }).catch(() => {});
                                        }
                                    } catch (checkErr) {
                                        // Ignore check error, webhook sudah dikirim
                                    }
                                    }
                                } catch (error) {
                                    // ✅ FIXED: Catch semua error di setTimeout async
                                    console.error(`❌ [DELAYED-CHECK] Error in delayed check for msgId ${msgId}:`, error.message);
                                    // Jangan throw, biarkan continue
                                }
                            }, 2000); // Check setelah 2 detik
                            
                            // Set lifecycle markers in Redis (async)
                            if (this.redis) {
                                Promise.all([
                                    this.redis.set(`lifecycle:delivered:${msgId}`, '1', { EX: 86400 }),
                                    this.redis.set(`lifecycle:device_sent:${msgId}`, '1', { EX: 86400 }),
                                    // ✅ CRITICAL: Cancel timeout mechanism jika status 3 benar-benar datang
                                    this.redis.set(`delivered_timeout:${msgId}`, '1', { EX: 86400 }),
                                    this.redis.del(`delivered_check:${msgId}`)
                                ]).then(() => {
                                    console.log(`✅ [DELIVERED] Redis lifecycle markers set for msgId: ${msgId}`);
                                }).catch(() => {});
                            }
                            
                            this.onMessageSent(upd);
                            this.messageCount++;
                        } else if (st === 4) {
                            // READ = Pesan dibaca (juga = ACK setara)
                            console.log(`📖 [READ] Triggering READ webhook for msgId: ${msgId} (direct, non-blocking)`);
                            
                            // ✅ SECURITY CHECK: Hanya process status update untuk pesan yang dikirim dari bot ini
                            // Prevent "bot hantu" yang kirim pesan tapi tidak ter-track
                            // ✅ NOTE: Pesan masuk (!fromMe) tetap process untuk READ, tapi tidak perlu BACKFILL karena bukan pesan yang kita kirim
                            
                            // 🔧 CRITICAL FALLBACK: Check if SERVER_ACK/DELIVERED/DEVICE_SENT were sent, if not send them now
                            // This handles WhatsApp behavior where status 2 (SERVER_ACK) atau status 3 (DELIVERED) is skipped
                            // ✅ LOGIC: Jika ada READ, berarti pasti ada DELIVERED/DEVICE_SENT/SERVER_ACK (logically), jadi check dan backfill jika perlu
                            // ✅ SECURITY: Hanya BACKFILL untuk pesan yang dikirim dari bot ini (fromMe = true + Redis check)
                            // ✅ PER BOT: Setiap wrapper instance punya backfill logic sendiri
                            // ✅ REDIS-BASED: Check apakah pesan dikirim dari wrapper ini (prevent bot hantu)
                            if (fromMe) {
                                // ✅ SECURITY: Check apakah pesan benar-benar dari wrapper ini (Redis-based)
                                // Jangan reset deaf sebelum hasSent — history sync fromMe bisa mask OUTBOUND-DEAF
                                this.hasSentMessageId(msgId).then(async (hasMessage) => {
                                    if (!hasMessage) {
                                        console.warn(`⚠️ [BOT-HANTU-SKIP] Skipping BACKFILL for msgId ${msgId} (not sent from this wrapper - bot hantu detected)`);
                                        return;
                                    }
                                    this.noteOutboundAckEquivalent('READ');
                                    this.cancelServerAckGuarantee(msgId);
                                    const ackLive = await this.isSessionAckLiveForGuarantee();

                                    if (this.redis) {
                                        // Non-blocking backfill check
                                        this.hasOrganicServerAckMarker(msgId).then((hasOrganicAck) => {
                                        const serverAckDedupeKey = `${msgId}_2`;
                                        const hasServerAckInMemory = this.dedupeCache.get(serverAckDedupeKey) && this.dedupeCache.get(serverAckDedupeKey) > Date.now();
                                        
                                        if (ackLive && !hasOrganicAck && !hasServerAckInMemory) {
                                            console.log(`🔧 [BACKFILL-SERVER_ACK] SERVER_ACK missing for msgId: ${msgId}, generating now (triggered by READ)`);
                                            this.enqueueWebhook({
                                                event: 'SERVER_ACK',
                                                bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                                                sessionId: this.sessionId,
                                                msg_id: msgId,
                                                status: 'server_ack',
                                                user_id: this.userId,
                                                port: this.port,
                                                backfill_generated: true // Flag bahwa ini dari backfill (dipicu oleh READ)
                                            }, { forceDirect: true }).then(() => {
                                                this.dedupeCache.set(serverAckDedupeKey, Date.now() + 30000);
                                                console.log(`✅ [BACKFILL-SERVER_ACK] SERVER_ACK generated for msgId: ${msgId}`);
                                            }).catch(err => {
                                                console.error(`❌ [BACKFILL-SERVER_ACK] Failed for msgId ${msgId}:`, err.message);
                                            });
                                        }
                                        
                                        return Promise.all([
                                            this.redis.get(`lifecycle:delivered:${msgId}`),
                                            this.redis.get(`lifecycle:device_sent:${msgId}`)
                                        ]);
                                        }).then(([deliveredSent, deviceSent]) => {
                                            if (!deliveredSent) {
                                                console.log(`🔧 [BACKFILL] DELIVERED missing for msgId: ${msgId}, sending now (WhatsApp skipped status 3)`);
                                                this.enqueueWebhook({
                                                    event: 'DELIVERED', bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                                                    sessionId: this.sessionId, msg_id: msgId, status: 'delivered', user_id: this.userId, port: this.port,
                                                    backfill_generated: true // Flag bahwa ini dari backfill
                                                }, { forceDirect: true }).catch(() => {});
                                                // ✅ CRITICAL: Cancel timeout mechanism jika backfill generate DELIVERED
                                                this.redis.set(`lifecycle:delivered:${msgId}`, '1', { EX: 86400 }).catch(() => {});
                                                this.redis.set(`delivered_timeout:${msgId}`, '1', { EX: 86400 }).catch(() => {});
                                                this.redis.del(`delivered_check:${msgId}`).catch(() => {});
                                            }
                                        }).catch(backfillErr => {
                                            console.error(`⚠️ [BACKFILL] Error during backfill check for msgId ${msgId}:`, backfillErr.message);
                                        });
                                    } else {
                                    // No Redis, check memory cache only
                                    const serverAckDedupeKey = `${msgId}_2`;
                                    const hasServerAckInMemory = this.dedupeCache.get(serverAckDedupeKey) && this.dedupeCache.get(serverAckDedupeKey) > Date.now();
                                    
                                    if (ackLive && !hasServerAckInMemory) {
                                        console.log(`🔧 [BACKFILL-SERVER_ACK] SERVER_ACK missing for msgId: ${msgId}, generating now (triggered by READ, no Redis)`);
                                        this.enqueueWebhook({
                                            event: 'SERVER_ACK',
                                            bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                                            sessionId: this.sessionId,
                                            msg_id: msgId,
                                            status: 'server_ack',
                                            user_id: this.userId,
                                            port: this.port,
                                            backfill_generated: true
                                        }, { forceDirect: true }).then(() => {
                                            this.dedupeCache.set(serverAckDedupeKey, Date.now() + 30000);
                                            console.log(`✅ [BACKFILL-SERVER_ACK] SERVER_ACK generated for msgId: ${msgId}`);
                                        }).catch(err => {
                                            console.error(`❌ [BACKFILL-SERVER_ACK] Failed for msgId ${msgId}:`, err.message);
                                        });
                                    }
                                }
                                }).catch(() => {
                                    // hasSentMessageId error, skip backfill
                                    console.warn(`⚠️ [BACKFILL] Error checking sentMessageIds for msgId ${msgId}, skipping backfill`);
                                });
                            }
                            
                            // ✅ READ webhook - non-blocking, langsung kirim
                            this.enqueueWebhook({
                                event: 'READ', bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                                sessionId: this.sessionId, msg_id: msgId, status: 'read', user_id: this.userId, port: this.port
                            }, { forceDirect: true }).catch(() => {});
                        } else if (st === 5) {
                            // PLAYED = Pesan diputar (audio/video)
                            console.log(`▶️ [PLAYED] Triggering PLAYED webhook for msgId: ${msgId}`);
                            this.enqueueWebhook({
                                event: 'PLAYED', bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                                sessionId: this.sessionId, msg_id: msgId, status: 'played', user_id: this.userId, port: this.port
                            }, { forceDirect: true }).catch(() => {});
                        } else if (st !== undefined && st !== null) {
                            console.warn(`⚠️ [MESSAGES.UPDATE] Unknown status ${st} for msgId: ${msgId}`);
                        }
                    } catch (e) {
                        // ✅ CRITICAL: Log error dengan detail untuk debugging
                        console.error(`❌ [MESSAGES.UPDATE] Error processing status ${st} for msgId ${msgId}:`, e.message);
                        console.error(`❌ [MESSAGES.UPDATE] Error stack:`, e.stack);
                        
                        // ✅ CRITICAL: Jika ini SERVER_ACK (st === 2), pastikan tetap masuk queue meskipun ada error
                        if (st === 2 && msgId) {
                            // ✅ VALIDASI: Pastikan message ID valid sebelum kirim emergency queue
                            const msgIdTrimmed = (typeof msgId === 'string' ? msgId.trim() : String(msgId || ''));
                            if (msgIdTrimmed.length >= 10 && /^[A-Z0-9]+$/i.test(msgIdTrimmed)) {
                                const claimed = await this.tryClaimServerAck(msgIdTrimmed);
                                if (!claimed) {
                                    console.log(`⏭️ [SERVER_ACK] Emergency skip, already claimed msgId=${msgIdTrimmed}`);
                                } else {
                                console.warn(`⚠️ [SERVER_ACK] Error in handler, attempting emergency queue for msgId ${msgIdTrimmed}`);
                                const cleanBotNumber = this.getCleanBotNumber();
                                this.enqueueWebhook({
                                    event: 'SERVER_ACK',
                                    bot: cleanBotNumber || this.sessionId,
                                    bot_id: this.cachedBotId || 0,
                                    sessionId: this.sessionId,
                                    msg_id: msgIdTrimmed,
                                    status: 'server_ack',
                                    user_id: this.userId,
                                    port: this.port,
                                    original_from_whatsapp: true,
                                }, { forceDirect: false }).catch((emergencyErr) => {
                                    console.error(`💀 [SERVER_ACK] Emergency queue also failed for msgId ${msgIdTrimmed}:`, emergencyErr.message);
                                });
                                }
                            } else {
                                console.error(`❌ [SERVER_ACK] Invalid message ID in catch block: ${msgId}, cannot send emergency queue`);
                            }
                        }
                    }
                }
                } catch (error) {
                    // ✅ FIXED: Comprehensive error handling untuk prevent crash
                    console.error(`❌ [MESSAGES.UPDATE] Error processing updates in session ${this.sessionId}:`, error.message);
                    console.error(`❌ [MESSAGES.UPDATE] Error stack:`, error.stack);
                    // Jangan throw, biarkan process continue
                }
            });
            
            // ============================================================================
            // ?? CONNECTION UPDATE HANDLER - MUST BE AFTER messages.update
            // ============================================================================
            // ✅ FIXED: Connection update dengan comprehensive error handling
            this.sock.ev.on('connection.update', async (update) => {
                try {
                // ✅ SUPER SMART: Update last activity time (untuk cleanup tracking)
                this.lastActivityTime = Date.now();
                
                const { connection, lastDisconnect, qr } = update;
                // fork-fix 2026-09-07: notifikasi reachout timelock (NACK 463) dari messages-recv.js yang ditambal
                if (update && update.reachoutTimeLock && typeof update.reachoutTimeLock === 'object') {
                    try { this.applyReachoutTimeLock(update.reachoutTimeLock); } catch (_) {}
                }
                
                    // ✅ FIXED: Non-blocking console.log untuk prevent blocking di critical path
                    setImmediate(() => console.log(`?? Connection update for ${this.sessionId}:`, connection));
                
                const pairingModeActive = this.isPairingMode && this.requestedPhoneNumber;
                
                // ✅ DEBUG: Log pairing mode status di connection.update
                if (pairingModeActive) {
                    // fleet-quiet 2026-09-07: connection.update terpancar sangat sering saat pairing — catat maks 1x/60s per sesi.
                    const nowPairLog = Date.now();
                    if (!this._lastPairingActiveLogAt || nowPairLog - this._lastPairingActiveLogAt > 60000) {
                        this._lastPairingActiveLogAt = nowPairLog;
                        setImmediate(() => console.log(`📱 [PAIRING] Pairing mode ACTIVE in connection.update for session ${this.sessionId}`));
                    }
                }
                
                // ? QR CODE GENERATED (skip untuk pairing method)
                if (qr && !pairingModeActive) {
                    // ✅ ANTI-BLOCK: Check limit untuk QR generation
                    const now = Date.now();
                    const oneHourAgo = now - (60 * 60 * 1000);
                    
                    // Reset counter jika sudah lebih dari 1 jam
                    if (this.qrGenerationResetTime < oneHourAgo) {
                        this.qrGenerationCount = 0;
                        this.qrGenerationResetTime = now;
                    }
                    
                    // Check limit
                    if (this.qrGenerationCount >= this.maxQrGenerationsPerHour) {
                        setImmediate(() => console.warn(`⚠️ [ANTI-BLOCK] QR generation limit reached (${this.maxQrGenerationsPerHour}/hour), please wait before next QR`));
                        return; // Skip QR generation, sudah mencapai limit
                    }
                    
                    // Increment counter
                    this.qrGenerationCount++;
                    
                    setImmediate(() => console.log(`📱 QR code generated for session ${this.sessionId} (${this.qrGenerationCount}/${this.maxQrGenerationsPerHour} this hour)`));
                    this.qr = qr;
                    
                    // Generate QR code as base64 image
                    try {
                        const qrImage = await qrcode.toDataURL(qr);
                        this.qrImage = qrImage;
                        
                        // ⏰ START QR TIMEOUT - Auto-close 2 menit jika tidak berhasil scan
                        // ✅ FIX: Hanya timeout jika socket belum open (jika sudah open, berarti QR sudah di-scan)
                        if (!this.pairingTimeout) {
                            this.pairingTimeout = setTimeout(() => {
                                // ✅ CRITICAL FIX: Check socket state - jika sudah open, berarti QR sudah di-scan, jangan timeout
                                const socketOpen = isBaileysWebSocketOpen(this.sock);
                                
                                if (!this.isAuthenticated && !socketOpen) {
                                    // ✅ Hanya timeout jika belum authenticated DAN socket belum open
                                    setImmediate(() => {
                                    console.log(`⏰ QR TIMEOUT (${Math.round(PAIRING_TIMEOUT_DURATION / 1000)}s) - Session ${this.sessionId} tidak berhasil scan`);
                                    console.log(`🔌 Auto-closing socket and cleaning up...`);
                                    });
                                    
                                    try {
                                        // Close WebSocket
                                        if (this.sock && this.sock.ws) {
                                            this.sock.ws.close();
                                        }
                                        
                                        // Send timeout event to webhook
                                        this.enqueueWebhook({
                                            event: 'QR_TIMEOUT',
                                            sessionId: this.sessionId,
                                            bot: this.sessionId,
                                            user_id: this.userId,
                                            reason: `QR code tidak di-scan dalam ${Math.round(PAIRING_TIMEOUT_DURATION / 1000)} detik`,
                                            timestamp: new Date().toISOString()
                                        }).catch(err => console.error('Webhook error:', err.message));
                                        
                                    } catch (error) {
                                        console.error(`Error during timeout cleanup:`, error.message);
                                    }
                                } else if (socketOpen && !this.isAuthenticated) {
                                    // ✅ Socket sudah open tapi belum authenticated - extend timeout (masih dalam proses authentication)
                                    setImmediate(() => console.log(`⏳ [QR] Socket OPEN, authentication in progress, extending timeout...`));
                                    // ✅ Clear timeout lama, set timeout baru untuk batas pairing yang sama
                                    if (this.pairingTimeout) {
                                        clearTimeout(this.pairingTimeout);
                                        this.pairingTimeout = null;
                                    }
                                    // ✅ Set timeout baru untuk authentication
                                    this.pairingTimeout = setTimeout(() => {
                                        if (this.shouldKillIncompletePairing() || !this.isAuthenticated) {
                                            setImmediate(() => {
                                            console.log(`⏰ AUTH TIMEOUT (${Math.round(PAIRING_TIMEOUT_DURATION / 1000)}s) - Session ${this.sessionId} authentication tidak selesai`);
                                            console.log(`🔌 Auto-closing socket and cleaning up...`);
                                            });
                                            
                                            try {
                                                if (this.sock && this.sock.ws) {
                                                    this.sock.ws.close();
                                                }
                                                
                                                this.enqueueWebhook({
                                                    event: 'AUTH_TIMEOUT',
                                                    sessionId: this.sessionId,
                                                    bot: this.sessionId,
                                                    user_id: this.userId,
                                                    reason: `Authentication tidak selesai dalam ${Math.round(PAIRING_TIMEOUT_DURATION / 1000)} detik setelah QR scan`,
                                                    timestamp: new Date().toISOString()
                                                }).catch(err => console.error('Webhook error:', err.message));
                                            } catch (error) {
                                                console.error(`Error during auth timeout cleanup:`, error.message);
                                            }
                                        }
                                    }, PAIRING_TIMEOUT_DURATION);
                                }
                            }, PAIRING_TIMEOUT_DURATION);
                            
                            setImmediate(() => console.log(`⏰ QR timeout set: Will auto-close in ${Math.round(PAIRING_TIMEOUT_DURATION / 1000)}s if not scanned`));
                        }
                        
                        // ? Callback ke parent dengan QR image
                        this.onQR(qrImage);
                        
                        // ✅ FIXED: Deduplication sudah di-handle di enqueueWebhook (prevent duplicate)
                        // ? Kirim ke webhook untuk store di database
                        await this.enqueueWebhook({
                            event: 'QR_CODE',
                            sessionId: this.sessionId,
                            bot: this.sessionId,
                            qr: qrImage,
                            user_id: this.userId,
                            timeoutIn: '3 minutes'
                        });
                        
                    } catch (error) {
                        console.error('Error generating QR image:', error?.message || 'Unknown error');
                    }
                }
                
                // Skrip git lama + forum Baileys: SATU kode per sesi, valid ~3 menit.
                // Ganti tiap QR 18s = kode di HP sudah mati, paste tidak nyambung.
                if (pairingModeActive && qr && !this.isAuthenticated) {
                    const now = Date.now();
                    this._lastPairingQr = qr;
                    // Tahan kode selama window pairing penuh (180s) — kalau lebih pendek,
                    // kode di layar bisa berganti saat user masih mengetik yang lama.
                    const holdMs = Math.max(60000, Math.min(180000, Number(process.env.PAIRING_CODE_HOLD_MS) || 180000));
                    const cooldownMs = Math.max(30000, Number(this.pairingCodeCooldown) || 45000);
                    const codeAge = this.pairingCodeGeneratedTime ? (now - this.pairingCodeGeneratedTime) : Number.POSITIVE_INFINITY;
                    const timeSinceLastRequest = now - this.lastPairingCodeRequest;

                    const socketOpenForHold = isBaileysWebSocketOpen(this.sock);
                    // Forum: SATU requestPairingCode per handshake. Socket masih OPEN + kode ada
                    // → jangan terbitkan kode baru (kode lama di HP langsung "Gagal menautkan").
                    if (this.pairingCode && (codeAge < holdMs || socketOpenForHold)) {
                        // fleet-quiet 2026-09-07: maks 1x/30s per sesi.
                        if (!this._lastKeepCodeLogAt || now - this._lastKeepCodeLogAt > 30000) {
                            this._lastKeepCodeLogAt = now;
                            setImmediate(() => console.log(`⏭️ [PAIRING] keep code age=${Math.round(codeAge / 1000)}s hold=${Math.round(holdMs / 1000)}s socket=${socketOpenForHold ? 'open' : 'down'}`));
                        }
                    } else if (timeSinceLastRequest < cooldownMs) {
                        setImmediate(() => console.warn(`⏳ [PAIRING] cooldown ${Math.ceil((cooldownMs - timeSinceLastRequest) / 1000)}s — keep showing current code`));
                    } else {
                    const phoneInfo = this.normalizePhoneNumber(this.requestedPhoneNumber);
                    setImmediate(() => console.log(`📱 QR event detected, requesting pairing code for: ${phoneInfo.raw}`));
                    
                    if (!phoneInfo.isValid) {
                        const invalidMsg = `Invalid phone format: ${phoneInfo.raw} -> ${phoneInfo.normalized || 'null'} (${phoneInfo.reason})`;
                        console.error(`❌ ${invalidMsg}`);
                        await this.enqueueWebhook({
                            event: 'PAIRING_CODE_ERROR',
                            sessionId: this.sessionId,
                            bot: this.sessionId,
                            phoneNumber: phoneInfo.raw,
                            rawPhoneNumber: phoneInfo.raw,
                            error: invalidMsg,
                            user_id: this.userId,
                            timestamp: new Date().toISOString()
                        }).catch(() => {});
                    } else {
                    
                    const normalizedPhone = phoneInfo.normalized;
                    
                    if (phoneInfo.appliedDefaultCountryCode && phoneInfo.defaultCodeUsed) {
                        setImmediate(() => console.log(`🌍 Applied default country code ${phoneInfo.defaultCodeUsed} -> ${normalizedPhone}`));
                    } else {
                        setImmediate(() => console.log(`🌍 Using international number: ${normalizedPhone}`));
                    }
                    
                    const sockWait = await this.waitForPairingSocketReady();
                    
                    try {
                        if (!sockWait.ok) {
                            throw new Error(
                                `Socket not ready for pairing code (${sockWait.reason}, waited ${sockWait.waitedMs}ms, wsState=${sockWait.wsState ?? 'n/a'})`
                            );
                        }
                        setImmediate(() => console.log(`✅ Socket OPEN after ${sockWait.waitedMs}ms — requesting pairing code`));
                        
                        // ✅ ANTI-BLOCK: Update last request time SEBELUM request (prevent race condition)
                        this.lastPairingCodeRequest = now;
                        
                        setImmediate(() => console.log(`📞 Requesting pairing code for: ${normalizedPhone}`));
                        
                        if (!this.sock || typeof this.sock.requestPairingCode !== 'function') {
                            throw new Error('Socket not initialized for pairing code request');
                        }
                        
                        // byee-wamod: requestPairingCode(phone) tanpa arg ke-2 default-nya "BAILLITE"
                        // (custom pair). null → kode acak Crockford yang unik per handshake.
                        const code = await this.sock.requestPairingCode(normalizedPhone, null);
                        const codeClean = String(code || '').replace(/[^A-Za-z0-9]/g, '').toUpperCase();
                        if (!codeClean || codeClean.length < 6 || codeClean === 'BAILLITE') {
                            throw new Error(`Invalid pairing code received: ${code}`);
                        }
                        
                        this.pairingCode = code;
                        this._pairingNeedRefresh = false;
                        this._pairingRefreshSoftCd = false;
                        // ✅ CRITICAL: Set timestamp saat pairing code di-generate (untuk prevent premature CONNECTED)
                        this.pairingCodeGeneratedTime = Date.now();
                        const cleanBeforeCode = this.getCleanBotNumber();
                        const wantDigits = String(normalizedPhone || '').replace(/\D/g, '');
                        this.pairingLeftoverCredsBlocked = Boolean(
                            cleanBeforeCode && wantDigits && cleanBeforeCode === wantDigits
                        );
                        this.pairingPasteAcknowledged = false;
                        if (this.pairingLeftoverCredsBlocked) {
                            console.warn(
                                `⚠️ [PAIRING] creds sudah auth sebelum kode — CONNECTED diblokir sampai paste ack session=${this.sessionId}`
                            );
                        }
                        
                        setImmediate(() => {
                        console.log(`✅ PAIRING CODE GENERATED: ${code}`);
                        console.log(`🔑 Code valid for 3 minutes — jangan request kode baru sebelum itu`);
                        console.log(`⏰ Pairing code generated at: ${new Date(this.pairingCodeGeneratedTime).toISOString()}`);
                        });
                        
                        // ⏰ START PAIRING TIMEOUT - Auto-close sesuai PAIRING_INCOMPLETE_TTL_MS jika tidak berhasil
                        // ✅ FIX: Hanya timeout jika socket belum open (jika sudah open, berarti pairing code sudah di-input)
                        if (!this.pairingTimeout) {
                            this.pairingTimeout = setTimeout(() => {
                                // ✅ CRITICAL FIX: Check socket state - jika sudah open, berarti pairing code sudah di-input, jangan timeout
                                const socketOpen = isBaileysWebSocketOpen(this.sock);
                                
                                if (this.shouldKillIncompletePairing()) {
                                    // Termasuk CONNECTED palsu (flag auth tanpa identity WA)
                                    setImmediate(() => {
                                    console.log(`⏰ PAIRING TIMEOUT (${Math.round(PAIRING_TIMEOUT_DURATION / 1000)}s) - Session ${this.sessionId} pairing anomaly / belum taut`);
                                    console.log(`🔌 Auto-closing socket and cleaning up...`);
                                    });
                                    
                                    try {
                                        // Close WebSocket
                                        if (this.sock && this.sock.ws) {
                                            this.sock.ws.close();
                                        }
                                        
                                        // Send timeout event to webhook
                                        this.enqueueWebhook({
                                            event: 'PAIRING_TIMEOUT',
                                            sessionId: this.sessionId,
                                            bot: this.sessionId,
                                            user_id: this.userId,
                                            reason: `Pairing tidak berhasil dalam ${Math.round(PAIRING_TIMEOUT_DURATION / 1000)} detik`,
                                            timestamp: new Date().toISOString()
                                        }).catch(err => console.error('Webhook error:', err.message));
                                        
                                        // Cleanup akan dilakukan otomatis via connection close event
                                    } catch (error) {
                                        console.error(`Error during timeout cleanup:`, error.message);
                                    }
                                } else if (socketOpen && !this.isAuthenticated) {
                                    // ✅ Socket sudah open tapi belum authenticated - extend timeout (masih dalam proses authentication)
                                    setImmediate(() => console.log(`⏳ [PAIRING] Socket OPEN, authentication in progress, extending timeout...`));
                                    // ✅ Clear timeout lama, set timeout baru untuk batas pairing yang sama
                                    if (this.pairingTimeout) {
                                        clearTimeout(this.pairingTimeout);
                                        this.pairingTimeout = null;
                                    }
                                    // ✅ Set timeout baru untuk authentication
                                    this.pairingTimeout = setTimeout(() => {
                                        if (this.shouldKillIncompletePairing() || !this.isAuthenticated) {
                                            setImmediate(() => {
                                            console.log(`⏰ AUTH TIMEOUT (${Math.round(PAIRING_TIMEOUT_DURATION / 1000)}s) - Session ${this.sessionId} pairing anomaly / authentication tidak selesai`);
                                            console.log(`🔌 Auto-closing socket and cleaning up...`);
                                            });
                                            
                                            try {
                                                if (this.sock && this.sock.ws) {
                                                    this.sock.ws.close();
                                                }
                                                
                                                this.enqueueWebhook({
                                                    event: 'AUTH_TIMEOUT',
                                                    sessionId: this.sessionId,
                                                    bot: this.sessionId,
                                                    user_id: this.userId,
                                                    reason: `Authentication tidak selesai dalam ${Math.round(PAIRING_TIMEOUT_DURATION / 1000)} detik setelah pairing code input`,
                                                    timestamp: new Date().toISOString()
                                                }).catch(err => console.error('Webhook error:', err.message));
                                            } catch (error) {
                                                console.error(`Error during auth timeout cleanup:`, error.message);
                                            }
                                        }
                                    }, PAIRING_TIMEOUT_DURATION);
                                }
                            }, PAIRING_TIMEOUT_DURATION);
                            
                            setImmediate(() => console.log(`⏰ Pairing timeout set: Will auto-close in ${Math.round(PAIRING_TIMEOUT_DURATION / 1000)}s if not connected`));
                        }
                        
                        // Callback ke parent
                        const pairingData = {
                            code,
                            phoneNumber: normalizedPhone,
                            rawPhoneNumber: phoneInfo.raw,
                            appliedDefaultCountryCode: phoneInfo.appliedDefaultCountryCode,
                            defaultCountryCode: phoneInfo.defaultCountryCode,
                            generatedAt: new Date().toISOString()
                        };
                        // Hanya via onPairingCode → index.js dispatchWebhook (hindari double PAIRING_CODE)
                        this.onPairingCode(pairingData);
                        this.startPairingStickWatchdog(10000);
                        setImmediate(() => console.log(`✅ Pairing code sent to webhook (single path)`));
                        
                    } catch (error) {
                        // ✅ console.error tetap berfungsi (tidak di-disable oleh quiet-console.js)
                        const errorMsg = error.message || error.toString() || 'Unknown error';
                        console.error(`❌ Error requesting pairing code: ${errorMsg}`);
                        
                        // ✅ FIX: Deteksi error "check your phone internet" untuk WhatsApp Business
                        // ✅ Untuk Business account, pairing code tidak support, fallback ke QR code
                        const isBusinessError = errorMsg.toLowerCase().includes('check your phone') || 
                                              errorMsg.toLowerCase().includes('internet') ||
                                              errorMsg.toLowerCase().includes('business') ||
                                              errorMsg.toLowerCase().includes('not supported');
                        
                        if (isBusinessError) {
                            console.warn(`⚠️ [BUSINESS-ACCOUNT] Pairing code tidak support untuk WhatsApp Business, fallback ke QR code`);
                            
                            // ✅ Fallback: Reset pairing mode, biarkan QR code di-generate
                            this.isPairingMode = false;
                            this.requestedPhoneNumber = null;
                            this.pairingCode = null;
                            
                            // ✅ Send webhook dengan info fallback ke QR
                            await this.enqueueWebhook({
                                event: 'PAIRING_CODE_BUSINESS_FALLBACK',
                                sessionId: this.sessionId,
                                bot: this.sessionId,
                                phoneNumber: normalizedPhone,
                                rawPhoneNumber: phoneInfo.raw,
                                error: errorMsg,
                                message: 'WhatsApp Business tidak support pairing code, silakan gunakan QR code',
                                fallback_to_qr: true,
                                user_id: this.userId,
                                timestamp: new Date().toISOString()
                            }).catch(() => {});
                            
                            // Jangan send PAIRING_CODE_ERROR; lanjut connection.update (QR fallback)
                        } else {
                        
                        // ✅ Error biasa, send PAIRING_CODE_ERROR
                        await this.enqueueWebhook({
                            event: 'PAIRING_CODE_ERROR',
                            sessionId: this.sessionId,
                            bot: this.sessionId,
                            phoneNumber: normalizedPhone,
                            rawPhoneNumber: phoneInfo.raw,
                            error: errorMsg,
                            user_id: this.userId,
                            timestamp: new Date().toISOString()
                        }).catch(() => {});
                        }
                    }
                    }
                    }
                }
                
                // ? CONNECTED
                if (connection === 'open') {
                    this.terminalDisconnect = false;
                    // Socket benar-benar open: 401 sebelumnya memang blip, mulai hitung dari nol.
                    this._pairing401Remaps = 0;
                    this._pairing401Exhausted = false;
                    this.lastEventAt = Date.now();
                    this.connectedAt = Date.now(); // grace MESSAGE_DELETED dari saat socket benar-benar open
                    const rampMs = Math.max(10000, Math.min(120000, Number(process.env.RECONNECT_RAMP_MS) || 60000));
                    this.reconnectRampUntil = Date.now() + rampMs;
                    setImmediate(() => console.log(`🔍 [CONNECTION.OPEN] connection === 'open' detected for session ${this.sessionId}, isAuthenticated: ${this.isAuthenticated}, connectedWebhookSent: ${this.connectedWebhookSent}, isPairingMode: ${this.isPairingMode}, reconnectRampMs=${rampMs}`));
                    // NOTE: Jangan return hanya karena CONNECTED webhook sudah pernah terkirim.
                    // Webhook dedupe tidak boleh memotong flow auth/heartbeat/updateSessionLogin.
                    
                    // ✅ CRITICAL FIX: Cancel QR/Pairing timeout saat socket open, set AUTH timeout yang lebih lama
                    // ✅ Socket open berarti QR sudah di-scan atau pairing code sudah di-input, authentication sedang berjalan
                    if (this.pairingTimeout) {
                        clearTimeout(this.pairingTimeout);
                        this.pairingTimeout = null;
                        setImmediate(() => console.log(`⏰ [TIMEOUT] QR/Pairing timeout cancelled - Socket OPEN (authentication in progress)`));
                        
                        // ✅ Tetap pakai batas pair supaya session gagal tidak lama nempel di RAM.
                        if (this.shouldKillIncompletePairing() || !this.isAuthenticated) {
                            this.pairingTimeout = setTimeout(() => {
                                if (this.shouldKillIncompletePairing()) {
                                    setImmediate(() => {
                                    console.log(`⏰ AUTH TIMEOUT (${Math.round(PAIRING_TIMEOUT_DURATION / 1000)}s) - Session ${this.sessionId} pairing anomaly / auth tidak selesai`);
                                    console.log(`🔌 Auto-closing socket and cleaning up...`);
                                    });
                                    
                                    try {
                                        if (this.sock && this.sock.ws) {
                                            this.sock.ws.close();
                                        }
                                        
                                        this.enqueueWebhook({
                                            event: 'AUTH_TIMEOUT',
                                            sessionId: this.sessionId,
                                            bot: this.sessionId,
                                            user_id: this.userId,
                                            reason: `Authentication tidak selesai dalam ${Math.round(PAIRING_TIMEOUT_DURATION / 1000)} detik setelah socket open`,
                                            timestamp: new Date().toISOString()
                                        }).catch(err => console.error('Webhook error:', err.message));
                                    } catch (error) {
                                        console.error(`Error during auth timeout cleanup:`, error.message);
                                    }
                                }
                            }, PAIRING_TIMEOUT_DURATION);
                            
                            setImmediate(() => console.log(`⏰ AUTH timeout set: Will auto-close in ${Math.round(PAIRING_TIMEOUT_DURATION / 1000)}s if pairing not fully taut`));
                        }
                    }
                    
                    // ✅ FIXED: Non-blocking console.log untuk prevent blocking di critical path
                    setImmediate(() => {
                    console.log(`🔌 [CONNECTION] Socket OPEN for session ${this.sessionId}`);
                    console.log(`?? DEBUG - sock.user:`, JSON.stringify(this.sock?.user));
                    console.log(`?? DEBUG - authState.creds.me:`, JSON.stringify(this.authState?.creds?.me));
                    });
                    
                    // ✅ CRITICAL FIX: Jangan langsung set isAuthenticated = true
                    // ✅ Connection 'open' hanya berarti socket terbuka, belum tentu sudah authenticated
                    // ✅ Tunggu sampai sock.user.id atau authState.creds.me.id sudah ada dan valid
                    
                    // ✅ FIX: Check apakah sudah authenticated (bisa dari creds.update handler sebelumnya)
                    let botJid = null;
                    let isReallyAuthenticated = false;
                    
                    // ✅ CRITICAL: Check isAuthenticated flag dulu (bisa sudah di-set oleh creds.update handler)
                    if (this.isAuthenticated) {
                        // ✅ Sudah authenticated (kemungkinan dari creds.update handler)
                        botJid = this.resolveAuthenticatedJid();
                        if (botJid) {
                            isReallyAuthenticated = true;
                            setImmediate(() => console.log(`✅ [AUTH] Already authenticated (from creds.update): ${botJid}`));
                        }
                    }
                    
                    // ✅ Jika belum authenticated, wait untuk authentication (max 5 detik)
                    // Blocking polling tetap pendek, biarkan lightweight polling yang handle waktu lebih lama
                    // ✅ FIX: Untuk pairing mode, extend blocking wait time karena authentication bisa lebih lambat
                    if (!isReallyAuthenticated) {
                        // ✅ Untuk pairing mode: 10 detik blocking (100 × 100ms), untuk non-pairing: 5 detik (50 × 100ms)
                        let authAttempts = this.isPairingMode ? 100 : 50;
                        
                        if (this.isPairingMode) {
                            setImmediate(() => console.log(`📱 [PAIRING] Waiting for authentication (max 10 seconds blocking, then lightweight polling)...`));
                        }
                        
                        while (authAttempts > 0 && !isReallyAuthenticated) {
                            botJid = this.resolveAuthenticatedJid();

                            if (botJid) {
                                isReallyAuthenticated = true;
                                const maxAttempts = this.isPairingMode ? 100 : 50;
                                console.log(`✅ [AUTH] Authenticated confirmed: ${botJid} (after ${(maxAttempts - authAttempts) * 100}ms${this.isPairingMode ? ' - pairing mode' : ''})`);

                                const botNumber = this.normalizeConnectedBotNumber(botJid);
                                
                                if (this.connectedWebhookSent) {
                                    console.log(`⏭️ [AUTH-BLOCK] CONNECTED webhook already sent, skipping duplicate`);
                                    break;
                                }
                                if (this.processingConnectedWebhook) {
                                    console.log(`⏭️ [AUTH-BLOCK] CONNECTED webhook sedang diproses, skip blocking send (fast-path/retry will handle)`);
                                    break;
                                }

                                console.log(`📤 [AUTH-BLOCK] Calling sendConnectedOnce for CONNECTED, botNumber: ${botNumber}`);

                                let webhookResult = null;
                                try {
                                    const sendResult = await this.sendConnectedOnce('auth_blocking_wait', botNumber, { forceDirect: true });
                                    webhookResult = sendResult?.raw || sendResult;
                                    
                                    console.log(`✅ [AUTH-POLL] enqueueWebhook resolved, skipped: ${webhookResult?.skipped}, queued: ${webhookResult?.queued}, direct: ${webhookResult?.direct}`);
                                    
                                    // Check apakah webhook di-skip oleh deduplication
                                    if (webhookResult?.skipped) {
                                        console.warn(`⚠️ [AUTH-BLOCK] CONNECTED webhook was SKIPPED, reason: ${webhookResult.reason || 'unknown'}`);
                                        break;
                                    }

                                    const webhookResponse = sendResult?.response || webhookResult?.response || webhookResult;
                                    await this.cacheConnectedBotIdFromResponse(webhookResponse);
                                    
                                    console.log(`✅ [CONNECTED] CONNECTED webhook sent for session ${this.sessionId} (via auth polling)`);
                                } catch (webhookError) {
                                    console.error(`❌ [AUTH-POLL] Error calling enqueueWebhook:`, webhookError.message);
                                    this.connectedWebhookSent = false;
                                    this.processingConnectedWebhook = false;
                                }
                                
                                break;
                            }
                            
                            await new Promise(resolve => setTimeout(resolve, 100));
                            authAttempts--;
                        }
                    }
                    
                    // Set isAuthenticated jika belum di-set
                    if (isReallyAuthenticated && !this.isAuthenticated) {
                        this.isAuthenticated = true;
                    }
                    
                        // ✅ CRITICAL: Hanya kirim CONNECTED webhook jika benar-benar authenticated dan socket masih OPEN
                        if (isReallyAuthenticated && botJid && isBaileysWebSocketOpen(this.sock)) {
                            // ✅ Set authenticated jika belum
                            if (!this.isAuthenticated) {
                                this.isAuthenticated = true;
                            }
                            setImmediate(() => console.log(`✅ WhatsApp CONNECTED & AUTHENTICATED for session ${this.sessionId}`));
                            this.markPairingAuthSucceeded('connection.open');
                            
                            // ✅ CRITICAL FIX: Kirim CONNECTED webhook juga di connection.open handler (jika belum dikirim)
                            // ✅ Ini penting karena connection.open bisa fire sebelum creds.update
                            // ✅ Untuk pairing mode, pastikan webhook dikirim jika belum dikirim (meskipun creds.update sudah fire tapi webhook di-skip)
                            const shouldSendConnected = !this.connectedWebhookSent;
                            
                            console.log(`🔍 [CONNECTION.OPEN] shouldSendConnected: ${shouldSendConnected}, connectedWebhookSent: ${this.connectedWebhookSent}, isAuthenticated: ${this.isAuthenticated}, botJid: ${botJid}, isPairingMode: ${this.isPairingMode}`);
                            
                            if (shouldSendConnected) {
                                console.log(`🚀 [CONNECTION.OPEN] Starting CONNECTED webhook flow (webhook not sent yet)...`);
                                // ✅ FIX: Kirim CONNECTED webhook juga di fast path (jika authentication langsung terdeteksi)
                            (async () => {
                                try {
                                    const botNumber = this.normalizeConnectedBotNumber(botJid);
                                    
                                    // Reset connection tracking
                                    this.reconnectAttempts = 0;
                                    this.deafRecoveryCount = 0;
                                    this.reconnectInFlight = false;
                                    this.connectionStreak++;
                                    this.lastStableConnection = Date.now();
                                    this.currentDisconnectCycle = null;
                                    this.reconnectingSent = false;
                                    this.sessionDeadWebhookSent = false;
                                    
                                    this.startHeartbeatMonitoring();
                                    await this.sessionManager.updateSessionLogin(this.sessionId, true);
                                    this.onConnected();
                                
                                    let webhookResult = null;
                                    try {
                                        const sendResult = await this.sendConnectedOnce('connection.open.fast_path', botNumber, { forceDirect: true });
                                        webhookResult = sendResult?.raw || sendResult;
                                        
                                        if (webhookResult?.skipped) {
                                            console.warn(`⚠️ [CONNECTION.OPEN] CONNECTED webhook was SKIPPED, reason: ${webhookResult.reason || 'unknown'}`);
                                            if (!this.connectedWebhookSent && webhookResult.reason === 'CONNECTED_PROCESSING') {
                                                setTimeout(async () => {
                                                    if (this.connectedWebhookSent) return;
                                                    try {
                                                        const retry = await this.sendConnectedOnce('connection.open.retry', botNumber, { forceDirect: true });
                                                        if (!retry?.skipped) {
                                                            console.log(`✅ [CONNECTION.OPEN] CONNECTED webhook sent on retry`);
                                                        }
                                                    } catch (e) {
                                                        console.error(`❌ [CONNECTION.OPEN] CONNECTED retry failed:`, e.message);
                                                    }
                                                }, 1500);
                                            }
                                            return;
                                        }
                                        
                                        const webhookResponse = sendResult?.response || webhookResult?.response || webhookResult;
                                        await this.cacheConnectedBotIdFromResponse(webhookResponse);
                                        if (webhookResponse && webhookResponse.error === '1' && webhookResponse.action === 'logout') {
                                            console.log(`🔌 Webhook requested disconnect (not logout) for: ${this.sessionId}`);
                                            await this.smartDisconnect({ 
                                                force: false, 
                                                reason: `Webhook requested: ${webhookResponse.msg || 'Session rejected'}` 
                                            });
                                            return;
                                        }
                                        
                                        console.log(`✅ [CONNECTED] CONNECTED webhook sent for session ${this.sessionId} (via connection.open)`);
                                    } catch (webhookError) {
                                        console.error(`❌ [CONNECTION.OPEN] Error calling enqueueWebhook:`, webhookError.message);
                                        this.connectedWebhookSent = false;
                                    }
                                } catch (error) {
                                    console.error(`❌ [CONNECTED] Error sending CONNECTED webhook:`, error.message);
                                    this.connectedWebhookSent = false;
                                }
                            })();
                        } else {
                            console.log(`⏭️ [CONNECTION.OPEN] CONNECTED webhook already sent, skipping duplicate`);
                        }
                    } else {
                        // ✅ Socket open tapi belum authenticated, atau socket berubah state saat menunggu auth.
                        setImmediate(() => console.warn(
                            `⚠️ [CONNECTION] Socket not send-ready yet for session ${this.sessionId}, starting lightweight polling... ` +
                            `(authenticated=${isReallyAuthenticated}, wsState=${getBaileysWebSocketReadyState(this.sock)})`
                        ));
                        
                        // ✅ LIGHTWEIGHT POLLING MECHANISM: Background polling yang ringan (tidak blocking)
                        // ✅ Detail mekanisme:
                        // 1. Polling interval: 2 detik sekali (tidak terlalu sering, tidak membebani)
                        // 2. Maksimal polling: 2 menit (120 detik / 2 detik = 60 attempts)
                        // 3. Auto-cleanup: Stop polling saat authenticated atau timeout
                        // 4. Non-blocking: Menggunakan setInterval (background, tidak block event loop)
                        // 5. Safe: Check socket state sebelum polling, prevent error
                        
                        // ✅ Cleanup polling lama jika ada (prevent duplicate)
                        if (this.authPollingInterval) {
                            clearInterval(this.authPollingInterval);
                            this.authPollingInterval = null;
                        }
                        
                        let pollingAttempts = 0;
                        const MAX_POLLING_ATTEMPTS = 60; // 60 × 1 detik = 1 menit maksimal
                        const POLLING_INTERVAL_MS = 1000; // 1 detik (was 2s — QR scan terasa lambat)
                        
                        // Start lightweight background polling
                        this.authPollingInterval = setInterval(() => {
                            // Safety check: Pastikan socket masih ada dan open
                            if (!this.sock || !isBaileysWebSocketOpen(this.sock)) {
                                setImmediate(() => console.warn(`⚠️ [AUTH-POLL] Socket not open, stopping polling`));
                                if (this.authPollingInterval) {
                                    clearInterval(this.authPollingInterval);
                                    this.authPollingInterval = null;
                                }
                                return;
                            }
                            
                            // Stop polling hanya jika auth + webhook CONNECTED sudah terkirim
                            if (this.isAuthenticated && this.connectedWebhookSent) {
                                setImmediate(() => console.log(`✅ [AUTH-POLL] Authenticated + CONNECTED webhook sent, stopping polling`));
                                if (this.authPollingInterval) {
                                    clearInterval(this.authPollingInterval);
                                    this.authPollingInterval = null;
                                }
                                return;
                            }
                            
                            pollingAttempts++;
                            
                            // Check authentication state
                            const botJid = this.resolveAuthenticatedJid();

                            if (botJid) {
                                // Valid JID ditemukan - authentication selesai!
                                setImmediate(() => console.log(`✅ [AUTH-POLL] Authentication detected after ${pollingAttempts * 2} seconds: ${botJid}`));
                                
                                // Stop polling
                                if (this.authPollingInterval) {
                                    clearInterval(this.authPollingInterval);
                                    this.authPollingInterval = null;
                                }
                                
                                this.isAuthenticated = true;
                                
                                // Extract clean bot number
                                let botNumber = botJid;
                                if (botNumber && botNumber.includes('@')) {
                                    botNumber = botNumber.split('@')[0];
                                }
                                if (botNumber && botNumber.includes(':')) {
                                    botNumber = botNumber.split(':')[0];
                                }
                                
                                // Validate bot number
                                if (!botNumber || !botNumber.match(/^\d{10,15}$/)) {
                                    botNumber = this.sessionId;
                                }
                                
                                // Cancel AUTH timeout
                                if (this.pairingTimeout) {
                                    clearTimeout(this.pairingTimeout);
                                    this.pairingTimeout = null;
                                    setImmediate(() => console.log(`⏰ AUTH timeout cancelled - authentication completed via polling`));
                                }
                                
                                // Kirim CONNECTED webhook jika belum dikirim
                                const shouldSendConnected = !this.connectedWebhookSent;
                                
                                if (shouldSendConnected) {
                                    setImmediate(async () => {
                                        try {
                                            // Reset connection tracking
                                            this.reconnectAttempts = 0;
                                            this.reconnectInFlight = false;
                                            this.connectionStreak++;
                                            this.lastStableConnection = Date.now();
                                            
                                            // ✅ FIXED: Reset disconnect cycle flags saat CONNECTED (new cycle, prevent spam webhook)
                                            this.currentDisconnectCycle = null;
                                            this.reconnectingSent = false;
                                            this.sessionDeadWebhookSent = false; // Reset flag saat CONNECTED
                                            
                                            // Start heartbeat
                                            this.startHeartbeatMonitoring();
                                            
                                            // Update session status
                                            await this.sessionManager.updateSessionLogin(this.sessionId, true);
                                            
                                            // Callback
                                            this.onConnected();
                                            
                                            const sendResult = await this.sendConnectedOnce('connection.open.polling', botNumber, { forceDirect: true });
                                            if (sendResult?.skipped) {
                                                return;
                                            }
                                            const webhookResponse = sendResult?.response || sendResult?.raw?.response || sendResult?.raw;
                                            
                                            await this.cacheConnectedBotIdFromResponse(webhookResponse);
                                            
                                            setImmediate(() => console.log(`✅ [CONNECTED] CONNECTED webhook sent for session ${this.sessionId} (via polling)`));
                                            
                                            // Check for logout request - HANYA disconnect, bukan logout
                                            // Logout hanya boleh dipanggil dari API endpoint, bukan dari webhook
                                            if (webhookResponse && webhookResponse.error === '1' && webhookResponse.action === 'logout') {
                                                console.log(`🔌 Webhook requested disconnect (not logout) for: ${this.sessionId}`);
                                                await this.smartDisconnect({ 
                                                    force: false, 
                                                    reason: `Webhook requested: ${webhookResponse.msg || 'Session rejected'}` 
                                                });
                                                return;
                                            }
                                        } catch (error) {
                                            console.error(`❌ [CONNECTED] Error sending CONNECTED webhook (polling):`, error.message);
                                        }
                                    });
                                } else {
                                    // ✅ CONNECTED webhook sudah pernah dikirim, skip untuk prevent duplicate
                                    setImmediate(() => console.log(`⏭️ [AUTH-POLL] CONNECTED webhook already sent, skipping duplicate`));
                                }
                                
                                return; // Exit polling
                            }
                            
                            // ✅ Max attempts reached - stop polling (tidak akan timeout, karena sudah ada AUTH timeout 10 menit)
                            if (pollingAttempts >= MAX_POLLING_ATTEMPTS) {
                                setImmediate(() => console.warn(`⚠️ [AUTH-POLL] Max polling attempts reached (${MAX_POLLING_ATTEMPTS}), stopping polling. AUTH timeout (10 min) will handle if authentication fails.`));
                                if (this.authPollingInterval) {
                                    clearInterval(this.authPollingInterval);
                                    this.authPollingInterval = null;
                                }
                                return;
                            }
                            
                            // ✅ Log progress setiap 10 attempts (20 detik) untuk monitoring
                            if (pollingAttempts % 10 === 0) {
                                setImmediate(() => console.log(`⏳ [AUTH-POLL] Still waiting for authentication... (${pollingAttempts * 2}s elapsed, ${(MAX_POLLING_ATTEMPTS - pollingAttempts) * 2}s remaining)`));
                            }
                        }, POLLING_INTERVAL_MS);
                        
                        setImmediate(() => console.log(`🔄 [AUTH-POLL] Started lightweight polling (every ${POLLING_INTERVAL_MS}ms, max ${MAX_POLLING_ATTEMPTS} attempts = ${MAX_POLLING_ATTEMPTS * POLLING_INTERVAL_MS / 1000}s)`));
                        
                        // ✅ Return tanpa set isAuthenticated (polling akan handle)
                        return;
                    }

                    return;
                }
                
                // ✅ WHATSAPP STANDARD FLOW: DISCONNECTED event handler
                // Flow yang benar sesuai standar WhatsApp:
                // 1. DISCONNECTED → selalu dikirim saat connection close
                // 2. Setelah DISCONNECTED, tentukan action:
                //    - Jika reason = loggedOut → tidak akan reconnect
                //    - Jika reason != loggedOut dan masih bisa reconnect → RECONNECTING
                //    - Jika max attempts → tidak akan reconnect lagi
                if (connection === 'close') {
                    await this.clearSessionAckLiveRedis();
                    await this.clearConnectedWebhookDedupe();
                    // ✅ CRITICAL: Jangan set isAuthenticated = false jika dalam pairing mode (pairing perlu bisa connect)
                    // ✅ Pairing mode mungkin masih dalam proses authentication, jangan reset flag
                    if (!this.isPairingMode) {
                        this.isAuthenticated = false;
                    } else {
                        setImmediate(() => console.log(`⏭️ [PAIRING] Connection close detected but pairing mode active, keeping isAuthenticated flag: ${this.isAuthenticated}`));
                    }
                    
                    // ✅ TRACK DISCONNECT TIME: Untuk quick reconnect detection
                    const previousDisconnectTime = this.lastDisconnectTime;
                    this.lastDisconnectTime = Date.now();
                    
                    // ⏰ Clear pairing timeout jika masih ada (prevent memory leak)
                    if (this.pairingTimeout) {
                        clearTimeout(this.pairingTimeout);
                        this.pairingTimeout = null;
                        setImmediate(() => console.log(`⏰ Pairing timeout cleared on disconnect`));
                    }
                    
                    // ✅ Cleanup auth polling interval (prevent memory leak)
                    if (this.authPollingInterval) {
                        clearInterval(this.authPollingInterval);
                        this.authPollingInterval = null;
                        setImmediate(() => console.log(`🔄 Auth polling cleared on disconnect`));
                    }
                    
                    // 🔧 Stop heartbeat monitoring saat disconnect
                    this.stopHeartbeatMonitoring();
                    
                    // ✅ Stop connection health check saat disconnect
                    this.stopConnectionHealthCheck();
                    
                    // ✅ Reset heartbeat failure count saat disconnect
                    this.heartbeatFailureCount = 0;
                    this.idleTimeoutWarningSent = false;
                    
                    // ✅ WHATSAPP STANDARD: Determine disconnect reason dan action dengan robust error handling
                    const lastError = lastDisconnect?.error;
                    let disconnectReason = DisconnectReason.connectionClosed; // Default ke connectionClosed, bukan 'unknown'

                    if (lastError) {
                        // ✅ Cek apakah error adalah Boom object (dari Baileys library)
                        if (lastError.isBoom && lastError.output?.statusCode) {
                            disconnectReason = lastError.output.statusCode;
                            
                            // ✅ VALIDASI: Pastikan statusCode adalah valid DisconnectReason
                            const validReasons = [
                                DisconnectReason.connectionClosed,
                                DisconnectReason.connectionLost,
                                DisconnectReason.connectionReplaced,
                                DisconnectReason.timedOut,
                                DisconnectReason.loggedOut,
                                DisconnectReason.badSession,
                                DisconnectReason.restartRequired,
                                DisconnectReason.multideviceMismatch,
                                DisconnectReason.forbidden,
                                DisconnectReason.unavailableService
                            ];
                            
                            if (!validReasons.includes(disconnectReason)) {
                                console.warn(`⚠️ [DISCONNECT] Invalid DisconnectReason from Boom: ${disconnectReason}, defaulting to connectionClosed`);
                                disconnectReason = DisconnectReason.connectionClosed;
                            }
                        } 
                        // ✅ Handle error biasa (bukan Boom) - extract dari error properties
                        else if (lastError instanceof Error) {
                            // Cek error code atau message untuk determine reason
                            if (lastError.message?.includes('timed out') || 
                                lastError.code === 'ETIMEDOUT' || 
                                lastError.code === 'ETIME') {
                                disconnectReason = DisconnectReason.timedOut;
                            } else if (lastError.code === 'ECONNREFUSED' || 
                                       lastError.code === 'ENOTFOUND' ||
                                       lastError.code === 'ECONNRESET') {
                                disconnectReason = DisconnectReason.connectionLost;
                            } else if (lastError.message?.includes('logged out') ||
                                       lastError.message?.includes('logout') ||
                                       lastError.message?.includes('Intentional Logout')) {
                                disconnectReason = DisconnectReason.loggedOut;
                            } else if (lastError.message?.includes('restart required') ||
                                       lastError.message?.includes('restart')) {
                                disconnectReason = DisconnectReason.restartRequired;
                            } else if (lastError.message?.includes('bad session') ||
                                       lastError.message?.includes('invalid session')) {
                                disconnectReason = DisconnectReason.badSession;
                            } else if (lastError.message?.includes('forbidden') ||
                                       lastError.message?.includes('blocked') ||
                                       lastError.message?.includes('403')) {
                                disconnectReason = DisconnectReason.forbidden;
                            } else if (lastError.message?.includes('connection replaced') ||
                                       lastError.message?.includes('multidevice')) {
                                disconnectReason = DisconnectReason.connectionReplaced;
                            } else if (lastError.message?.includes('unavailable') ||
                                       lastError.message?.includes('503')) {
                                disconnectReason = DisconnectReason.unavailableService;
                            } else {
                                // Default ke connectionClosed untuk error biasa
                                disconnectReason = DisconnectReason.connectionClosed;
                            }
                            
                            // ✅ LOG untuk debugging jika error tidak standard
                            console.warn(`⚠️ [DISCONNECT] Non-Boom error detected for ${this.sessionId}, mapped to: ${disconnectReason}`, {
                                errorMessage: lastError.message,
                                errorCode: lastError.code,
                                errorName: lastError.name,
                                errorStack: lastError.stack?.substring(0, 200)
                            });
                        }
                        // ✅ Handle jika error adalah number (statusCode langsung)
                        else if (typeof lastError === 'number') {
                            disconnectReason = lastError;
                            // ✅ VALIDASI: Pastikan statusCode adalah valid DisconnectReason
                            const validReasons = [
                                DisconnectReason.connectionClosed,
                                DisconnectReason.connectionLost,
                                DisconnectReason.connectionReplaced,
                                DisconnectReason.timedOut,
                                DisconnectReason.loggedOut,
                                DisconnectReason.badSession,
                                DisconnectReason.restartRequired,
                                DisconnectReason.multideviceMismatch,
                                DisconnectReason.forbidden,
                                DisconnectReason.unavailableService
                            ];
                            if (!validReasons.includes(disconnectReason)) {
                                console.warn(`⚠️ [DISCONNECT] Invalid DisconnectReason from number: ${disconnectReason}, defaulting to connectionClosed`);
                                disconnectReason = DisconnectReason.connectionClosed;
                            }
                        }
                        // ✅ Handle jika error adalah object dengan statusCode property
                        else if (lastError && typeof lastError === 'object' && 'statusCode' in lastError) {
                            disconnectReason = lastError.statusCode;
                            // ✅ VALIDASI: Pastikan statusCode adalah valid DisconnectReason
                            const validReasons = [
                                DisconnectReason.connectionClosed,
                                DisconnectReason.connectionLost,
                                DisconnectReason.connectionReplaced,
                                DisconnectReason.timedOut,
                                DisconnectReason.loggedOut,
                                DisconnectReason.badSession,
                                DisconnectReason.restartRequired,
                                DisconnectReason.multideviceMismatch,
                                DisconnectReason.forbidden,
                                DisconnectReason.unavailableService
                            ];
                            if (!validReasons.includes(disconnectReason)) {
                                console.warn(`⚠️ [DISCONNECT] Invalid DisconnectReason from object.statusCode: ${disconnectReason}, defaulting to connectionClosed`);
                                disconnectReason = DisconnectReason.connectionClosed;
                            }
                        }
                        // ✅ Fallback: log untuk debugging
                        else {
                            console.warn(`⚠️ [DISCONNECT] Unknown error type for ${this.sessionId}:`, {
                                errorType: typeof lastError,
                                error: lastError,
                                lastDisconnect: lastDisconnect
                            });
                            disconnectReason = DisconnectReason.connectionClosed; // Safe default
                        }
                    } else {
                        // ✅ Jika tidak ada error, kemungkinan connection close biasa
                        disconnectReason = DisconnectReason.connectionClosed;
                    }

                    // Forum Baileys: 401 / loggedOut = WA menolak session. Jangan remap, jangan reconnect.
                    if (disconnectReason === 401) {
                        disconnectReason = DisconnectReason.loggedOut;
                    }

                    const isLoggedOut = disconnectReason === DisconnectReason.loggedOut;
                    const isTerminalBan =
                        disconnectReason === DisconnectReason.badSession ||
                        disconnectReason === DisconnectReason.forbidden;
                    // ✅ CRITICAL: Pairing mode HARUS bisa reconnect (pairing perlu bisa connect)
                    const canReconnect =
                        !isLoggedOut &&
                        !isTerminalBan &&
                        (this.isPairingMode || this.reconnectAttempts < this.maxReconnectAttempts);
                    
                    // ✅ FIXED: Non-blocking console.log untuk prevent blocking di critical reconnect path
                    // ✅ Hanya log jika di retry terakhir untuk prevent spam console
                    const reconnectWebhookThreshold = Math.max(2, this.maxReconnectAttempts - 1);
                    const shouldLogDisconnected = this.reconnectAttempts >= reconnectWebhookThreshold;
                    if (shouldLogDisconnected) {
                        setImmediate(() => console.error(`❌ DISCONNECTED: ${this.sessionId}, reason: ${disconnectReason}, isLoggedOut: ${isLoggedOut}, canReconnect: ${canReconnect}, attempts: ${this.reconnectAttempts}/${this.maxReconnectAttempts}`));
                    }
                    
                    // ✅ INTERNAL: Call onDisconnected callback untuk internal cleanup (tidak kirim webhook)
                    this.onDisconnected(disconnectReason);
                    
                    // ✅ FIXED: Standardize bot number format untuk webhook (clean format)
                    let botNumber = this.sock?.user?.id || this.sessionId;
                    if (botNumber.includes('@')) {
                        botNumber = botNumber.split('@')[0]; // Remove @s.whatsapp.net
                    }
                    if (botNumber.includes(':')) {
                        botNumber = botNumber.split(':')[0]; // Remove :69 suffix
                    }
                    
                    // ✅ FIXED: Pastikan user_id selalu ada (jika null, extract dari sessionId)
                    let webhookUserId = this.userId;
                    if (!webhookUserId) {
                        const userIdMatch = this.sessionId.match(/sess_(\d+)_/);
                        webhookUserId = userIdMatch ? userIdMatch[1] : 'N/A';
                    }
                    
                    // ✅ CRITICAL FIX: Jika user disconnect dari HP (isLoggedOut), LANGSUNG kirim webhook dan return
                    // TIDAK perlu masuk retry logic karena ini logout permanen dari WhatsApp
                    if (isLoggedOut) {
                        console.log(`📱 [LOGOUT FROM PHONE] User disconnected from phone (reason: ${disconnectReason})`);
                        
                        // ✅ CRITICAL: Check deduplication SEBELUM kirim webhook (prevent spam)
                        // ✅ enqueueWebhook akan handle deduplication juga, tapi check dulu untuk prevent multiple triggers
                        // ✅ NOTE: Double check ini AMAN - redundant check lebih baik daripada miss duplicate
                        if (this.redis) {
                            const redisDedupeKey = `lifecycle:disconnected:${this.sessionId}`;
                            const redisLastTime = await this.redis.get(redisDedupeKey).catch(() => null);
                            if (redisLastTime) {
                                const now = Date.now();
                                const redisTimeDiff = now - parseInt(redisLastTime);
                                const DISCONNECTED_COOLDOWN = 10000; // 10 detik
                                if (redisTimeDiff < DISCONNECTED_COOLDOWN) {
                                    console.log(`⏭️ [LOGOUT-FROM-PHONE] DISCONNECTED for ${this.sessionId} sent recently (${Math.floor(redisTimeDiff/1000)}s ago), skipping duplicate`);
                                    return; // Skip, sudah pernah dikirim baru-baru ini
                                }
                            }
                        }
                        
                        // ✅ VALIDATION: Cek apakah bot masih connected sebelum kirim DISCONNECTED (prevent false disconnect)
                        // ✅ IMPROVED: Double check dengan delay untuk prevent race condition
                        const socketStillOpen = isBaileysWebSocketOpen(this.sock);
                        const stillAuthenticated = this.isAuthenticated === true;
                        
                        if (socketStillOpen && stillAuthenticated) {
                            // ✅ DOUBLE CHECK: Tunggu 500ms dan cek lagi untuk prevent race condition
                            await new Promise(resolve => setTimeout(resolve, 500));
                            
                            const socketStillOpenAfterDelay = isBaileysWebSocketOpen(this.sock);
                            const stillAuthenticatedAfterDelay = this.isAuthenticated === true;
                            
                            if (socketStillOpenAfterDelay && stillAuthenticatedAfterDelay) {
                                // ✅ Bot masih connected setelah delay - jangan kirim DISCONNECTED!
                                console.warn(`⚠️ [VALIDATION] Prevented false DISCONNECTED webhook - bot still connected after delay check (socket: ${socketStillOpenAfterDelay ? 'OPEN' : 'CLOSED'}, authenticated: ${stillAuthenticatedAfterDelay})`);
                                return; // Skip webhook - bot masih connected!
                            } else {
                                // ✅ Socket close setelah delay - ini real disconnect
                                console.log(`✅ [VALIDATION] Socket closed after delay check, proceeding with DISCONNECTED webhook`);
                            }
                        } else if (socketStillOpen || stillAuthenticated) {
                            // ✅ Partial connection - tunggu sebentar untuk memastikan
                            await new Promise(resolve => setTimeout(resolve, 300));
                            
                            const finalSocketState = isBaileysWebSocketOpen(this.sock);
                            const finalAuthState = this.isAuthenticated === true;
                            
                            if (finalSocketState && finalAuthState) {
                                console.warn(`⚠️ [VALIDATION] Connection recovered, skipping DISCONNECTED webhook`);
                                return;
                            }
                        }
                        
                        // ✅ CRITICAL: Skip DISCONNECTED jika Node.js sedang shutdown/restart (hard restart atau graceful shutdown)
                        if (typeof global !== 'undefined' && global.isShuttingDown) {
                            setImmediate(() => console.log(`⏭️ [DISCONNECTED] Skipping DISCONNECTED webhook (loggedOut from phone) - Node.js shutdown/restart detected (hard restart atau graceful shutdown)`));
                            return; // Skip DISCONNECTED - ini bukan real logout, ini restart!
                        }
                        
                        // loggedOut dari HP harus DISCONNECTED (bersihkan UI). Skip hanya restart intentional.
                        if (this.isRestartingForPairing) {
                            setImmediate(() => console.log(`⏭️ [DISCONNECTED] Skipping DISCONNECTED webhook — restarting for pairing`));
                            return;
                        }
                        
                        // ✅ LANGSUNG kirim webhook DISCONNECTED tanpa cek retry attempts
                        // ✅ enqueueWebhook akan otomatis retry 3x, fallback to queue jika server down
                        // ✅ enqueueWebhook akan check deduplication lagi (double check untuk safety)
                        const webhookResult = await this.enqueueWebhook({
                            event: 'DISCONNECTED',
                            sessionId: this.sessionId,
                            bot: botNumber,
                            reason: disconnectReason,
                            user_id: webhookUserId,
                            port: this.port,
                            reconnectAttempt: 0,
                            isLoggedOut: true,
                            canReconnect: false,
                            // ✅ DEBUG: Detail untuk debugging
                            _debug: {
                                timestamp: new Date().toISOString(),
                                connectionState: {
                                    isAuthenticated: this.isAuthenticated,
                                    reconnectInFlight: this.reconnectInFlight,
                                    connectionStreak: this.connectionStreak,
                                    lastStableConnection: this.lastStableConnection ? new Date(this.lastStableConnection).toISOString() : null
                                },
                                lastLifecycleEvent: this.lastLifecycleEvent,
                                lastLifecycleEventTime: this.lastLifecycleEventTime ? new Date(this.lastLifecycleEventTime).toISOString() : null,
                                sendReason: 'LOGGED_OUT_FROM_PHONE'
                            }
                        }).catch(async (err) => {
                            // ✅ CRITICAL: Jika gagal, retry masuk queue dengan exponential backoff
                            console.error(`❌ [LOGOUT FROM PHONE] Failed to send webhook: ${err.message}, retrying to queue...`);
                            
                            // Retry 3x masuk queue dengan delay
                            for (let retry = 1; retry <= 3; retry++) {
                                try {
                                    await new Promise(r => setTimeout(r, retry * 1000)); // 1s, 2s, 3s
                                    const retryResult = await this.enqueueWebhook({
                                        event: 'DISCONNECTED',
                                        sessionId: this.sessionId,
                                        bot: botNumber,
                                        reason: disconnectReason,
                                        user_id: webhookUserId,
                                        port: this.port,
                                        reconnectAttempt: 0,
                                        isLoggedOut: true,
                                        canReconnect: false,
                                        _debug: {
                                            timestamp: new Date().toISOString(),
                                            sendReason: 'LOGGED_OUT_FROM_PHONE_RETRY',
                                            retryAttempt: retry
                                        }
                                    }, { forceDirect: false }); // Force queue
                                    
                                    if (retryResult?.queued || retryResult?.success) {
                                        console.log(`✅ [LOGOUT FROM PHONE] Webhook enqueued successfully on retry ${retry}`);
                                        return retryResult;
                                    }
                                } catch (retryErr) {
                                    console.error(`❌ [LOGOUT FROM PHONE] Retry ${retry}/3 failed: ${retryErr.message}`);
                                    if (retry === 3) {
                                        console.error(`💀 [CRITICAL] All retries failed for LOGOUT webhook - data may be lost!`);
                                    }
                                }
                            }
                            return { failed: true };
                        });
                        
                        // Log result
                        if (webhookResult?.success || webhookResult?.queued) {
                            console.log(`✅ [LOGOUT FROM PHONE] Webhook ${webhookResult.success ? 'sent successfully' : 'enqueued'} (direct: ${webhookResult.direct}, queued: ${webhookResult.queued})`);
                        } else if (webhookResult?.failed) {
                            console.error(`⚠️ [LOGOUT FROM PHONE] Webhook delivery uncertain, but continuing with logout`);
                        }
                        
                        // Update session status
                        await this.sessionManager.updateSessionLogin(this.sessionId, false);
                        this.isAuthenticated = false;
                        console.log(`✅ [LOGOUT FROM PHONE] Session logged out, will not reconnect`);
                        return; // ✅ Tidak akan reconnect jika logged out dari HP
                    }
                    
                    // ✅ CRITICAL: JANGAN kirim DISCONNECTED untuk connection close biasa!
                    // ✅ Hanya kirim DISCONNECTED jika:
                    //    1. User logout dari HP (isLoggedOut = true) - sudah handle di atas
                    //    2. Terblokir/terminal WhatsApp (badSession/forbidden)
                    // restartRequired biasanya cuma minta re-create socket, bukan ban.
                    // ✅ Connection close biasa (network issue, timeout, dll) TIDAK perlu kirim DISCONNECTED
                    // ✅ Biarkan reconnect dulu, baru kirim DISCONNECTED jika benar-benar tidak bisa reconnect
                    
                    // ✅ Check apakah ini banned/blocked dari WhatsApp
                    const isBanned = disconnectReason === DisconnectReason.badSession ||
                                     disconnectReason === DisconnectReason.loggedOut ||
                                     disconnectReason === DisconnectReason.forbidden;
                    if (isBanned || isLoggedOut) {
                        this.terminalDisconnect = true;
                    }
                    if (isTerminalBan) {
                        this.writeSessionBannedMarker(disconnectReason);
                    }
                    if (isTerminalBan || isLoggedOut) {
                        this.terminalDisconnect = true;
                        this.reconnectAttempts = this.maxReconnectAttempts;
                        setImmediate(() => {
                            try {
                                if (typeof this.onBannedImmediate === 'function') {
                                    Promise.resolve(this.onBannedImmediate(disconnectReason)).catch(() => {});
                                }
                            } catch (_) {
                                /* ignore */
                            }
                        });
                    }

                    // ✅ WHATSAPP STANDARD FLOW STEP 1: DISCONNECTED HARUS dikirim dulu
                    // ✅ FIXED: Hanya kirim DISCONNECTED di retry terakhir (2-3 terakhir dari max attempts) untuk prevent spam
                    // Disconnect cycle = waktu disconnect pertama kali (dalam 5 detik window)
                    const now = Date.now();
                    const isNewDisconnectCycle = !this.currentDisconnectCycle || (now - this.currentDisconnectCycle) > 5000;
                    
                    // ✅ Hanya kirim DISCONNECTED jika sudah mendekati max attempts (misal attempt >= maxAttempts - 2)
                    // ✅ CRITICAL: Jangan kirim DISCONNECTED jika dalam pairing mode (pairing perlu bisa connect)
                    // ✅ CRITICAL: Jangan kirim DISCONNECTED jika sedang restart untuk pairing (restart adalah intentional, bukan disconnect)
                    // ✅ CRITICAL: Jangan kirim DISCONNECTED jika node sedang restart/shutdown (hard restart atau graceful shutdown)
                    const isNodeShuttingDown = typeof global !== 'undefined' && global.isShuttingDown === true;
                    // Pairing / grace pasca-pair: tahan DISCONNECTED agar 401/515 bisa resume creds.
                    const shouldSendDisconnected = !this.isPairingMode &&
                                                  !this.inPostPairGrace() &&
                                                  !this.isRestartingForPairing &&
                                                  !isNodeShuttingDown &&
                                                  (isBanned || !canReconnect || this.reconnectAttempts >= this.maxReconnectAttempts);
                    
                    if (isNewDisconnectCycle) {
                        // ✅ New disconnect cycle - set cycle ID, reset flags
                        this.currentDisconnectCycle = now;
                        this.reconnectingSent = false;
                        // ✅ Note: sessionDeadWebhookSent tidak di-reset di sini (hanya reset saat CONNECTED)
                        
                        // ✅ CRITICAL: Mark DISCONNECTED di cache SEBELUM kirim webhook untuk validasi urutan
                        // ✅ Tapi hanya kirim webhook jika sudah di retry terakhir
                        const disconnectKey = `disconnected:${this.sessionId}`;
                        this.dedupeCache.set(disconnectKey, now);
                        setTimeout(() => this.dedupeCache.delete(disconnectKey), 10000); // 10s TTL untuk validasi urutan
                        
                        // ✅ Kirim DISCONNECTED hanya di retry terakhir
                        // ✅ CRITICAL: Double check untuk pairing mode dan shutdown (termasuk saat node restart)
                        if (shouldSendDisconnected) {
                            // ✅ CRITICAL: Double check - Skip DISCONNECTED jika Node.js sedang shutdown/restart
                            if (typeof global !== 'undefined' && global.isShuttingDown) {
                                setImmediate(() => console.log(`⏭️ [DISCONNECTED] Skipping DISCONNECTED webhook - Node.js shutdown/restart detected (hard restart atau graceful shutdown)`));
                                return; // Skip DISCONNECTED - ini bukan real disconnect, ini restart!
                            }
                            
                            // ✅ FALLBACK: Detect mass disconnect (kemungkinan hard kill/SIGKILL)
                            // ✅ Jika banyak session ter-disconnect bersamaan, kemungkinan besar ini adalah restart
                            if (typeof global !== 'undefined' && global.massDisconnectWindow) {
                                const now = Date.now();
                                const window = global.massDisconnectWindow;
                                
                                // Reset window jika sudah expired
                                if (window.startTime && (now - window.startTime) > window.windowMs) {
                                    window.startTime = now;
                                    window.disconnectCount = 1;
                                } else if (!window.startTime) {
                                    window.startTime = now;
                                    window.disconnectCount = 1;
                                } else {
                                    window.disconnectCount++;
                                }
                                
                                // Jika banyak session ter-disconnect bersamaan, skip DISCONNECTED webhook
                                if (window.disconnectCount >= window.threshold) {
                                    setImmediate(() => console.log(`⏭️ [DISCONNECTED] Skipping DISCONNECTED webhook - Mass disconnect detected (${window.disconnectCount} sessions in ${Math.floor((now - window.startTime) / 1000)}s), likely hard kill/restart`));
                                    return; // Skip DISCONNECTED - ini kemungkinan restart, bukan real disconnect!
                                }
                            }
                            
                            // Skip hanya restart intentional — pair gagal/logout tetap DISCONNECTED
                            if (this.isRestartingForPairing) {
                                setImmediate(() => console.log(`⏭️ [DISCONNECTED] Skipping DISCONNECTED webhook — restarting for pairing`));
                                return;
                            }
                            
                            // Ban/terminal: webhook segera. Network blip: tunggu 5s + validasi ulang.
                            let statusAfterWait = null;
                            let socketState = null;
                            if (!isTerminalBan) {
                                console.log(`⏳ [DISCONNECTED] Waiting 5 seconds before sending DISCONNECTED webhook to ensure no quick reconnect...`);
                                await new Promise(resolve => setTimeout(resolve, 5000));

                                statusAfterWait = this.getRealDeviceStatus();
                                if (statusAfterWait.isConnected || statusAfterWait.isAuthenticated) {
                                    console.log(`✅ [DISCONNECTED] Connection recovered after delay, skipping DISCONNECTED webhook`);
                                    return;
                                }

                                socketState = getBaileysWebSocketReadyState(this.sock);
                                if (socketState === 1) {
                                    console.warn(`⚠️ [DISCONNECTED] Socket still open after delay, skipping DISCONNECTED webhook`);
                                    return;
                                }

                                if (this.reconnectInFlight) {
                                    console.log(`⏳ [DISCONNECTED] Reconnect still in flight, skipping DISCONNECTED webhook (will retry later)`);
                                    return;
                                }
                            } else {
                                setImmediate(() =>
                                    console.log(`🚫 [BANNED] Fast DISCONNECTED webhook for ${this.sessionId} (reason=${disconnectReason})`)
                                );
                            }

                            await this.enqueueWebhook({
                                event: 'DISCONNECTED',
                                sessionId: this.sessionId,
                                bot: botNumber,
                                reason: disconnectReason,
                                user_id: webhookUserId,
                                port: this.port,
                                reconnectAttempt: this.reconnectAttempts,
                                isLoggedOut: false,
                                canReconnect: canReconnect,
                                isBanned: isBanned,
                                // ✅ DEBUG: Detail untuk debugging
                                _debug: {
                                    timestamp: new Date().toISOString(),
                                    disconnectCycleId: this.currentDisconnectCycle,
                                    connectionState: {
                                        isAuthenticated: this.isAuthenticated,
                                        reconnectInFlight: this.reconnectInFlight,
                                        connectionStreak: this.connectionStreak,
                                        lastStableConnection: this.lastStableConnection ? new Date(this.lastStableConnection).toISOString() : null
                                    },
                                    flags: {
                                        reconnectingSent: this.reconnectingSent,
                                    },
                                    lastLifecycleEvent: this.lastLifecycleEvent,
                                    lastLifecycleEventTime: this.lastLifecycleEventTime ? new Date(this.lastLifecycleEventTime).toISOString() : null,
                                    sendReason: 'LAST_RETRIES_AFTER_DELAY',
                                    validation: {
                                        statusAfterWait: statusAfterWait,
                                        socketState: socketState,
                                        reconnectInFlight: this.reconnectInFlight
                                    }
                                }
                            });
                        } else {
                            setImmediate(() => console.log(`⏭️ [FLOW] DISCONNECTED skipped for ${this.sessionId} - still reconnectable (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`));
                        }
                    } else {
                        // ✅ Same disconnect cycle - skip DISCONNECTED (sudah dikirim sebelumnya, ini retry)
                        setImmediate(() => console.log(`⏭️ [FLOW] DISCONNECTED skipped for ${this.sessionId} - already sent for this disconnect cycle (retry attempt)`));
                    }
                    
                    // ✅ WHATSAPP STANDARD FLOW STEP 2: Tentukan action setelah DISCONNECTED
                    // Hanya kirim RECONNECTING jika benar-benar terjadi
                    
                    // ✅ BOT HANTU CHECK: Prevent reconnect jika session terdeteksi sebagai bot hantu
                    // ✅ CRITICAL: Jangan block pairing mode (pairing perlu bisa connect)
                    const isBotHantu = await this.isSessionBotHantu();
                    if (isBotHantu && !this.isPairingMode) {
                        console.error(`🚨 [BOT-HANTU] Reconnect BLOCKED for session ${this.sessionId} - Session marked as BOT HANTU`);
                        
                        // Send webhook untuk inform bahwa reconnect diblokir (SETELAH DISCONNECTED)
                        await this.enqueueWebhook({
                            event: 'RECONNECT_BLOCKED',
                            sessionId: this.sessionId,
                            bot: botNumber,
                            reason: 'BOT_HANTU_DETECTED',
                            message: 'Session terdeteksi sebagai bot hantu, reconnect diblokir untuk 24 jam',
                            user_id: webhookUserId,
                            port: this.port
                        }).catch(() => {});
                        
                        return; // ✅ BLOCK RECONNECT - tidak reconnect bot hantu
                    } else if (isBotHantu && this.isPairingMode) {
                        console.log(`⏭️ [BOT-HANTU] Skipping bot hantu check - pairing mode active (must complete until connect): ${this.sessionId}`);
                    }
                    
                    // ✅ WHATSAPP STANDARD: Jika bisa reconnect, kirim RECONNECTING dan mulai reconnect
                    // ✅ DEBUG: Log reconnect status untuk pairing mode
                    if (this.isPairingMode) {
                        setImmediate(() => console.log(`📱 [PAIRING] Reconnect check - canReconnect: ${canReconnect}, isAuthenticated: ${this.isAuthenticated}, reconnectAttempts: ${this.reconnectAttempts}/${this.maxReconnectAttempts}`));
                    }
                    if (canReconnect) {
                        const isRestartRequired =
                            disconnectReason === DisconnectReason.restartRequired ||
                            disconnectReason === 515;
                        this.reconnectAttempts++;
                        if (isRestartRequired && this.reconnectAttempts > 0) {
                            this.reconnectAttempts -= 1;
                        }
                        
                        // ✅ IMPROVED: Quick reconnect untuk temporary disconnects (< 5 detik)
                        // ✅ Jika disconnect baru terjadi (< quickReconnectWindow), gunakan delay lebih pendek
                        const now = Date.now();
                        const timeSinceLastDisconnect = now - this.lastDisconnectTime;
                        const isQuickReconnect = timeSinceLastDisconnect < this.quickReconnectWindow && timeSinceLastDisconnect > 0;
                        
                        // ✅ ANTI-BLOCK: Exponential backoff dengan jitter untuk prevent WhatsApp block
                        const exponentialDelay = Math.min(
                            this.baseReconnectDelay * Math.pow(2, Math.min(this.reconnectAttempts - 1, 4)),
                            this.maxReconnectDelay
                        );
                        let delay = Math.max(exponentialDelay, this.minReconnectDelay);
                        
                        // Forum #2488/#2512: 515 wajib reconnect SEGERA dengan creds baru.
                        // Delay 3s+ → HP "Gagal menautkan perangkat".
                        if (isRestartRequired) {
                            delay = 0;
                            setImmediate(() => console.log(
                                `⚡ [PAIRING] immediate reconnect delay=0` +
                                `${isRestartRequired ? ' [515]' : ''}` +
                                ` session=${this.sessionId}`
                            ));
                        } else if (isQuickReconnect && this.reconnectAttempts === 1) {
                            delay = 1000; // Quick reconnect: 1 detik untuk attempt pertama
                            setImmediate(() => console.log(`⚡ QUICK RECONNECT in 1s (temporary disconnect detected, ${Math.floor(timeSinceLastDisconnect/1000)}s since last disconnect)`));
                        } else {
                            // Tambah jitter (10-20% dari delay) untuk avoid thundering herd
                            const jitter = delay * (0.1 + Math.random() * 0.1);
                            delay = delay + jitter;
                            delay = Math.round(delay / 100) * 100;
                            setImmediate(() => console.log(`🔄 RECONNECT in ${Math.floor(delay/1000)}s (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`));
                        }
                        
                        // ✅ Update last disconnect time untuk quick reconnect detection
                        this.lastDisconnectTime = now;
                        
                        // ✅ FIXED: RECONNECTING hanya dikirim di retry terakhir (2-3 terakhir dari max attempts) untuk prevent spam
                        // ✅ Hanya kirim jika sudah mendekati max attempts (misal attempt >= maxAttempts - 2)
                        const shouldSendReconnecting = this.reconnectAttempts >= reconnectWebhookThreshold;
                        
                        // ✅ CRITICAL FIX: Check dedupe cache juga untuk prevent multiple RECONNECTING
                        const reconnectingDedupeKey = `reconnecting:${this.sessionId}`;
                        const lastReconnectingTime = this.dedupeCache.get(reconnectingDedupeKey) || 0;
                        const timeSinceLastReconnecting = now - lastReconnectingTime;
                        
                        // ✅ Hanya kirim jika belum pernah dikirim dalam 15 detik terakhir DAN flag belum set
                        if (!this.reconnectingSent && shouldSendReconnecting && timeSinceLastReconnecting >= 15000) {
                            this.reconnectingSent = true;
                            
                            // ✅ CRITICAL: RECONNECTING hanya dikirim jika benar-benar akan reconnect dan di retry terakhir
                            // SETELAH DISCONNECTED selesai, baru kirim RECONNECTING
                            await this.enqueueWebhook({
                                event: 'RECONNECTING',
                                sessionId: this.sessionId,
                                bot: botNumber,
                                reason: disconnectReason,
                                user_id: webhookUserId,
                                port: this.port,
                                attempt: this.reconnectAttempts,
                                maxAttempts: this.maxReconnectAttempts,
                                delayMs: Math.floor(delay),
                                // ✅ DEBUG: Detail untuk debugging
                                _debug: {
                                    timestamp: new Date().toISOString(),
                                    disconnectCycleId: this.currentDisconnectCycle,
                                    connectionState: {
                                        isAuthenticated: this.isAuthenticated,
                                        reconnectInFlight: this.reconnectInFlight,
                                        connectionStreak: this.connectionStreak,
                                        lastStableConnection: this.lastStableConnection ? new Date(this.lastStableConnection).toISOString() : null
                                    },
                                    flags: {
                                        reconnectingSent: this.reconnectingSent,
                                    },
                                    lastLifecycleEvent: this.lastLifecycleEvent,
                                    lastLifecycleEventTime: this.lastLifecycleEventTime ? new Date(this.lastLifecycleEventTime).toISOString() : null,
                                    wasDisconnectedSent: this.dedupeCache.has(`disconnected:${this.sessionId}`),
                                    reconnectDelay: {
                                        exponential: exponentialDelay,
                                        withJitter: Math.floor(delay),
                                        minDelay: this.minReconnectDelay,
                                        maxDelay: this.maxReconnectDelay
                                    },
                                    sendReason: shouldSendReconnecting ? 'LAST_RETRIES' : 'TOO_EARLY'
                                }
                            });
                            
                            // ✅ CRITICAL: Mark di dedupe cache setelah kirim webhook
                            // ✅ MEMORY LEAK FIX: Use Redis TTL instead of setTimeout
                            this.dedupeCache.set(reconnectingDedupeKey, now + 15000);
                            if (this.redis) {
                                const redisKey = `dedupe:${this.sessionId}:${reconnectingDedupeKey}`;
                                this.redis.set(redisKey, (now + 15000).toString(), { EX: 15 }).catch(() => {});
                            }
                        } else if (!shouldSendReconnecting) {
                            setImmediate(() => console.log(`⏭️ [FLOW] RECONNECTING skipped for ${this.sessionId} - too early (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts}, will send at attempt ${reconnectWebhookThreshold}+)`));
                        } else if (timeSinceLastReconnecting < 15000) {
                            setImmediate(() => console.log(`⏭️ [FLOW] RECONNECTING skipped for ${this.sessionId} - sent recently (${Math.floor(timeSinceLastReconnecting/1000)}s ago, cooldown: 15s)`));
                        } else {
                            setImmediate(() => console.log(`⏭️ [FLOW] RECONNECTING skipped for ${this.sessionId} - already sent for this disconnect cycle (retry ${this.reconnectAttempts})`));
                        }
                        
                        // Start reconnect process
                        // Forum Baileys (#2488): 515 restartRequired SETELAH pair-success WAJIB reconnect
                        // dengan creds tersimpan. Skip reconnect di sini → HP "couldn't link" / cepat 401.
                        if (!this.reconnectInFlight) {
                            const isRestartRequired =
                                disconnectReason === DisconnectReason.restartRequired ||
                                disconnectReason === 515;
                            if (isRestartRequired && this.isPairingMode && this.pairingCodeGeneratedTime) {
                                this.markPairingPasteAcknowledged('disconnect.515');
                            }
                            const pairingAuthAgeMs = this.pairingAuthSucceededAt
                                ? Date.now() - this.pairingAuthSucceededAt
                                : Number.POSITIVE_INFINITY;
                            // Blip 401/515 pasca-pair: resume creds. Jangan pakai lastStableConnection
                            // (itu bisa remap logout bot lama yang baru saja connected).
                            const allowPairingBlipResume =
                                pairingAuthAgeMs < this.getPostPairGraceMs() &&
                                this.reconnectAttempts <= 5;
                            const shouldReconnect = this.isPairingMode
                                ? (!this.isAuthenticated ||
                                    (this.isAuthenticated && !this.connectedWebhookSent) ||
                                    isRestartRequired ||
                                    allowPairingBlipResume)
                                : true;

                            // Jangan buat handshake baru selama user masih menempel kode.
                            // Socket baru = kode lama mati di WA → "Gagal menautkan perangkat".
                            const pairingWaitForPaste =
                                this.isPairingMode &&
                                this.pairingCode &&
                                !this.isAuthenticated &&
                                !this.pairingAuthSucceededAt &&
                                !isRestartRequired;
                            if (shouldReconnect && pairingWaitForPaste) {
                                const holdMs = Math.max(60000, Math.min(180000, Number(process.env.PAIRING_CODE_HOLD_MS) || 180000));
                                const codeAge = this.pairingCodeGeneratedTime
                                    ? (Date.now() - this.pairingCodeGeneratedTime)
                                    : Number.POSITIVE_INFINITY;
                                if (codeAge < holdMs && isBaileysWebSocketOpen(this.sock)) {
                                    setImmediate(() => console.log(
                                        `⏭️ [PAIRING] skip reconnect — hold code age=${Math.round(codeAge / 1000)}s session=${this.sessionId}`
                                    ));
                                }
                                // Batas umur pairing: user yang tidak pernah menempel kode jangan dibiarkan minta kode baru
                                // tiap 3 menit berjam-jam (sesi zombie → kode nomor sama saling membatalkan di WhatsApp).
                                const pairingMaxAgeMs = Math.max(300000, Number(process.env.PAIRING_MAX_AGE_MS) || 900000);
                                const pairingAgeMs = this.pairingStartedAt ? (Date.now() - this.pairingStartedAt) : 0;
                                if (pairingAgeMs > pairingMaxAgeMs && !isBaileysWebSocketOpen(this.sock)) {
                                    const ageS = Math.round(pairingAgeMs / 1000);
                                    setImmediate(() => console.log(
                                        `⏳ [PAIRING] kedaluwarsa age=${ageS}s (> ${Math.round(pairingMaxAgeMs / 1000)}s) — hentikan pairing, tidak minta kode lagi session=${this.sessionId}`
                                    ));
                                    this.pairingCode = null;
                                    this.pairingCodeGeneratedTime = null;
                                    this.terminalDisconnect = true;
                                    this.workerStopped = true;
                                    try { this.emitDegradedWebhook('PAIRING_EXPIRED', `age=${ageS}s`, { force: true }); } catch (_) { /* ignore */ }
                                    if (typeof this.onBannedImmediate === 'function') {
                                        try { this.onBannedImmediate('pairing_expired'); } catch (_) { /* ignore */ }
                                    }
                                    return;
                                }
                                if (codeAge < holdMs && !isBaileysWebSocketOpen(this.sock)) {
                                    this.pairingCode = null;
                                    this.pairingCodeGeneratedTime = null;
                                    setImmediate(() => console.log(
                                        `🔁 [PAIRING] socket mati — buang kode lama, minta kode baru session=${this.sessionId}`
                                    ));
                                }
                            }
                            
                            if (shouldReconnect && !(pairingWaitForPaste && this.pairingCode && isBaileysWebSocketOpen(this.sock))) {
                                this.reconnectInFlight = true;
                                if (this.reconnectTimer) {
                                    clearTimeout(this.reconnectTimer);
                                }
                                this.reconnectTimer = setTimeout(() => {
                                    this.reconnectTimer = null;
                                    setImmediate(() => console.log(
                                        `🔄 RECONNECTING NOW: ${this.sessionId}` +
                                        `${this.isPairingMode ? ' (pairing mode)' : ''}` +
                                        `${isRestartRequired ? ' [515]' : ''}` +
                                        `${allowPairingBlipResume ? ' [pair-blip]' : ''}`
                                    ));
                                    // 515 / auth sukses / blip: selalu resume creds (null), JANGAN requestPairingCode ulang
                                    const reconnectPhoneNumber =
                                        this.isAuthenticated || isRestartRequired || allowPairingBlipResume
                                            ? null
                                            : (this.isPairingMode ? this.requestedPhoneNumber : phoneNumber);
                                    const flushThenInit = async () => {
                                        if (typeof this.saveCreds?.flush === 'function') {
                                            await this.saveCreds.flush();
                                        }
                                        await this.initialize(reconnectPhoneNumber);
                                    };
                                    flushThenInit().catch(err => {
                                        console.error(`❌ RECONNECT FAILED: ${err.message}`);
                                    }).finally(() => {
                                        this.reconnectInFlight = false;
                                    });
                                }, delay);
                            } else if (this.isPairingMode && this.isAuthenticated && this.connectedWebhookSent) {
                                setImmediate(() => console.log(`⏭️ [PAIRING] Reconnect skipped - pairing mode active, authenticated, and CONNECTED webhook already sent: ${this.sessionId}`));
                            } else {
                                setImmediate(() => console.log(`⏭️ [PAIRING] Reconnect skipped - pairing mode active but conditions not met: ${this.sessionId}`));
                            }
                        } else {
                            setImmediate(() => console.log('⏭️  Reconnect already in flight, skipping duplicate'));
                        }
                    } else {
                        // ✅ Max reconnect attempts reached - tidak akan reconnect lagi
                        setImmediate(() => console.log(`⚠️ Max reconnect attempts (${this.maxReconnectAttempts}) reached, session will stop reconnecting`));
                        
                        // Update session status
                        await this.sessionManager.updateSessionLogin(this.sessionId, false);
                        this.isAuthenticated = false; // ✅ Set isAuthenticated = false setelah max attempts
                        
                        // 🔧 FIX CRASH: JANGAN hapus session folder saat max reconnect
                        setImmediate(() => console.warn(`⚠️ Max reconnect reached, session will be cleaned up via API/guardian`));
                        
                        // ? Trigger global orphan session cleanup (check ALL sessions vs DB)
                        // try {
                        //     console.log('?? Triggering global orphan session cleanup...');
                        //     const cleanupUrl = `http://localhost:${this.port || 7001}/baileys/cleanup-sessions`;
                        //     await axios.get(cleanupUrl, { timeout: 30000 });
                        //     console.log('? Global orphan cleanup triggered');
                        // } catch (cleanupError) {
                        //     console.error('?? Failed to trigger global cleanup:', cleanupError.message);
                        //     // Non-critical, just log and continue
                        // }
                    }
                }
                } catch (error) {
                    // ✅ FIXED: Comprehensive error handling untuk prevent crash
                    console.error(`❌ [CONNECTION.UPDATE] Error processing connection update in session ${this.sessionId}:`, error.message);
                    console.error(`❌ [CONNECTION.UPDATE] Error stack:`, error.stack);
                    // Jangan throw, biarkan process continue dan coba reconnect jika perlu
                    if (this.sock && !this.isAuthenticated) {
                    // Jika error terjadi saat belum authenticated, coba reconnect
                    if (this.reconnectAttempts < this.maxReconnectAttempts && !this.reconnectInFlight) {
                        setImmediate(() => console.warn(`🔄 [CONNECTION.UPDATE] Attempting reconnect after error (attempt ${this.reconnectAttempts + 1})`));
                        this.reconnectInFlight = true;
                        setTimeout(() => {
                            this.initialize(this.requestedPhoneNumber).catch(err => {
                                // ✅ console.error tetap berfungsi (tidak di-disable oleh quiet-console.js)
                                console.error(`❌ RECONNECT FAILED after error: ${err.message}`);
                            }).finally(() => {
                                this.reconnectInFlight = false;
                            });
                        }, 2000);
                    }
                    }
                }
            });
            
            // ============================================================================
            // ?? OTHER EVENT HANDLERS (message-receipt, messages.revoke, messages.delete)
            // ============================================================================
            
            // ✅ FIXED: Message receipt updates dengan comprehensive error handling
            this.sock.ev.on('message-receipt.update', async (receipts) => {
                try {
                console.log(`?? [MESSAGE-RECEIPT.UPDATE] Received ${receipts.length} receipt updates in session ${this.sessionId}`);
                for (const receipt of receipts) {
                    const msgId = receipt.key?.id;
                    if (!msgId) continue;
                    
                    // dedupe
                    const now = Date.now();
                    const exp = this.dedupeCache.get(`receipt_delivered_${msgId}`) || 0;
                    if (exp > now) {
                        continue;
                    }
                    // ✅ MEMORY LEAK FIX: Use Redis TTL for dedupe
                    this.dedupeCache.set(`receipt_delivered_${msgId}`, now + 20000);
                    if (this.redis) {
                        const redisKey = `dedupe:${this.sessionId}:receipt_delivered_${msgId}`;
                        this.redis.set(redisKey, (now + 20000).toString(), { EX: 20 }).catch(() => {});
                    }
                    
                    // Debug receipt data
                    console.log(`?? [RECEIPT] msgId: ${msgId}, receipt:`, JSON.stringify(receipt.receipt || {}));
                    
                    // Receipt type can be number or string
                    // Number: 0 = delivered, 1 = read, 2 = played
                    // String: 'read', 'read-self', 'hist_sync', 'peer_msg', 'sender', 'inactive', 'played'
                    const receiptType = receipt.receipt?.type;
                    const receiptTimestamp = receipt.receipt?.receiptTimestamp;
                    
                    // DELIVERED: Check if this is a delivery receipt (not read or played)
                    // If receiptTimestamp exists but type is not 'read' or 'played', it's likely delivered
                    const isDelivered = (
                        receiptType === 0 || 
                        (receiptTimestamp && 
                         receiptType !== 1 && 
                         receiptType !== 2 && 
                         receiptType !== 'read' && 
                         receiptType !== 'read-self' && 
                         receiptType !== 'played')
                    );
                    
                    if (isDelivered) {
                        try {
                            await this.enqueueWebhook({
                                event: 'DELIVERED',
                                bot: (this.sock?.user?.id||'').split('@')[0]||this.sessionId,
                                sessionId: this.sessionId,
                                msg_id: msgId,
                                status: 'delivered',
                                user_id: this.userId,
                                port: this.port,
                                receiptTimestamp: receiptTimestamp,
                                receiptType: receiptType
                            });
                            console.log(`✅ DELIVERED webhook sent for msg: ${msgId}, type: ${receiptType}`);
                        } catch (e) {
                            console.error('Webhook DELIVERED send error:', e.message);
                        }
                    }
                }
                } catch (error) {
                    // ✅ FIXED: Comprehensive error handling untuk prevent crash
                    console.error(`❌ [MESSAGE-RECEIPT.UPDATE] Error processing receipts in session ${this.sessionId}:`, error.message);
                    console.error(`❌ [MESSAGE-RECEIPT.UPDATE] Error stack:`, error.stack);
                    // Jangan throw, biarkan process continue
                }
            });
            
            // messages.revoke / messages.delete: JANGAN emit MESSAGE_DELETED (semantik longgar → false blacklist).
            // Revoke valid hanya via protocolMessage + shouldReportMessageDeleted().
            this.sock.ev.on('messages.revoke', async (revoke) => {
                try {
                    console.log(
                        `⏭️ [SKIP MESSAGE_DELETED] messages.revoke ignored session=${this.sessionId} data=${JSON.stringify(revoke).slice(0, 200)}`
                    );
                } catch (error) {
                    console.error(`❌ [MESSAGES.REVOKE] Error in session ${this.sessionId}:`, error.message);
                }
            });
            
            // Track connection time untuk ignore initial history (grace MESSAGE_DELETED)
            this.connectedAt = Date.now();
            
            this.sock.ev.on('messages.delete', async (deletion) => {
                try {
                    const keys = deletion?.keys || [];
                    console.log(
                        `⏭️ [SKIP MESSAGE_DELETED] messages.delete ignored session=${this.sessionId} keys=${keys.length}`
                    );
                } catch (error) {
                    console.error(`❌ [MESSAGES.DELETE] Error in session ${this.sessionId}:`, error.message);
                }
            });
            
            console.log(`? Baileys wrapper initialized for session ${this.sessionId}`);
            
        } catch (error) {
            console.error(`? Error initializing session ${this.sessionId}:`, error?.message || 'Unknown error');
            throw error;
        }
    }
    
    // ============================================================================
    // ?? SEND MESSAGE
    // ============================================================================
    
    // ============================================================================
    // 🔍 CHECK IF PHONE NUMBER EXISTS ON WHATSAPP
    // ============================================================================
    
    async checkWhatsAppNumber(phoneNumber) {
        try {
            // ✅ FIXED: Hapus check isAuthenticated - biarkan WhatsApp yang handle timeout/error sendiri
            await this.waitUntilCanSendReady();
            
            // Format phone number to JID
            const jid = phoneNumber.includes('@') ? phoneNumber : `${phoneNumber}@s.whatsapp.net`;
            
            // ✅ HIGH PERFORMANCE: Check cache first (reduce API calls)
            // ✅ MEMORY LEAK FIX: Check Redis first untuk whatsappNumberCache
            const now = Date.now();
            let cached = null;
            
            // ✅ STEP 1: Check Redis (prevent memory heap growth)
            if (this.redis) {
                try {
                    const redisKey = `whatsapp_number:${this.sessionId}:${jid}`;
                    const redisCached = await this.redis.get(redisKey).catch(() => null);
                    if (redisCached) {
                        cached = JSON.parse(redisCached);
                        if (cached && (now - cached.timestamp) < this.whatsappNumberCacheTTL) {
                            // ✅ Restore to memory cache (fallback)
                            this.whatsappNumberCache.set(jid, cached);
                            return cached.exists;
                        }
                    }
                } catch (err) {
                    // Fallback to memory cache
                }
            }
            
            // ✅ STEP 2: Check memory cache (fallback jika Redis tidak ada)
            cached = this.whatsappNumberCache.get(jid);
            if (cached && (now - cached.timestamp) < this.whatsappNumberCacheTTL) {
                // Cache hit - return cached result
                return cached.exists;
            }
            
            // ✅ OPTIMIZED: Timeout sangat cepat untuk prevent slowdown (1 detik max)
            const TIMEOUT_MS = 1000; // 1 detik max - sangat cepat untuk prevent slowdown
            
            // Use Baileys socket.onWhatsApp to check if number exists
            // onWhatsApp expects JID(s) and returns array of { jid, exists }
            // ✅ FIXED: Race dengan timeout untuk prevent hang
            const result = await Promise.race([
                this.sock.onWhatsApp(jid),
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('Check WhatsApp number timeout (2s)')), TIMEOUT_MS)
                )
            ]);
            
            // ✅ REMOVED: Verbose logging untuk speed (check di background)
            
            if (!result || result.length === 0) {
                // Cache negative result
                // ✅ MEMORY LEAK FIX: Set ke Redis dan memory cache
                const cacheData = { exists: false, timestamp: now };
                this.whatsappNumberCache.set(jid, cacheData);
                if (this.redis) {
                    const redisKey = `whatsapp_number:${this.sessionId}:${jid}`;
                    this.redis.set(redisKey, JSON.stringify(cacheData), { EX: Math.ceil(this.whatsappNumberCacheTTL / 1000) }).catch(() => {});
                }
                return false;
            }
            
            // Return true if any result exists
            const exists = result.some(r => r.exists === true);
            
            // ✅ HIGH PERFORMANCE: Cache result
            // ✅ MEMORY LEAK FIX: Set ke Redis dan memory cache
            const cacheData = { exists, timestamp: now };
            this.whatsappNumberCache.set(jid, cacheData);
            if (this.redis) {
                const redisKey = `whatsapp_number:${this.sessionId}:${jid}`;
                this.redis.set(redisKey, JSON.stringify(cacheData), { EX: Math.ceil(this.whatsappNumberCacheTTL / 1000) }).catch(() => {});
            }
            
            // ✅ OPTIMIZED: Cleanup old cache entries (keep last 500 - reduced untuk prevent memory heap)
            if (this.whatsappNumberCache.size > this.whatsappNumberCacheMaxSize) {
                const entries = Array.from(this.whatsappNumberCache.entries());
                const toDelete = entries.filter(([_, value]) => (now - value.timestamp) > this.whatsappNumberCacheTTL);
                toDelete.forEach(([key]) => this.whatsappNumberCache.delete(key));
            }
            
            return exists;
        } catch (error) {
            // ✅ FIXED: Jangan throw error, return false saja untuk prevent slowdown
            // Error akan di-log tapi tidak blocking send message
            console.error('⚠️ [CHECK WHATSAPP NUMBER] Error (non-blocking):', error.message);
            
            // ✅ OPTIMIZED: Return false untuk allow send message (might be network issue)
            // Jangan throw error karena akan menyebabkan slowdown
            return false; // Conservative: assume not valid jika error
        }
    }
    
    // ✅ HIGH PERFORMANCE: Process message queue dengan parallel processing
    async processMessageQueue() {
        if (this.isProcessingMessageQueue || this.messageSendQueue.length === 0) {
            return;
        }
        
        this.isProcessingMessageQueue = true;
        const queueMaxAgeMs = Math.max(1000, Math.min(120000, Number(process.env.MESSAGE_QUEUE_MAX_AGE_MS) || 30000));
        
        // ✅ OPTIMIZED: Process multiple messages in parallel (up to maxConcurrentSends)
        const processNextBatch = async () => {
            while (this.messageSendQueue.length > 0) {
                const nowQueue = Date.now();
                if (this.reconnectInFlight) {
                    this.queueWasPausedForReconnect = true;
                    for (const item of this.messageSendQueue) {
                        if (!item.processing) {
                            item.queueStartTime = nowQueue;
                        }
                    }
                } else {
                    if (this.queueWasPausedForReconnect) {
                        for (const item of this.messageSendQueue) {
                            if (!item.processing) {
                                item.queueStartTime = nowQueue;
                            }
                        }
                        this.queueWasPausedForReconnect = false;
                    }
                    for (let i = this.messageSendQueue.length - 1; i >= 0; i--) {
                        const item = this.messageSendQueue[i];
                        if (!item.processing && item.queueStartTime && nowQueue - item.queueStartTime > queueMaxAgeMs) {
                            this.messageSendQueue.splice(i, 1);
                            item.reject?.(new Error('Queue timeout, please retry'));
                        }
                    }
                }
                if (this.messageSendQueue.length === 0) break;

                // Check concurrent sends limit
                if (this.activeSends >= this.maxConcurrentSends) {
                    break;
                }
                
                // Get next message from queue
                const queueItem = this.messageSendQueue.find(item => !item.processing);
                if (!queueItem) {
                    break;
                }
                
                // Check rate limit (per session, not global)
                const now = Date.now();
                const timeSinceLastSend = now - this.lastMessageSendTime;
                const minInterval = this.getEffectiveMinMessageSendInterval();
                if (timeSinceLastSend < minInterval) {
                    const waitTime = minInterval - timeSinceLastSend;
                    await new Promise(resolve => setTimeout(resolve, waitTime));
                }
                
                // Mark as processing and increment active sends
                queueItem.processing = true;
                this.activeSends++;
                // ✅ AMAN: Track item yang sedang diproses (prevent dihapus saat circular buffer)
                if (queueItem.itemId) {
                    this.queueProtection.processingItems.add(queueItem.itemId);
                }
                this.lastMessageSendTime = Date.now();
                
                // ✅ OPTIMIZED: Send message in parallel (non-blocking) - support both text and button
                const sendPromise = queueItem.messageType === 'button'
                    ? this.sendButtonMessageDirect(queueItem.to, queueItem.message, queueItem.footer || '', queueItem.url, queueItem.buttonText || '', queueItem.options)
                    : this.sendMessageDirect(queueItem.to, queueItem.message, queueItem.messageType, queueItem.options);
                
                sendPromise
                    .then(result => {
                        // ✅ CRITICAL: Process result dengan tempMsgId dari queueItem
                        if (result && this.processSendMessageResult) {
                            const baseType = queueItem.messageType || 'text';
                            const baseMsg = queueItem.message || '';
                            const procMeta =
                                result &&
                                typeof result === 'object' &&
                                Object.prototype.hasOwnProperty.call(result, PROCESS_SEND_AS)
                                    ? result[PROCESS_SEND_AS]
                                    : null;
                            const messageType = procMeta?.messageType ?? baseType;
                            const finalMessage = procMeta?.message ?? baseMsg;
                            if (procMeta) {
                                try {
                                    delete result[PROCESS_SEND_AS];
                                } catch (_) {}
                            }
                            const tempMsgId = queueItem.tempMsgId || null;
                            this.registerOutboundMessageSync(result, queueItem.to, queueItem.enableCallback === true);
                            this.processSendMessageResult(result, queueItem.to, finalMessage, messageType, queueItem.options, queueItem.skipWebhook, queueItem.enableCallback, tempMsgId)
                                .catch(err => console.error(`[ERROR] processSendMessageResult failed in queue:`, err.message));
                        }
                        queueItem.resolve(result);
                    })
                    .catch(error => {
                        queueItem.reject(error);
                    })
                    .finally(() => {
                        // Decrement active sends and remove from queue
                        this.activeSends--;
                        // ✅ AMAN: Remove dari processingItems tracking
                        if (queueItem.itemId) {
                            this.queueProtection.processingItems.delete(queueItem.itemId);
                            // ✅ AMAN: Mark sebagai completed (prevent duplicate processing)
                            this.queueProtection.completedItems.add(queueItem.itemId);
                        }
                        const index = this.messageSendQueue.indexOf(queueItem);
                        if (index > -1) {
                            this.messageSendQueue.splice(index, 1);
                        }
                        if (this.messageSendQueue.length > 0) {
                            this.processMessageQueue().catch(() => {});
                        }
                    });
            }
        };
        
        // Start processing
        await processNextBatch();
        
        // Mark as not processing only if queue is truly empty
        if (this.messageSendQueue.length === 0 && this.activeSends === 0) {
            this.isProcessingMessageQueue = false;
        } else {
            // Queue still has items or still processing, keep processing
            this.isProcessingMessageQueue = false;
            // Trigger next batch if needed
            if (this.messageSendQueue.length > 0) {
                this.processMessageQueue().catch(() => {});
            }
        }
    }
    
    // ✅ ANTI-BLOCK: Direct send message (internal, dengan rate limiting)
    /**
     * ✅ ANTI-SPAM: Apply unicode variants untuk prevent spam detection
     * Replace 5% of 'a' characters with unicode variants (à, á, â, ä, ã, å, ā)
     * @param {string} text Original text
     * @returns {string} Text with unicode variants applied
     */
    applyAntiSpamPattern(text) {
        if (!config?.antiSpam?.enabled || typeof text !== 'string') {
            return text;
        }
        
        const replacePercentage = config.antiSpam.replacePercentage || 5; // Default 5%
        const unicodeVariantsMap = config.antiSpam.unicodeVariants || {};
        const characters = config.antiSpam.characters || ['a', 'A'];
        
        // ✅ Find all characters that can be replaced
        const charPositions = [];
        for (let i = 0; i < text.length; i++) {
            if (characters.includes(text[i])) {
                charPositions.push(i);
            }
        }
        
        if (charPositions.length === 0) {
            return text; // No characters to replace
        }
        
        // ✅ Calculate how many to replace (5% of total)
        const totalToReplace = Math.max(1, Math.floor((charPositions.length * replacePercentage) / 100));
        
        // ✅ Randomly select positions to replace
        const positionsToReplace = [];
        const shuffled = [...charPositions].sort(() => Math.random() - 0.5);
        for (let i = 0; i < totalToReplace && i < shuffled.length; i++) {
            positionsToReplace.push(shuffled[i]);
        }
        
        // ✅ Replace selected characters with random unicode variants
        let result = text.split('');
        for (const pos of positionsToReplace) {
            const originalChar = result[pos];
            // ✅ Get variants for this specific character (case-sensitive)
            const variants = unicodeVariantsMap[originalChar] || [];
            
            if (variants.length > 0) {
                // ✅ Randomly select a variant
                const randomVariant = variants[Math.floor(Math.random() * variants.length)];
                result[pos] = randomVariant;
            }
            // ✅ If no variants found for this character, skip (keep original)
        }
        
        return result.join('');
    }

    async raceSendMessage(jid, payload, timeoutMs) {
        return Promise.race([
            this.sock.sendMessage(jid, payload),
            new Promise((_, reject) =>
                setTimeout(() => reject(new Error(`Operation timed out after ${timeoutMs / 1000}s`)), timeoutMs)
            )
        ]);
    }

    /** Retry kirim — tetap tunggu msg_id asli; tidak pakai temp/callback. */
    async sendMessageWithRetry(jid, payload, timeoutMs) {
        let lastError = null;
        const maxAttempts = SEND_MESSAGE_RETRY_MAX + 1;
        for (let attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                if (attempt > 0) {
                    console.warn(`[SEND-RETRY] session=${this.sessionId || 'unknown'} attempt=${attempt + 1}/${maxAttempts}`);
                    await new Promise((resolve) => setTimeout(resolve, 350 * attempt));
                }
                return await this.raceSendMessage(jid, payload, timeoutMs);
            } catch (error) {
                lastError = error;
                const errorMsg = error?.message || String(error);
                if (
                    errorMsg.includes('Connection Closed') ||
                    errorMsg.includes('connection closed') ||
                    errorMsg.includes('Empty message')
                ) {
                    break;
                }
            }
        }
        throw lastError || new Error('Send failed after retry');
    }

    async sendMessageDirect(to, message, messageType = 'text', options = {}) {
        // ✅ LOGGING DI AWAL FUNCTION - PASTI MUNCUL JIKA FUNCTION DIPANGGIL
        const timestamp = new Date().toISOString();
        const sessionInfo = this.sessionId || 'unknown';
        if (process.env.SEND_DIRECT_DEBUG === '1') {
            console.error(`[SEND-DIRECT-DEBUG] ${timestamp} | Session: ${sessionInfo} | ACTION: sendMessageDirect CALLED`);
            console.error(`[SEND-DIRECT-DEBUG] ${timestamp} | Session: ${sessionInfo} | To: ${to || '(empty)'}`);
            console.error(`[SEND-DIRECT-DEBUG] ${timestamp} | Session: ${sessionInfo} | MessageType: ${messageType || 'text'}`);
            console.error(`[SEND-DIRECT-DEBUG] ${timestamp} | Session: ${sessionInfo} | Message: ${typeof message === 'string' ? (message.length > 100 ? message.substring(0, 100) + '...' : message) : typeof message}`);
        }
        
        await this.waitUntilCanSendReady();
        
        // ✅ STABLE: Biarkan library handle connection check sendiri (lebih reliable)
        // ✅ Library byee-wamod sudah punya check di sendRawMessage: if (!ws.isOpen) → throw
        // ✅ Double check bisa menyebabkan race condition, biarkan library handle lebih natural
        
        const jid = this.ensurePnSendJid(to);
        
        // ✅ HIGH TRAFFIC FIX: Timeout wrapper 10 detik (sync dengan Node.js API timeout)
        // ✅ CRITICAL: Sync dengan index.js SEND_TIMEOUT_MS (10s) + 2s buffer = 12s di PHP
        // Default 16s — HARUS ≤ PHP BAILEYS_CALLAPI_TIMEOUT_SEC (18). Env lib lama 55s menimpa ini → samakan .env.
        const SEND_MESSAGE_TIMEOUT_MS = Math.max(8000, Math.min(60000, Number(process.env.SEND_MESSAGE_DIRECT_TIMEOUT_MS) || 16000));

        try {
                if (messageType === 'text') {
                    // ✅ ANTI-SPAM: Apply unicode variants untuk prevent spam detection
                    let processedMessage = message;
                    if (config?.antiSpam?.enabled && typeof message === 'string') {
                        processedMessage = this.applyAntiSpamPattern(message);
                    }
                    if (!String(processedMessage ?? '').trim()) {
                        throw new Error('Empty message text');
                    }

                    const result = await this.sendMessageWithRetry(jid, { text: processedMessage }, SEND_MESSAGE_TIMEOUT_MS);
                    
                    // ✅ Reset timeout counter jika send berhasil
                    if (this.redis && result) {
                        const timeoutKey = `send_timeout_count:${this.sessionId}`;
                        await this.redis.del(timeoutKey).catch(() => {}); // Reset counter jika send berhasil
                    }
                    
                    // ✅ Update last activity untuk idle timeout check
                    if (result) {
                        this.lastActivityForIdleCheck = Date.now();
                    }

                    // ✅ Cache getMessage SEBELUM return msg_id (kurangi bubble kosong / #676)
                    await this.cacheOutboundMessageForGetMessage(result, to, processedMessage, 'text', options);
                    
                    return await this.finalizeSendResult(result, to);
                } else if (messageType === 'image') {
                    // ✅ ULTRA-LIGHTWEIGHT: Smart logic dengan URL cache management
                    // ✅ SPEED OPTIMIZATION: Pre-download dan cache images untuk eliminate 1s delay
                    let caption = options.caption || '';
                    
                    // ✅ ANTI-SPAM: Apply unicode variants untuk caption juga
                    if (config?.antiSpam?.enabled && caption) {
                        caption = this.applyAntiSpamPattern(caption);
                    }
                    
                    let imageMessage;
                    // ✅ FAST PATH: URL check dulu (paling common, paling ringan)
                    if (typeof message === 'string' && (message.startsWith('http://') || message.startsWith('https://'))) {
                        const timestamp = new Date().toISOString();
                        const sessionInfo = this.sessionId || 'unknown';
                        
                        // ✅ SPEED OPTIMIZATION: Check cache untuk local file (eliminate download delay)
                        const cached = await this.getImageUrlCache(message).catch(() => null);
                        let localFilePath = null;
                        
                        if (cached && cached.localPath) {
                            // ✅ Check if local file exists
                            try {
                                if (fs.existsSync(cached.localPath)) {
                                    localFilePath = cached.localPath;
                                    // ✅ REMOVED: Image URL logging (prevent binary data in logs)
                                }
                            } catch (err) {
                                // File tidak ada, akan download
                            }
                        }
                        
                        // ✅ Jika tidak ada cached file, download dan cache (non-blocking untuk next time)
                        if (!localFilePath) {
                            // ✅ Try to download and cache (with timeout untuk prevent blocking)
                            const downloadPromise = this.downloadAndCacheImage(message).catch(() => null);
                            localFilePath = await Promise.race([
                                downloadPromise,
                                new Promise((resolve) => setTimeout(() => resolve(null), 200)) // Max 200ms wait
                            ]);
                            
                            if (localFilePath) {
                                // ✅ REMOVED: Image URL logging (prevent binary data in logs)
                            } else {
                                // ✅ REMOVED: Image URL logging (prevent binary data in logs)
                            }
                        }
                        
                        // ✅ CRITICAL: Use local file if available (eliminate 1s download delay)
                        // ✅ Jika local file ada, gunakan Buffer (fast, no download)
                        // ✅ Jika tidak ada, fallback ke URL (library akan download)
                        if (localFilePath) {
                            try {
                                const fileBuffer = fs.readFileSync(localFilePath);
                                imageMessage = {
                                    image: fileBuffer,
                                    caption: caption || undefined
                                };
                                // ✅ REMOVED: Image cache logging (prevent binary data in logs)
                            } catch (err) {
                                // ✅ Fallback to URL jika read file gagal
                                console.warn(`⚠️ [IMAGE-CACHE] Failed to read cached file, using URL: ${err.message}`);
                                imageMessage = {
                                    image: { url: message },
                                    caption: caption || undefined
                                };
                            }
                        } else {
                            // ✅ Fallback: Pass URL ke library (library akan download)
                            imageMessage = {
                                image: { url: message },
                                caption: caption || undefined
                            };
                        }
                    } else {
                        // ✅ SMART PATH: Try-catch lebih ringan dari existsSync (single syscall)
                        const timestamp = new Date().toISOString();
                        const sessionInfo = this.sessionId || 'unknown';
                        const recipientInfo = jid || 'unknown';
                        try {
                            // ✅ Try read as Buffer (jika local file) - lebih cepat dari existsSync + readFileSync
                            // ✅ REMOVED: Image debug logging (prevent binary data in logs)
                            const fileBuffer = fs.readFileSync(message);
                            imageMessage = {
                                image: fileBuffer,
                                caption: caption || undefined
                            };
                        } catch (error) {
                            // ✅ Fallback: Pass as URL/path (Baileys akan handle)
                            const timestamp = new Date().toISOString();
                            const sessionInfo = this.sessionId || 'unknown';
                            const recipientInfo = jid || 'unknown';
                            // ✅ REMOVED: Image debug logging (prevent binary data in logs)
                            console.warn(`⚠️ [IMAGE] Local file read failed, using URL/path: ${error.message}`);
                            imageMessage = {
                                image: { url: message },
                                caption: caption || undefined
                            };
                            // ✅ REMOVED: Image debug logging (prevent binary data in logs)
                        }
                    }
                    
                    // ✅ CRITICAL: Wrap sendMessage dengan timeout + retry
                    const result = await this.sendMessageWithRetry(jid, imageMessage, SEND_MESSAGE_TIMEOUT_MS);
                    
                    // ✅ Reset timeout counter jika send berhasil
                    if (this.redis && result) {
                        const timeoutKey = `send_timeout_count:${this.sessionId}`;
                        await this.redis.del(timeoutKey).catch(() => {}); // Reset counter jika send berhasil
                    }
                    
                    // ✅ Update last activity untuk idle timeout check
                    if (result) {
                        this.lastActivityForIdleCheck = Date.now();
                        
                        // ✅ SMART CACHE: Update cache setelah berhasil send (untuk URL)
                        if (typeof message === 'string' && (message.startsWith('http://') || message.startsWith('https://'))) {
                            const messageId = result?.key?.id;
                            this.setImageUrlCache(message, { 
                                processed: true, 
                                messageId: messageId,
                                lastUsed: Date.now()
                            });
                        }
                    }
                    
                    await this.cacheOutboundMessageForGetMessage(result, to, caption || message, 'image', options);
                    
                    return await this.finalizeSendResult(result, to);
                } else {
                    throw new Error('Unsupported message type');
                }
            } catch (error) {
                let lastError = error;
                let errorMsg = error.message || error.toString() || '';

                // ✅ CEPAT: media_conn / 1006 — jangan anggap socket mati; kirim teks (caption) tanpa gambar
                if (messageType === 'image' && isMediaConnFlakyError(error)) {
                    try {
                        let cap = options.caption || '';
                        if (config?.antiSpam?.enabled && cap) {
                            cap = this.applyAntiSpamPattern(cap);
                        }
                        const textBody =
                            (cap && String(cap).trim()) || '(Gambar tidak terkirim — gangguan koneksi media.)';
                        console.warn(
                            `⚠️ [IMAGE→TEXT] Media/conn flaky — kirim teks tanpa gambar (${this.sessionId || 'unknown'})`
                        );
                        const textResult = await Promise.race([
                            this.sock.sendMessage(jid, { text: textBody }),
                            new Promise((_, reject) =>
                                setTimeout(
                                    () =>
                                        reject(
                                            new Error(
                                                `Operation timed out after ${SEND_MESSAGE_TIMEOUT_MS / 1000}s`
                                            )
                                        ),
                                    SEND_MESSAGE_TIMEOUT_MS
                                )
                            )
                        ]);
                        if (this.redis && textResult) {
                            await this.redis.del(`send_timeout_count:${this.sessionId}`).catch(() => {});
                        }
                        if (textResult) {
                            this.lastActivityForIdleCheck = Date.now();
                        }
                        if (textResult && typeof textResult === 'object') {
                            Object.defineProperty(textResult, PROCESS_SEND_AS, {
                                value: { messageType: 'text', message: textBody },
                                enumerable: false,
                                configurable: true
                            });
                        }
                        return await this.finalizeSendResult(textResult, to);
                    } catch (fallbackErr) {
                        lastError = fallbackErr;
                        errorMsg = fallbackErr.message || fallbackErr.toString() || '';
                    }
                }

                const isTimeout = errorMsg.includes('timed out') || errorMsg.includes('timeout') ||
                                 errorMsg.includes('ETIMEDOUT') || errorMsg.includes('TIMEOUT') ||
                                 (lastError.code && (lastError.code === 'ETIMEDOUT' || lastError.code === 'TIMEOUT'));
                const isConnectionClosed = errorMsg.includes('Connection Closed') ||
                                          errorMsg.includes('connection closed') ||
                                          errorMsg.includes('Connection closed - cannot send message') ||
                                          (lastError.output?.payload?.message === 'Connection Closed');

                if (isConnectionClosed) {
                    throw new Error('Connection closed - cannot send message');
                }

                if (isTimeout && this.redis) {
                    const timeoutKey = `send_timeout_count:${this.sessionId}`;
                    await this.redis.incr(timeoutKey).catch(() => {});
                    await this.redis.expire(timeoutKey, 60).catch(() => {});
                }

                throw lastError;
            }
    }
    
    /** Post-reconnect throttle: ramp 10%→100% rate over RECONNECT_RAMP_MS (default 60s) */
    getEffectiveMinMessageSendInterval() {
        const base = this.baseMinMessageSendInterval ?? this.minMessageSendInterval ?? 100;
        const rampMs = Math.max(10000, Math.min(120000, Number(process.env.RECONNECT_RAMP_MS) || 60000));
        const until = this.reconnectRampUntil || 0;
        if (!until || Date.now() >= until) {
            return base;
        }
        const elapsed = rampMs - (until - Date.now());
        const mult = Math.max(0.1, Math.min(1, elapsed / rampMs));
        return Math.max(base, Math.ceil(base / mult));
    }

    /** Identitas WA yang cukup untuk attempt send: sock.user ATAU creds.me (sama aturan assertCanSendNow). */
    hasSendableWaIdentity() {
        const botJid = this.sock?.user?.id || this.authState?.creds?.me?.id;
        return !!(botJid && !String(botJid).includes('sess_') && String(botJid).includes('@'));
    }

    assertCanSendNow() {
        if (this.sendGateBlockedUntil && Date.now() < this.sendGateBlockedUntil) {
            throw new Error(
                `Send gate blocked (echo verify / deaf session, retry in ${Math.ceil((this.sendGateBlockedUntil - Date.now()) / 1000)}s)`
            );
        }
        if (this.terminalDisconnect) {
            throw new Error('connection closed');
        }
        if (!this.sock) {
            throw new Error('Connection closed: reconnecting, not authenticated (socket missing)');
        }
        if (!isBaileysWebSocketOpen(this.sock)) {
            throw new Error(
                `Connection closed: reconnecting, not authenticated (wsState=${getBaileysWebSocketReadyState(this.sock)}, reconnectAttempts=${this.reconnectAttempts}/${this.maxReconnectAttempts})`
            );
        }
        if (!this.hasSendableWaIdentity()) {
            throw new Error('Connection closed: not authenticated (missing WA identity)');
        }
    }

    async waitUntilCanSendReady() {
        // fork-fix 2026-09-07: WhatsApp sedang mengunci DM ke non-kontak untuk nomor ini — jangan kirim apa pun
        // (teks/button/gambar). Error khusus supaya processor mem-DISCONNECT tanpa logout, bukan menghitung zombie.
        if (this.isReachoutTimeLocked()) {
            const tl = this.reachoutTimeLock;
            throw new Error(`REACHOUT_TIMELOCK: WhatsApp membatasi DM ke non-kontak sampai ${tl.until || '+60s'} (${tl.type})`);
        }
        const maxWaitMs = Math.max(0, Math.min(30000, Number(process.env.SEND_RECONNECT_GRACE_MS) || 12000));
        const pollMs = 250;
        const started = Date.now();
        let lastError = null;

        while (true) {
            try {
                this.assertCanSendNow();
                return true;
            } catch (err) {
                lastError = err;
                if (this.terminalDisconnect || maxWaitMs <= 0 || Date.now() - started >= maxWaitMs) {
                    throw lastError;
                }
                await new Promise((resolve) => setTimeout(resolve, pollMs));
            }
        }
    }

    async sendMessage(to, message, messageType = 'text', options = {}) {
        try {
            
            // ✅ FIXED: Hapus check isAuthenticated - biarkan WhatsApp yang handle timeout/error sendiri
            // ✅ ALASAN: Biarkan "bablas" - jika session masih bisa kirim (socket OPEN), biarkan kirim
            // ✅ Jika tidak bisa kirim, WhatsApp akan return timeout/error sendiri (lebih natural)
            await this.waitUntilCanSendReady();
            
            // ✅ NEW: Extract skipWebhook, enableCallback, dan tempMsgId dari options
            // ✅ CRITICAL: Default enableCallback = false (konsisten dengan index.js)
            // ✅ enableCallback hanya true jika explicitly set ke true
            const skipWebhook = options.skipWebhook === true || options.skipWebhook === '1' || options.skipWebhook === 1;
            const enableCallback = options.enableCallback === true || options.enableCallback === '1' || options.enableCallback === 1 || options.enableCallback === 'true';
            const tempMsgId = options.tempMsgId || null; // ✅ CRITICAL: Extract tempMsgId dari options
            
            // ✅ HIGH PERFORMANCE: Fast path - langsung kirim jika bisa
            // Check jika bisa langsung kirim (queue kosong, tidak processing, dan rate limit OK)
            const now = Date.now();
            const timeSinceLastSend = now - this.lastMessageSendTime;
            const canSendImmediately = 
                this.messageSendQueue.length === 0 && 
                !this.isProcessingMessageQueue && 
                this.activeSends < this.maxConcurrentSends &&
                timeSinceLastSend >= this.getEffectiveMinMessageSendInterval();
            
            if (canSendImmediately) {
                // ✅ FAST PATH: Langsung kirim tanpa queue
                this.lastMessageSendTime = now;
                this.activeSends++;
                
                try {
                    const result = await this.sendMessageDirect(to, message, messageType, options);
                    const procMeta =
                        result && typeof result === 'object' && Object.prototype.hasOwnProperty.call(result, PROCESS_SEND_AS)
                            ? result[PROCESS_SEND_AS]
                            : null;
                    const procType = procMeta?.messageType ?? messageType;
                    const procMsg = procMeta?.message ?? message;
                    if (procMeta) {
                        try {
                            delete result[PROCESS_SEND_AS];
                        } catch (_) {}
                    }
                    this.registerOutboundMessageSync(result, to, enableCallback);
                    // ✅ Process result in background — msgId sudah terdaftar sync di atas
                    this.processSendMessageResult(result, to, procMsg, procType, options, skipWebhook, enableCallback, tempMsgId)
                        .catch(err => console.error(`[ERROR] processSendMessageResult failed:`, err.message));
                    this.activeSends--;
                    // ✅ FAST: Return result immediately (don't wait for webhook/Redis processing)
                    return result;
                } catch (error) {
                    this.activeSends--;
                    throw error;
                }
            }
            
            // ✅ QUEUE PATH: Masukkan ke queue untuk rate limiting
            return new Promise((resolve, reject) => {
                const queueStartTime = Date.now();
                const itemId = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`; // ✅ UNIQUE ID untuk tracking
                
                const queueItem = {
                    to,
                    message,
                    messageType,
                    options,
                    skipWebhook,
                    enableCallback,
                    tempMsgId, // ✅ CRITICAL: Pass tempMsgId ke queue
                    queueStartTime, // Track waktu masuk queue
                    itemId, // ✅ UNIQUE ID untuk protection tracking
                    // processSendMessageResult sudah di processMessageQueue (fire-and-forget).
                    // Jangan await di sini — dulu double-process + tahan HTTP /send sampai Redis/webhook selesai.
                    resolve: (result) => {
                        try {
                            const waitTime = Date.now() - queueStartTime;
                            this.queueMetrics.totalProcessed++;
                            this.queueMetrics.avgWaitTime = (this.queueMetrics.avgWaitTime * (this.queueMetrics.totalProcessed - 1) + waitTime) / this.queueMetrics.totalProcessed;
                            if (result && typeof result === 'object' && Object.prototype.hasOwnProperty.call(result, PROCESS_SEND_AS)) {
                                try {
                                    delete result[PROCESS_SEND_AS];
                                } catch (_) {}
                            }
                            resolve(result);
                        } catch (error) {
                            reject(error);
                        }
                    },
                    reject,
                    processing: false
                };
                
                // ✅ CIRCULAR BUFFER: Jika queue penuh, hapus yang TIDAK sedang diproses
                if (this.messageSendQueue.length >= this.maxQueueSize) {
                    // ✅ AMAN: Hanya hapus item yang TIDAK sedang diproses
                    const nonProcessingItems = this.messageSendQueue.filter(item => 
                        !item.processing && 
                        !this.queueProtection.processingItems.has(item.itemId)
                    );
                    
                    if (nonProcessingItems.length > 0) {
                        // Hapus item tertua yang tidak sedang diproses
                        const oldestItem = nonProcessingItems[0];
                        const index = this.messageSendQueue.indexOf(oldestItem);
                        if (index > -1) {
                            // ✅ CRITICAL: Reject item yang dihapus untuk prevent webhook kelewat
                            oldestItem.reject(new Error('Queue full, item removed (circular buffer)'));
                            this.messageSendQueue.splice(index, 1);
                            console.warn(`⚠️ [QUEUE] Removed oldest item from queue (circular buffer), queue size: ${this.messageSendQueue.length}`);
                        }
                    } else {
                        // ✅ FALLBACK: Semua sedang diproses, reject item baru (prevent queue overflow)
                        reject(new Error('Queue full and all items are processing, please retry'));
                        return;
                    }
                }
                
                this.messageSendQueue.push(queueItem);
                
                // ✅ MONITORING: Track max queue length
                if (this.messageSendQueue.length > this.queueMetrics.maxQueueLength) {
                    this.queueMetrics.maxQueueLength = this.messageSendQueue.length;
                }
                
                // ✅ MONITORING: Log queue metrics setiap 30 detik
                const now = Date.now();
                if (now - this.queueMetrics.lastLogTime > 30000) {
                    if (this.queueMetrics.maxQueueLength > 5 || this.queueMetrics.avgWaitTime > 1000) {
                        console.warn(`⚠️ [QUEUE-MONITOR] Session ${this.sessionId}: maxQueue=${this.queueMetrics.maxQueueLength}, avgWait=${Math.round(this.queueMetrics.avgWaitTime)}ms, activeSends=${this.activeSends}`);
                    }
                    this.queueMetrics.lastLogTime = now;
                    this.queueMetrics.maxQueueLength = 0; // Reset
                }
                
                // Start processing queue (non-blocking)
                this.processMessageQueue().catch(err => {
                    console.error('Error processing message queue:', err?.message || 'Unknown error');
                });
            });
        } catch (error) {
            console.error('Error in sendMessage:', error?.message || 'Unknown error');
            throw error;
        }
    }
    
    // ✅ Helper: Process send message result
    async processSendMessageResult(result, to, message, messageType, options, skipWebhook, enableCallback, tempMsgId = null) {
        // ✅ GREP: [PROCESS-RESULT-START]
        console.log(`[PROCESS-RESULT-START] enableCallback=${enableCallback} tempMsgId=${tempMsgId || 'NONE'} skipWebhook=${skipWebhook} redis=${!!this.redis}`);
        
        if (!result || !result.key) {
            // ✅ GREP: [PROCESS-RESULT-ERROR]
            console.log(`[PROCESS-RESULT-ERROR] No result or result.key enableCallback=${enableCallback}`);
            return result; // Return as-is jika tidak ada result
        }
        
        const messageId = result?.key?.id;
        const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
        const remoteJid = result?.key?.remoteJid || jid;

        // Sync register (idempotent) — penting untuk image/button yang pakai setImmediate
        this.registerOutboundMessageSync(result, to, enableCallback);
        void this.cacheOutboundMessageForGetMessage(result, to, message, messageType, options).catch(() => {});
        
        // ✅ GREP: [PROCESS-RESULT-MSGID]
        if (!messageId) {
            console.log(`[PROCESS-RESULT-MSGID] messageId=NOT_FOUND enableCallback=${enableCallback} tempMsgId=${tempMsgId || 'NONE'}`);
        } else {
            console.log(`[PROCESS-RESULT-MSGID] messageId=${messageId} enableCallback=${enableCallback} tempMsgId=${tempMsgId || 'NONE'}`);
        }
        
        // ✅ CRITICAL: Generate tempMsgId WAJIB jika enableCallback=true dan tempMsgId belum ada
        // ✅ tempMsgId diperlukan sebagai patokan untuk mapping ke real messageId nanti
        // ✅ Response cepat dengan tempMsgId, messageId real nyusul dari WhatsApp
        if (enableCallback && !tempMsgId) {
            tempMsgId = this.generateTempMessageId();
            // ✅ GREP: [TEMP-MSGID-GENERATED]
            console.log(`[TEMP-MSGID-GENERATED] tempMsgId=${tempMsgId} enableCallback=${enableCallback}`);
        }
        
        // ✅ CRITICAL: Simpan context lengkap ke Redis SEBELUM polling (prevent data loss)
        // ✅ Smart queue: Simpan semua data penting ke Redis untuk handle race condition
        // ✅ Ini jalur response cepat, jadi harus simpan context dulu sebelum tunggu messageId
        // ✅ Simpan context bahkan jika messageId sudah ada (defensive programming, jaga-jaga)
        if (enableCallback && tempMsgId && this.redis) {
            const contextKey = `pending_msgsent:${tempMsgId}`;
            const contextData = {
                to,
                message,
                remoteJid,
                tempMsgId,
                messageType,
                timestamp: Date.now(),
                sessionId: this.sessionId,
                userId: this.userId,
                port: this.port
            };
            // ✅ Simpan context dengan TTL 10 menit (lebih lama untuk handle WhatsApp yang lambat)
            // ✅ Non-blocking save (fire and forget) untuk tidak memperlambat response
            this.redis.set(contextKey, JSON.stringify(contextData), { EX: 600 }).catch(() => {});
            // ✅ GREP: [TEMP-MSGID-CONTEXT-SAVED]
            console.log(`[TEMP-MSGID-CONTEXT-SAVED] tempMsgId=${tempMsgId} messageId=${messageId || 'PENDING'} enableCallback=${enableCallback}`);
        } else {
            // ✅ GREP: [TEMP-MSGID-CONTEXT-SKIP]
            if (enableCallback && !tempMsgId) {
                console.log(`[TEMP-MSGID-CONTEXT-SKIP] tempMsgId=NONE enableCallback=${enableCallback} reason=no_tempMsgId`);
            } else if (enableCallback && !this.redis) {
                console.log(`[TEMP-MSGID-CONTEXT-SKIP] tempMsgId=${tempMsgId || 'NONE'} enableCallback=${enableCallback} reason=no_redis`);
            } else if (!enableCallback) {
                console.log(`[TEMP-MSGID-CONTEXT-SKIP] tempMsgId=${tempMsgId || 'NONE'} enableCallback=${enableCallback} reason=enableCallback_false`);
            }
        }
        
        // ✅ TEMPORARY MESSAGE ID: Update mapping jika ada temporary ID
        if (tempMsgId && messageId && this.redis) {
            // ✅ GREP: [TEMP-MSGID-MAPPING-START]
            console.log(`[TEMP-MSGID-MAPPING-START] tempMsgId=${tempMsgId} messageId=${messageId} enableCallback=${enableCallback}`);
            await this.saveTempMessageIdMapping(tempMsgId, messageId);
            // ✅ GREP: [TEMP-MSGID-MAPPED]
            console.log(`[TEMP-MSGID-MAPPED] tempMsgId=${tempMsgId} messageId=${messageId}`);
        } else {
            // ✅ GREP: [TEMP-MSGID-MAPPING-SKIP]
            if (tempMsgId && !messageId) {
                console.log(`[TEMP-MSGID-MAPPING-SKIP] tempMsgId=${tempMsgId} messageId=NOT_AVAILABLE enableCallback=${enableCallback}`);
            } else if (!tempMsgId && messageId) {
                console.log(`[TEMP-MSGID-MAPPING-SKIP] tempMsgId=NONE messageId=${messageId} enableCallback=${enableCallback}`);
            }
        }
        
        // ✅ Handle messageId tracking dan enableCallback (baik messageId ada atau tidak)
        if (messageId) {
            // ✅ VALIDASI: Log untuk debugging jika messageId ada
            console.log(`🔍 [PROCESS-RESULT] Processing result for msgId: ${messageId} | enableCallback: ${enableCallback} | redis: ${!!this.redis} | tempMsgId: ${tempMsgId || 'none'}`);
            
            // ✅ REDIS-BASED: Track messageId sebagai pesan yang dikirim dari wrapper ini (prevent "bot hantu")
            // ✅ GLOBAL DEDUPLICATION: Menggunakan Redis untuk prevent duplicate di semua server/instance
            // ✅ FIXED: Non-blocking - memory cache instant, Redis write di background
            this.addSentMessageId(messageId).catch(() => {}); // Fire-and-forget, tidak blocking response
            
            // ✅ CRITICAL: Simpan remoteJid ke Redis untuk validasi nanti (check apakah pesan ada di WhatsApp)
            // ✅ FIXED: Non-blocking - sudah ada .catch(), tidak blocking
            if (this.redis && remoteJid) {
                this.redis.set(`sent_message_jid:${messageId}`, remoteJid, { EX: this.sentMessagesTTL }).catch(() => {});
            }
            
            // ✅ NEW: Simpan enableCallback flag ke Redis untuk kontrol webhook per messageId
            // ✅ CRITICAL: Flag ini akan di-check di messages.update handler untuk skip webhook jika false
            if (this.redis) {
                const callbackKey = `callback_enabled:${messageId}`;
                if (enableCallback) {
                    // ✅ Enable callback (default) - set flag ke Redis dengan TTL 7 hari
                    this.redis.set(callbackKey, '1', { EX: this.sentMessagesTTL }).catch(() => {});
                } else {
                    // ✅ Disable callback - set flag ke Redis dengan TTL 7 hari
                    this.redis.set(callbackKey, '0', { EX: this.sentMessagesTTL }).catch(() => {});
                }
            }
        }
        
        // ✅ CYCLE START: MESSAGE_SENT sebagai awal lifecycle events cycle
        // ✅ enable_callback=true: Tunggu messageId tersedia, baru queue MESSAGE_SENT sebagai awal cycle
        // ✅ enable_callback=false: Jalur normal - tunggu MESSAGE_SENT dari WhatsApp events sebagai awal cycle
        // ✅ CRITICAL: Pastikan setiap message ID punya webhook payload di queue untuk guarantee delivery
        // ✅ PERSISTENT: Masuk queue dengan priority tinggi, retry sampai terkirim
        // ✅ FIX: Handle enableCallback di luar block if(messageId) agar polling tetap jalan jika messageId belum ada
        if (this.redis && enableCallback) {
                // ✅ GREP: [MESSAGE_SENT-POLLING-START]
                console.log(`[MESSAGE_SENT-POLLING-START] tempMsgId=${tempMsgId || 'NONE'} messageId=${messageId || 'NONE'} enableCallback=${enableCallback} redis=${!!this.redis}`);
        } else {
                // ✅ GREP: [MESSAGE_SENT-POLLING-SKIP]
                if (!this.redis) {
                    console.log(`[MESSAGE_SENT-POLLING-SKIP] tempMsgId=${tempMsgId || 'NONE'} messageId=${messageId || 'NONE'} enableCallback=${enableCallback} reason=no_redis`);
                } else if (!enableCallback) {
                    console.log(`[MESSAGE_SENT-POLLING-SKIP] tempMsgId=${tempMsgId || 'NONE'} messageId=${messageId || 'NONE'} enableCallback=${enableCallback} reason=enableCallback_false`);
                }
        }
        
        if (this.redis && enableCallback) {
                // ✅ CRITICAL: enableCallback=true → Tunggu messageId tersedia dulu, baru queue MESSAGE_SENT
                // ✅ FIX: Jangan langsung queue, tunggu sampai messageId benar-benar tersedia
                // ✅ OPTIMIZED: Non-blocking, lightweight polling dengan early exit
                setImmediate(async () => {
                    try {
                        // ✅ SMART QUEUE: Load context dari Redis jika ada (prevent data loss)
                        // ✅ Context sudah disimpan lengkap di awal, jadi load dulu untuk pastikan data tidak hilang
                        let contextFromRedis = null;
                        if (tempMsgId && this.redis) {
                            const contextKey = `pending_msgsent:${tempMsgId}`;
                            const contextStr = await this.redis.get(contextKey).catch(() => null);
                            if (contextStr) {
                                try {
                                    contextFromRedis = JSON.parse(contextStr);
                                    console.log(`📦 [MESSAGE_SENT-WAIT] Loaded context from Redis for tempMsgId: ${tempMsgId}`);
                                } catch (e) {
                                    // Ignore parse error, use function params
                                }
                            }
                        }
                        
                        // ✅ Use context from Redis if available, otherwise use function params (fallback)
                        const finalTo = contextFromRedis?.to || to;
                        const finalMessage = contextFromRedis?.message || message;
                        const finalRemoteJid = contextFromRedis?.remoteJid || remoteJid;
                        
                        // ✅ OPTIMIZED: Lightweight polling - max 12 retries (~5 detik total), early exit jika ketemu
                        const waitForMessageIdAndQueue = async (retryCount = 0, maxRetries = 12) => {
                            // ✅ FAST PATH: Check messageId sudah ada → langsung queue (no delay)
                            if (messageId) {
                                // ✅ OPTIMIZED: Check dulu apakah sudah di-queue (prevent duplicate attempt)
                                if (this.redis) {
                                    const alreadyQueued = await this.redis.get(`force_queued:${messageId}:MESSAGE_SENT`).catch(() => null);
                                    if (alreadyQueued === '1') {
                                        // ✅ GREP: [MESSAGE_SENT-QUEUE-SKIP]
                                        console.log(`[MESSAGE_SENT-QUEUE-SKIP] messageId=${messageId} tempMsgId=${tempMsgId || 'NONE'} reason=already_queued`);
                                        return;
                                    }
                                }
                                // ✅ GREP: [MESSAGE_SENT-QUEUE-IMMEDIATE]
                                console.log(`[MESSAGE_SENT-QUEUE-IMMEDIATE] messageId=${messageId} tempMsgId=${tempMsgId || 'NONE'}`);
                                await this.queueMessageSentWebhook(messageId, finalTo, finalMessage, finalRemoteJid, tempMsgId);
                                return;
                            }
                            
                            // ✅ FAST PATH: Check temp mapping (hanya jika ada tempMsgId, 1 Redis call)
                            if (tempMsgId && this.redis) {
                                const mappedRealId = await this.getRealMessageIdFromTemp(tempMsgId).catch(() => null);
                                if (mappedRealId) {
                                    // ✅ GREP: [MESSAGE_SENT-QUEUE-MAPPED]
                                    console.log(`[MESSAGE_SENT-QUEUE-MAPPED] tempMsgId=${tempMsgId} messageId=${mappedRealId}`);
                                    await this.queueMessageSentWebhook(mappedRealId, finalTo, finalMessage, finalRemoteJid, tempMsgId);
                                    return;
                                }
                            }
                            
                            // ✅ Retry dengan delay (progressive: cepat di awal, lebih lama di akhir)
                            if (retryCount < maxRetries) {
                                // ✅ OPTIMIZED: Progressive delay - 100ms, 200ms, 300ms, lalu 400ms (total ~5 detik)
                                const delay = Math.min(100 + (retryCount * 100), 400);
                                
                                // ✅ GREP: [MESSAGE_SENT-POLLING-RETRY]
                                if (retryCount % 3 === 0 || retryCount === 0) {
                                    console.log(`[MESSAGE_SENT-POLLING-RETRY] retry=${retryCount + 1}/${maxRetries} tempMsgId=${tempMsgId || 'NONE'}`);
                                }
                                
                                await new Promise(resolve => setTimeout(resolve, delay));
                                
                                // ✅ Retry: Check temp mapping lagi (1 Redis call)
                                if (tempMsgId && this.redis) {
                                    const mappedRealId = await this.getRealMessageIdFromTemp(tempMsgId).catch(() => null);
                                    if (mappedRealId) {
                                        // ✅ GREP: [MESSAGE_SENT-QUEUE-MAPPED-RETRY]
                                        console.log(`[MESSAGE_SENT-QUEUE-MAPPED-RETRY] tempMsgId=${tempMsgId} messageId=${mappedRealId} retry=${retryCount + 1}`);
                                        await this.queueMessageSentWebhook(mappedRealId, finalTo, finalMessage, finalRemoteJid, tempMsgId);
                                        return;
                                    }
                                }
                                
                                // ✅ Continue retry (recursive, tapi lightweight - hanya setTimeout)
                                return await waitForMessageIdAndQueue(retryCount + 1, maxRetries);
                            } else {
                                // ✅ Max retries reached → context sudah disimpan di awal (prevent data loss)
                                // ✅ Context sudah ada di Redis dari awal, jadi tidak perlu save lagi
                                // ✅ GREP: [MESSAGE_SENT-POLLING-TIMEOUT]
                                console.log(`[MESSAGE_SENT-POLLING-TIMEOUT] tempMsgId=${tempMsgId || 'NONE'} maxRetries=${maxRetries} context_saved=true`);
                            }
                        };
                        
                        // ✅ Start waiting (non-blocking, tidak blocking main thread)
                        waitForMessageIdAndQueue().catch(err => {
                            // ✅ GREP: [MESSAGE_SENT-POLLING-ERROR]
                            console.error(`[MESSAGE_SENT-POLLING-ERROR] tempMsgId=${tempMsgId || 'NONE'} error=${err.message}`);
                        });
                    } catch (err) {
                        // ✅ GREP: [MESSAGE_SENT-POLLING-ERROR]
                        console.error(`[MESSAGE_SENT-POLLING-ERROR] tempMsgId=${tempMsgId || 'NONE'} error=${err.message}`);
                    }
                });
        } else {
            // ✅ DEFAULT: enable_callback = false → TIDAK kirim webhook MESSAGE_SENT (tidak perlu, sudah dapat original ID di response)
            // ✅ enable_callback=false: Client sudah dapat original message ID di response, tidak perlu webhook MESSAGE_SENT
            // ✅ Webhook MESSAGE_SENT hanya untuk enable_callback=true (mapping temp ke original)
            setImmediate(() => {
                const skipReason = !this.redis ? 'redis_not_available' : !enableCallback ? 'enable_callback_false_no_webhook_needed' : 'unknown';
                console.log(`⏭️ [SKIP] MESSAGE_SENT webhook skipped for msgId: ${messageId || 'pending'} (${skipReason}, client already got original ID in response, no webhook needed)`);
            });
        }
        
        // ✅ CRITICAL: SERVER_ACK check SELALU dijalankan untuk SEMUA message ID yang ada (tidak peduli skipWebhook)
        // ✅ OPTIMIZED: Minimal logging, fast check, direct generate
        // ✅ Hanya autoGenerate (OFF default) — tidak ada synthetic SERVER_ACK fallback di sini
        if (messageId && config.autoGenerate?.enabled) {
                const checkServerAckWithRetry = async (retryCount = 0, maxRetries = 2) => {
                    // ✅ SECURITY CHECK: Fast check - hanya check sentMessageIds
                    const hasMessage = await this.hasSentMessageId(messageId);
                    if (!hasMessage) {
                        return; // ✅ FIX: Return early jika message tidak ada
                    }
                    
                    // Check apakah WhatsApp sudah kirim SERVER_ACK
                    const serverAckDedupeKey = `${messageId}_2`;
                    const hasServerAckInMemory = this.dedupeCache.get(serverAckDedupeKey) && this.dedupeCache.get(serverAckDedupeKey) > Date.now();
                    
                    if (this.redis) {
                        const hasOrganicAck = await this.hasOrganicServerAckMarker(messageId);
                        
                        // ✅ CASE 1: WhatsApp sudah kirim SERVER_ACK
                        if (hasOrganicAck || hasServerAckInMemory) {
                            setImmediate(() => console.log(`✅ [SERVER_ACK] WhatsApp sent SERVER_ACK for msgId: ${messageId}`));
                            return;
                        }
                        
                        // ✅ CASE 2: WhatsApp belum kirim SERVER_ACK - retry dulu
                        if (retryCount < maxRetries) {
                            // ✅ OPTIMIZED: Smart delay - tunggu lebih lama untuk WhatsApp Server ACK asli
                            const delay = retryCount === 0 ? 1500 : 2500; // 1.5s, 2.5s (progressive backoff)
                            await new Promise(resolve => setTimeout(resolve, delay));
                            return await checkServerAckWithRetry(retryCount + 1, maxRetries);
                        }
                        
                        // ✅ CASE 3: Setelah retry, generate SERVER_ACK jika session/nomor sudah warm
                        setTimeout(async () => {
                            const jid = await this.getSentMessageRemoteJid(messageId);
                            if (!(await this.canFallbackServerAck(jid, messageId))) return;
                            const hasOrganicAck2 = await this.hasOrganicServerAckMarker(messageId);
                            
                            if (hasOrganicAck2) {
                                setImmediate(() => console.log(`✅ [SERVER_ACK] SERVER_ACK confirmed for msgId: ${messageId}`));
                                return;
                            }
                                    
                            setImmediate(() => console.log(`🔍 [AUTO-GEN] SERVER_ACK fallback for msgId: ${messageId}`));
                            
                            // ✅ FAST PATH: Langsung generate SERVER_ACK (skip validasi berat untuk speed)
                            const cleanBotNumber = this.getCleanBotNumber();
                            const serverAckDedupeKey2 = `${messageId}_2`;
                            
                            // ✅ FAST PATH: Langsung generate SERVER_ACK (skip validasi berat untuk speed)
                            this.dedupeCache.set(serverAckDedupeKey2, Date.now() + 30000);
                            
                            // ✅ CRITICAL: Generate SERVER_ACK untuk SEMUA message ID (tidak peduli skipWebhook)
                            this.enqueueWebhook({
                                event: 'SERVER_ACK',
                                bot: cleanBotNumber || this.sessionId,
                                bot_id: this.cachedBotId || 0,
                                sessionId: this.sessionId,
                                msg_id: messageId,
                                status: 'server_ack',
                                user_id: this.userId,
                                port: this.port,
                                fallback_generated: true,
                                force_send: true // ✅ FLAG: Paksa kirim
                            }, { forceDirect: true }).then(() => {
                                setImmediate(() => console.log(`✅ [SERVER_ACK] SERVER_ACK fallback generated for msgId: ${messageId}${skipWebhook ? ' (skip_webhook=true, but ACK tracked)' : ''}`));
                            }).catch(() => {});
                        }, 500); // ✅ OPTIMIZED: Delay 500ms (reduced for faster fallback after final check)
                    } else {
                        // No Redis, check memory cache only
                        if (!hasServerAckInMemory) {
                            if (retryCount < maxRetries) {
                                const delay = retryCount === 0 ? 1500 : 2500; // Same delay as Redis path
                                await new Promise(resolve => setTimeout(resolve, delay));
                                return await checkServerAckWithRetry(retryCount + 1, maxRetries);
                            }
                            setImmediate(() => console.log(`⚠️ [SERVER_ACK] No Redis, cannot check SERVER_ACK for msgId: ${messageId}`));
                        }
                    }
                };
                
                // ✅ OPTIMIZED: Smart start - delay 1s untuk beri waktu WhatsApp kirim Server ACK asli
                // ✅ CRITICAL: SELALU check SERVER_ACK untuk setiap message ID
                setTimeout(() => {
                    checkServerAckWithRetry().catch(() => {}); // Silent fail untuk speed
                }, 1000);
            }
            
            // ✅ FIXED: Cleanup old messageIds di background (non-blocking)
            // ✅ OPTIMIZED: Async cleanup untuk tidak blocking response
            if (this.sentMessageIds.size > 1000) {
                setImmediate(() => {
                    const firstId = this.sentMessageIds.values().next().value;
                    this.sentMessageIds.delete(firstId);
                });
            }
        
        // ✅ Return result untuk normal case
        return result;
    }
    
    // ✅ HIGH PERFORMANCE: Direct send button message (internal, dengan rate limiting)
    async sendButtonMessageDirect(to, message, footer, url, buttonText, title = undefined, subtitle = undefined, options = {}) {
        // ✅ FIXED: Hapus check isAuthenticated - biarkan WhatsApp yang handle timeout/error sendiri
        // ✅ ALASAN: Biarkan "bablas" - jika session masih bisa kirim (socket OPEN), biarkan kirim
        // ✅ Jika tidak bisa kirim, WhatsApp akan return timeout/error sendiri (lebih natural)
        await this.waitUntilCanSendReady();
        
        const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
        
        // ✅ HIGH TRAFFIC FIX: Timeout wrapper 10 detik (sync dengan Node.js API timeout)
        const SEND_BUTTON_MESSAGE_TIMEOUT_MS = Math.max(8000, Math.min(60000, Number(process.env.SEND_BUTTON_DIRECT_TIMEOUT_MS) || 16000));

        try {
                let result;
                
                // ✅ ANTI-SPAM: Apply unicode variants untuk semua text fields
                let processedMessage = message;
                let processedTitle = title;
                let processedSubtitle = subtitle;
                let processedFooter = footer;
                let processedButtonText = buttonText;
                
                if (config?.antiSpam?.enabled) {
                    if (typeof message === 'string') processedMessage = this.applyAntiSpamPattern(message);
                    if (typeof title === 'string') processedTitle = this.applyAntiSpamPattern(title);
                    if (typeof subtitle === 'string') processedSubtitle = this.applyAntiSpamPattern(subtitle);
                    if (typeof footer === 'string') processedFooter = this.applyAntiSpamPattern(footer);
                    if (typeof buttonText === 'string') processedButtonText = this.applyAntiSpamPattern(buttonText);
                }
                
                // Jika ada URL, gunakan format interactive message
                if (url) {
                    // ✅ BYEE-WAMOD: Format interactiveButtons dengan merchant_url (seperti di test.cjs)
                    const interactiveButtons = [
                        {
                            name: "cta_url",
                            buttonParamsJson: JSON.stringify({
                                display_text: processedButtonText,
                                url: url,
                                merchant_url: url
                            })
                        }
                    ];
                    
                    const interactiveMessage = {
                        text: processedMessage,
                        title: processedTitle || undefined, // Only include if provided (not empty string)
                        subtitle: processedSubtitle || undefined, // Only include if provided (not empty string)
                        footer: processedFooter || undefined, // Only include if provided (not empty string)
                        interactiveButtons
                    };
                    
                    // ✅ CRITICAL: Wrap sendMessage dengan timeout untuk prevent hang lebih dari 14 detik
                    result = await Promise.race([
                        this.sock.sendMessage(jid, interactiveMessage),
                        new Promise((_, reject) => 
                            setTimeout(() => reject(new Error(`Operation timed out after ${SEND_BUTTON_MESSAGE_TIMEOUT_MS/1000}s`)), SEND_BUTTON_MESSAGE_TIMEOUT_MS)
                        )
                    ]);
                } else {
                    // Jika tidak ada URL, gunakan format button message biasa
                    const buttonMessage = {
                        text: processedMessage,
                        title: processedTitle || undefined,
                        subtitle: processedSubtitle || undefined,
                        footer: processedFooter || undefined,
                        buttons: [
                            { 
                                buttonId: 'btn1', 
                                buttonText: { displayText: processedButtonText }, 
                                type: 1
                            }
                        ],
                        headerType: 1
                    };
                    
                    // ✅ CRITICAL: Wrap sendMessage dengan timeout untuk prevent hang lebih dari 14 detik
                    result = await Promise.race([
                        this.sock.sendMessage(jid, buttonMessage),
                        new Promise((_, reject) => 
                            setTimeout(() => reject(new Error(`Operation timed out after ${SEND_BUTTON_MESSAGE_TIMEOUT_MS/1000}s`)), SEND_BUTTON_MESSAGE_TIMEOUT_MS)
                        )
                    ]);
                }
                
                // ✅ Reset timeout counter jika send berhasil
                if (this.redis && result) {
                    const timeoutKey = `send_timeout_count:${this.sessionId}`;
                    await this.redis.del(timeoutKey).catch(() => {}); // Reset counter jika send berhasil
                }
                
                const messageId = result?.key?.id;
                const remoteJid = result?.key?.remoteJid || jid; // Simpan remoteJid untuk validasi nanti
                
                // ✅ REDIS-BASED: Track messageId sebagai pesan yang dikirim dari wrapper ini (prevent "bot hantu")
                // ✅ GLOBAL DEDUPLICATION: Menggunakan Redis untuk prevent duplicate di semua server/instance
                // ✅ FIXED: Non-blocking - memory cache instant, Redis write di background
                if (messageId) {
                    const enableCallback = options.enableCallback === true || options.enableCallback === '1' || options.enableCallback === 1 || options.enableCallback === 'true';
                    this.registerOutboundMessageSync(result, to, enableCallback);
                }
                
                return result;
            } catch (error) {
                const errorMsg = error.message || error.toString() || '';
                const isTimeout = errorMsg.includes('timed out') || errorMsg.includes('timeout') ||
                                 errorMsg.includes('ETIMEDOUT') || errorMsg.includes('TIMEOUT') ||
                                 (error.code && (error.code === 'ETIMEDOUT' || error.code === 'TIMEOUT'));

                if (isTimeout && this.redis) {
                    const timeoutKey = `send_timeout_count:${this.sessionId}`;
                    await this.redis.incr(timeoutKey).catch(() => {});
                    await this.redis.expire(timeoutKey, 60).catch(() => {});
                }

                throw error;
            }
    }
    
    async sendButtonMessage(to, message, footer, url, buttonText, title = undefined, subtitle = undefined, options = {}) {
        try {
            // ✅ FIXED: Hapus check isAuthenticated - biarkan WhatsApp yang handle timeout/error sendiri
            // ✅ ALASAN: Biarkan "bablas" - jika session masih bisa kirim (socket OPEN), biarkan kirim
            // ✅ Jika tidak bisa kirim, WhatsApp akan return timeout/error sendiri (lebih natural)
            await this.waitUntilCanSendReady();
            
            // ✅ NEW: Extract skipWebhook dan enableCallback dari options
            // ✅ CRITICAL: Default enableCallback = false (konsisten dengan index.js)
            // ✅ enableCallback hanya true jika explicitly set ke true
            const skipWebhook = options.skipWebhook === true || options.skipWebhook === '1' || options.skipWebhook === 1;
            const enableCallback = options.enableCallback === true || options.enableCallback === '1' || options.enableCallback === 1 || options.enableCallback === 'true';
            
            // ✅ HIGH PERFORMANCE: Fast path - langsung kirim jika bisa
            const now = Date.now();
            const timeSinceLastSend = now - this.lastMessageSendTime;
            const canSendImmediately = 
                this.messageSendQueue.length === 0 && 
                !this.isProcessingMessageQueue && 
                this.activeSends < this.maxConcurrentSends &&
                timeSinceLastSend >= this.getEffectiveMinMessageSendInterval();
            
            if (canSendImmediately) {
                // ✅ FAST PATH: Langsung kirim tanpa queue
                this.lastMessageSendTime = now;
                this.activeSends++;
                
                try {
                    const result = await this.sendButtonMessageDirect(to, message, footer, url, buttonText, title, subtitle, options);
                    
                    // ✅ OPTIMIZED: Process result in background untuk fast API response
                    // ✅ CRITICAL: Non-blocking - return result immediately, process webhook/Redis in background
                    // ✅ CRITICAL: Extract tempMsgId dari options dan pass ke processSendMessageResult
                    const tempMsgId = options.tempMsgId || null;
                    this.registerOutboundMessageSync(result, to, enableCallback);
                    this.processSendMessageResult(result, to, message, 'button', options, skipWebhook, enableCallback, tempMsgId)
                        .catch(err => console.error(`[ERROR] processSendMessageResult failed in sendButtonMessage:`, err.message));
                    
                    this.activeSends--;
                    // ✅ FAST: Return result immediately (don't wait for Redis/webhook processing)
                    return result;
                } catch (error) {
                    this.activeSends--;
                    throw error;
                }
            }
            
            // ✅ QUEUE PATH: Masukkan ke queue untuk rate limiting
            return new Promise((resolve, reject) => {
                const queueStartTime = Date.now();
                const itemId = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`; // ✅ UNIQUE ID untuk tracking
                const tempMsgId = options.tempMsgId || null;
                
                const queueItem = {
                    to,
                    message,
                    footer,
                    url,
                    buttonText,
                    messageType: 'button',
                    options,
                    skipWebhook,
                    enableCallback,
                    tempMsgId, // ✅ CRITICAL: Pass tempMsgId ke queue
                    queueStartTime, // Track waktu masuk queue
                    itemId, // ✅ UNIQUE ID untuk protection tracking
                    // processSendMessageResult sudah di processMessageQueue — jangan double.
                    resolve: (result) => {
                        try {
                            resolve(result);
                        } catch (error) {
                            reject(error);
                        }
                    },
                    reject,
                    processing: false
                };
                
                // ✅ CIRCULAR BUFFER: Jika queue penuh, hapus yang TIDAK sedang diproses
                if (this.messageSendQueue.length >= this.maxQueueSize) {
                    // ✅ AMAN: Hanya hapus item yang TIDAK sedang diproses
                    const nonProcessingItems = this.messageSendQueue.filter(item => 
                        !item.processing && 
                        !this.queueProtection.processingItems.has(item.itemId)
                    );
                    
                    if (nonProcessingItems.length > 0) {
                        // Hapus item tertua yang tidak sedang diproses
                        const oldestItem = nonProcessingItems[0];
                        const index = this.messageSendQueue.indexOf(oldestItem);
                        if (index > -1) {
                            // ✅ CRITICAL: Reject item yang dihapus untuk prevent webhook kelewat
                            oldestItem.reject(new Error('Queue full, item removed (circular buffer)'));
                            this.messageSendQueue.splice(index, 1);
                            console.warn(`⚠️ [QUEUE] Removed oldest item from queue (circular buffer), queue size: ${this.messageSendQueue.length}`);
                        }
                    } else {
                        // ✅ FALLBACK: Semua sedang diproses, reject item baru (prevent queue overflow)
                        reject(new Error('Queue full and all items are processing, please retry'));
                        return;
                    }
                }
                
                this.messageSendQueue.push(queueItem);
                
                // Start processing queue (non-blocking)
                this.processMessageQueue().catch(err => {
                    console.error('Error processing message queue:', err?.message || 'Unknown error');
                });
            });
        } catch (error) {
            // ✅ FIXED: Non-blocking console.error (fire-and-forget)
            setImmediate(() => console.error('Error sending button message:', error?.message || 'Unknown error'));
            throw error;
        }
    }
    
    // Method baru untuk interactive message dengan URL
    async sendInteractiveMessage(to, message, footer, url, buttonText, title = undefined, subtitle = undefined) {
        try {
            // ✅ FIXED: Hapus check isAuthenticated - biarkan WhatsApp yang handle timeout/error sendiri
            await this.waitUntilCanSendReady();
            
            const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
            
            // ✅ BYEE-WAMOD: Menggunakan format interactive message dari byee-wamod (seperti di test.cjs)
            const interactiveButtons = [
                {
                    name: "cta_url",
                    buttonParamsJson: JSON.stringify({
                        display_text: buttonText,
                        url: url,
                        merchant_url: url
                    })
                }
            ];
            
            const interactiveMessage = {
                text: message,
                title: title || undefined, // Only include if provided (not empty string)
                subtitle: subtitle || undefined, // Only include if provided (not empty string)
                footer: footer || undefined, // Only include if provided (not empty string)
                interactiveButtons
            };
            
            const result = await this.sock.sendMessage(jid, interactiveMessage);
            
            // ✅ SECURITY: Track messageId untuk prevent "bot hantu"
            const messageId = result?.key?.id;
            if (messageId) {
                const enableCallback = options?.enableCallback === true || options?.enableCallback === '1' || options?.enableCallback === 1 || options?.enableCallback === 'true';
                this.registerOutboundMessageSync(result, to, enableCallback);
            }
            
            return result;
            
        } catch (error) {
            // ✅ FIXED: Non-blocking console.error (fire-and-forget)
            setImmediate(() => console.error('Error sending interactive message:', error?.message || 'Unknown error'));
            throw error;
        }
    }
    
    // Method untuk mengirim album (multiple images)
    async sendAlbumMessage(to, mediaArray, caption = '') {
        try {
            // ✅ FIXED: Hapus check isAuthenticated - biarkan WhatsApp yang handle timeout/error sendiri
            await this.waitUntilCanSendReady();
            
            const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
            
            const result = await this.sock.sendMessage(jid, { 
                album: mediaArray, 
                caption: caption 
            });
            
            // ✅ SECURITY: Track messageId untuk prevent "bot hantu"
            const messageId = result?.key?.id;
            if (messageId) {
                this.registerOutboundMessageSync(result, to, false);
            }
            
            return result;
            
        } catch (error) {
            // ✅ FIXED: Non-blocking console.error (fire-and-forget)
            setImmediate(() => console.error('Error sending album message:', error?.message || 'Unknown error'));
            throw error;
        }
    }
    
    // ============================================================================
    // 📸 SEND IMAGE WITH BUTTON URL (cta_url) - Support header, footer, image, button, text
    // ============================================================================
    /**
     * Send image with button URL (cta_url) - seperti di test.cjs
     * Support: header, footer, image, button, text (caption, title, subtitle)
     * @param {string} to - Nomor telepon (dengan atau tanpa @s.whatsapp.net)
     * @param {string} imageUrl - URL gambar
     * @param {string} caption - Caption untuk gambar
     * @param {string} title - Title (optional)
     * @param {string} subtitle - Subtitle (optional)
     * @param {string} footer - Footer text (optional)
     * @param {string} url - URL untuk button
     * @param {string} buttonText - Text untuk button
     * @param {Object} options - Options (skipWebhook, enableCallback, dll)
     * @returns {Promise<Object>} Result dari sendMessage
     */
    async sendImageButtonMessage(to, imageUrl, caption, title, subtitle, footer, url, buttonText, options = {}) {
        // ✅ LOGGING DI AWAL FUNCTION - PASTI MUNCUL JIKA FUNCTION DIPANGGIL
        const timestamp = new Date().toISOString();
        // ✅ REMOVED: Image debug logging (prevent binary data in logs, too verbose)
        
        try {
            await this.waitUntilCanSendReady();
            
            if (!imageUrl) {
                throw new Error('Image URL or local file path is required');
            }
            
            if (!url) {
                throw new Error('URL is required for button');
            }
            
            const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
            const recipientInfo = jid || 'unknown';
            
            // ✅ REMOVED: Image debug logging (prevent binary data in logs, too verbose)
            
            // ✅ SMART: Support local file path atau URL
            // Check jika imageUrl adalah local file path (starts with /) atau URL (starts with http)
            const isLocalFile = imageUrl.startsWith('/') && !imageUrl.startsWith('http');
            const isUrl = imageUrl.startsWith('http://') || imageUrl.startsWith('https://');
            
            // ✅ SPEED OPTIMIZATION: Pre-download dan cache images untuk eliminate 1s delay
            let imageConfig;
            if (isUrl) {
                // ✅ Check cache untuk local file (eliminate download delay)
                const cached = await this.getImageUrlCache(imageUrl).catch(() => null);
                let localFilePath = null;
                
                if (cached && cached.localPath) {
                    try {
                        if (fs.existsSync(cached.localPath)) {
                            localFilePath = cached.localPath;
                            // ✅ REMOVED: Image URL logging (prevent binary data in logs)
                        }
                    } catch (err) {
                        // File tidak ada, akan download
                    }
                }
                
                // ✅ Jika tidak ada cached file, download dan cache (non-blocking untuk next time)
                if (!localFilePath) {
                    const downloadPromise = this.downloadAndCacheImage(imageUrl).catch(() => null);
                    localFilePath = await Promise.race([
                        downloadPromise,
                        new Promise((resolve) => setTimeout(() => resolve(null), 200)) // Max 200ms wait
                    ]);
                    
                    if (localFilePath) {
                        // ✅ REMOVED: Image URL logging (prevent binary data in logs)
                    }
                }
                
                // ✅ Use local file if available (eliminate 1s download delay)
                if (localFilePath) {
                    try {
                        imageConfig = { url: localFilePath };
                        // ✅ REMOVED: Image cache logging (prevent binary data in logs)
                    } catch (err) {
                        // ✅ Fallback to URL jika read file gagal
                        console.warn(`⚠️ [IMAGE-CACHE] Failed to read cached file, using URL: ${err.message}`);
                        imageConfig = { url: imageUrl };
                    }
                } else {
                    // ✅ Fallback: Pass URL ke library (library akan download)
                    imageConfig = { url: imageUrl };
                }
            } else if (isLocalFile) {
                // ✅ Local file path - byee-wamod akan handle
                imageConfig = { url: imageUrl };
            } else {
                // ✅ Unknown format, pass as-is
                imageConfig = { url: imageUrl };
            }
            
            // ✅ REMOVED: Image debug logging (prevent binary data in logs)
            
            // ✅ BYEE-WAMOD: Format image dengan interactiveButtons (seperti di test.cjs)
            // Dokumentasi: https://github.com/bylinggha/byee-wamod
            const imageWithButton = {
                image: imageConfig,
                caption: caption || '',
                title: title || undefined, // Only include if provided (not empty string)
                subtitle: subtitle || undefined, // Only include if provided (not empty string)
                footer: footer || undefined, // Only include if provided (not empty string)
                interactiveButtons: [
                    {
                        name: "cta_url",
                        buttonParamsJson: JSON.stringify({
                            display_text: buttonText,
                            url: url,
                            merchant_url: url
                        })
                    }
                ],
                hasMediaAttachment: false
            };
            
            // ✅ REMOVED: Image URL logging (prevent binary data in logs)
            console.log(`   Caption: ${caption || '(empty)'}`);
            console.log(`   Title: ${title || '(empty)'}`);
            console.log(`   Subtitle: ${subtitle || '(empty)'}`);
            console.log(`   Footer: ${footer || '(empty)'}`);
            console.log(`   Button: ${buttonText}`);
            console.log(`   URL: ${url}`);
            console.log(`   📦 Using byee-wamod with image + interactiveButtons`);
            // ✅ REMOVED: Image debug logging (prevent binary data in logs)
            
            // ✅ HIGH TRAFFIC FIX: Timeout wrapper 9 detik untuk prevent hang
            const SEND_IMAGE_BUTTON_TIMEOUT_MS = Math.max(8000, Math.min(60000, Number(process.env.SEND_IMAGE_BUTTON_DIRECT_TIMEOUT_MS) || 16000));

            // Samakan gate dengan jalur teks/button: assertCanSendNow (sock.user ATAU creds.me).
            // Dulu wajib hasUser saja → false reject saat reconnect (isAuthenticated=true, hasUser=false, State:open).
            const deviceStatus = this.getRealDeviceStatus();
            try {
                this.assertCanSendNow();
            } catch (gateErr) {
                const errorMsg = `${gateErr?.message || 'not ready'} | State: ${deviceStatus.connectionState}, isAuthenticated: ${deviceStatus.isAuthenticated}, hasUser: ${deviceStatus.hasUser}, hasIdentity: ${this.hasSendableWaIdentity()}`;
                throw new Error(`Connection closed: ${errorMsg}`);
            }
            if (!deviceStatus.isConnected) {
                // WS helper kadang unknown — tetap coba jika assertCanSendNow lolos
            }
            
            try {
                    const result = await Promise.race([
                        this.sock.sendMessage(jid, imageWithButton),
                        new Promise((_, reject) => 
                            setTimeout(() => reject(new Error(`Operation timed out after ${SEND_IMAGE_BUTTON_TIMEOUT_MS/1000}s`)), SEND_IMAGE_BUTTON_TIMEOUT_MS)
                        )
                    ]);
                    
                    // ✅ DETAILED LOGGING: Log result structure untuk debugging
                    console.info(`[INFO] [IMAGE-BUTTON] Send result received:`);
                    console.info(`[INFO]   - result type: ${typeof result}`);
                    console.info(`[INFO]   - result is null: ${result === null}`);
                    console.info(`[INFO]   - result is undefined: ${result === undefined}`);
                    console.info(`[INFO]   - result keys: ${result ? Object.keys(result).join(', ') : 'N/A'}`);
                    console.info(`[INFO]   - result.key exists: ${!!(result && result.key)}`);
                    console.info(`[INFO]   - result.key type: ${result && result.key ? typeof result.key : 'N/A'}`);
                    console.info(`[INFO]   - result.key keys: ${result && result.key ? Object.keys(result.key).join(', ') : 'N/A'}`);
                    console.info(`[INFO]   - result.key.id exists: ${!!(result && result.key && result.key.id)}`);
                    console.info(`[INFO]   - result.key.id value: ${result?.key?.id || 'UNDEFINED'}`);
                    console.info(`[INFO]   - result.key.remoteJid: ${result?.key?.remoteJid || 'UNDEFINED'}`);
                    console.info(`[INFO]   - Full result structure: ${JSON.stringify(result, null, 2).substring(0, 500)}`);
                    
                    // ✅ CRITICAL: Check if result.key is missing (connection might be closed)
                    if (!result) {
                        console.info(`[INFO] [IMAGE-BUTTON] ⚠️ Result is null/undefined - connection might be closed`);
                        // Check connection status again
                        const statusAfterSend = this.getRealDeviceStatus();
                        console.info(`[INFO] [IMAGE-BUTTON] Connection status after send:`);
                        console.info(`[INFO]   - isConnected: ${statusAfterSend.isConnected}`);
                        console.info(`[INFO]   - isAuthenticated: ${statusAfterSend.isAuthenticated}`);
                        console.info(`[INFO]   - hasUser: ${statusAfterSend.hasUser}`);
                        console.info(`[INFO]   - connectionState: ${statusAfterSend.connectionState}`);
                        
                        const recoverable = this.hasSendableWaIdentity() && (statusAfterSend.isAuthenticated || !!this.authState?.creds?.me?.id);
                        if (!recoverable) {
                            throw new Error('Connection closed: Result is null and connection is not authenticated');
                        }
                        throw new Error('Send failed: Result is null but connection is still authenticated');
                    }
                    
                    if (!result.key) {
                        const statusAfterSend = this.getRealDeviceStatus();
                        const recoverable = this.hasSendableWaIdentity() && (statusAfterSend.isAuthenticated || !!this.authState?.creds?.me?.id);
                        if (!recoverable) {
                            throw new Error('Connection closed: result.key is undefined and connection is not authenticated');
                        }
                        throw new Error('Send failed: result.key is undefined but connection is still authenticated');
                    }
                    
                    // ✅ Reset timeout counter jika send berhasil
                    if (this.redis && result) {
                        const timeoutKey = `send_timeout_count:${this.sessionId}`;
                        await this.redis.del(timeoutKey).catch(() => {});
                    }
                    
                    const messageId = result?.key?.id;
                    const remoteJid = result?.key?.remoteJid || jid;
                    
                    // ✅ LOG: Check if messageId is missing
                    if (!messageId) {
                        console.info(`[INFO] [IMAGE-BUTTON] ⚠️ messageId is undefined - result.key.id is missing`);
                        console.info(`[INFO]   - This might indicate the message was not successfully sent`);
                        console.info(`[INFO]   - But connection is still active, so might be recoverable`);
                    } else {
                        console.info(`[INFO] [IMAGE-BUTTON] ✅ Message ID obtained: ${messageId}`);
                    }
                    
                    // ✅ SECURITY: Track messageId — SYNC sebelum setImmediate (fix race SERVER_ACK)
                    if (messageId) {
                        const skipWebhook = options.skipWebhook === true || options.skipWebhook === '1' || options.skipWebhook === 1;
                        const enableCallback = options.enableCallback === true || options.enableCallback === '1' || options.enableCallback === 1 || options.enableCallback === 'true';
                        this.registerOutboundMessageSync(result, to, enableCallback);

                        const tempMsgId = options.tempMsgId || null;
                        if (!skipWebhook) {
                            setImmediate(() => {
                                this.processSendMessageResult(result, to, caption || '', 'image-button', options, skipWebhook, enableCallback, tempMsgId)
                                    .catch(err => console.error(`[ERROR] processSendMessageResult failed:`, err.message));
                            });
                        }
                    }
                    
                    console.log(`✅ [IMAGE-BUTTON] Image with button sent! ID: ${messageId}`);
                    
                    // ✅ SMART CACHE: Update cache setelah berhasil send (untuk URL)
                    // ✅ OPTIMIZED: Non-blocking, fire and forget (tidak menghalangi pengiriman)
                    if (isUrl) {
                        setImmediate(() => {
                            this.setImageUrlCache(imageUrl, { 
                                processed: true, 
                                messageId: messageId,
                                lastUsed: Date.now()
                            });
                        });
                    }
                    
                    return result;
                    
                } catch (error) {
                    const errorMsg = error.message || error.toString() || '';
                    const isTimeout = errorMsg.includes('timed out') || errorMsg.includes('timeout') ||
                                     errorMsg.includes('ETIMEDOUT') || errorMsg.includes('TIMEOUT') ||
                                     (error.code && (error.code === 'ETIMEDOUT' || error.code === 'TIMEOUT'));

                    const statusOnError = this.getRealDeviceStatus();
                    // Connection "closed" hanya jika tak ada identitas kirim (user/creds) — jangan pakai hasUser saja
                    const isConnectionClosed = !this.hasSendableWaIdentity() && !statusOnError.isAuthenticated;

                    if (isConnectionClosed) {
                        throw error;
                    }
                    if (!isConnectionClosed && isMediaConnFlakyError(error)) {
                        console.warn(
                            `⚠️ [IMAGE-BUTTON] Media/conn flaky — fallback tombol tanpa gambar (${this.sessionId || 'unknown'})`
                        );
                        return await this.sendButtonMessage(
                            to,
                            caption || '',
                            footer || '',
                            url,
                            buttonText,
                            title,
                            subtitle,
                            options
                        );
                    }
                    if (isTimeout && this.redis) {
                        const timeoutKey = `send_timeout_count:${this.sessionId}`;
                        await this.redis.incr(timeoutKey).catch(() => {});
                        await this.redis.expire(timeoutKey, 60).catch(() => {});
                    }

                    throw error;
                }
            
        } catch (error) {
            // ✅ Final error logging dengan kesimpulan
            // Logging ringkas — jangan spam journal (kesimpulan panjang diganti 1 baris)
            const finalStatus = this.getRealDeviceStatus();
            const closed = !this.hasSendableWaIdentity() && !finalStatus.isAuthenticated;
            console.error(
                `❌ [IMAGE-BUTTON] Failed: ${error?.message || error} | closed=${closed} state=${finalStatus.connectionState} auth=${finalStatus.isAuthenticated} hasUser=${finalStatus.hasUser} hasIdentity=${this.hasSendableWaIdentity()}`
            );
            throw error;
        }
    }
    
    // ============================================================================
    // 🔧 SMART DEVICE STATUS CHECK
    // ============================================================================
    
    /**
     * Check real device status dengan comprehensive validation
     * @returns {Object} { isConnected, isAuthenticated, hasUser, connectionState, details }
     */
    getRealDeviceStatus() {
        const status = {
            isConnected: false,
            isAuthenticated: false,
            hasUser: false,
            connectionState: 'unknown',
            details: {}
        };
        
        try {
            // Check socket exists
            if (!this.sock) {
                status.connectionState = 'no_socket';
                status.details.reason = 'Socket tidak ada';
                return status;
            }
            
            // ✅ byee-wamod: `sock.ws.readyState` sering undefined — pakai helper (baca .socket / isOpen)
            const wsState = getBaileysWebSocketReadyState(this.sock);
            const credMeId = this.authState?.creds?.me?.id;
            const hasUser = !!(this.sock.user && this.sock.user.id);
            // 0 = CONNECTING, 1 = OPEN, 2 = CLOSING, 3 = CLOSED
            if (wsState === 1) {
                status.isConnected = true;
                status.connectionState = 'open';
            } else if (wsState === 0) {
                status.connectionState = 'connecting';
                status.details.reason = 'WebSocket sedang connecting';
            } else if (wsState === 2) {
                status.connectionState = 'closing';
                status.details.reason = 'WebSocket sedang closing';
            } else if (wsState === 3) {
                status.connectionState = 'closed';
                status.details.reason = 'WebSocket sudah closed';
            } else if (wsState === undefined) {
                // Jangan anggap open jika wsState tidak terbaca — hindari kirim saat socket sebenarnya mati (#2491)
                status.isConnected = false;
                status.connectionState = 'unknown_ws_state';
                status.details.reason = 'WebSocket state tidak terbaca';
            } else {
                status.connectionState = 'unknown_ws_state';
                status.details.reason = `WebSocket state: ${wsState}`;
            }
            
            // ✅ Reconcile: flag auth sering ketinggalan setelah reconnect saat user/creds sudah ada
            if ((hasUser || credMeId) && this.isAuthenticated !== true) {
                if (wsState === 1 || wsState === 0 || (wsState === undefined && (hasUser || credMeId))) {
                    this.isAuthenticated = true;
                }
            }
            
            status.isAuthenticated = this.isAuthenticated === true;
            status.hasUser = hasUser;
            
            if (hasUser) {
                status.details.userId = this.sock.user.id;
                status.details.botNumber = this.getCleanBotNumber();
            }
            
            // Final validation: device is truly connected if all conditions met
            const trulyConnected = status.isConnected && status.isAuthenticated && status.hasUser;
            status.details.trulyConnected = trulyConnected;
            
            return status;
        } catch (error) {
            status.connectionState = 'error';
            status.details.error = error.message;
            return status;
        }
    }
    
    stopAllBackgroundIntervals(reason = 'cleanup') {
        this.terminalDisconnect = true;
        const clearTimer = (name, clearFn = clearTimeout) => {
            if (this[name]) {
                try {
                    clearFn(this[name]);
                } catch (_) {}
                this[name] = null;
            }
        };

        clearTimer('pairingTimeout', clearTimeout);
        clearTimer('reconnectTimer', clearTimeout);
        clearTimer('authPollingInterval', clearInterval);
        clearTimer('cacheCleanupInterval', clearInterval);
        this.stopHeartbeatMonitoring();
        this.stopConnectionHealthCheck();
        this.stopOutboxWorker(reason);

        if (Array.isArray(this.messageSendQueue) && this.messageSendQueue.length > 0) {
            for (const item of this.messageSendQueue.splice(0)) {
                try {
                    item.reject?.(new Error('connection closed'));
                } catch (_) {}
            }
        }
        this.queueProtection?.processingItems?.clear?.();
        this.queueProtection?.completedItems?.clear?.();
        this.activeSends = 0;
        this.isProcessingMessageQueue = false;

        this.dedupeCache?.clear?.();
        this.imageUrlCache?.clear?.();
        this.imageDownloadPromises?.clear?.();
        this.whatsappNumberCache?.clear?.();
        this.sentMessageIds?.clear?.();
        this.ackCountPerNumber?.clear?.();
        this.statusHistory?.clear?.();

        try {
            this.sock?.ev?.removeAllListeners?.();
        } catch (_) {}
        try {
            this.sock?.ws?.removeAllListeners?.();
        } catch (_) {}
        // close() pada socket yang masih CONNECTING memicu abortHandshake → emit 'error'
        // secara async. Listener baru saja dihapus di atas, jadi tanpa penampung ini
        // EventEmitter melempar ke process (uncaughtException) dan try/catch tidak menangkapnya.
        try {
            this.sock?.ws?.on?.('error', () => {});
            this.sock?.ws?.socket?.on?.('error', () => {});
        } catch (_) {}
        try {
            this.sock?.noise?.destroy?.();
        } catch (_) {}
        try {
            const ws = this.sock?.ws;
            if (ws && typeof ws.close === 'function') {
                const state = ws.readyState;
                // 0=CONNECTING, 1=OPEN — skip close if already closing/closed
                if (state === 0 || state === 1) {
                    ws.close();
                }
            }
        } catch (_) {}
        try {
            this.sock?.end?.();
        } catch (_) {}
        try {
            this.sock?.destroy?.();
        } catch (_) {}
        this.sock = null;
    }

    // ============================================================================
    // 🔧 SMART DISCONNECT dengan retry 3 kali
    // ============================================================================

    /** Tulis marker banned di disk agar cron cleanup-sessions-minute hapus folder. */
    writeSessionBannedMarker(reason) {
        try {
            const sessionDir = path.join(process.cwd(), 'sessions', this.sessionId);
            if (!fs.existsSync(sessionDir)) return;
            const marker = path.join(sessionDir, '.banned');
            const payload = JSON.stringify({
                reason: reason ?? 'forbidden',
                at: new Date().toISOString(),
                sessionId: this.sessionId,
            });
            fs.writeFileSync(marker, payload, { encoding: 'utf8', flag: 'w' });
        } catch (e) {
            console.warn(`⚠️ [BANNED] Failed to write marker for ${this.sessionId}: ${e?.message || e}`);
        }
    }
    
    /**
     * Smart disconnect dengan retry 3 kali dan status checking
     * Hanya disconnect jika device benar-benar connected
     * @param {Object} options - { force: boolean, reason: string }
     * @returns {Promise<boolean>} true jika berhasil disconnect
     */
    async smartDisconnect(options = {}) {
        const { force = false, reason = 'Manual disconnect' } = options;
        const maxRetries = 3;
        let lastError = null;
        
        // Check status dulu
        const status = this.getRealDeviceStatus();
        
        // Jika tidak connected dan tidak force, skip
        if (!force && !status.isConnected && !status.isAuthenticated) {
            console.log(`⏭️  Skip disconnect: Device sudah tidak connected (${status.connectionState})`);
            return true; // Already disconnected
        }
        
        console.log(`🔌 Attempting smart disconnect for ${this.sessionId} (reason: ${reason})`);
        console.log(`📊 Device status: connected=${status.isConnected}, authenticated=${status.isAuthenticated}, hasUser=${status.hasUser}`);
        
        // ✅ MEMORY LEAK FIX: Stop semua timers dan intervals untuk prevent memory leak
        this.stopHeartbeatMonitoring();
        this.stopConnectionHealthCheck();
        
        // ✅ Stop cache cleanup interval
        if (this.cacheCleanupInterval) {
            clearInterval(this.cacheCleanupInterval);
            this.cacheCleanupInterval = null;
            console.log(`🛑 Stopped cache cleanup interval for ${this.sessionId}`);
        }
        
        // ✅ SMART: Stop outbox worker dengan proper cleanup
        this.stopOutboxWorker('smart_disconnect');

        // Clear timeouts
        if (this.pairingTimeout) {
            clearTimeout(this.pairingTimeout);
            this.pairingTimeout = null;
        }
        if (this.authPollingInterval) {
            clearInterval(this.authPollingInterval);
            this.authPollingInterval = null;
        }
        
        // ✅ MEMORY LEAK FIX: Clear semua caches untuk prevent memory heap growth
        this.dedupeCache.clear();
        this.imageUrlCache.clear();
        this.imageDownloadPromises.clear();
        this.whatsappNumberCache.clear();
        this.sentMessageIds.clear();
        this.ackCountPerNumber.clear();
        if (this.statusHistory) {
            this.statusHistory.clear();
        }
        
        // ✅ REDIS CLEANUP: Close Redis connection jika ada (prevent connection leak)
        if (this.redis && this.redis !== global.sharedRedis) {
            try {
                // ✅ Hanya close jika bukan shared Redis (shared Redis digunakan oleh semua sessions)
                this.redis.quit().catch(() => {}); // Fire and forget, prevent hang
                this.redis = null;
                console.log(`🔌 Redis connection closed for ${this.sessionId}`);
            } catch (err) {
                console.warn(`⚠️ Error closing Redis connection: ${err.message}`);
                this.redis = null; // Set null anyway untuk prevent leak
            }
        }
        
        // Retry disconnect up to 3 times
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // Check status again before each attempt
                const currentStatus = this.getRealDeviceStatus();
                
                if (!currentStatus.isConnected && !force) {
                    console.log(`✅ Device already disconnected (attempt ${attempt})`);
                    return true;
                }
                
                if (this.sock) {
                    // ✅ MEMORY LEAK FIX: Remove all event listeners sebelum close
                    if (this.sock.ev) {
                        try {
                            this.sock.ev.removeAllListeners();
                            console.log(`🧹 Removed all event listeners for ${this.sessionId}`);
                        } catch (err) {
                            console.warn(`⚠️ Error removing event listeners: ${err.message}`);
                        }
                    }
                    if (this.sock.ws) {
                        try {
                            this.sock.ws.removeAllListeners();
                        } catch (err) {
                            console.warn(`⚠️ Error removing ws listeners: ${err.message}`);
                        }
                    }
                    
                    // Close WebSocket gracefully (byee-wamod: cek open via helper)
                    if (isBaileysWebSocketOpen(this.sock) && this.sock.ws) {
                        console.log(`🔌 Closing WebSocket (attempt ${attempt}/${maxRetries})...`);
                        this.sock.ws.close();
                        
                        // Wait a bit for graceful close
                        await new Promise(resolve => setTimeout(resolve, 500));
                    }
                }
                
                // Verify disconnect
                const verifyStatus = this.getRealDeviceStatus();
                if (!verifyStatus.isConnected || force) {
                    console.log(`✅ Successfully disconnected (attempt ${attempt})`);
                    this.isAuthenticated = false;
                    return true;
                }
                
                // If still connected and not last attempt, wait before retry
                if (attempt < maxRetries) {
                    const waitTime = attempt * 1000; // 1s, 2s
                    console.log(`⚠️  Still connected, retrying in ${waitTime}ms...`);
                    await new Promise(resolve => setTimeout(resolve, waitTime));
                }
                
            } catch (error) {
                lastError = error;
                console.warn(`⚠️  Disconnect attempt ${attempt} failed: ${error.message}`);
                
                if (attempt < maxRetries) {
                    const waitTime = attempt * 1000;
                    await new Promise(resolve => setTimeout(resolve, waitTime));
                }
            }
        }
        
        // Final check
        const finalStatus = this.getRealDeviceStatus();
        if (!finalStatus.isConnected) {
            console.log(`✅ Disconnect successful after retries`);
            this.isAuthenticated = false;
            return true;
        }
        
        // If still connected after all retries, log warning but don't throw
        console.warn(`⚠️  Disconnect incomplete after ${maxRetries} attempts, but continuing...`);
        this.isAuthenticated = false;
        return false; // Partial success
    }
    
    // ============================================================================
    // ?? LOGOUT - Hanya dipanggil dari API logout
    // ============================================================================
    
    /**
     * LOGOUT PENUH dari WhatsApp server dengan retry 3 kali dan status checking
     * 
     * PERBEDAAN dengan smartDisconnect():
     * - smartDisconnect() = hanya close WebSocket (disconnect)
     * - logout() = memanggil sock.logout() untuk logout dari WhatsApp server + cleanup
     * 
     * WARNING: Hanya dipanggil dari API logout endpoint (/logout, /api/logout, /baileys/logout)
     * JANGAN dipanggil dari webhook response - gunakan smartDisconnect() untuk itu!
     * 
     * @returns {Promise<void>}
     */
    async logout() {
        const maxRetries = 3;
        let lastError = null;
        
        console.log(`🚪 [LOGOUT API] Starting FULL LOGOUT from WhatsApp server for session ${this.sessionId}`);
        console.log(`📌 Note: This will call sock.logout() to logout from WhatsApp server (not just disconnect)`);
        
        // Check status dulu
        const status = this.getRealDeviceStatus();
        console.log(`📊 Device status before logout: connected=${status.isConnected}, authenticated=${status.isAuthenticated}, hasUser=${status.hasUser}`);
        
        // ⏰ Clear pairing timeout jika ada
        if (this.pairingTimeout) {
            clearTimeout(this.pairingTimeout);
            this.pairingTimeout = null;
            console.log(`⏰ Pairing timeout cleared`);
        }
        
        // ✅ MEMORY LEAK FIX: Stop semua timers dan intervals untuk prevent memory leak
        this.stopHeartbeatMonitoring();
        this.stopConnectionHealthCheck();
        
        // ✅ Stop cache cleanup interval
        if (this.cacheCleanupInterval) {
            clearInterval(this.cacheCleanupInterval);
            this.cacheCleanupInterval = null;
        }
        
        // ✅ SMART: Stop outbox worker dengan proper cleanup
        this.stopOutboxWorker('logout');

        // Clear auth polling
        if (this.authPollingInterval) {
            clearInterval(this.authPollingInterval);
            this.authPollingInterval = null;
        }
        
        // ✅ MEMORY LEAK FIX: Clear semua caches untuk prevent memory heap growth
        this.dedupeCache.clear();
        this.imageUrlCache.clear();
        this.imageDownloadPromises.clear();
        this.whatsappNumberCache.clear();
        this.sentMessageIds.clear();
        this.ackCountPerNumber.clear();
        if (this.statusHistory) {
            this.statusHistory.clear();
        }
        
        // ✅ REDIS CLEANUP: Close Redis connection jika ada (prevent connection leak)
        // 2026-09-12: this.redis = global.sharedRedis (satu klien untuk semua sesi di shard). quit() di sini
        // mematikan Redis SELURUH shard ~9 s tiap ada logout (60-130×/jam/lib) → ACK/dedupe sesi lain gagal diam-diam
        // → DEAF_SESSION_STALE → zombie. smartDisconnect() sudah punya pengaman ini; logout() belum.
        if (this.redis && this.redis !== global.sharedRedis) {
            try {
                // ✅ Redis client akan auto-reconnect, jadi cukup quit untuk graceful close
                this.redis.quit().catch(() => {}); // Fire and forget, prevent hang
                this.redis = null;
                console.log(`🔌 Redis connection closed for ${this.sessionId}`);
            } catch (err) {
                console.warn(`⚠️ Error closing Redis connection: ${err.message}`);
                this.redis = null; // Set null anyway untuk prevent leak
            }
        }
        
        // Sesi stub (requestPairingCode menulis creds.me sendiri, belum pernah pair-success): JANGAN kirim
        // sock.logout() ke WhatsApp. Logout dengan identitas stub tepat sebelum kode baru diminta untuk nomor
        // yang sama membuat pendaftaran companion nomor itu dibatalkan server → "Gagal menautkan perangkat".
        // Data 7–8 Sep: sesi yang dibuat ≤5 s setelah logout stub nomor sama tersambung 11 % vs 66 %.
        const stubOnly = Boolean(this.authState?.creds?.me?.id) && !credsLookAuthenticated(this.authState?.creds);
        if (stubOnly) {
            console.log(`⏭️ [LOGOUT API] sesi stub pairing (belum pair-success) — buang lokal saja, tanpa sock.logout() session=${this.sessionId}`);
        }

        // Retry logout up to 3 times
        for (let attempt = 1; attempt <= maxRetries && !stubOnly; attempt++) {
            try {
                // Check status before each attempt
                const currentStatus = this.getRealDeviceStatus();
                
                if (!currentStatus.isConnected && !currentStatus.hasUser) {
                    console.log(`⏭️  Device already disconnected, skipping sock.logout() (attempt ${attempt})`);
                    break; // Skip logout call, proceed to cleanup
                }
                
                if (this.sock && currentStatus.hasUser) {
                    console.log(`🚪 [LOGOUT API] Calling sock.logout() to logout from WhatsApp server (attempt ${attempt}/${maxRetries})...`);
                    await this.sock.logout();
                    console.log(`✅ [LOGOUT API] sock.logout() successful - logged out from WhatsApp server (attempt ${attempt})`);
                    
                    // Wait a bit for Baileys to complete cleanup
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    
                    // Verify logout
                    const verifyStatus = this.getRealDeviceStatus();
                    if (!verifyStatus.hasUser) {
                        console.log(`✅ Logout verified successful (attempt ${attempt})`);
                        break; // Success, exit retry loop
                    }
                    
                    // If still has user and not last attempt, retry
                    if (attempt < maxRetries) {
                        const waitTime = attempt * 1000;
                        console.log(`⚠️  Still has user, retrying in ${waitTime}ms...`);
                        await new Promise(resolve => setTimeout(resolve, waitTime));
                    }
                } else {
                    console.log(`⏭️  No socket or user, skipping sock.logout() (attempt ${attempt})`);
                    break; // No need to logout, proceed to cleanup
                }
                
            } catch (error) {
                lastError = error;
                console.warn(`⚠️  Logout attempt ${attempt} failed: ${error.message}`);
                
                // If it's a "not logged in" error, that's actually success 
                if (error.message && (error.message.includes('not logged in') || error.message.includes('logged out'))) {
                    console.log(`✅ Already logged out (attempt ${attempt})`);
                    break; // Success, exit retry loop
                }
                
                if (attempt < maxRetries) {
                    const waitTime = attempt * 1000;
                    await new Promise(resolve => setTimeout(resolve, waitTime));
                }
            }
        }
        
        // Final status check
        const finalStatus = this.getRealDeviceStatus();
        if (finalStatus.hasUser && lastError) {
            console.warn(`⚠️  Logout incomplete after ${maxRetries} attempts, but proceeding with cleanup...`);
        } else {
            console.log(`✅ Logout completed for session ${this.sessionId}`);
        }
        
        // 🔧 FIX CRASH: Wait untuk Baileys selesai cleanup dulu
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        // ✅ DISABLED: Jangan hapus session folder - biarkan cron yang handle cleanup
        // ✅ User ingin manual control via cron job untuk cleanup session
        // ? Cleanup this session folder SETELAH wait
        // ✅ V2: Use process.cwd() directly (same as v1, guardian runs from parent)
        // const sessionDir = path.join(process.cwd(), 'sessions', this.sessionId);
        // if (fs.existsSync(sessionDir)) {
        //     try {
        //         fs.rmSync(sessionDir, { recursive: true, force: true });
        //         console.log(`🗑️ Deleted session folder: ${this.sessionId}`);
        //     } catch (rmError) {
        //         console.warn(`⚠️ Failed to delete session folder: ${rmError.message}`);
        //         // Non-critical, lanjut aja
        //     }
        // }
        console.log(`ℹ️  Session folder kept for cron cleanup: ${this.sessionId}`);
        
        // ✅ Hapus dari database/memory saja, folder biarkan cron yang handle
        await this.sessionManager.deleteSession(this.sessionId);
        
        // ✅ DESTROY: Destroy socket SETELAH semua cleanup selesai (final cleanup)
        // ✅ Tempat ini AMAN karena logout API hanya dipanggil dari endpoint, tidak mengganggu pairing
        if (this.sock) {
            try {
                // ✅ MEMORY LEAK FIX: Remove all event listeners sebelum destroy
                if (this.sock.ev) {
                    try {
                        this.sock.ev.removeAllListeners();
                        console.log(`🧹 Removed all event listeners for logout session: ${this.sessionId}`);
                    } catch (evErr) {
                        console.warn(`⚠️ Error removing event listeners: ${evErr.message}`);
                    }
                }
                
                // Close WebSocket jika masih open
                if (isBaileysWebSocketOpen(this.sock) && this.sock.ws) {
                    this.sock.ws.close();
                }
                // Destroy socket jika ada method destroy
                if (typeof this.sock.end === 'function') {
                    this.sock.end();
                }
                if (typeof this.sock.destroy === 'function') {
                    this.sock.destroy();
                }
                // Clear socket reference untuk cleanup memory
                this.sock = null;
                console.log(`🗑️ [DESTROY] Socket destroyed for logout session: ${this.sessionId}`);
            } catch (destroyErr) {
                console.warn(`⚠️ [DESTROY] Error destroying socket: ${destroyErr.message}`);
            }
        }
        
        // ? Trigger global orphan session cleanup (check ALL sessions vs DB)
        try {
            console.log('🧹 Triggering global orphan session cleanup...');
            const cleanupUrl = `http://localhost:${this.port || 7001}/baileys/cleanup-sessions`;
            await axios.get(cleanupUrl, { timeout: 30000 });
            console.log('✅ Global orphan cleanup triggered');
        } catch (cleanupError) {
            console.error('⚠️ Failed to trigger global cleanup:', cleanupError.message);
            // Non-critical, just log and continue
        }
        
        // Set authenticated to false
        this.isAuthenticated = false;
        
        if (lastError && finalStatus.hasUser) {
            throw lastError; // Throw only if still has user after all retries
        }
    }
    
    // 🔧 ULTRA STABLE: Heartbeat monitoring dengan active ping untuk keep connection alive
    startHeartbeatMonitoring() {
        // Stop existing heartbeat dulu
        this.stopHeartbeatMonitoring();
        
        console.log(`💓 Starting heartbeat monitoring with active ping for ${this.sessionId}`);
        
        this.heartbeatInterval = setInterval(async () => {
            if (!this.isAuthenticated || !this.sock) {
                return;
            }
            
            try {
                // ✅ ACTIVE PING: Kirim presence update untuk keep connection alive
                // ✅ WhatsApp akan close connection setelah ~30 menit idle tanpa activity
                // ✅ sendPresenceUpdate('available') akan keep connection alive
                let heartbeatSuccess = false;
                if (this.sock.sendPresenceUpdate) {
                    try {
                        await this.sock.sendPresenceUpdate('available').then(() => {
                            heartbeatSuccess = true;
                            // ✅ Reset failure count jika berhasil
                            this.heartbeatFailureCount = 0;
                        }).catch((presenceError) => {
                            // ✅ Track heartbeat failure
                            this.heartbeatFailureCount++;
                            console.warn(`⚠️ [HEARTBEAT] Presence update failed (${this.heartbeatFailureCount}/${this.maxHeartbeatFailures}):`, presenceError.message);
                            
                            // ✅ Jika heartbeat gagal 3x berturut-turut, trigger connection health check
                            if (this.heartbeatFailureCount >= this.maxHeartbeatFailures) {
                                console.error(`❌ [HEARTBEAT] Failed ${this.heartbeatFailureCount} times, triggering connection health check`);
                                this.checkConnectionHealth().catch(() => {});
                            }
                        });
                    } catch (presenceError) {
                        this.heartbeatFailureCount++;
                        console.warn(`⚠️ [HEARTBEAT] Presence update exception (${this.heartbeatFailureCount}/${this.maxHeartbeatFailures}):`, presenceError.message);
                    }
                }
                
                // ✅ Update last activity untuk idle timeout check
                if (heartbeatSuccess) {
                    this.lastActivityForIdleCheck = Date.now();
                }
                
                // Check connection health
                const now = Date.now();
                const timeSinceStable = now - this.lastStableConnection;
                const timeSinceLastActivity = now - this.lastActivityForIdleCheck;
                
                // ✅ IDLE TIMEOUT PREVENTION: Jika idle > 25 menit, send presence update lagi
                if (timeSinceLastActivity > (25 * 60 * 1000)) {
                    if (!this.idleTimeoutWarningSent) {
                        console.warn(`⚠️ [IDLE] Session ${this.sessionId} idle for ${Math.floor(timeSinceLastActivity/60000)} minutes, sending presence update to prevent timeout`);
                        this.idleTimeoutWarningSent = true;
                    }
                    // Send extra presence update untuk prevent idle timeout
                    if (this.sock.sendPresenceUpdate) {
                        this.sock.sendPresenceUpdate('available').catch(() => {});
                    }
                } else {
                    this.idleTimeoutWarningSent = false;
                }
                
                // Log heartbeat setiap 60 detik (simplified check)
                const minutes = Math.floor(timeSinceStable / 60000);
                const lastCheck = Math.floor((timeSinceStable - 30000) / 60000);
                if (minutes > lastCheck) {
                    console.log(`💓 HEARTBEAT OK: ${this.sessionId} (uptime: ${Math.floor(timeSinceStable/1000)}s, streak: ${this.connectionStreak}, failures: ${this.heartbeatFailureCount})`);
                }
                
                // Kalau sudah >5 menit stabil, reset reconnect attempts sepenuhnya
                if (timeSinceStable > 300000 && this.reconnectAttempts > 0) {
                    console.log(`✅ Connection stable >5 min, resetting reconnect counter`);
                    this.reconnectAttempts = 0;
                }
                
            } catch (error) {
                this.heartbeatFailureCount++;
                console.error(`❌ Heartbeat error for ${this.sessionId} (${this.heartbeatFailureCount}/${this.maxHeartbeatFailures}):`, error.message);
            }
        }, 90000); // ✅ LIGHTWEIGHT: Send ping every 90 seconds (reduced overhead, masih prevent idle timeout)
    }
    
    stopHeartbeatMonitoring() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
            console.log(`💔 Stopped heartbeat monitoring for ${this.sessionId}`);
        }
    }
    
    /** Webhook DEGRADED — BE pause bot_sending sebelum ACK 0% menumpuk (CONNECTED palsu). */
    /** fork-fix 2026-09-07: reachout timelock dari WhatsApp (NACK 463 NackCallerReachoutTimelocked). */
    applyReachoutTimeLock(tl) {
        const until = tl.timeEnforcementEnds ? new Date(tl.timeEnforcementEnds) : null;
        const type = String(tl.enforcementType || 'DEFAULT');
        if (tl.isActive) {
            this.reachoutTimeLock = { isActive: true, until: until && !isNaN(until.getTime()) ? until.toISOString() : null, type, at: new Date().toISOString() };
            console.warn(`🔒 [REACHOUT-TIMELOCK] ${this.sessionId} aktif sampai ${this.reachoutTimeLock.until || '(tak disebut, +60s)'} type=${type}`);
            this.emitDegradedWebhook('REACHOUT_TIMELOCK', `until=${this.reachoutTimeLock.until || ''} type=${type}`, { force: true }).catch(() => {});
            return;
        }
        if (this.reachoutTimeLock?.isActive) {
            console.warn(`🔓 [REACHOUT-TIMELOCK] ${this.sessionId} dicabut`);
        }
        this.reachoutTimeLock = { isActive: false, until: null, type, at: new Date().toISOString() };
    }
    isReachoutTimeLocked() {
        const tl = this.reachoutTimeLock;
        if (!tl || !tl.isActive) return false;
        if (tl.until) return Date.parse(tl.until) > Date.now();
        return Date.parse(tl.at) + 60000 > Date.now();
    }

    async emitDegradedWebhook(reason, detail = '', opts = {}) {
        const force = !!(opts && opts.force === true);
        if (!force && this.deafKeepSending()) {
            console.warn(`⏭️ [DEGRADED] keep-sending skip reason=${reason} session=${this.sessionId}`);
            return;
        }
        const now = Date.now();
        // Jendela pasca-pair: jangan DEGRADED dulu (bikin RECONNECTING → user susah taut ulang)
        const pairGraceMs = Math.max(0, Math.min(600000, Number(process.env.PAIR_DEGRADED_GRACE_MS) || 180000));
        if (!force && this.isPairingMode) {
            console.warn(`⏭️ [DEGRADED] skip during pairing mode reason=${reason}`);
            return;
        }
        if (!force && pairGraceMs > 0 && this.lastStableConnection && now - this.lastStableConnection < pairGraceMs) {
            console.warn(
                `⏭️ [DEGRADED] skip pair-grace ${Math.floor((now - this.lastStableConnection) / 1000)}s<${Math.floor(pairGraceMs / 1000)}s reason=${reason}`
            );
            return;
        }
        const cooldownMs = Math.max(30000, Math.min(300000, Number(process.env.DEGRADED_WEBHOOK_COOLDOWN_MS) || 120000));
        if (!force && this.lastDegradedWebhookAt && now - this.lastDegradedWebhookAt < cooldownMs) return;
        this.lastDegradedWebhookAt = now;
        const port = Number(process.env.PORT) || this.port || 0;
        await this.enqueueWebhook(
            {
                event: 'DEGRADED',
                status: 'DEGRADED',
                sessionId: this.sessionId,
                bot: this.sessionId,
                reason: String(reason || 'deaf_session'),
                detail: String(detail || ''),
                port,
            },
            { forceDirect: true }
        ).catch(() => {});
        console.warn(`⚠️ [DEGRADED] ${this.sessionId} reason=${reason} detail=${detail}`);
    }

    /**
     * Forum #2491: in-process reconnect sering tetap deaf — setelah N gagal, exit shard
     * (systemd restart) karena hanya full process restart yang reliably recover.
     */
    noteShardDeafEscalation(reason = 'deaf') {
        if (String(process.env.DEAF_SHARD_PROCESS_RESTART || '0') !== '1') {
            return;
        }
        const windowMs = Math.max(60000, Math.min(3600000, Number(process.env.DEAF_ESCALATION_WINDOW_MS) || 600000));
        const maxIn = Math.max(2, Math.min(30, Number(process.env.DEAF_ESCALATION_MAX_IN_WINDOW) || 8));
        if (!global.__deafEscalation) {
            global.__deafEscalation = { start: Date.now(), count: 0 };
        }
        const e = global.__deafEscalation;
        if (Date.now() - e.start > windowMs) {
            e.start = Date.now();
            e.count = 0;
        }
        e.count++;
        console.error(
            `🛑 [DEAF-ESCALATION] ${e.count}/${maxIn} in ${Math.floor(windowMs / 1000)}s port=${process.env.PORT || '?'} reason=${reason}`
        );
        if (e.count >= maxIn) {
            console.error('🛑 [DEAF-ESCALATION] threshold — minta shutdown RAPI (SIGTERM), bukan exit mendadak');
            if (typeof global !== 'undefined') {
                global.isShuttingDown = true;
            }
            // Exit mendadak memutus socket WhatsApp secara kotor. Server WA menganggap
            // perangkat pendamping hilang, lalu menolak dengan 401 saat sesi dipulihkan
            // sesudah systemd menghidupkan shard. Terukur: satu restart membunuh 17 bot
            // yang sebenarnya masih sehat.
            //
            // SIGTERM masuk ke gracefulShutdown() di index.js, yang menjalankan
            // markRealShutdown() lalu smartDisconnect tiap sesi TANPA logout, sehingga
            // login WhatsApp terjaga dan restore sesudah restart tidak ditolak.
            try {
                process.kill(process.pid, 'SIGTERM');
            } catch (sigErr) {
                console.error(`⚠️ [DEAF-ESCALATION] SIGTERM gagal (${sigErr.message}) — terpaksa exit mendadak`);
                setTimeout(() => process.exit(1), 500).unref?.();
                return;
            }
            // Jaring pengaman: gracefulShutdown punya batas 45 detik sendiri.
            // Kalau sampai 50 detik belum keluar juga, paksa.
            setTimeout(() => {
                console.error('⚠️ [DEAF-ESCALATION] graceful melewati 50s — paksa keluar');
                process.exit(1);
            }, 50000).unref?.();
        }
    }

    /**
     * Forum #2491: in-process reconnect sering tetap deaf — teardown penuh lalu initialize.
     */
    async forceHardSessionRecycle(reason = 'deaf_recovery') {
        if (this.reconnectInFlight || this.terminalDisconnect) return false;
        this.reconnectInFlight = true;
        console.warn(`🔄 [HARD-RECYCLE] ${this.sessionId} reason=${reason}`);
        try {
            this.stopHeartbeatMonitoring();
            this.stopConnectionHealthCheck();
            if (this.authPollingInterval) {
                clearInterval(this.authPollingInterval);
                this.authPollingInterval = null;
            }
            if (this.sock) {
                try { this.sock.ev?.removeAllListeners?.(); } catch (_) {}
                try { this.sock.ws?.removeAllListeners?.(); } catch (_) {}
                // Penampung 'error' setelah listener dibuang: end()/close() pada socket yang
                // belum OPEN melempar async ke process kalau tidak ada yang menangkap.
                try {
                    this.sock.ws?.on?.('error', () => {});
                    this.sock.ws?.socket?.on?.('error', () => {});
                } catch (_) {}
                try { this.sock.noise?.destroy?.(); } catch (_) {}
                try {
                    await Promise.race([
                        this.sock.end?.(new Error(`hard-recycle:${reason}`)),
                        new Promise((r) => setTimeout(r, 4000)),
                    ]);
                } catch (_) {}
                try { this.sock.destroy?.(); } catch (_) {}
                this.sock = null;
            }
            this.isAuthenticated = false;
            this.reconnectAttempts = 0;
            this.outboundSendSinceLastAck = 0;
            this.echoVerifyFailCount = 0;
            this.sendGateBlockedUntil = 0;
            await new Promise((r) => setTimeout(r, 1500));
            await this.initialize();
            console.log(`✅ [HARD-RECYCLE] ${this.sessionId} done`);
            const postProbe = await this.probeOnWhatsAppAlive();
            if (!postProbe) {
                this.noteShardDeafEscalation(`${reason}_post_recycle_probe_fail`);
            }
            return postProbe;
        } catch (e) {
            console.error(`❌ [HARD-RECYCLE] ${this.sessionId}:`, e?.message || e);
            this.noteShardDeafEscalation(`${reason}_hard_recycle_error`);
            return false;
        } finally {
            this.reconnectInFlight = false;
        }
    }

    /** Deaf/outbound stale: probe dulu, lalu soft reconnect, lalu hard recycle. */
    async triggerDeafRecovery(kind) {
        // DEAF_RECOVERY=0 → jangan soft/hard recycle (hindari terasa "cepat putus")
        if (String(process.env.DEAF_RECOVERY || '1').trim() === '0') {
            console.warn(`⏭️ [DEAF] recovery skipped (DEAF_RECOVERY=0) kind=${kind} session=${this.sessionId}`);
            return;
        }
        if (this.reconnectInFlight || this.terminalDisconnect) return;
        this.deafRecoveryCount = (this.deafRecoveryCount || 0) + 1;
        const hardAfter = Math.max(1, Math.min(5, Number(process.env.DEAF_HARD_RECYCLE_AFTER) || 2));
        this.outboundSendSinceLastAck = 0;
        console.warn(`🦻 [${kind}] ${this.sessionId} recovery ${this.deafRecoveryCount}/${hardAfter}`);

        if (this.deafRecoveryCount >= hardAfter) {
            this.deafRecoveryCount = 0;
            const ok = await this.forceHardSessionRecycle(kind);
            if (!ok) {
                this.noteShardDeafEscalation(`${kind}_hard_recycle_fail`);
            }
            return;
        }

        const probeOk = await this.probeOnWhatsAppAlive();
        if (!probeOk) {
            this.deafRecoveryCount = 0;
            const ok = await this.forceHardSessionRecycle(`${kind}_probe_fail`);
            if (!ok) {
                this.noteShardDeafEscalation(`${kind}_probe_fail_recycle_fail`);
            }
            return;
        }

        this.reconnectAttempts++;
        this.reconnectInFlight = true;
        try {
            await this.initialize();
        } catch (e) {
            console.error(`❌ [${kind}] soft reconnect failed for ${this.sessionId}:`, e?.message || e);
        } finally {
            this.reconnectInFlight = false;
        }
    }

    /** Forum #2491: onWhatsApp probe — receive path hidup meski messages.upsert mandek. */
    async probeOnWhatsAppAlive() {
        if (!this.sock?.onWhatsApp || !this.isAuthenticated) return false;
        const botNum = this.getCleanBotNumber();
        if (!botNum || !/^\d{10,15}$/.test(botNum)) return false;
        const jid = `${botNum}@s.whatsapp.net`;
        const timeoutMs = Math.max(3000, Math.min(15000, Number(process.env.ON_WHATSAPP_PROBE_TIMEOUT_MS) || 8000));
        try {
            const rows = await Promise.race([
                this.sock.onWhatsApp(jid),
                new Promise((_, reject) => setTimeout(() => reject(new Error('onWhatsApp probe timeout')), timeoutMs)),
            ]);
            const alive = Array.isArray(rows) && rows.some((r) => r?.exists && r?.jid);
            if (alive) {
                this.lastEventAt = Date.now();
                console.log(`✅ [ON-WA-PROBE] ${this.sessionId} receive path alive`);
                return true;
            }
            console.warn(`⚠️ [ON-WA-PROBE] ${this.sessionId} onWhatsApp returned not-exists`);
        } catch (e) {
            console.warn(`⚠️ [ON-WA-PROBE] ${this.sessionId} failed: ${e?.message || e}`);
        }
        return false;
    }

    // ✅ CONNECTION STABILITY: Connection health check untuk deteksi issues lebih cepat
    async checkConnectionHealth() {
        if (!this.isAuthenticated || !this.sock) {
            return false;
        }
        
        try {
            const wsState = getBaileysWebSocketReadyState(this.sock);
            const isOpen = wsState === 1;
            
            if (!isOpen) {
                console.warn(`⚠️ [HEALTH] Connection not open (state: ${wsState}) for ${this.sessionId}`);
                return false;
            }
            
            // Try send presence update untuk test connection
            if (this.sock.sendPresenceUpdate) {
                await Promise.race([
                    this.sock.sendPresenceUpdate('available'),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('Health check timeout')), 5000))
                ]).then(() => {
                    console.log(`✅ [HEALTH] Connection healthy for ${this.sessionId}`);
                    this.heartbeatFailureCount = 0; // Reset on success
                    return true;
                }).catch((error) => {
                    console.warn(`⚠️ [HEALTH] Connection health check failed: ${error.message}`);
                    return false;
                });
            }
            
            return true;
        } catch (error) {
            console.error(`❌ [HEALTH] Health check error: ${error.message}`);
            return false;
        }
    }
    
    // ✅ CONNECTION STABILITY: Start periodic connection health check
    startConnectionHealthCheck() {
        this.stopConnectionHealthCheck();
        
        console.log(`🏥 Starting connection health check for ${this.sessionId}`);
        
        this.connectionHealthInterval = setInterval(async () => {
            if (!this.isAuthenticated || !this.sock) {
                return;
            }
            
            const now = Date.now();
            const wsState = getBaileysWebSocketReadyState(this.sock);
            if (this.isAuthenticated && wsState !== 1 && !this.terminalDisconnect) {
                if (!this.lastNonOpenWsAt) {
                    this.lastNonOpenWsAt = now;
                }
                const stuckMs = Math.max(5000, Math.min(60000, Number(process.env.STUCK_CLOSING_RECONNECT_MS) || 15000));
                if (
                    now - this.lastNonOpenWsAt >= stuckMs &&
                    !this.reconnectInFlight &&
                    this.reconnectAttempts < this.maxReconnectAttempts
                ) {
                    this.reconnectAttempts++;
                    this.reconnectInFlight = true;
                    console.warn(
                        `🛟 [WS-WATCHDOG] ${this.sessionId} wsState=${wsState} stuck ${Math.floor((now - this.lastNonOpenWsAt) / 1000)}s; forcing reconnect attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts}`
                    );
                    this.initialize().catch((err) => {
                        console.error(`❌ [WS-WATCHDOG] Forced reconnect failed for ${this.sessionId}:`, err?.message || err);
                    }).finally(() => {
                        this.reconnectInFlight = false;
                    });
                }
                return;
            }
            this.lastNonOpenWsAt = 0;

            const maxAgeH = Number(process.env.SESSION_MAX_AGE_HOURS) || 0;
            if (
                maxAgeH > 0 &&
                this.connectedAt > 0 &&
                now - this.connectedAt > maxAgeH * 3600000 &&
                !this.reconnectInFlight &&
                !this.terminalDisconnect
            ) {
                console.warn(`⏰ [SESSION-MAX-AGE] ${this.sessionId} ≥${maxAgeH}h — hard recycle`);
                await this.forceHardSessionRecycle('session_max_age');
                return;
            }

            // Outbound deaf: API dapat msg_id tapi 0 SERVER_ACK — HP/WA Business tampak konek (#2491 / #2363)
            // OUTBOUND_DEAF_SENDS=0 → mati (gas mode)
            const outboundDeafSendsRaw = Number(process.env.OUTBOUND_DEAF_SENDS);
            const outboundDeafSends =
                outboundDeafSendsRaw === 0
                    ? 0
                    : Math.max(3, Math.min(500, Number.isFinite(outboundDeafSendsRaw) ? outboundDeafSendsRaw : 5));
            const outboundDeafMs = Math.max(45000, Math.min(600000, Number(process.env.OUTBOUND_DEAF_MS) || 90000));
            const rampActive = this.reconnectRampUntil && now < this.reconnectRampUntil;
            if (
                !this.deafKeepSending() &&
                outboundDeafSends > 0 &&
                !rampActive &&
                wsState === 1 &&
                this.isAuthenticated &&
                (this.outboundSendSinceLastAck || 0) >= outboundDeafSends &&
                this.lastOutboundSendAt > 0 &&
                now - this.lastOutboundSendAt < outboundDeafMs &&
                (!this.lastOutboundAckAt || now - this.lastOutboundAckAt > outboundDeafMs) &&
                !this.reconnectInFlight &&
                !this.terminalDisconnect
            ) {
                console.warn(
                    `📤🦻 [OUTBOUND-DEAF] ${this.sessionId} ${outboundDeafSends}+ kirim tanpa ACK/DELIVERED`
                );
                void this.emitDegradedWebhook('OUTBOUND_DEAF', `${outboundDeafSends}_sends`);
                await this.triggerDeafRecovery('OUTBOUND-DEAF');
                return;
            }

            // Probe onWhatsApp tiap ~90s saat ada kirim tanpa ACK (forum #2491 mitigation)
            const probeIntervalMs = Math.max(60000, Math.min(300000, Number(process.env.ON_WHATSAPP_PROBE_MS) || 90000));
            if (
                wsState === 1 &&
                this.isAuthenticated &&
                (this.outboundSendSinceLastAck || 0) >= 2 &&
                now - (this.lastOnWhatsAppProbeAt || 0) >= probeIntervalMs
            ) {
                this.lastOnWhatsAppProbeAt = now;
                this.probeOnWhatsAppAlive().catch(() => {});
            }

            // Deaf session (#2491): ws OPEN tapi messages.update/upsert mandek
            const deafStaleMs = Math.max(60000, Math.min(600000, Number(process.env.DEAF_SESSION_STALE_MS) || 90000));
            // Ambil yang paling baru, bukan yang pertama truthy: probeOnWhatsAppAlive() hanya
            // menyegarkan lastEventAt, jadi lastMessagesUpdateAt yang basi akan selalu menang dan
            // sesi dicap deaf selamanya walau probe membuktikan jalur terima masih hidup.
            const lastEvt = Math.max(this.lastMessagesUpdateAt || 0, this.lastEventAt || 0);
            const connectedAt = this.connectedAt || 0;
            if (
                !rampActive &&
                wsState === 1 &&
                this.isAuthenticated &&
                connectedAt > 0 &&
                now - connectedAt > deafStaleMs &&
                lastEvt > 0 &&
                now - lastEvt > deafStaleMs &&
                !this.reconnectInFlight &&
                !this.terminalDisconnect
            ) {
                // fleet-quiet 2026-09-07: dulu tiap 30s per sesi tuli; kini saat pertama lalu tiap 5 menit.
                if (!this._lastDeafLogAt || now - this._lastDeafLogAt > 300000) {
                    this._lastDeafLogAt = now;
                    console.warn(
                        `🦻 [DEAF-SESSION] ${this.sessionId} events stale ${Math.floor((now - lastEvt) / 1000)}s`
                    );
                }
                if (this.deafKeepSending()) {
                    return;
                }
                void this.emitDegradedWebhook('DEAF_SESSION_STALE', `${Math.floor((now - lastEvt) / 1000)}s`);
                await this.triggerDeafRecovery('DEAF-SESSION');
                return;
            }

            // Check health setiap 2 menit
            if (now - this.lastConnectionHealthCheck < 120000) {
                return; // Skip jika sudah check dalam 2 menit terakhir
            }
            
            this.lastConnectionHealthCheck = now;
            
            const isHealthy = await this.checkConnectionHealth();
            if (!isHealthy && this.heartbeatFailureCount >= this.maxHeartbeatFailures) {
                console.error(`❌ [HEALTH] Connection unhealthy, may need reconnect for ${this.sessionId}`);
                // Note: Reconnect akan di-handle oleh connection.update handler jika benar-benar disconnect
            }
        }, 30000); // ✅ Ringan: watchdog socket closing tiap 30 detik, health ping tetap tiap 2 menit
    }
    
    stopConnectionHealthCheck() {
        if (this.connectionHealthInterval) {
            clearInterval(this.connectionHealthInterval);
            this.connectionHealthInterval = null;
            console.log(`🏥 Stopped connection health check for ${this.sessionId}`);
        }
    }
    
    // ============================================================================
    // ?? WEBHOOK HELPER
    // ============================================================================
    
    async sendToWebhook(data) {
        const webhookUrl = config.webhook.url;
        try {
            const baseApiKey = config.webhook.apiKey;
            const baseSecret = config.webhook.secret || baseApiKey; // Use same as API key if not set
            const now = Date.now();
            // ✅ CIRCUIT BREAKER: Jika banyak gagal, pause sebentar (60s)
            // Tapi untuk billing events, kita skip circuit breaker (biarkan retry terus)
            const isBillingEvent = ['SERVER_ACK', 'DEVICE_SENT', 'DELIVERED', 'READ'].includes((data.event || '').toString().toUpperCase());
            if (now < this.webhookCircuitUntil && !isBillingEvent) {
                console.warn('⏸️  Webhook circuit open, skipping send (non-billing event)');
                return null;
            }
            // Billing events bypass circuit breaker untuk persistent retry
            
            const eventName = (data.event || data.status || 'UNKNOWN').toString().toUpperCase();
            const msgId = data.msg_id || data.msgId || data.messageId || 'N/A';
            
            // ✅ FIX: Pastikan bot dan bot_id field selalu ada (required untuk webhook server)
            // ✅ CRITICAL: bot harus nomor WhatsApp (10-15 digits string), bot_id harus integer (0 jika belum tersedia)
            let botNumber = data.bot || '';
            // Extract nomor WhatsApp dari JID jika ada
            if (botNumber && typeof botNumber === 'string') {
                if (botNumber.includes('@')) {
                    botNumber = botNumber.split('@')[0];
                }
                if (botNumber.includes(':')) {
                    botNumber = botNumber.split(':')[0];
                }
            }
            // Jika botNumber tidak valid (bukan 10-15 digits), gunakan dari sock
            if (!botNumber || !/^\d{10,15}$/.test(botNumber)) {
                const sockBot = (this.sock?.user?.id||'').split('@')[0];
                if (sockBot && /^\d{10,15}$/.test(sockBot)) {
                    botNumber = sockBot;
                } else {
                    botNumber = data.bot || ''; // Keep original jika tidak bisa extract
                }
            }
            
            // ✅ CRITICAL: Coba ambil bot_id dari data, cache instance, atau Redis
            let botIdInt = 0;
            const botIdValue = data.bot_id || data.botId;
            if (botIdValue) {
                botIdInt = typeof botIdValue === 'number' ? botIdValue : (typeof botIdValue === 'string' && /^\d+$/.test(botIdValue) ? parseInt(botIdValue, 10) : 0);
            }
            
            // ✅ FIX: Jika bot_id masih 0, coba ambil dari cache instance atau Redis
            if (botIdInt === 0) {
                // Cek cache instance dulu (paling cepat)
                if (this.cachedBotId && this.cachedBotId > 0) {
                    botIdInt = this.cachedBotId;
                } else if (this.redis) {
                    // Cek Redis (async, tapi kita perlu await)
                    try {
                        const cachedBotId = await this.redis.get(`bot_id:${this.sessionId}`).catch(() => null);
                        if (cachedBotId && /^\d+$/.test(cachedBotId)) {
                            botIdInt = parseInt(cachedBotId, 10);
                            // Update cache instance untuk akses cepat berikutnya
                            this.cachedBotId = botIdInt;
                        }
                    } catch (redisErr) {
                        // Redis error, skip (akan tetap 0)
                    }
                }
            }
            
            if (!data.bot) data.bot = botNumber;
            if (!data.bot_id) data.bot_id = botIdInt; // ✅ FIX: Webhook server requires bot_id as integer
            
            // ✅ GREP: [WEBHOOK-SEND-START]
            const tempMsgId = data.temp_msg_id || 'NONE';
            console.log(`[WEBHOOK-SEND-START] event=${eventName} msgId=${msgId} tempMsgId=${tempMsgId}`);
            
            const body = JSON.stringify(data);
            // ✅ SIMPLE: hash based on sessionId from payload (must match what server reads)
            // Use data.sessionId FIRST because that's what server will read from JSON payload
            const seed = (data.sessionId || this.sessionId || '').toString();
            
            const dynamicSecret = seed ? crypto.createHmac('sha256', baseSecret).update(seed).digest('hex') : baseSecret;
            const signature = crypto.createHmac('sha256', dynamicSecret).update(body).digest('hex');
            // ✅ SIMPLE API key: hash of baseApiKey + sessionId
            const dynamicApiKey = seed ? crypto.createHmac('sha256', baseApiKey).update(seed).digest('hex') : baseApiKey;

            const startTime = Date.now();
            const response = await axios.post(webhookUrl, body, {
                headers: {
                    'Content-Type': 'application/json',
                    'X-Broadcast-Key': dynamicApiKey,
                    'X-Webhook-Signature': signature
                },
                timeout: 5000, // ✅ OPTIMIZED: 5s timeout untuk prevent webhook loss (was 1.5s, too short)
                validateStatus: (status) => {
                    // Accept 2xx, 3xx as success, 4xx as client error (but still valid response), 5xx as server error
                    // For non-lifecycle events, server might return 200 with empty body or no response
                    return status >= 200 && status < 500;
                }
            });
            const responseTime = Date.now() - startTime;
            
            // ✅ GREP: [WEBHOOK-SEND-SUCCESS]
            console.log(`[WEBHOOK-SEND-SUCCESS] event=${eventName} msgId=${msgId} tempMsgId=${tempMsgId} status=${response.status} time=${responseTime}ms`);
            
            // ✅ CRITICAL: Log FULL response untuk debugging billing issues
            const isNonLifecycleEvent = data.event && !['DEVICE_SENT','DELIVERED','READ','PLAYED','FAILED'].includes(data.event);
            
            if (response.status === 404 && isNonLifecycleEvent) {
                // 404 for non-lifecycle events (CONNECTED, etc) might be expected if endpoint doesn't exist
                setImmediate(() => console.warn(`⚠️ [WEBHOOK] ${eventName} returned 404 for msgId: ${msgId} - this might be expected (${responseTime}ms)`));
                this.webhookFailCount = 0; // Don't count as failure for non-lifecycle events
                return response.data || { error: '0', msg: 'Event processed (404 response)' };
            }
            
            // ✅ OPTIMIZED: Minimal logging untuk speed (hanya log penting)
            if (response.status >= 200 && response.status < 300) {
                setImmediate(() => console.log(`✅ [WEBHOOK] ${eventName} SUCCESS for msgId: ${msgId} (${responseTime}ms)`));
                
                // ✅ CRITICAL: Untuk billing events, cek apakah response mengandung error
                if (['DEVICE_SENT', 'DELIVERED', 'READ'].includes(eventName)) {
                    if (typeof response.data === 'object' && response.data?.error !== '0' && response.data?.error !== 0) {
                        // ✅ REMOVED: Response data logging (prevent binary data in logs)
                        setImmediate(() => console.error(`❌ [BILLING] ${eventName} returned ERROR for msgId: ${msgId}`));
                    } else {
                        setImmediate(() => console.log(`💰 [BILLING] ${eventName} confirmed OK for msgId: ${msgId}`));
                    }
                }
            } else if (response.status >= 300 && response.status < 400) {
                setImmediate(() => console.warn(`⚠️ [WEBHOOK] ${eventName} REDIRECT for msgId: ${msgId} - Status: ${response.status} (${responseTime}ms)`));
            } else if (response.status >= 400 && response.status < 500) {
                setImmediate(() => console.error(`❌ [WEBHOOK] ${eventName} CLIENT ERROR for msgId: ${msgId} - Status: ${response.status} (${responseTime}ms)`));
            }
            
            // ? Return response data for validation
            this.webhookFailCount = 0; // reset on success
            return response.data || { error: '0', msg: 'Event processed' };
            
        } catch (error) {
            // ✅ ENHANCED: Log FULL error response untuk debugging
            const eventName = (data?.event || data?.status || 'UNKNOWN').toString().toUpperCase();
            const msgId = data?.msg_id || data?.msgId || data?.messageId || 'N/A';
            
            if (error.response) {
                // Server responded with error status
                const responseData = error.response.data;
                const responseText = typeof responseData === 'string' ? responseData : JSON.stringify(responseData);
                const responsePreview = responseText.length > 500 ? responseText.substring(0, 500) + '...' : responseText;
                
                console.error(`❌ [WEBHOOK] ${eventName} FAILED for msgId: ${msgId}`);
                console.error(`   URL: ${webhookUrl}`);
                console.error(`   Status: ${error.response.status} ${error.response.statusText}`);
                // ✅ REMOVED: Response body logging (prevent binary data in logs)
                console.error(`   Response Headers:`, JSON.stringify(error.response.headers));
                console.error(`   SessionId: ${data?.sessionId || 'unknown'}`);
            } else if (error.request) {
                // Request made but no response
                console.error(`❌ [WEBHOOK] ${eventName} NO RESPONSE for msgId: ${msgId}`);
                console.error(`   URL: ${webhookUrl}`);
                console.error(`   Error: No response from server (timeout or connection error)`);
                // ✅ REMOVED: Payload logging (prevent binary data in logs)
            } else {
                // Error setting up request
                console.error(`❌ [WEBHOOK] ${eventName} REQUEST ERROR for msgId: ${msgId}`);
                console.error(`   URL: ${webhookUrl}`);
                console.error(`   Error: ${error.message}`);
                // ✅ REMOVED: Payload logging (prevent binary data in logs)
            }
            
            // ✅ CRITICAL: Untuk billing events, log extra detail
            if (['DEVICE_SENT', 'DELIVERED', 'READ'].includes(eventName)) {
                console.error(`💀 [BILLING] ${eventName} webhook FAILED for msgId: ${msgId} - This will be retried via queue`);
            }
            // ✅ CIRCUIT BREAKER: Hanya untuk non-billing events
            // Billing events tidak pakai circuit breaker untuk allow persistent retry
            const isBillingEvent = ['SERVER_ACK', 'DEVICE_SENT', 'DELIVERED', 'READ'].includes((data.event || '').toString().toUpperCase());
            if (!isBillingEvent) {
                this.webhookFailCount++;
                if (this.webhookFailCount >= 5) {
                    this.webhookCircuitUntil = Date.now() + 60000; // pause 60s
                    this.webhookFailCount = 0;
                    console.warn('🚧 Webhook circuit opened for 60s due to repeated failures (non-billing events)');
                }
            } else {
                // Reset fail count untuk billing events (persistent retry)
                this.webhookFailCount = 0;
            }
            return null; 
        }
    }

    async sendToWebhookWithRetry(data, retries = 2) {
        let attempt = 0;
        while (attempt < retries) {
            const res = await this.sendToWebhook(data);
            if (res) return res;
            attempt++;
            const backoff = attempt === 1 ? 300 : 700;
            await new Promise(r => setTimeout(r, backoff));
        }
        return null;
    } 
}

export default BaileysWrapper;



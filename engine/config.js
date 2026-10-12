/**
 * URL backend dibaca dari .env (WEBHOOK_URL / BE_BASE_URL) supaya deploy + scp config tidak menimpa domain produksi.
 * Systemd: EnvironmentFile=/opt/baileys/.env — pastikan file UTF-8 (bukan UTF-16).
 */
import dotenv from 'dotenv';
import os from 'os';

dotenv.config();

/** Deteksi spek: default kapasitas mengikuti tier (override: BAILEYS_RESOURCE_TIER=low|mid|high). */
const TOTAL_MEM_MB = Math.round(os.totalmem() / 1024 / 1024);
const CPU_COUNT = Math.max(1, os.cpus()?.length || 1);

function detectResourceTier() {
    const o = String(process.env.BAILEYS_RESOURCE_TIER || '').toLowerCase();
    if (o === 'low' || o === 'mid' || o === 'high') {
        return o;
    }
    if (TOTAL_MEM_MB >= 28000) {
        return 'high';
    }
    if (TOTAL_MEM_MB >= 12000) {
        return 'mid';
    }
    return 'low';
}

const RESOURCE_TIER = detectResourceTier();

const BY_TIER = {
    low: {
        httpMaxConcurrentSends: 128,
        restoreConcurrency: 10,
        warmupMaxSessions: 120,
        sendSlotHeapSoftMb: 2200,
        sendSlotHeapPressureMax: 64,
        sendSlotRssSoftMb: 3000,
        sendSlotRssPressureMax: 48,
        sendSlotMaxQueuedWaiters: 250,
        memoryRestartRssMb: 2500,
        memoryRestartHeapMb: 2800,
        webhookHttpMaxConcurrent: 6,
        maxConcurrentSendsPerSession: 5,
        maxMessageQueueSize: 24,
        maxConcurrentSessionLoads: 28
    },
    mid: {
        httpMaxConcurrentSends: 192,
        restoreConcurrency: 15,
        warmupMaxSessions: 200,
        sendSlotHeapSoftMb: 4000,
        sendSlotHeapPressureMax: 112,
        sendSlotRssSoftMb: 6500,
        sendSlotRssPressureMax: 88,
        sendSlotMaxQueuedWaiters: 450,
        memoryRestartRssMb: 2500,
        memoryRestartHeapMb: 8192,
        webhookHttpMaxConcurrent: 12,
        maxConcurrentSendsPerSession: 6,
        maxMessageQueueSize: 32,
        maxConcurrentSessionLoads: 36
    },
    high: {
        // Sedikit lebih rendah dari CPU×40: kurangi puncak RSS & lonjakan event loop (API bisa 25–30s saat semua kirim bareng).
        httpMaxConcurrentSends: Math.min(320, Math.max(256, CPU_COUNT * 35)),
        restoreConcurrency: Math.min(40, Math.max(24, CPU_COUNT * 4)),
        warmupMaxSessions: 420,
        sendSlotHeapSoftMb: 9000,
        sendSlotHeapPressureMax: 224,
        sendSlotRssSoftMb: 14000,
        sendSlotRssPressureMax: 160,
        sendSlotMaxQueuedWaiters: 1000,
        /** RSS ≥ ini (MB) → graceful restart. Default 2500 semua tier; override: MEMORY_RESTART_RSS_MB */
        memoryRestartRssMb: 2500,
        memoryRestartHeapMb: 12288,
        webhookHttpMaxConcurrent: 18,
        maxConcurrentSendsPerSession: 8,
        maxMessageQueueSize: 48,
        maxConcurrentSessionLoads: Math.min(64, Math.max(40, CPU_COUNT * 6))
    }
};

const TD = BY_TIER[RESOURCE_TIER];

function envInt(name, fallback, min, max) {
    const raw = process.env[name];
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n)) {
            return Math.min(max, Math.max(min, Math.floor(n)));
        }
    }
    return Math.min(max, Math.max(min, fallback));
}

/** Untuk MEMORY_RESTART_* : 0 = matikan. */
function envUIntGeZero(name, fallback, maxVal) {
    const raw = process.env[name];
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.min(maxVal, Math.floor(n));
        }
    }
    return fallback;
}

function stripSlash(s) {
    return String(s || '').replace(/\/+$/, '');
}

/** Fallback bila `.env` tanpa WEBHOOK_URL — set DEFAULT_BE_BASE=https://backend-anda.example */
const DEFAULT_BASE =
    stripSlash(process.env.DEFAULT_BE_BASE || '') || 'http://localhost';

function originFromUrl(url) {
    try {
        const u = new URL(url);
        return stripSlash(`${u.protocol}//${u.host}`);
    } catch {
        return null;
    }
}

const webhookUrl = process.env.WEBHOOK_URL || `${DEFAULT_BASE}/webhookv2.php`;

let beBase = stripSlash(process.env.BE_BASE_URL || '');
if (!beBase) {
    beBase = originFromUrl(webhookUrl) || DEFAULT_BASE;
}

const apiBaseHost = beBase.replace(/^https?:\/\//, '');

const webhookApiKey = process.env.WEBHOOK_API_KEY || '';
const webhookSecret = process.env.WEBHOOK_SECRET || webhookApiKey;

const proxyPrimary = process.env.GET_PROXY_URL || `${beBase}/proxy.php`;
const proxySecondary = process.env.GET_PROXY_URL_SECONDARY || `${beBase}/proxyv2.php`;
const proxyApiKey = process.env.GET_PROXY_APIKEY || process.env.GET_PROXY_API_KEY || '';

const sessionWarmupUrl =
    process.env.API_CONNECTED_SESSIONS_URL || `${beBase}/api_connected_sessions.php`;
const sessionWarmupApiKey = process.env.SESSION_WARMUP_API_KEY || webhookApiKey;

const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

/** Warmup: default mengikuti tier RAM; override RESTORE_STARTUP_CONCURRENCY. */
const restoreStartupDelayMsVal = Math.max(0, Number(process.env.RESTORE_STARTUP_DELAY_MS) || 450);
const restoreConcurrencyVal = envInt('RESTORE_STARTUP_CONCURRENCY', TD.restoreConcurrency, 1, 128);

/** Jeda antar start restore (ms × urutan index) — kurangi thundering herd; 0 = off */
const restoreStartStaggerMsVal = Math.max(
    0,
    Math.min(500, Number(process.env.RESTORE_START_STAGGER_MS) || 42)
);
const restoreStaggerCapMsVal = Math.max(
    restoreStartStaggerMsVal,
    Math.min(120000, Number(process.env.RESTORE_STAGGER_CAP_MS) || 8000)
);

const warmupMaxSessionsVal = envInt('WARMUP_MAX_SESSIONS', TD.warmupMaxSessions, 1, 500);

/** Tier: low/mid/high; 0 = matikan restart RSS. */
const memoryRestartRssMbVal = envUIntGeZero('MEMORY_RESTART_RSS_MB', TD.memoryRestartRssMb, 64000);

/** global.gc() mem-pause event loop → API terasa melambat. Default mati; nyalakan lewat env jika perlu RSS. */
const gcIfHeapUsedExceedsMbVal = (() => {
    const raw = process.env.GC_IF_HEAP_USED_EXCEEDS_MB;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.floor(n);
        }
    }
    return 0;
})();

const gcAfterRamEvictVal = (() => {
    const v = String(process.env.GC_AFTER_RAM_EVICT || '').toLowerCase();
    return v === '1' || v === 'true' || v === 'yes';
})();

const httpMaxConcurrentSendsVal = envInt('HTTP_MAX_CONCURRENT_SENDS', TD.httpMaxConcurrentSends, 4, 2048);

/** Saat heapUsed > ini (MB), throttle; 0 = mati. */
const sendSlotHeapSoftMbVal = (() => {
    const raw = process.env.SEND_SLOT_HEAP_SOFT_MB;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.min(32000, Math.floor(n));
        }
    }
    return TD.sendSlotHeapSoftMb;
})();

const sendSlotHeapPressureMaxVal = envInt(
    'SEND_SLOT_HEAP_PRESSURE_MAX',
    TD.sendSlotHeapPressureMax,
    4,
    2048
);

const sendSlotRssSoftMbVal = (() => {
    const raw = process.env.SEND_SLOT_RSS_SOFT_MB;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.min(64000, Math.floor(n));
        }
    }
    return TD.sendSlotRssSoftMb;
})();

const sendSlotRssPressureMaxVal = envInt(
    'SEND_SLOT_RSS_PRESSURE_MAX',
    TD.sendSlotRssPressureMax,
    4,
    2048
);

const sendSlotMaxQueuedWaitersVal = (() => {
    const raw = process.env.SEND_SLOT_MAX_QUEUED_WAITERS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.min(50000, Math.floor(n));
        }
    }
    return TD.sendSlotMaxQueuedWaiters;
})();

/**
 * Saat pipeline send global penuh (inFlight ≥ effectiveMax) DAN antre waiters slot ≥ ini → 503 JSON segera.
 * Default 1 = permintaan berikutnya dapat JSON (bukan 0-byte saat curl PHP ~12–15s). Dulu default ~88% max queue (~880) hampir tidak pernah terpenuhi.
 * 0 / off = mati. Env: SEND_HTTP_BUSY_REJECT_MIN_WAITERS
 */
const sendHttpBusyRejectMinWaitersVal = (() => {
    const raw = process.env.SEND_HTTP_BUSY_REJECT_MIN_WAITERS;
    if (raw === '0' || String(raw).toLowerCase() === 'off') return 0;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) return Math.min(50000, Math.floor(n));
    }
    return 1;
})();

const memoryRestartHeapMbVal = envUIntGeZero('MEMORY_RESTART_HEAP_MB', TD.memoryRestartHeapMb, 64000);

/** Tier high: cooldown lebih pendek supaya systemd bisa bangkit lagi cepat setelah RSS spike. */
const memoryRestartCooldownMsVal = (() => {
    const raw = process.env.MEMORY_RESTART_COOLDOWN_MS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.min(600000, Math.max(20000, Math.floor(n)));
        }
    }
    return RESOURCE_TIER === 'high' ? 45000 : 120000;
})();

const memoryRestartMinUptimeSecVal = (() => {
    const raw = process.env.MEMORY_RESTART_MIN_UPTIME_SEC;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.min(3600, Math.max(45, Math.floor(n)));
        }
    }
    return RESOURCE_TIER === 'high' ? 75 : 120;
})();

/**
 * RSS ≥ threshold harus bertahan N tick interval RAM cleanup (bukan 1 sampel) — kurangi restart goyah di batas tipis.
 * Env: RSS_RESTART_CONSECUTIVE_TICKS (naikkan bila restart RSS terlalu sering di batas 2500MB).
 */
const rssRestartConsecutiveTicksVal = envInt('RSS_RESTART_CONSECUTIVE_TICKS', 1, 1, 60);

/**
 * Setelah kirim gambar dari URL sukses: hapus file cache + entry mem/redis.
 * Default **mati** (0): template URL sama dipakai berulang → cache + file lokal tetap dipakai (lebih cepat).
 * Nyalakan evict: IMAGE_CACHE_EVICT_AFTER_SEND=1 (hemat disk, unduh ulang tiap kirim).
 */
const imageCacheEvictAfterSendVal = (() => {
    const v = String(process.env.IMAGE_CACHE_EVICT_AFTER_SEND ?? '0').toLowerCase().trim();
    if (v === '1' || v === 'true' || v === 'on' || v === 'yes') return true;
    return false;
})();

/** TTL cache URL gambar (ms). Template statis: default 6 jam. Env: IMAGE_URL_CACHE_TTL_MS */
const imageUrlCacheTtlMsVal = envInt(
    'IMAGE_URL_CACHE_TTL_MS',
    6 * 60 * 60 * 1000,
    60 * 1000,
    7 * 24 * 60 * 60 * 1000
);

/** Max entri cache URL gambar per sesi (mem). Env: IMAGE_URL_CACHE_MAX_SIZE */
const imageUrlCacheMaxSizeVal = envInt('IMAGE_URL_CACHE_MAX_SIZE', 200, 32, 2000);

/**
 * Max tunggu giliran slot global sebelum error JSON. Default 10s: sering dipasangkan curl PHP pendek (~12s);
 * naikkan env bila processor timeout lebih besar. Env: SEND_SLOT_ACQUIRE_TIMEOUT_MS
 */
const sendSlotAcquireTimeoutMsVal = (() => {
    const raw = process.env.SEND_SLOT_ACQUIRE_TIMEOUT_MS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 500) {
            return Math.min(120000, Math.floor(n));
        }
    }
    return 10000;
})();

/** Timeout middleware untuk jalur kirim POST (ms). Env: REQUEST_TIMEOUT_MS. Selaraskan nginx proxy_read_timeout & curl processor. */
const requestTimeoutMsVal = (() => {
    const raw = process.env.REQUEST_TIMEOUT_MS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 15000) {
            return Math.min(180000, Math.floor(n));
        }
    }
    /** 90s: pipeline sync-send butuh >55s; 55s lawas memotong sync di bawah slot+WA */
    return 90000;
})();

/**
 * Max tunggu waitForStableSendReady sebelum sock.sendMessage (ms).
 * Harus didefinisikan sebelum syncSendMaxWaitMsVal agar default sync mengikuti pipeline nyata.
 */
const sendStableReadyMaxMsVal = (() => {
    const raw = process.env.SEND_STABLE_READY_MAX_MS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.min(30000, Math.floor(n));
        }
    }
    return 5500;
})();

/**
 * enable_callback=false: Promise.race luar membungkus seluruh send (slot + stable + retry WA).
 * Default lawas 22s sering kalah: SEND_SLOT bisa 18s + STABLE ~5.5s + 3× send race 10s (wrapper) > 22s.
 * Env: SYNC_SEND_MAX_WAIT_MS | naikkan curl timeout PHP/nginx agar > nilai ini (mis. +15s headroom).
 */
const syncSendMaxWaitMsVal = (() => {
    const raw = process.env.SYNC_SEND_MAX_WAIT_MS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 5000) {
            return Math.min(175000, Math.floor(n));
        }
    }
    /** Selaras sendMessageDirect: SEND_MESSAGE_TIMEOUT_MS 10s, MAX_RETRIES 2 → sampai 3 percobaan + backoff */
    const sendRacePerAttemptMs = 10000;
    const maxSendAttempts = 3;
    const retryBackoffMarginMs = 2000;
    const fromPipeline =
        sendSlotAcquireTimeoutMsVal +
        sendStableReadyMaxMsVal +
        maxSendAttempts * sendRacePerAttemptMs +
        retryBackoffMarginMs;
    const sensibleFloor = 28000;
    const computed = Math.max(sensibleFloor, fromPipeline);
    /** Biarkan handler sync selesai sebelum middleware timeout Express */
    const expressCap = Math.max(15000, requestTimeoutMsVal - 8000);
    return Math.min(175000, computed, expressCap);
})();

/**
 * Mean event-loop lag (ms) dari monitorEventLoopDelay, sebanyak strikes berturut-turut → exit(1) (systemd restart).
 * Env: EVENT_LOOP_LAG_RESTART_MS — `0` = mati. Default: semua tier nyala (low/mid sedikit lebih longgar dari high).
 */
const eventLoopLagRestartMsVal = (() => {
    const raw = process.env.EVENT_LOOP_LAG_RESTART_MS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n)) {
            if (n === 0) return 0;
            if (n >= 500) return Math.min(60000, Math.floor(n));
        }
        return 0;
    }
    if (RESOURCE_TIER === 'high') {
        return 8000;
    }
    if (RESOURCE_TIER === 'mid') {
        return 9000;
    }
    return 10000;
})();

const eventLoopLagRestartStrikesVal = envInt('EVENT_LOOP_LAG_RESTART_STRIKES', 2, 1, 20);
/** Interval sampling histogram lag (ms). Env: EVENT_LOOP_LAG_CHECK_MS — default lebih rapat = deteksi hang lebih cepat */
const eventLoopLagCheckMsVal = envInt('EVENT_LOOP_LAG_CHECK_MS', 5000, 2000, 60000);

const enforceConnectedAllowlistVal = (() => {
    const v = String(process.env.ENFORCE_CONNECTED_ALLOWLIST || '1').toLowerCase().trim();
    return !(v === '0' || v === 'false' || v === 'off' || v === 'no');
})();

const newSessionGraceMsVal = (() => {
    const raw = process.env.NEW_SESSION_GRACE_MS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.min(86400000, Math.floor(n));
        }
    }
    return 30 * 60 * 1000;
})();

const connectedAllowlistSyncMsVal = (() => {
    const raw = process.env.CONNECTED_ALLOWLIST_SYNC_MS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.min(3600000, Math.floor(n));
        }
    }
    return 45000;
})();

const lightRamWithConnectedAllowlistVal = (() => {
    const v = String(process.env.LIGHT_RAM_WITH_CONNECTED_ALLOWLIST ?? '1').toLowerCase().trim();
    return !(v === '0' || v === 'false' || v === 'off' || v === 'no');
})();

const disconnectedRamTtlAllowlistMsVal = envInt('DISCONNECTED_RAM_TTL_ALLOWLIST_MS', 10000, 1500, 120000);
const permanentDisconnectRamTtlAllowlistMsVal = envInt('PERMANENT_DISCONNECT_RAM_TTL_ALLOWLIST_MS', 1200, 200, 60000);

/**
 * Tanpa enable_callback di body: perlakukan seperti enable_callback=true → respons JSON cepat (temp id), kirim WA di background.
 * Untuk processor PHP dengan curl 30s. Env: SEND_DEFAULT_ENABLE_CALLBACK=1
 */
const sendDefaultEnableCallbackVal = (() => {
    const v = String(process.env.SEND_DEFAULT_ENABLE_CALLBACK || '').toLowerCase().trim();
    return v === '1' || v === 'true' || v === 'yes';
})();

const maxConcurrentSendsPerSessionVal = envInt(
    'MAX_CONCURRENT_SENDS_PER_SESSION',
    TD.maxConcurrentSendsPerSession,
    1,
    8
);

/** Jeda minimal antar *start* kirim per session (ms). Lebih kecil = lebih cepat tapi risiko rate-limit WA naik. */
const minMessageSendIntervalMsVal = (() => {
    const raw = process.env.MIN_MESSAGE_SEND_INTERVAL_MS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 0) {
            return Math.min(2000, Math.floor(n));
        }
    }
    return 100;
})();

const maxMessageQueueSizeVal = envInt('MAX_MESSAGE_QUEUE_SIZE', TD.maxMessageQueueSize, 1, 200);

const webhookHttpMaxConcurrentVal = envInt(
    'WEBHOOK_HTTP_MAX_CONCURRENT',
    TD.webhookHttpMaxConcurrent,
    1,
    64
);

const webhookOutboxBatchConcurrencyVal = (() => {
    const raw = process.env.WEBHOOK_OUTBOX_BATCH_CONCURRENCY;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 1) {
            return Math.min(32, Math.floor(n));
        }
    }
    return Math.min(32, Math.max(8, webhookHttpMaxConcurrentVal + 4));
})();

const maxConcurrentSessionLoadsVal = envInt(
    'MAX_CONCURRENT_SESSION_LOADS',
    TD.maxConcurrentSessionLoads,
    4,
    128
);

/** Tunggu slot webhook: 0 = tidak timeout (antri saja). >0 = ms max tunggu giliran. */
const webhookSlotAcquireTimeoutMsVal = (() => {
    const raw = process.env.WEBHOOK_SLOT_ACQUIRE_TIMEOUT_MS;
    if (raw != null && String(raw).trim() !== '') {
        const n = Number(raw);
        if (n === 0) {
            return 0;
        }
        if (Number.isFinite(n) && n >= 500) {
            return Math.min(600000, Math.floor(n));
        }
    }
    return 0;
})();

/** TTL key `webhook:confirmed:*` di Redis (detik). Default 12h vs 24h lawas → ~setengah key steady-state. Env: WEBHOOK_CONFIRMED_TTL_SEC */
const webhookConfirmedTtlSecVal = envInt('WEBHOOK_CONFIRMED_TTL_SEC', 43200, 300, 172800);

const config = {
    domains: { apiBase: apiBaseHost },
    webhook: {
        url: webhookUrl,
        apiKey: webhookApiKey,
        secret: webhookSecret,
        confirmedTtlSec: webhookConfirmedTtlSecVal
    },
    proxy: {
        enabled: false,
        primaryUrl: proxyPrimary,
        secondaryUrl: proxySecondary,
        apiKey: proxyApiKey,
        timeoutMs: 10000
    },
    redis: { enabled: true, url: redisUrl },
    whatsapp: {
        /** false = ambil client_revision dari web.whatsapp.com (byee-wamod), di-cache di wrapper. */
        useStaticVersion: false,
        /** Fallback bila fetch gagal (tetap isi 3 angka). */
        staticVersion: [2, 3000, 1040990833],
        defaultCountryCode: '62',
        minPhoneLength: 10,
        maxPhoneLength: 15
    },
    security: {
        allowedIPs: [
            '127.0.0.1',
            '::1',
            '::ffff:127.0.0.1',
            ...(process.env.ALLOWED_IPS || '').split(',').map((s) => s.trim()).filter(Boolean)
        ],
        broadcastKey: process.env.BROADCAST_KEY || webhookApiKey
    },
    server: {
        httpPort: 7001,
        wsPort: 8001,
        controlApiPort: 9000,
        uploadTmpPath: '/var/lib/baileys/tmp',
        maxHttpConnections: 16000,
        enforceSessionIdPortMatchesWorker: false
    },
    admin: { proxyReassignKey: process.env.ADMIN_PROXY_REASSIGN_KEY || '' },
    logging: { enableConsoleLog: false, silentOutput: true },
    validation: { checkWhatsAppNumber: false, skipWebhook: false },
    autoGenerate: { enabled: false, smooth: true, delayMs: 100, minAckCount: 5 },
    sessionWarmup: {
        enabled: true,
        apiUrl: sessionWarmupUrl,
        apiKey: sessionWarmupApiKey,
        loadAllConnected: true,
        maxSessions: warmupMaxSessionsVal,
        backgroundLoad: true,
        onlyConnected: true,
        minSessionsToWarmup: 1,
        validateSessionFiles: true,
        // Session “baru” di disk (creds baru) — default off; nyalakan: INCLUDE_NEW_SCAN_ON_RESTORE=1
        includeNewScanOnRestore:
            process.env.INCLUDE_NEW_SCAN_ON_RESTORE === '1' ||
            process.env.INCLUDE_NEW_SCAN_ON_RESTORE === 'true',
        /** Hanya sessi dari api_connected (+ grace pair baru). Mati: ENFORCE_CONNECTED_ALLOWLIST=0 */
        enforceConnectedAllowlist: enforceConnectedAllowlistVal,
        /** Sessi dengan creds.json muda ≤ ini diizinkan walau belum di API (pair baru). */
        newSessionGraceMs: newSessionGraceMsVal,
        /** Periodik fetch API + evict RAM yang tidak terdaftar (ms). 0 = mati. */
        connectedAllowlistSyncMs: connectedAllowlistSyncMsVal,
        /** Jeda minimal antar fetch API untuk getOrLoad (ms); burst request tidak serial di axios 20s. Env: CONNECTED_ALLOWLIST_MIN_REFRESH_MS */
        connectedAllowlistMinRefreshMs: envInt('CONNECTED_ALLOWLIST_MIN_REFRESH_MS', 25000, 5000, 600000),
        newScanRestoreWindowMs: 72 * 60 * 60 * 1000,
        apiTimeoutMs: 20000,
        apiMethod: 'get',
        restoreStartupDelayMs: restoreStartupDelayMsVal,
        delayBetweenLoads: 120,
        restoreConcurrency: restoreConcurrencyVal,
        /** Spread start initialize() biar event loop + WA tidak macet barengan */
        restoreStartStaggerMs: restoreStartStaggerMsVal,
        restoreStaggerCapMs: restoreStaggerCapMsVal
    },
    performance: {
        httpMaxConcurrentSends: httpMaxConcurrentSendsVal,
        /** Max tunggu giliran global send slot (ms); lewat → error JSON, hindari 0-byte sampai PHP timeout */
        sendSlotAcquireTimeoutMs: sendSlotAcquireTimeoutMsVal,
        /** Env: SEND_DEFAULT_ENABLE_CALLBACK — respons cepat bila client tidak kirim enable_callback */
        sendDefaultEnableCallback: sendDefaultEnableCallbackVal,
        /** Env: SEND_STABLE_READY_MAX_MS */
        sendStableReadyMaxMs: sendStableReadyMaxMsVal,
        /** Env: SEND_SLOT_HEAP_SOFT_MB (0=off), SEND_SLOT_HEAP_PRESSURE_MAX */
        sendSlotHeapSoftMb: sendSlotHeapSoftMbVal,
        sendSlotHeapPressureMax: sendSlotHeapPressureMaxVal,
        sendSlotRssSoftMb: sendSlotRssSoftMbVal,
        sendSlotRssPressureMax: sendSlotRssPressureMaxVal,
        sendSlotMaxQueuedWaiters: sendSlotMaxQueuedWaitersVal,
        sendHttpBusyRejectMinWaiters: sendHttpBusyRejectMinWaitersVal,
        /** Env: MEMORY_RESTART_HEAP_MB (0=off) */
        memoryRestartHeapMb: memoryRestartHeapMbVal,
        /** Env: IMAGE_CACHE_EVICT_AFTER_SEND (default 0 = pertahankan cache untuk URL template berulang) */
        imageCacheEvictAfterSend: imageCacheEvictAfterSendVal,
        /** Env: IMAGE_URL_CACHE_TTL_MS — cache unduhan gambar per URL (template sama) */
        imageUrlCacheTtlMs: imageUrlCacheTtlMsVal,
        /** Env: IMAGE_URL_CACHE_MAX_SIZE — batas entri cache gambar per wrapper */
        imageUrlCacheMaxSize: imageUrlCacheMaxSizeVal,
        maxConcurrentSendsPerSession: maxConcurrentSendsPerSessionVal,
        minMessageSendIntervalMs: minMessageSendIntervalMsVal,
        maxMessageQueueSize: maxMessageQueueSizeVal,
        webhookHttpMaxConcurrent: webhookHttpMaxConcurrentVal,
        webhookOutboxBatchConcurrency: webhookOutboxBatchConcurrencyVal,
        webhookSlotAcquireTimeoutMs: webhookSlotAcquireTimeoutMsVal,
        maxConcurrentSessionLoads: maxConcurrentSessionLoadsVal,
        /** Auto dari os.totalmem / os.cpus; override kapasitas dengan env per key atau BAILEYS_RESOURCE_TIER. */
        autoResourceTier: RESOURCE_TIER,
        autoTotalMemMb: TOTAL_MEM_MB,
        autoCpuCount: CPU_COUNT,
        loadSlotWaitMs: 24000,
        permanentDisconnectRamTtlMs: 5000,
        disconnectedSessionRamTtlMs: 65000,
        /** Saat allowlist connected + LIGHT_RAM on: TTL disconnect dipendekkan (session API yang mati tidak lama nempel). */
        lightRamWithConnectedAllowlist: lightRamWithConnectedAllowlistVal,
        disconnectedSessionRamTtlMsAllowlist: disconnectedRamTtlAllowlistMsVal,
        permanentDisconnectRamTtlMsAllowlist: permanentDisconnectRamTtlAllowlistMsVal,
        ramCleanupStartupGraceMs: 20000,
        ramCleanupIntervalMs: 10000,
        // Paksa GC hanya jika GC_AFTER_RAM_EVICT=1 (butuh --expose-gc). Default false → hindari pause API.
        gcAfterRamEvict: gcAfterRamEvictVal,
        gcPassesAfterEvict: 1,
        // >0 = tiap interval, heapUsed > ini → global.gc() (bikin API melambat). Default 0 = off; set GC_IF_HEAP_USED_EXCEEDS_MB=3200 kalau mau.
        gcIfHeapUsedExceedsMb: gcIfHeapUsedExceedsMbVal,
        gcHeapCooldownMs: 120000,
        gcPassesOnHeapPressure: 1,
        // Tier high: cooldown 45s, min uptime 75s; RSS dari env. RSS_RESTART_CONSECUTIVE_TICKS = tick berturut-turut.
        memoryRestartRssMb: memoryRestartRssMbVal,
        memoryRestartMinUptimeSec: memoryRestartMinUptimeSecVal,
        memoryRestartCooldownMs: memoryRestartCooldownMsVal,
        rssRestartConsecutiveTicks: rssRestartConsecutiveTicksVal,
        recipientSendDelayMs: 0,
        /** @deprecated pakai maxConcurrentSendsPerSession — nilai disamakan untuk kompatibilitas */
        maxConcurrentPerSession: maxConcurrentSendsPerSessionVal,
        /** Batas teoretis; slot nyata di httpMaxConcurrentSends + send-slot.js */
        maxGlobalConcurrent: httpMaxConcurrentSendsVal,
        minMessageInterval: 1,
        sessionInactiveTimeout: 7200000,
        cleanupInterval: 300000,
        enableCircuitBreaker: true,
        enableRateLimiting: false,
        requestTimeout: requestTimeoutMsVal,
        httpPipelineSlotWaitMs: 26000,
        httpPipelineSecondPhaseWaitMs: 14000,
        releasePipelineDuringAuthWait: true,
        sessionAuthMaxWaitMs: 18000,
        sessionAuthPollMs: 100,
        slowRequestThreshold: 8000,
        /** enable_callback=false: Promise.race ke WA; cegah hang tanpa JSON (curl 0-byte). */
        syncSendMaxWaitMs: syncSendMaxWaitMsVal,
        /** monitorEventLoopDelay mean ≥ ini (ms) sebanyak strikes → exit(1). 0 = off. */
        eventLoopLagRestartMs: eventLoopLagRestartMsVal,
        eventLoopLagRestartStrikes: eventLoopLagRestartStrikesVal,
        eventLoopLagCheckMs: eventLoopLagCheckMsVal
    },
    antiSpam: { enabled: false, replacePercentage: 25, unicodeVariants: {}, characters: [] },
    antiBlock: {
        enabled: true,
        delayJitter: { enabled: false, min: 0.3, max: 0.7 },
        typing: {
            enabled: false,
            text: { enabled: false, min: 1.0, max: 2.5, minMessageLength: 10, multiplier: 15 },
            image: { enabled: false, min: 0.6, max: 1.4 }
        },
        burstPrevention: { enabled: false, cooldownSeconds: 30, redisTTL: 60 },
        messageLengthVariation: { enabled: false, threshold: 200, maxExtraDelay: 2.0, multiplier: 0.005 },
        timeBasedVariation: {
            enabled: true,
            morning: { start: 6, end: 9, multiplier: 0.9 },
            day: { start: 9, end: 17, multiplier: 1.0 },
            evening: { start: 17, end: 22, multiplier: 1.2 },
            night: { start: 22, end: 6, multiplier: 1.5 }
        },
        captionVariation: { enabled: false, emojiChance: 0.3, emojis: [], spacingChance: 0.2, unicodeVariants: true },
        imageUploadDelay: { enabled: false, min: 0.3, max: 1.0 }
    }
};

export default config;

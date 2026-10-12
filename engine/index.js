import './quiet-console.js';
import express from 'express';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import dotenv from 'dotenv';
import SessionManager from './session.js';
import BaileysWrapper, { ensureSharedRedis } from './wrapper.js';
import axios from 'axios';
import http from 'http';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import config from './config/config.js';
import { createWsFleetGuard } from './ws-fleet-guard.js';
import { globalSendSlot } from './send-slot.js';
import { getBaileysWebSocketReadyState, isBaileysWebSocketOpen } from './ws-ready-state.js';
import { DisconnectReason } from './disconnect-reason.js';

// Load environment variables
dotenv.config();

/** Fleet: selalu tunggu msg_id asli WA — tidak pernah temp/callback mode. */
function resolveSendCallbackMode(body) {
    const { enable_callback, enableCallback, callback } = body || {};
    const paramEnableCallback = enable_callback !== undefined || enableCallback !== undefined || callback !== undefined;
    const paramCallbackValue = enable_callback !== undefined ? enable_callback : (enableCallback !== undefined ? enableCallback : callback);
    let requested = false;
    if (paramEnableCallback) {
        if (typeof paramCallbackValue === 'boolean') {
            requested = paramCallbackValue;
        } else if (typeof paramCallbackValue === 'string') {
            const lowerValue = paramCallbackValue.toLowerCase().trim();
            requested = lowerValue === 'true' || lowerValue === '1' || lowerValue === 'yes' || lowerValue === 'on';
        } else if (typeof paramCallbackValue === 'number') {
            requested = paramCallbackValue === 1;
        }
    } else if (config?.performance?.sendDefaultEnableCallback === true) {
        requested = true;
    }
    if (requested) {
        console.warn('[FLEET] enable_callback ignored — waiting real msg_id only (no TMP_)');
    }
    return false;
}

// ============================================================================
// 🔒 IP Whitelist HTTP (Express shard 7001–7007) — WS terpisah di ws-fleet-guard.js :8001
// ============================================================================
const HTTP_EXTRA_ALLOWED_IPS = (process.env.ALLOWED_IPS_EXTRA || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
const HTTP_ALLOWED_IPS = [...(config.security.allowedIPs || []), ...HTTP_EXTRA_ALLOWED_IPS];

function getHttpClientIP(req) {
    const h = req?.headers || {};
    return h['cf-connecting-ip'] ||
        h['x-forwarded-for']?.split(',')[0]?.trim() ||
        req?.socket?.remoteAddress ||
        req?.connection?.remoteAddress ||
        '';
}

/** Alias — beberapa middleware timeout masih memanggil getRealIP */
function getRealIP(req) {
    return getHttpClientIP(req);
}

function isHttpClientAllowed(clientIP) {
    if (!clientIP) return false;
    const ip = String(clientIP).trim();
    return HTTP_ALLOWED_IPS.some((allowed) => ip.includes(allowed));
}

// ✅ CRITICAL: Initialize global shutdown flags di awal (sebelum handler lainnya)
// Ini penting untuk prevent false DISCONNECTED saat crash/restart
if (typeof global !== 'undefined') {
    global.isShuttingDown = false;
    // ✅ Mass disconnect detection untuk detect hard kill/restart
    global.massDisconnectWindow = {
        startTime: null,
        disconnectCount: 0,
        windowMs: 10000, // 10 detik window
        threshold: 5 // Jika 5+ session disconnect dalam 10s, kemungkinan restart
    };
    global.shardDrainMode = false;
    global.shardDrainSince = 0;
    global.__realShutdown = false;
    console.log('✅ Global shutdown flags initialized');
}

/**
 * Redam DISCONNECTED sementara untuk kejadian yang BELUM tentu mematikan proses
 * (uncaughtException yang tertangkap, lonjakan memori).
 *
 * Dulu kejadian seperti ini menyetel global.isShuttingDown = true tanpa pernah
 * mengembalikannya. Kalau prosesnya ternyata selamat, flag itu menempel selamanya dan
 * SEMUA webhook DISCONNECTED berikutnya ikut dibuang — backend tidak pernah tahu bot
 * putus, jadi kelihatan "connected" padahal mati.
 */
function suppressDisconnectWebhooks(reason, ms = 15000) {
    if (typeof global === 'undefined') return;
    if (global.__realShutdown) return;
    global.isShuttingDown = true;
    console.warn(`🛑 [SHUTDOWN-FLAG] suppress DISCONNECTED ${ms}ms — ${reason}`);
    clearTimeout(global.__suppressDisconnectTimer);
    global.__suppressDisconnectTimer = setTimeout(() => {
        if (global.__realShutdown) return;
        global.isShuttingDown = false;
        console.warn(`✅ [SHUTDOWN-FLAG] suppress selesai — ${reason}`);
    }, ms);
    global.__suppressDisconnectTimer.unref?.();
}

/** Proses memang benar-benar berhenti: kunci flag permanen. */
function markRealShutdown() {
    if (typeof global === 'undefined') return;
    global.__realShutdown = true;
    global.isShuttingDown = true;
    clearTimeout(global.__suppressDisconnectTimer);
}

function isShardDraining() {
    return global.shardDrainMode === true;
}

function shardDrainReject(res) {
    if (!isShardDraining()) return false;
    res.status(503).json({
        error: '1',
        msg: 'shard draining — retry another shard or wait',
        draining: true,
        since: global.shardDrainSince || null,
        port: PORT,
    });
    return true;
}

// ----------------------------------------------------------------------------
// Recipient existence check toggle
// ----------------------------------------------------------------------------
const disableNumberCheckEnv = String(process.env.DISABLE_WHATSAPP_NUMBER_CHECK || '').trim().toLowerCase();
const numberCheckDisabledByEnv = ['1', 'true', 'yes', 'on'].includes(disableNumberCheckEnv);
const numberCheckDisabledByConfig = config?.validation?.checkWhatsAppNumber === false;
let runtimeNumberCheckOverride = null;

function isRecipientCheckEnabled() {
    if (runtimeNumberCheckOverride !== null) {
        return runtimeNumberCheckOverride;
    }
    if (numberCheckDisabledByEnv) {
        return false;
    }
    if (numberCheckDisabledByConfig) {
        return false;
    }
    return true;
}

function setRecipientCheckEnabled(enabled) {
    runtimeNumberCheckOverride = Boolean(enabled);
    console.log(`⚙️  WhatsApp recipient check ${runtimeNumberCheckOverride ? 'ENABLED' : 'DISABLED'} via runtime toggle`);
    return runtimeNumberCheckOverride;
}

globalThis.setWhatsAppNumberCheckEnabled = setRecipientCheckEnabled;
globalThis.getWhatsAppNumberCheckState = () => ({
    enabled: isRecipientCheckEnabled(),
    override: runtimeNumberCheckOverride,
    disabledByEnv: numberCheckDisabledByEnv,
    disabledByConfig: numberCheckDisabledByConfig
});

// ----------------------------------------------------------------------------
// Helper function to validate WhatsApp phone number
// ----------------------------------------------------------------------------
function validateWhatsAppNumber(phoneNumber) {
    if (!phoneNumber || typeof phoneNumber !== 'string') {
        return { valid: false, error: 'Phone number is required' };
    }
    
    // Remove all non-digit characters except +
    let cleaned = phoneNumber.replace(/[^\d+]/g, '').trim();
    
    // If starts with +, remove it for validation
    if (cleaned.startsWith('+')) {
        cleaned = cleaned.substring(1);
    }
    
    // Handle Indonesian format (0xx -> 62xx)
    if (cleaned.startsWith('0')) {
        cleaned = '62' + cleaned.substring(1);
    }
    
    // Must be digits only now
    if (!/^\d+$/.test(cleaned)) {
        return { valid: false, error: 'Invalid phone number format. Must contain only digits' };
    }
    
    // WhatsApp international format: 10-15 digits (with country code)
    if (cleaned.length < 10 || cleaned.length > 15) {
        return { valid: false, error: `Invalid phone number length. Got ${cleaned.length} digits, must be 10-15 digits (international format with country code)` };
    }
    
    // Basic validation: must have country code (starts with 1-9, not 0)
    if (cleaned.startsWith('0')) {
        return { valid: false, error: 'Phone number must include country code (e.g., 62 for Indonesia, 1 for US)' };
    }
    
    // WhatsApp valid format check: must start with country code (1-9)
    const firstDigit = parseInt(cleaned[0]);
    if (isNaN(firstDigit) || firstDigit < 1 || firstDigit > 9) {
        return { valid: false, error: 'Invalid country code. Phone number must start with digits 1-9' };
    }
    
    return { valid: true, cleaned: cleaned };
}

// ----------------------------------------------------------------------------
// Helper function to generate webhook authentication headers
// ----------------------------------------------------------------------------
function generateWebhookHeaders(sessionId, payload) {
    const baseApiKey = config.webhook.apiKey;
    const baseSecret = config.webhook.secret;
    
    // ✅ Use sessionId from payload FIRST (must match what server reads)
    // Parse payload to get sessionId if it's a string, or use passed sessionId
    let seed = sessionId || '';
    if (typeof payload === 'object' && payload.sessionId) {
        seed = payload.sessionId.toString();
    }
    
    const dynamicApiKey = seed ? crypto.createHmac('sha256', baseApiKey).update(seed).digest('hex') : baseApiKey;
    const dynamicSecret = seed ? crypto.createHmac('sha256', baseSecret).update(seed).digest('hex') : baseSecret;
    
    const body = JSON.stringify(payload);
    const signature = crypto.createHmac('sha256', dynamicSecret).update(body).digest('hex');
    
    return {
        'Content-Type': 'application/json',
        'X-Broadcast-Key': dynamicApiKey,
        'X-Webhook-Signature': signature
    };
}

// ----------------------------------------------------------------------------
// Webhook ke backend — default ringan (tanpa stringify/log panjang tiap hit).
// Detail penuh hanya jika config.logging.enableConsoleLog === true.
// ----------------------------------------------------------------------------
async function sendWebhookWithLogging(payload, sessionId = null, eventName = null) {
    const webhookUrl = config.webhook.url;
    const event = eventName || payload?.event || 'UNKNOWN';
    const actualSessionId = sessionId || payload?.sessionId || 'N/A';
    const verbose = config.logging?.enableConsoleLog === true;

    if (verbose) {
        console.log(`📤 [WEBHOOK] Sending ${event} sessionId=${actualSessionId}`);
        console.log(`   URL: ${webhookUrl}`);
        console.log(`   Payload:`, JSON.stringify(payload));
    }

    const startTime = Date.now();

    try {
        const response = await axios.post(webhookUrl, payload, {
            headers: generateWebhookHeaders(actualSessionId, payload),
            timeout: 10000
        });

        const responseTime = Date.now() - startTime;

        if (response.status >= 200 && response.status < 300) {
            if (verbose) {
                const responseBody =
                    typeof response.data === 'string'
                        ? response.data
                        : typeof response.data === 'object'
                          ? JSON.stringify(response.data)
                          : String(response.data);
                const responsePreview =
                    responseBody.length > 300 ? responseBody.substring(0, 300) + '...' : responseBody;
                console.log(`✅ [WEBHOOK] ${event} OK session=${actualSessionId} ${responseTime}ms`);
                console.log(`   Status: ${response.status} Body: ${responsePreview || '(empty)'}`);
            }
        } else {
            console.warn(`⚠️ [WEBHOOK] ${event} status ${response.status} session=${actualSessionId} ${responseTime}ms`);
        }

        return response.data;
    } catch (error) {
        const responseTime = Date.now() - startTime;

        if (error.response) {
            const responseData = error.response.data;
            const responseText =
                typeof responseData === 'string' ? responseData : JSON.stringify(responseData);
            const short = responseText.length > 220 ? responseText.substring(0, 220) + '…' : responseText;
            console.error(
                `❌ [WEBHOOK] ${event} HTTP ${error.response.status} session=${actualSessionId} ${responseTime}ms — ${short || '(empty)'}`
            );
            if (verbose) {
                console.error(`   URL: ${webhookUrl}`);
                console.error(`   Headers:`, JSON.stringify(error.response.headers));
            }
        } else if (error.request) {
            console.error(`❌ [WEBHOOK] ${event} no response session=${actualSessionId} ${responseTime}ms`);
            if (verbose) {
                console.error(`   URL: ${webhookUrl}`);
                console.error(`   Payload:`, JSON.stringify(payload));
            }
        } else {
            console.error(`❌ [WEBHOOK] ${event} ${error.message} session=${actualSessionId}`);
            if (verbose) {
                console.error(`   URL: ${webhookUrl}`);
                console.error(`   Payload:`, JSON.stringify(payload || {}));
            }
        }

        throw error;
    }
}

// Fire-and-forget helper so HTTP handlers don't block on webhook completion
function dispatchWebhook(payload, sessionId = null, eventName = null) {
    const sid = sessionId || payload?.sessionId || payload?.session || null;
    const home = sid ? sessionIdHomePort(sid) : null;
    if (home != null && payload && typeof payload === 'object') {
        payload.port = home;
    }
    const name = eventName || payload?.event || 'UNKNOWN';
    // Pengaman: MESSAGE_DELETED mentah / false-positive jangan sampai ke BE (blacklist massal)
    if (name === 'MESSAGE_DELETED' || payload?.event === 'MESSAGE_DELETED') {
        const validated = payload?.validated === true;
        const deletedBy = payload?.deletedBy;
        if (!validated || deletedBy !== 'bot_revoke_sent') {
            console.warn(
                `⏭️ [WEBHOOK] Drop MESSAGE_DELETED (unvalidated) session=${sessionId || payload?.sessionId || '?'} ` +
                `deletedBy=${deletedBy || 'n/a'} validated=${validated}`
            );
            return Promise.resolve({ skipped: true, reason: 'MESSAGE_DELETED_UNVALIDATED' });
        }
    }
    return sendWebhookWithLogging(payload, sessionId, eventName).catch((err) => {
        if (config.logging?.enableConsoleLog === true) {
            console.error(`❌ [WEBHOOK][${name}] Async dispatch failed: ${err.message || err}`);
        }
    });
}

// ----------------------------------------------------------------------------
// Global error guards to prevent process crash on transient Baileys errors
// ----------------------------------------------------------------------------
function isIgnorableConnectionClosed(err) {
    try {
        // Baileys throws Boom('Connection Closed') with statusCode 428
        return (
            (err && (err.isBoom || err.name === 'Boom') &&
             err.output && err.output.payload && err.output.payload.message === 'Connection Closed') ||
            (typeof err?.message === 'string' && /Connection Closed/i.test(err.message))
        );
    } catch (_) {
        return false;
    }
}

// ✅ FIXED: Helper function untuk check apakah error dari Baileys yang bisa di-ignore
function isIgnorableBaileysError(err) {
    try {
        if (!err) return false;
        
        const errorMessage = typeof err?.message === 'string' ? err.message.toLowerCase() : '';
        const errorName = typeof err?.name === 'string' ? err.name.toLowerCase() : '';
        
        // ✅ Ignore common Baileys connection errors yang tidak critical
        const ignorablePatterns = [
            'connection closed',
            'connection reset',
            'connection issue',
            'failed to fetch media',
            'media connection',
            '1006',
            'socket hang up',
            'econnreset',
            'etimedout',
            'timeout',
            'network error',
            'disconnected',
            'reconnect',
            'end of stream',
            'stream ended',
            'connection lost',
            'cannot send image',
            'websocket was closed before the connection was established',
            'websocket was closed',
        ];
        
        // Check error message
        if (ignorablePatterns.some(pattern => errorMessage.includes(pattern))) {
            return true;
        }
        
        // Check error name
        if (ignorablePatterns.some(pattern => errorName.includes(pattern))) {
            return true;
        }
        
        // Check Boom errors dengan status code yang tidak critical
        if (err.isBoom || err.name === 'Boom') {
            const statusCode = err.output?.statusCode || err.statusCode;
            // Ignore 4xx errors (client errors) dan 428 (Connection Closed)
            if (statusCode >= 400 && statusCode < 500) {
                return true;
            }
        }
        
        return false;
    } catch (_) {
        return false;
    }
}

/** Session folder dihapus (cron/logout) saat saveCreds masih jalan — bukan crash shard. */
function isIgnorableSessionPathGone(err) {
    try {
        if (!err) return false;
        const code = err?.code;
        const msg = typeof err?.message === 'string' ? err.message : String(err);
        if (code !== 'ENOENT') return false;
        return msg.includes('creds.json') || (msg.includes('/sessions/') && msg.includes('sess_'));
    } catch (_) {
        return false;
    }
}

// ✅ FIXED: Prevent crash pada unhandledRejection
process.on('unhandledRejection', (reason) => {
    if (isIgnorableConnectionClosed(reason)) {
        console.warn('⚠️  Ignored unhandledRejection: Connection Closed');
        return;
    }
    
    // ✅ FIXED: Check apakah error dari Baileys yang bisa di-ignore
    if (isIgnorableBaileysError(reason)) {
        console.warn('⚠️  Ignored unhandledRejection (Baileys connection error):', reason instanceof Error ? reason.message : String(reason));
        return;
    }

    if (isIgnorableSessionPathGone(reason)) {
        console.warn('⚠️  Ignored unhandledRejection (session folder gone / creds ENOENT):',
            reason instanceof Error ? reason.message : String(reason));
        return;
    }

    // Jika unhandled rejection spam, lebih aman restart proses (hindari state corrupt + reconnect storm berkepanjangan)
    // Default: TIDAK exit pada 1 kejadian; hanya exit kalau berulang dalam window pendek.
    try {
        const now = Date.now();
        if (!global.__unhandledRejectionWindow) {
            global.__unhandledRejectionWindow = { start: now, count: 0 };
        }
        const windowMs = Math.max(5000, Math.min(120000, Number(process.env.UNHANDLED_REJECTION_WINDOW_MS) || 30000));
        const maxRaw = process.env.UNHANDLED_REJECTION_MAX_IN_WINDOW;
        const maxInWindow =
            maxRaw != null && String(maxRaw).trim() !== ''
                ? Math.max(0, Math.min(50, Number(maxRaw) || 0))
                : 0;
        const w = global.__unhandledRejectionWindow;
        if (now - w.start > windowMs) {
            w.start = now;
            w.count = 0;
        }
        w.count++;
        if (maxInWindow > 0 && w.count >= maxInWindow) {
            console.error(`🛑 [UNHANDLED] Too many unhandledRejection (${w.count}/${maxInWindow} in ${windowMs}ms) - exiting for clean restart`);
            markRealShutdown();
            // beri waktu logCriticalError enqueue (non-blocking)
            setTimeout(() => process.exit(1), 250).unref?.();
        }
    } catch (_) {}
    
    // ✅ PREVENT CRASH: Log error tapi jangan crash process
    logCriticalError({
        event: 'PROCESS_UNHANDLED_REJECTION',
        reason: reason instanceof Error ? reason.message : JSON.stringify(reason),
        stack: reason instanceof Error ? reason.stack : undefined
    });
    
    // ✅ CRITICAL: Jangan crash process, hanya log dan continue
    // Node.js default behavior untuk unhandledRejection adalah warning, tidak crash
    // Tapi kita tetap log untuk monitoring
});

// ✅ FIXED: Prevent crash pada uncaughtException
process.on('uncaughtException', (err) => {
    if (isIgnorableConnectionClosed(err)) {
        console.warn('⚠️  Ignored uncaughtException: Connection Closed');
        return;
    }
    
    // ✅ FIXED: Check apakah error dari Baileys yang bisa di-ignore
    if (isIgnorableBaileysError(err)) {
        console.warn('⚠️  Ignored uncaughtException (Baileys connection error):', err?.message || 'Unknown error');
        return;
    }

    if (isIgnorableSessionPathGone(err)) {
        console.warn('⚠️  Ignored uncaughtException (session folder gone / creds ENOENT):', err?.message || 'Unknown error');
        return;
    }
    
    // Redam sementara saja: exception ini sering tidak mematikan proses, dan flag yang
    // menempel permanen membuat semua DISCONNECTED berikutnya hilang.
    suppressDisconnectWebhooks('uncaughtException', 20000);
    
    // ✅ PREVENT CRASH: Log error dulu
    logCriticalError({
        event: 'PROCESS_UNCAUGHT_EXCEPTION',
        error: err?.message || 'Unknown error',
        stack: err?.stack
    });
    
    // ✅ CRITICAL: Prevent crash - jangan exit process
    // Node.js default behavior untuk uncaughtException adalah exit(1)
    // Kita prevent ini untuk stabilitas, tapi tetap log error
    console.error('🚨 [CRITICAL] UncaughtException detected but process will continue to prevent crash');
    console.error('🚨 [CRITICAL] Error details:', err?.message || 'Unknown error');
    
    // ✅ FIXED: Prevent exit dengan set exitCode = 0
    process.exitCode = 0; // Prevent automatic exit
    
    // ✅ GRACEFUL HANDLING: Jangan exit, biarkan process continue
    // PM2 atau process manager lain akan handle restart jika diperlukan
    // Note: Tidak perlu removeAllListeners karena kita sudah set exitCode = 0
});


const app = express();

// ✅ HIGH PERFORMANCE: Response compression (optional - tidak critical)
// Compression di-disable untuk sekarang karena optional
// Jika perlu compression, install: npm install compression
// Lalu tambahkan middleware compression di sini

// ✅ FIXED: Increase connection limit untuk prevent bad gateway (default Node.js: 10)
// ✅ CRITICAL: Bad gateway biasanya karena connection pool exhaustion
const server = createServer(app);
server.maxConnections = 5000; // ✅ INCREASED: 5000 concurrent connections (prevent connection exhaustion)
// ✅ HIGH TRAFFIC: Keep-alive optimization untuk prevent connection exhaustion & faster reuse
// Selaras curl processor 18s: keepAlive < headersTimeout, keduanya di atas send pipeline (~17s)
server.keepAliveTimeout = 20000;
server.headersTimeout = 22000;
const PORT = Number(process.env.PORT) || config.server.httpPort;
const WS_PORT = Number(process.env.WS_PORT) || config.server.wsPort;

// Ensure the resolved ports are reflected globally for other modules
config.server.httpPort = PORT;
config.server.wsPort = WS_PORT;
/** WS dashboard di baileys-ws-hub.service bila BAILEYS_WS_EXTERNAL=1 */
const WS_EXTERNAL = String(process.env.BAILEYS_WS_EXTERNAL || '').trim() === '1';
const WS_HUB_HTTP_BASE = (process.env.WS_HUB_HTTP_BASE || 'http://127.0.0.1:8010').replace(/\/$/, '');

/**
 * Port di sessionId harus konsisten dengan shard (7001–7007 fleet 7×).
 * Pair di fleet: actualPort = PORT proses penerima (nginx path /a–/j atau /p/); body.port/header tidak override.
 */
const SHARD_PORT_MIN = 7001;
const SHARD_PORT_MAX = 7007;
const SHARD_COUNT = SHARD_PORT_MAX - SHARD_PORT_MIN + 1;
/** Fleet HTTP (7001–7007): tiap proses punya RAM cleanup + optional MEMORY_RESTART (bukan hanya 7001). */
const IS_SHARD_HTTP_PORT =
    Number.isFinite(PORT) && PORT >= SHARD_PORT_MIN && PORT <= SHARD_PORT_MAX;
/**
 * Satu shard fleet = 7001–7007. Port canonical langsung dipakai.
 * Legacy 7008–7050 / slot 1–50 dilipat ke 7001–7007 (tidak dipakai untuk pair baru).
 */
function normalizeToShardPort(n) {
    if (!Number.isFinite(n)) return null;
    const p = Math.floor(n);
    if (p >= SHARD_PORT_MIN && p <= SHARD_PORT_MAX) return p;
    if (p > SHARD_PORT_MAX) {
        const idx = (p - SHARD_PORT_MIN) % SHARD_COUNT;
        const folded = SHARD_PORT_MIN + ((idx + SHARD_COUNT) % SHARD_COUNT);
        console.warn(`⚠️ legacy port ${p} folded → ${folded} (fleet7 canonical 7001–7007)`);
        return folded;
    }
    if (p >= 1 && p < SHARD_PORT_MIN) {
        return SHARD_PORT_MIN + ((p - 1) % SHARD_COUNT);
    }
    return null;
}
function resolvePairServicePort(req, bodyPort) {
    const here = normalizeToShardPort(Number(PORT)) ?? Number(PORT);
    /** Fleet 7001–7007: sessionId harus = shard yang benar-benar menerima pair (nginx /a–/j atau /p/). Body/header salah tidak boleh override. */
    if (IS_SHARD_HTTP_PORT) {
        let claimed = null;
        if (bodyPort !== undefined && bodyPort !== null && String(bodyPort).trim() !== '') {
            claimed = normalizeToShardPort(parseInt(String(bodyPort), 10));
        }
        if (claimed === null) {
            const h = req.headers['x-session-port'] ?? req.headers['X-Session-Port'];
            if (h !== undefined && h !== null && String(h).trim() !== '') {
                claimed = normalizeToShardPort(parseInt(String(h), 10));
            }
        }
        if (claimed !== null && claimed !== here) {
            console.warn(
                `⚠️ pair: body.port/X-Session-Port (${claimed}) ≠ shard penerima (${here}) — sess_* memakai PORT=${here} (${req.method} ${req.path})`
            );
        }
        return here;
    }
    if (bodyPort !== undefined && bodyPort !== null && String(bodyPort).trim() !== '') {
        const normalized = normalizeToShardPort(parseInt(String(bodyPort), 10));
        if (normalized !== null) {
            return normalized;
        }
    }
    const h = req.headers['x-session-port'] ?? req.headers['X-Session-Port'];
    if (h !== undefined && h !== null && String(h).trim() !== '') {
        const normalized = normalizeToShardPort(parseInt(String(h), 10));
        if (normalized !== null) {
            return normalized;
        }
    }
    return here;
}
function logPairPortFallbackIfNeeded(req, bodyPort) {
    if (IS_SHARD_HTTP_PORT) {
        return;
    }
    const emptyBody =
        bodyPort === undefined || bodyPort === null || String(bodyPort).trim() === '';
    const h = req.headers['x-session-port'] ?? req.headers['X-Session-Port'];
    const emptyHeader = h === undefined || h === null || String(h).trim() === '';
    if (emptyBody && emptyHeader) {
        console.warn(
            `⚠️ pair: body.port & X-Session-Port kosong → sess_* memakai shard PORT=${PORT} (${req.method} ${req.path})`
        );
    }
}

/** Port rumah dari sessionId `sess_{user}_{port}_{ts}` (sudah dilipat ke 7001–7007). */
function sessionIdHomePort(sessionId) {
    if (sessionId == null) return null;
    const m = String(sessionId).match(/^sess_[^_]+_(\d+)_/);
    if (!m) return null;
    return normalizeToShardPort(parseInt(m[1], 10));
}

/** True bila sessionId boleh hidup di worker PORT ini. Session tak terbaca formatnya tidak diblokir (legacy). */
function sessionIdBelongsToThisShard(sessionId) {
    if (!IS_SHARD_HTTP_PORT || sessionId == null) return true;
    const fromId = sessionIdHomePort(sessionId);
    const here = normalizeToShardPort(Number(PORT));
    if (fromId == null || here == null) return true;
    return fromId === here;
}

/** Blokir load session di proses shard salah (nginx /p/ atau path /a–/j harus cocok dengan PORT di sessionId). */
function assertSessionIdBelongsToThisShard(sessionId) {
    if (sessionIdBelongsToThisShard(sessionId)) return;
    const fromId = sessionIdHomePort(sessionId);
    const here = normalizeToShardPort(Number(PORT));
    throw new Error(
        `Wrong shard: ${sessionId} is for port ${fromId}; this worker is ${here}. Route via /p/ with X-Session-Port or letter path /a–/g (or /h–/j cycle) for that port.`
    );
}

// ============================================================================
// ✅ WEBSOCKET SERVER - HANYA UNTUK PORT 7001 (Shared untuk semua instances)
// ============================================================================
let wss = null;
let sessionWebSockets = new Map();
let wsCleanupInterval = null;
/** RAM eviction tick (shard HTTP 7001–7007); wajib di-clear saat graceful shutdown */
let ramCleanupInterval = null;
/** Monitor RSS/heap warning (30s); wajib di-clear saat graceful shutdown */
let memoryMonitorInterval = null;

// ⏰ WebSocket Configuration (OPTIMAL - Production Ready)
const WS_PING_INTERVAL = 30000; // 30 detik - kirim ping
const WS_TIMEOUT_DURATION = Math.max(
    60000,
    Math.min(900000, Number(process.env.DASHBOARD_WS_TIMEOUT_MS) || 5 * 60 * 1000)
); // default 5 menit idle → terminate
const WS_CLEANUP_INTERVAL = 2 * 60 * 1000; // ✅ LIGHTWEIGHT: 2 menit (dari 1 menit) untuk reduce overhead
const MAX_DASHBOARD_WS_PER_USER = Math.max(1, Math.min(20, Number(process.env.MAX_DASHBOARD_WS_PER_USER) || 5));

// WS hub terpisah (systemd baileys-ws-hub) bila BAILEYS_WS_EXTERNAL=1
if ((PORT == 7001 || PORT == '7001') && !WS_EXTERNAL) {
    const wsFleetGuard = createWsFleetGuard(config);
    console.log(
        `🔌 WS service on port ${WS_PORT} (fleetIpLock=${wsFleetGuard.fleetIpLock ? 'on' : 'off'}, HTTP shard=${PORT})`
    );
    
    try {
        wss = new WebSocketServer({ 
            port: WS_PORT,
            path: '/ws',
            verifyClient: wsFleetGuard.verifyClient,
        });
        
        // ✅ FIX: Error handling untuk WebSocket server
        wss.on('error', (error) => {
            if (error.code === 'EADDRINUSE') {
                console.warn(`⚠️  WebSocket port ${WS_PORT} already in use - using existing instance`);
                wss = null; // Set ke null untuk prevent crash
            } else {
                console.error(`❌ WebSocket server error:`, error.message);
            }
        });
    } catch (error) {
        if (error.code === 'EADDRINUSE') {
            console.warn(`⚠️  WebSocket port ${WS_PORT} already in use - using existing instance`);
            wss = null; // Set ke null untuk prevent crash
        } else {
            console.error(`❌ Failed to create WebSocket server:`, error.message);
            wss = null;
        }
    }
    
    // WebSocket connection handler (upgrade sudah difilter verifyClient)
    wss.on('connection', (ws, req) => {
        const url = new URL(req.url, `http://${req.headers.host}`);
        const sessionId = url.searchParams.get('sessionId');
        const userId = url.searchParams.get('userId');
        const page = url.searchParams.get('page') || 'unknown';
        const providedClientId = url.searchParams.get('clientId');
        
        // 🔧 MULTI-TAB FIX: Generate unique clientId dengan page untuk avoid duplicate
        const clientId = providedClientId ? `${providedClientId}_${page}_${Date.now()}` : `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
        console.log(`🔌 WebSocket client connected: ${clientId} for user: ${userId}, page: ${page}`);
        
        // 🔧 FIX: Store client by userId (support multiple tabs)
        const userIdStr = String(userId); // Ensure string for consistency
        
        if (!sessionWebSockets.has(userIdStr)) {
            sessionWebSockets.set(userIdStr, new Map());
        }

        const existingClients = sessionWebSockets.get(userIdStr);
        if (existingClients && existingClients.size >= MAX_DASHBOARD_WS_PER_USER) {
            console.warn(`⚠️ Dashboard WS cap (${MAX_DASHBOARD_WS_PER_USER}) for user ${userIdStr} — rejecting ${clientId}`);
            try {
                ws.close(1008, 'Too many dashboard connections');
            } catch {
                /* ignore */
            }
            return;
        }
        
        const clientData = {
            ws: ws,
            sessionId: sessionId,
            userId: userIdStr,
            clientId: clientId,
            page: page, // Track which page this client is on
            connectedAt: new Date(),
            lastPing: Date.now(),
            lastPong: Date.now(),
            isAlive: true,
            pingInterval: null
        };
        
        sessionWebSockets.get(userIdStr).set(clientId, clientData);
        
        console.log(`✅ Total clients for user ${userIdStr}: ${sessionWebSockets.get(userIdStr).size}`);
        
        // Send welcome message
        ws.send(JSON.stringify({
            type: 'connected',
            message: 'WebSocket connected to Baileys server',
            clientId: clientId,
            wsPort: WS_PORT,
            pingInterval: WS_PING_INTERVAL,
            timeout: WS_TIMEOUT_DURATION
        }));
        
        // ⏰ START PING INTERVAL - Kirim ping setiap 30 detik
        clientData.pingInterval = setInterval(() => {
            try {
                if (ws.readyState === 1) { // OPEN
                    // Check if client masih alive (pong dalam 30 menit terakhir)
                    const timeSinceLastPong = Date.now() - clientData.lastPong;
                    
                    if (timeSinceLastPong > WS_TIMEOUT_DURATION) {
                        console.log(`⏰ Client ${clientId} timeout (no pong for ${Math.floor(timeSinceLastPong/60000)} minutes)`);
                        clearInterval(clientData.pingInterval);
                        ws.terminate(); // Force close
                        
                        // Cleanup from map
                        if (sessionWebSockets.has(userIdStr)) {
                            sessionWebSockets.get(userIdStr).delete(clientId);
                            if (sessionWebSockets.get(userIdStr).size === 0) {
                                sessionWebSockets.delete(userIdStr);
                            }
                        }
                        return;
                    }
                    
                    // Send ping
                    clientData.lastPing = Date.now();
                    ws.send(JSON.stringify({ 
                        type: 'ping', 
                        timestamp: Date.now() 
                    }));
                } else {
                    // Connection closed, clear interval
                    clearInterval(clientData.pingInterval);
                    clientData.pingInterval = null;
                }
            } catch (error) {
                console.error(`Error in ping interval for ${clientId}:`, error.message);
                clearInterval(clientData.pingInterval);
                clientData.pingInterval = null;
            }
        }, WS_PING_INTERVAL);
        
        // Handle messages from client
        ws.on('message', (data) => {
            try {
                const message = JSON.parse(data);
                
                // Handle ping from client (respond with pong)
                if (message.type === 'ping') {
                    const client = sessionWebSockets.get(userIdStr)?.get(clientId);
                    if (client) {
                        client.lastPing = Date.now();
                        client.lastPong = Date.now();
                        client.isAlive = true;
                    }
                    ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
                }
                
                // Handle pong from client (response to our ping)
                if (message.type === 'pong') {
                    const client = sessionWebSockets.get(userIdStr)?.get(clientId);
                    if (client) {
                        client.lastPong = Date.now();
                        client.isAlive = true;
                    }
                }
                
                // Handle subscribe events
                if (message.type === 'subscribe_events') {
                    console.log(`📡 Client ${clientId} subscribed to events:`, message.events);
                    ws.send(JSON.stringify({ 
                        type: 'subscribed', 
                        events: message.events 
                    }));
                }
            } catch (error) {
                console.error('Error handling WebSocket message:', error);
            }
        });
        
        // Handle disconnect
        ws.on('close', () => {
            console.log(`🔌 WebSocket client disconnected: ${clientId}`);
            
            // Clear ping interval
            const client = sessionWebSockets.get(userIdStr)?.get(clientId);
            if (client && client.pingInterval) {
                clearInterval(client.pingInterval);
            }
            
            // Remove from map
            if (sessionWebSockets.has(userIdStr)) {
                sessionWebSockets.get(userIdStr).delete(clientId);
                if (sessionWebSockets.get(userIdStr).size === 0) {
                    sessionWebSockets.delete(userIdStr);
                }
            }
        });
        
        // Handle errors
        ws.on('error', (error) => {
            console.error(`WebSocket error for ${clientId}:`, error.message);
        });
    });
    
    // ⏰ LIGHTWEIGHT: CLEANUP INTERVAL - Hapus dead connections setiap 2 menit (dari 1 menit) untuk reduce overhead
    wsCleanupInterval = setInterval(() => {
        try {
            let cleaned = 0;
            const now = Date.now();
            
            sessionWebSockets.forEach((clients, userId) => {
                clients.forEach((client, clientId) => {
                    try {
                        const timeSinceLastPong = now - client.lastPong;
                        
                        // Jika tidak ada pong >30 menit, disconnect
                        if (timeSinceLastPong > WS_TIMEOUT_DURATION) {
                            console.log(`🧹 Cleaning up dead client: ${clientId} (no activity for ${Math.floor(timeSinceLastPong/60000)} minutes)`);
                            
                            // Clear ping interval
                            if (client.pingInterval) {
                                clearInterval(client.pingInterval);
                                client.pingInterval = null;
                            }
                            
                            // Close connection
                            if (client.ws) {
                                try {
                                    if (client.ws.readyState === 1) {
                                        client.ws.close();
                                    }
                                } catch (wsError) {
                                    console.error(`Error closing WebSocket for ${clientId}:`, wsError.message);
                                }
                            }
                            
                            // Remove from map
                            clients.delete(clientId);
                            cleaned++;
                        }
                    } catch (clientError) {
                        console.error(`Error cleaning client ${clientId}:`, clientError.message);
                        // Still try to remove from map
                        clients.delete(clientId);
                    }
                });
                
                // Hapus userId jika tidak ada client lagi
                if (clients.size === 0) {
                    sessionWebSockets.delete(userId);
                }
            });
            
            if (cleaned > 0) {
                console.log(`🧹 Cleaned up ${cleaned} dead WebSocket connections`);
            }
        } catch (error) {
            console.error('Error in WebSocket cleanup interval:', error.message);
        }
    }, WS_CLEANUP_INTERVAL);
    
    console.log(`✅ WS service ready on port ${WS_PORT} (terpisah dari HTTP :${PORT})`);
    console.log(`⏰ Ping interval: ${WS_PING_INTERVAL/1000}s, Timeout: ${WS_TIMEOUT_DURATION/60000} minutes`);
} else if (PORT == 7001 || PORT == '7001') {
    console.log(`ℹ️  HTTP shard ${PORT}: WS di baileys-ws-hub.service :${WS_PORT} (BAILEYS_WS_EXTERNAL=1)`);
} else {
    console.log(`ℹ️  HTTP shard ${PORT}: tanpa WS listener (gunakan WS hub :${WS_PORT})`);
}

// ============================================================================
// BROADCAST FUNCTION - Available untuk semua ports
// ============================================================================
/** Satu port WS (8001) di 7001; shard lain kirim event lewat Redis PUBLISH (ringan) atau HTTP keep-alive (fallback). */
const BROADCAST_REDIS_CHANNEL = 'baileys:ws-broadcast';
const broadcastInternalHttpAgent = new http.Agent({ keepAlive: true, maxSockets: 64 });
let wsBroadcastRedisSubStarted = false;

function postBroadcastInternalFallback(body) {
    const target = WS_EXTERNAL
        ? `${WS_HUB_HTTP_BASE}/broadcast-internal`
        : 'http://127.0.0.1:7001/broadcast-internal';
    axios
        .post(target, body, {
            httpAgent: broadcastInternalHttpAgent,
            timeout: 8000,
            headers: { Connection: 'keep-alive' }
        })
        .catch((err) => {
            console.error(`Failed to broadcast via ${WS_EXTERNAL ? 'WS hub' : 'port 7001'}:`, err.message);
        });
}

/** Shard non-7001: PUBLISH ke Redis (ringan, tanpa Express); gagal → HTTP keep-alive ke 7001 (sama seperti sebelumnya). */
function relayBroadcastFromShardTo7001(httpBody, relayPayload) {
    const r = global.sharedRedis;
    if (!r?.isReady || String(process.env.BROADCAST_DISABLE_REDIS || '').trim() === '1') {
        postBroadcastInternalFallback(httpBody);
        return;
    }
    let json;
    try {
        json = JSON.stringify(relayPayload);
    } catch {
        postBroadcastInternalFallback(httpBody);
        return;
    }
    if (json.length > 4 * 1024 * 1024) {
        postBroadcastInternalFallback(httpBody);
        return;
    }
    r.publish(BROADCAST_REDIS_CHANNEL, json).catch(() => {
        postBroadcastInternalFallback(httpBody);
    });
}

/** Hanya dipanggil di shard 7001 setelah sharedRedis ready — sekali. */
async function maybeStartWsBroadcastRedisSubscriber() {
    if (wsBroadcastRedisSubStarted) return;
    if (WS_EXTERNAL) return;
    if (PORT != 7001 && PORT != '7001') return;
    const base = global.sharedRedis;
    if (!base?.isReady) return;
    wsBroadcastRedisSubStarted = true;
    try {
        const sub = base.duplicate();
        sub.on('error', (e) => console.error('[WS-BROADCAST-SUB]', e.message));
        await sub.connect();
        await sub.subscribe(BROADCAST_REDIS_CHANNEL, (message) => {
            try {
                const o = JSON.parse(message);
                if (o.k === 'u' && o.id != null) {
                    broadcastToUser(o.id, o.d);
                } else if (o.k === 's' && o.id != null) {
                    broadcastToSession(o.id, o.d);
                }
            } catch (e) {
                console.error('[WS-BROADCAST-SUB] handle:', e.message);
            }
        });
        console.log(
            `✅ [WS-BROADCAST] Redis relay aktif (${BROADCAST_REDIS_CHANNEL}) — shard lain pakai PUBLISH (tanpa HTTP Express per event)`
        );
    } catch (e) {
        wsBroadcastRedisSubStarted = false;
        console.error('[WS-BROADCAST] gagal subscribe:', e.message);
    }
}

global.maybeStartWsBroadcastRedisSubscriber = maybeStartWsBroadcastRedisSubscriber;

function broadcastToSession(sessionId, data) {
    if (WS_EXTERNAL || (PORT != 7001 && PORT != '7001')) {
        relayBroadcastFromShardTo7001(
            { sessionId: sessionId, data: data },
            { k: 's', id: String(sessionId), d: data }
        );
        return;
    }
    
    // Direct broadcast jika ini port 7001
    if (!sessionWebSockets.has(sessionId)) {
        return;
    }
    
    const clients = sessionWebSockets.get(sessionId);
    let sentCount = 0;
    
    clients.forEach((client) => {
        if (client.ws.readyState === 1) { // OPEN
            client.ws.send(JSON.stringify(data));
            sentCount++;
        }
    });

    if (String(process.env.BROADCAST_VERBOSE || '').trim() === '1') {
        console.log(`📡 Broadcasted to ${sentCount} clients in session ${sessionId}`);
    }
}

// Broadcast ke semua users
function broadcastToUser(userId, data) {
    if (WS_EXTERNAL || (PORT != 7001 && PORT != '7001')) {
        relayBroadcastFromShardTo7001(
            { userId: userId, data: data },
            { k: 'u', id: String(userId), d: data }
        );
        return;
    }

    if (userId == null || String(userId).trim() === '') {
        return;
    }

    // ✅ FIX: Convert userId to string for consistency (WebSocket stores as string)
    const userIdStr = String(userId);
    if (userIdStr === 'undefined' || userIdStr === 'null') {
        return;
    }

    const broadcastVerbose = String(process.env.BROADCAST_VERBOSE || '').trim() === '1';

    // Direct broadcast jika ini port 7001
    if (broadcastVerbose) {
        console.log(`📡 Broadcasting to user: ${userIdStr}, mapUsers=${sessionWebSockets.size}`);
    }

    if (!sessionWebSockets.has(userIdStr)) {
        if (broadcastVerbose) {
            console.warn(`⚠️ No WebSocket clients found for user ${userIdStr}`);
        }
        return;
    }

    const clients = sessionWebSockets.get(userIdStr);
    let sentCount = 0;

    clients.forEach((client) => {
        if (client.ws.readyState === 1) {
            client.ws.send(JSON.stringify(data));
            sentCount++;
            if (broadcastVerbose) {
                console.log(`✅ Sent to client ${client.clientId}`);
            }
        } else if (broadcastVerbose) {
            console.warn(`⚠️ Client ${client.clientId} not ready (state: ${client.ws.readyState})`);
        }
    });

    if (broadcastVerbose) {
        console.log(`📡 Broadcasted to ${sentCount} clients for user ${userIdStr}`);
    }
}

// ============================================================================
// 🔒 SECURITY MIDDLEWARE - IP Whitelist
// ============================================================================

// IP whitelist HTTP — fleet BE+lib only (sama daftar dengan nginx lib-http-fleet-allow)
app.use((req, res, next) => {
    const clientIP = getHttpClientIP(req);
    const isAllowed = isHttpClientAllowed(clientIP);
    
    if (!isAllowed) {
        console.warn(`🚫 Blocked request from: ${clientIP} to ${req.path}`);
        return res.status(403).json({ 
            error: '1', 
            msg: 'Access denied - IP not whitelisted',
            yourIP: clientIP
        });
    }
    
    // console.log(`✅ Allowed request from: ${clientIP} to ${req.path}`);
    next();
});

// ✅ REMOVED: Rate limiting (bukan penyebab bad gateway)
// ✅ FIXED: Bad gateway kemungkinan disebabkan oleh:
// 1. Connection pool exhaustion (terlalu banyak concurrent connections)
// 2. Memory leaks (dedupeCache, statusHistory yang tidak di-cleanup)
// 3. Hanging requests (timeout yang terlalu lama atau tidak ada)
// 4. Express body parser blocking (parsing large body)
// 5. Too many async operations tanpa proper error handling

// ✅ OPTIMIZED: Request timeout middleware (prevent hanging requests) - Smart timeout dengan per-endpoint
const REQUEST_TIMEOUT_MS = 15000; // Endpoint umum
const SEND_ENDPOINT_TIMEOUT_MS = 17000; // Selaras wrapper 16s + buffer; PHP curl 18s
app.use((req, res, next) => {
    // Skip timeout untuk WebSocket upgrade
    if (req.headers.upgrade === 'websocket') {
        return next();
    }

    const perf = config.performance || {};
    // Prioritas: env REQUEST_TIMEOUT_MS / SEND_ENDPOINT_TIMEOUT_MS → default 17s (jangan ikut config 55–90s lawas)
    const envSend = Number(process.env.REQUEST_TIMEOUT_MS || process.env.SEND_ENDPOINT_TIMEOUT_MS || '');
    const cfgSendTimeout = Number(perf.requestTimeout);
    const rawSend = Number.isFinite(envSend) && envSend > 0
        ? envSend
        : (Number.isFinite(cfgSendTimeout) && cfgSendTimeout > 0 ? Math.min(cfgSendTimeout, 20000) : SEND_ENDPOINT_TIMEOUT_MS);
    const sendTimeoutMs = Math.max(10000, Math.min(60000, rawSend));

    // ✅ Semua POST yang path-nya mengandung "/send" (termasuk /baileys/send, /api/send) — bukan cuma yang prefix /send
    const isSendEndpoint =
        req.method === 'POST' &&
        (req.path === '/send' ||
            req.path.startsWith('/send') ||
            (typeof req.path === 'string' && req.path.includes('/send')));
    const isPairEndpoint =
        req.method === 'POST' && typeof req.path === 'string' && req.path.includes('/pair');
    const pairTimeoutMs = (() => {
        const raw = process.env.PAIR_REQUEST_TIMEOUT_MS;
        const n = Number(raw);
        if (Number.isFinite(n) && n >= 30000) return Math.min(180000, Math.floor(n));
        return 120000;
    })();
    const timeoutMs = isSendEndpoint ? sendTimeoutMs : isPairEndpoint ? pairTimeoutMs : REQUEST_TIMEOUT_MS;
    
    // ✅ SMART TIMEOUT: Non-blocking timeout dengan early detection
    const timeout = setTimeout(() => {
        if (!res.headersSent) {
            console.warn(`⚠️ [TIMEOUT] Request timeout for ${req.method} ${req.path} from IP: ${getRealIP(req)} (${timeoutMs/1000}s)`);
            res.status(504).json({
                error: '6',
                msg: `Operation timed out after ${timeoutMs} milliseconds`
            });
        }
    }, timeoutMs);
    
    // ✅ SMART: Clear timeout saat response selesai (prevent memory leak, tidak ngelock)
    res.on('finish', () => clearTimeout(timeout));
    res.on('close', () => clearTimeout(timeout));
    
    next();
});

// ✅ FIXED: Express middleware dengan optimasi untuk high traffic
// ✅ ANALISIS UKURAN DATA:
// - bot: ~50-100 bytes (sessionId)
// - recipient: ~15 bytes (phone number)
// - message: ~1-50KB (text message, bisa panjang untuk long text)
// - footer: ~100-200 bytes (optional)
// - url: ~100-200 bytes (optional)
// - button: ~50-100 bytes (optional)
// - skip_webhook: 1 byte (boolean)
// TOTAL: ~1-50KB untuk text biasa, ~100KB untuk extreme case
// ✅ OPTIMIZED: Limit 100KB (cukup untuk semua use case, prevent memory issue)
app.use(express.json({ 
    limit: '250kb', // ✅ OPTIMIZED: 100KB (cukup untuk text message + button, prevent memory issue)
    verify: (req, res, buf) => {
        // Harus selaras dengan limit express.json di atas (250kb)
        if (buf.length > 250 * 1024) {
            console.warn(`⚠️ [BODY-SIZE] Request body too large: ${(buf.length / 1024).toFixed(2)}KB from IP: ${getRealIP(req)}`);
            throw new Error('Request body too large (max 250KB)');
        }
    }
}));

/**
 * Processor/nginx kadang kirim ke huruf salah (/h→7003) padahal sess_*_7001_* di 7001.
 * Relay internal ke shard sesuai port di sessionId (localhost). Mati: BAILEYS_INTERNAL_SHARD_RELAY=0
 */
app.use(async (req, res, next) => {
    if (req.method !== 'POST' || !IS_SHARD_HTTP_PORT) return next();
    if (String(process.env.BAILEYS_INTERNAL_SHARD_RELAY || '').trim() === '0') return next();
    if (String(req.headers['x-baileys-shard-relay'] || '').trim() === '1') return next();
    const pth = typeof req.path === 'string' ? req.path : '';
    if (
        !pth.includes('/baileys/') &&
        !pth.includes('/send') &&
        !pth.includes('/logout') &&
        !pth.includes('/reconnect')
    ) {
        return next();
    }
    const body = req.body;
    if (!body || typeof body !== 'object') return next();
    const sid = body.bot ?? body.sessionId;
    if (typeof sid !== 'string' || !sid.startsWith('sess_')) return next();
    const m = sid.match(/^sess_[^_]+_(\d+)_/);
    if (!m) return next();
    const targetPort = normalizeToShardPort(parseInt(m[1], 10));
    const here = normalizeToShardPort(Number(PORT));
    if (targetPort === null || here === null || targetPort === here) return next();
    const urlPath = req.originalUrl || req.url || pth;
    const url = `http://127.0.0.1:${targetPort}${urlPath.startsWith('/') ? urlPath : '/' + urlPath}`;
    try {
        const r = await axios({
            method: 'POST',
            url,
            data: body,
            headers: {
                'Content-Type': 'application/json',
                'X-Baileys-Shard-Relay': '1',
                'X-Forwarded-For': getRealIP(req) || '',
                ...(req.headers['x-broadcast-key']
                    ? { 'X-Broadcast-Key': req.headers['x-broadcast-key'] }
                    : {})
            },
            timeout: Math.min(600000, Math.max(5000, Number(process.env.BAILEYS_SHARD_RELAY_TIMEOUT_MS) || 180000)),
            validateStatus: () => true
        });
        const ct = r.headers['content-type'] || '';
        if (typeof r.data === 'object' && r.data !== null && ct.includes('application/json')) {
            return res.status(r.status).json(r.data);
        }
        if (typeof r.data === 'string' || Buffer.isBuffer(r.data)) {
            return res.status(r.status).type(ct || 'text/plain').send(r.data);
        }
        return res.status(r.status).json(r.data);
    } catch (e) {
        return res.status(502).json({ error: '1', msg: `Shard relay failed: ${e.message || e}` });
    }
});

// ✅ Kurangi curl PHP 30s "0 bytes": tolak cepat JSON saat antrean global send slot sudah padat (RSS/heap tekan effectiveMax).
app.use((req, res, next) => {
    if (req.method !== 'POST' || typeof req.path !== 'string' || !req.path.includes('/send')) {
        return next();
    }
    const perf = config.performance || {};
    const minW = Number(perf.sendHttpBusyRejectMinWaiters);
    if (!Number.isFinite(minW) || minW <= 0) {
        return next();
    }
    try {
        const s = globalSendSlot.getStats();
        if (s.queuedWaiters >= minW) {
            return res.status(503).json({
                error: '1',
                msg: `Send pipeline busy (${s.queuedWaiters} queued, effectiveMax≈${s.effectiveMax}) — retry shortly`,
                busy: true,
                q: s.queuedWaiters,
                eff: s.effectiveMax
            });
        }
    } catch {
        /* ignore */
    }
    next();
});

app.use(express.static('public'));

// Initialize session manager
const sessionManager = new SessionManager();

// Store active Baileys wrappers per session
const activeWrappers = new Map();
// ✅ NEW: Lock mechanism untuk prevent race condition saat multiple requests ke session yang sama
const loadingSessions = new Map(); // sessionId → Promise<BaileysWrapper>
/** sessionId → { since: ms, fast: boolean } untuk TTL RAM (fast = WS closed / no sock) */
const deadSessionSince = new Map();

// ============================================================================
// 📋 Connected API allowlist — hanya load/evict sesuai api_connected_sessions (+ grace pair baru)
// ============================================================================
let connectedAllowlistSet = new Set();
/** true setelah minimal satu fetch HTTP sukses (bukan network error). */
let connectedAllowlistReady = false;
/** Waktu proses shard start — untuk grace period allowlist warmup setelah restart. */
const processStartTime = Date.now();
let connectedAllowlistRefreshPromise = null;
/** Throttle: hindari setiap getOrLoad menunggu axios API 20s (burst PHP). */
let connectedAllowlistLastSuccessAt = 0;

function isEnforceConnectedAllowlist() {
    const sw = config.sessionWarmup || {};
    return !!(sw.enabled && sw.apiUrl && sw.enforceConnectedAllowlist);
}

function getSessionCredsMtimeMs(sessionId) {
    try {
        const p = path.join(process.cwd(), 'sessions', sessionId, 'creds.json');
        if (!fs.existsSync(p)) return 0;
        return fs.statSync(p).mtimeMs;
    } catch {
        return 0;
    }
}

/** Folder name: sess_{user}_{port}_{Date.now()} — anggap “baru” jika timestamp folder masih dalam grace. */
function isNewSessionIdByFolderTimestamp(sessionId) {
    const grace = Number(config.sessionWarmup?.newSessionGraceMs) || 0;
    if (grace <= 0) return false;
    const m = typeof sessionId === 'string' ? sessionId.match(/^sess_\d+_\d+_(\d{10,})$/) : null;
    if (!m) return false;
    const created = parseInt(m[1], 10);
    if (!Number.isFinite(created)) return false;
    return Date.now() - created <= grace;
}

function isSessionNewPairingGrace(sessionId) {
    const grace = Number(config.sessionWarmup?.newSessionGraceMs) || 0;
    if (grace <= 0) return false;
    const mt = getSessionCredsMtimeMs(sessionId);
    if (mt > 0 && Date.now() - mt <= grace) return true;
    return isNewSessionIdByFolderTimestamp(sessionId);
}

function assertSessionAllowedToLoad(sessionId) {
    if (!isEnforceConnectedAllowlist()) return;
    if (isSessionNewPairingGrace(sessionId)) return;
    // ✅ FIX 2026-06-07: Grace period setelah shard restart — API warmup bisa lambat 30-120s
    const startupGraceMs = Number(config.sessionWarmup?.allowlistStartupGraceMs) || 120000;
    if (!connectedAllowlistReady && Date.now() - processStartTime < startupGraceMs) {
        return;
    }
    if (!connectedAllowlistReady) {
        throw new Error(`Connected allowlist not ready — refuse load: ${sessionId}`);
    }
    if (connectedAllowlistSet.has(sessionId)) return;
    throw new Error(`Session not in connected API allowlist: ${sessionId}`);
}

async function refreshConnectedAllowlistFromApi(force = false) {
    if (!isEnforceConnectedAllowlist()) return;
    const minGap = Number(config.sessionWarmup?.connectedAllowlistMinRefreshMs) || 25000;
    const now = Date.now();
    if (!force && connectedAllowlistReady && now - connectedAllowlistLastSuccessAt < minGap) {
        return;
    }
    if (connectedAllowlistRefreshPromise) return connectedAllowlistRefreshPromise;
    connectedAllowlistRefreshPromise = (async () => {
        try {
            const r = await fetchWarmupConnectedSessionIdsResult();
            if (!r.ok) {
                // ✅ FIX: Kalau sudah pernah sync ATAU ada session connected di RAM, jangan block forever
                if (connectedAllowlistReady || connectedAllowlistSet.size > 0) {
                    console.error(
                        `❌ [ALLOWLIST] API failed (${r.error || 'unknown'}), keeping ${connectedAllowlistSet.size} previous id(s)`
                    );
                    return;
                }
                // First fetch gagal tapi ada wrapper connected di RAM → pakai local allowlist sementara
                const localConnected = [];
                for (const [sid, w] of activeWrappers.entries()) {
                    if (w?.isConnected) localConnected.push(sid);
                }
                if (localConnected.length > 0) {
                    connectedAllowlistSet = new Set(localConnected);
                    connectedAllowlistReady = true;
                    connectedAllowlistLastSuccessAt = Date.now();
                    console.warn(
                        `⚠️ [ALLOWLIST] API failed on first fetch — using ${localConnected.length} locally-connected session(s) as temporary allowlist`
                    );
                    return;
                }
                console.error(`❌ [ALLOWLIST] API failed (${r.error || 'unknown'}), allowlist not ready yet`);
                return;
            }
            connectedAllowlistSet = new Set(r.ids);
            connectedAllowlistReady = true;
            connectedAllowlistLastSuccessAt = Date.now();
            console.log(`📋 [ALLOWLIST] synced ${connectedAllowlistSet.size} id(s) from connected API`);
        } finally {
            connectedAllowlistRefreshPromise = null;
        }
    })();
    return connectedAllowlistRefreshPromise;
}

function evictWrappersNotInConnectedAllowlist() {
    if (!isEnforceConnectedAllowlist() || !connectedAllowlistReady) return;
    for (const [sessionId, wrapper] of activeWrappers.entries()) {
        if (connectedAllowlistSet.has(sessionId)) continue;
        if (isSessionNewPairingGrace(sessionId)) continue;
        try {
            if (wrapper?.isPairingMode || wrapper?.isRestartingForPairing) continue;
            if (shouldSkipRamDeadTimer(wrapper)) continue;
        } catch {
            /* ignore */
        }
        ramEvictSession(sessionId, 'not in connected API allowlist');
    }
}

/** Pair/reconnect baru: tetap di RAM sampai API `connected` menyusul. */
function registerConnectedAllowlistLocal(sessionId) {
    if (!sessionId || !isEnforceConnectedAllowlist()) return;
    connectedAllowlistSet.add(sessionId);
    connectedAllowlistReady = true;
}

// ============================================================================
// 🚀 SUPER SMART: Shared Resources untuk 1 Port (PORT 7001) - RINGAN & EFISIEN
// ============================================================================
// ✅ OPTIMIZED: Shared Redis connection untuk semua session di 1 port (ringan, efisien)
// ✅ SMART: 1 Redis connection untuk semua session = hemat memory & connection
global.sharedRedis = null;
global.sharedRedisPromise = null;

/**
 * Ada identitas WA yang valid (sock.user atau creds.me) — sama seperti auth-poll di wrapper setelah restore.
 * Jangan hanya sock.user: setelah restore sering WS sudah OPEN dulu, user.id baru menyusul → /health jangan 0/N selamanya.
 */
function hasValidWaIdentity(wrapper) {
    try {
        if (typeof wrapper?.resolveAuthenticatedJid === 'function') {
            return !!wrapper.resolveAuthenticatedJid();
        }
        const u = wrapper?.sock?.user?.id;
        if (typeof u === 'string' && u.includes('@') && !u.includes('sess_')) {
            return true;
        }
        const creds = wrapper?.authState?.creds;
        const id = creds?.me?.id;
        if (!id || typeof id !== 'string' || !id.includes('@') || id.includes('sess_')) {
            return false;
        }
        if (creds.account || creds.me?.lid || creds.platform) return true;
        const name = creds.me?.name;
        return Boolean(name && name !== '~');
    } catch {
        /* ignore */
    }
    return false;
}

/**
 * Session “live” untuk RAM cleanup & /health: **WA WebSocket OPEN (1) + identitas valid**.
 * Flag `isAuthenticated` sengaja tidak dipakai di sini (bisa telat vs sock.user).
 */
function isHealthConnectedSession(wrapper) {
    try {
        if (!isBaileysWebSocketOpen(wrapper?.sock)) {
            return false;
        }
        return hasValidWaIdentity(wrapper);
    } catch {
        return false;
    }
}

function shouldSkipRamDeadTimer(wrapper) {
    try {
        if (wrapper.pendingOperations?.size > 0) return true;
        if (wrapper.reconnectInFlight) return true;
        if (wrapper.isPairingMode) return true;
        if (wrapper.isRestartingForPairing) return true;
        const rs = getBaileysWebSocketReadyState(wrapper.sock);
        if (rs === 0 || rs === 1) return true;
    } catch {
        /* ignore */
    }
    return false;
}

/**
 * WA WebSocket sudah CLOSED (3), bukan fase pairing/reconnect — evict RAM cepat.
 * Jangan pakai !sock di sini: saat initialize() sock bisa belum ada → jangan fast-evict.
 * Banned/logout terminal sudah ramEvict langsung di onDisconnected.
 */
function isRamFastEvictDisconnected(wrapper) {
    try {
        if (wrapper.terminalDisconnect === true) {
            return true;
        }
        if (wrapper.reconnectInFlight || wrapper.isPairingMode || wrapper.isRestartingForPairing) {
            return false;
        }
        if (!wrapper.sock?.ws) {
            return false;
        }
        return getBaileysWebSocketReadyState(wrapper.sock) === 3;
    } catch {
        return false;
    }
}

/** Bukan WA connected tapi masih “perlu” di RAM: QR/pairing, reconnect, WS connecting. */
function isSessionWarmingInRam(wrapper) {
    return !isHealthConnectedSession(wrapper) && shouldSkipRamDeadTimer(wrapper);
}

/** Normalisasi reason → statusCode WA (401 logout, 403 banned/forbidden, dll.) untuk ramEvict konsisten. */
function disconnectReasonToStatusCode(reason) {
    if (reason === undefined || reason === null) return NaN;
    if (typeof reason === 'number' && Number.isFinite(reason)) return reason;
    if (typeof reason === 'string') {
        const t = reason.trim();
        if (/^\d+$/.test(t)) return Number(t);
        const lower = t.toLowerCase();
        if (lower.includes('logged out') || lower.includes('logout') || lower.includes('intentional logout')) {
            return DisconnectReason.loggedOut;
        }
        if (lower.includes('forbidden') || lower.includes('blocked') || lower.includes('banned') || lower.includes('403')) {
            return DisconnectReason.forbidden;
        }
        if (lower.includes('bad session') || lower.includes('invalid session')) return DisconnectReason.badSession;
        return NaN;
    }
    if (typeof reason === 'object') {
        const boom = reason.output?.statusCode ?? reason.statusCode;
        if (typeof boom === 'number' && Number.isFinite(boom)) return boom;
        if (typeof boom === 'string' && /^\d+$/.test(boom.trim())) return Number(boom.trim());
    }
    const n = Number(reason);
    return Number.isFinite(n) ? n : NaN;
}

function isTerminalDisconnectReason(reason) {
    const code = disconnectReasonToStatusCode(reason);
    if (Number.isNaN(code)) return false;
    return (
        code === DisconnectReason.loggedOut ||
        code === DisconnectReason.forbidden ||
        code === DisconnectReason.badSession ||
        code === DisconnectReason.multideviceMismatch ||
        code === DisconnectReason.unavailableService ||
        code === DisconnectReason.connectionReplaced
    );
}

function ramEvictSession(sessionId, tag = '') {
    if (!sessionId) return false;
    const wrapper = activeWrappers.get(sessionId);
    const had = activeWrappers.has(sessionId);
    if (wrapper && typeof wrapper.stopAllBackgroundIntervals === 'function') {
        try {
            wrapper.stopAllBackgroundIntervals(tag || 'ram_evict');
        } catch (e) {
            console.warn(`⚠️ [RAM] stopAllBackgroundIntervals failed ${sessionId}: ${e?.message || e}`);
        }
    }
    activeWrappers.delete(sessionId);
    loadingSessions.delete(sessionId);
    deadSessionSince.delete(sessionId);
    try {
        connectedAllowlistSet.delete(sessionId);
    } catch {
        /* ignore */
    }
    if (had && tag) {
        console.log(`🧹 [RAM] Evicted ${sessionId} (${tag})`);
    }
    return had;
}

/** Hitung wrapper rumah vs asing di RAM — untuk health + trace isolasi shard. */
function getShardIsolationStats(sampleLimit = 8) {
    const here = normalizeToShardPort(Number(PORT)) ?? Number(PORT);
    let own = 0;
    let foreign = 0;
    const foreignSample = [];
    for (const sid of activeWrappers.keys()) {
        if (sessionIdBelongsToThisShard(sid)) {
            own++;
        } else {
            foreign++;
            if (foreignSample.length < sampleLimit) foreignSample.push(sid);
        }
    }
    return { port: here, own, foreign, foreignSample };
}

/**
 * Session milik port lain tidak boleh punya socket di worker ini (dual-socket → disconnect bergantian).
 * Tutup socket tanpa logout WA (creds di disk tetap; shard rumah yang load).
 */
function evictWrappersNotBelongingToThisShard() {
    if (!IS_SHARD_HTTP_PORT) return 0;
    let n = 0;
    for (const [sessionId, wrapper] of [...activeWrappers.entries()]) {
        if (sessionIdBelongsToThisShard(sessionId)) continue;
        try {
            if (wrapper && typeof wrapper.smartDisconnect === 'function') {
                Promise.race([
                    wrapper.smartDisconnect({ force: true, reason: 'wrong_shard' }),
                    new Promise((r) => setTimeout(r, 3000))
                ]).catch(() => {});
            }
        } catch {
            /* ignore */
        }
        if (ramEvictSession(sessionId, 'wrong shard for this worker')) n++;
    }
    if (n > 0) {
        console.warn(`⚠️ [SHARD-ISOLATION] evicted ${n} foreign wrapper(s) from port ${PORT}`);
    }
    return n;
}

/** Callback standar disconnect — terminal/banned langsung buang dari RAM. */
const pairPhoneDigits = (v) => String(v ?? '').replace(/\D+/g, '');
/**
 * Satu nomor = satu sesi pairing. Sesi pairing lama (belum auth) untuk nomor yang sama dimatikan dulu —
 * di shard ini lewat activeWrappers, di shard lain lewat kunci Redis fleet:pair:phone:<nomor> → "sessionId|port".
 * Tanpa ini dua sesi minta kode bergantian dan WhatsApp hanya mengakui permintaan terakhir ("Gagal menautkan perangkat").
 */
async function evictRivalPairingSessions(newSessionId, phoneNumber) {
    const digits = pairPhoneDigits(phoneNumber);
    if (digits.length < 8) return;
    for (const [sid, w] of activeWrappers.entries()) {
        if (sid === newSessionId || !w) continue;
        if (!w.isPairingMode || w.isAuthenticated || w.pairingAuthSucceededAt) continue;
        if (pairPhoneDigits(w.requestedPhoneNumber) !== digits) continue;
        console.log(`🧹 [PAIR] sesi pairing lama nomor sama dimatikan: ${sid} (baru: ${newSessionId})`);
        try { await w.logout(); } catch (e) { console.warn(`⚠️ [PAIR] logout ${sid}: ${e?.message || e}`); }
        activeWrappers.delete(sid);
    }
    const r = global.sharedRedis;
    if (!r?.isReady) return;
    const key = `fleet:pair:phone:${digits}`;
    try {
        const prev = await r.get(key);
        if (prev) {
            const [psid, pport] = String(prev).split('|');
            if (psid && psid !== newSessionId && Number(pport) && Number(pport) !== Number(PORT)) {
                console.log(`🧹 [PAIR] sesi pairing lama nomor sama di shard ${pport} dimatikan: ${psid}`);
                axios.post(`http://127.0.0.1:${pport}/baileys/logout`, { sessionId: psid }, { timeout: 4000 }).catch(() => {});
            }
        }
        await r.set(key, `${newSessionId}|${PORT}`, { EX: 1800 });
    } catch (e) {
        console.warn(`⚠️ [PAIR] redis rival: ${e?.message || e}`);
    }
}

function onWrapperDisconnected(sessionId, reason) {
    console.error(`❌ WhatsApp disconnected: ${sessionId}${reason != null ? `, reason: ${reason}` : ''}`);
    if (isTerminalDisconnectReason(reason)) {
        ramEvictSession(sessionId, 'terminal disconnect');
    }
}

/** Ban / logout terminal: evict RAM segera (jangan tunggu TTL cleanup). */
function onWrapperBannedImmediate(sessionId, reason) {
    const w = activeWrappers.get(sessionId);
    if (w) {
        w.terminalDisconnect = true;
        try {
            w.reconnectAttempts = w.maxReconnectAttempts || 5;
            w.workerStopped = true;
            if (typeof w.stopAllBackgroundIntervals === 'function') {
                w.stopAllBackgroundIntervals('banned immediate');
            }
        } catch (_) {
            /* ignore */
        }
    }
    ramEvictSession(sessionId, `banned fast (${reason})`);
}

/** RSS ≠ heap: OS tetap pegang halaman sampai V8/allocator lepas. Perlu --expose-gc di NODE_OPTIONS. */
let lastHeapPressureGcAt = 0;
/** Cooldown untuk MEMORY_RESTART (hindari loop restart) */
let lastMemoryRestartAt = 0;
/** Berapa tick berturut-turut RSS ≥ threshold (lihat rssRestartConsecutiveTicks) */
let rssRestartStreak = 0;
/** Dipakai RAM interval + graceful shutdown */
let isShuttingDown = false;
function runExposedGc(reason, passes = 1) {
    if (typeof global.gc !== 'function') {
        return false;
    }
    try {
        const n = Math.max(1, Math.min(5, passes));
        for (let i = 0; i < n; i++) {
            global.gc();
        }
        const m = process.memoryUsage();
        setImmediate(() =>
            console.log(
                `🧹 [GC] ${reason} → heap≈${Math.round(m.heapUsed / 1024 / 1024)}MB rss≈${Math.round(m.rss / 1024 / 1024)}MB`
            )
        );
        return true;
    } catch {
        return false;
    }
}

// ✅ RAM: buang wrapper mati — WS CLOSED / no sock → TTL pendek (permanentDisconnectRamTtlMs); sisanya TTL lama.
// ✅ Mode ringan + allowlist: buang cepat yang bukan connected API (kecuali session baru); logout/block tidak dibawa lama.
// ✅ Semua shard HTTP 7001–7007 (bukan hanya 7001) — tanpa ini RSS shard lain membengkak sampai cgroup/OOM.
if (IS_SHARD_HTTP_PORT) {
    const perf = config.performance || {};
    const ramIntervalMs = perf.ramCleanupIntervalMs ?? 15000;
    const baseRamTtlMs = perf.disconnectedSessionRamTtlMs ?? 90000;
    const baseRamFastTtlMs = Math.max(
        2000,
        Math.min(baseRamTtlMs, Number(perf.permanentDisconnectRamTtlMs) || 5000)
    );
    const ramGraceMs = perf.ramCleanupStartupGraceMs ?? 20000;

    ramCleanupInterval = setInterval(() => {
        if (process.uptime() * 1000 < ramGraceMs) return;
        evictWrappersNotBelongingToThisShard();

        const lightRam =
            perf.lightRamWithConnectedAllowlist !== false &&
            isEnforceConnectedAllowlist() &&
            connectedAllowlistReady;
        const capAllowSlow = Number(perf.disconnectedSessionRamTtlMsAllowlist) || 10000;
        const capAllowFast = Math.max(200, Number(perf.permanentDisconnectRamTtlMsAllowlist) || 1200);

        let cleanedCount = 0;
        const now = Date.now();
        for (const [sessionId, wrapper] of activeWrappers.entries()) {
            try {
                if (wrapper.terminalDisconnect === true) {
                    if (ramEvictSession(sessionId, 'banned/terminal instant')) {
                        cleanedCount++;
                    }
                    continue;
                }
                if (isHealthConnectedSession(wrapper)) {
                    deadSessionSince.delete(sessionId);
                    continue;
                }
                if (shouldSkipRamDeadTimer(wrapper)) {
                    deadSessionSince.delete(sessionId);
                    continue;
                }

                const inApi = connectedAllowlistSet.has(sessionId);
                const graceNew = isSessionNewPairingGrace(sessionId);

                if (lightRam && !inApi && !graceNew) {
                    if (ramEvictSession(sessionId, 'light: outside connected API (not new)')) {
                        cleanedCount++;
                    }
                    continue;
                }

                let effSlow = baseRamTtlMs;
                let effFast = baseRamFastTtlMs;
                if (lightRam && (inApi || graceNew)) {
                    effSlow = Math.min(baseRamTtlMs, capAllowSlow);
                    effFast = Math.min(baseRamFastTtlMs, capAllowFast);
                }

                const fast = isRamFastEvictDisconnected(wrapper);
                const ttl = fast ? effFast : effSlow;
                let entry = deadSessionSince.get(sessionId);
                if (!entry || entry.fast !== fast) {
                    deadSessionSince.set(sessionId, { since: now, fast });
                    continue;
                }
                if (now - entry.since >= ttl) {
                    if (ramEvictSession(sessionId, fast ? `ws dead ≥${Math.round(ttl / 1000)}s` : `not live ≥${Math.round(ttl / 1000)}s`)) {
                        cleanedCount++;
                    }
                }
            } catch {
                /* ignore */
            }
        }

        if (cleanedCount > 0) {
            console.log(`✅ [RAM-CLEANUP] Evicted ${cleanedCount} dead wrapper(s). Remaining: ${activeWrappers.size}`);
            // Hanya true eksplisit (config/env) — global.gc() sinkron dan bisa melambatkan API.
            if (perf.gcAfterRamEvict === true) {
                setImmediate(() =>
                    runExposedGc(`after RAM evict ×${cleanedCount}`, perf.gcPassesAfterEvict ?? 1)
                );
            }
        }

        const heapThr = perf.gcIfHeapUsedExceedsMb ?? 0;
        if (heapThr > 0) {
            const heapMb = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
            const cooldown = perf.gcHeapCooldownMs ?? 90000;
            if (heapMb > heapThr && Date.now() - lastHeapPressureGcAt >= cooldown) {
                lastHeapPressureGcAt = Date.now();
                setImmediate(() =>
                    runExposedGc(`heap ${heapMb}MB > ${heapThr}MB`, perf.gcPassesOnHeapPressure ?? 1)
                );
            }
        }

        // Opsional: restart saat RSS atau heap kritis (systemd Restart=always). RSS Node sering tidak balik ke OS walau GC.
        const rssRestartThr = perf.memoryRestartRssMb ?? 0;
        const heapRestartThr = perf.memoryRestartHeapMb ?? 0;
        const minUp = perf.memoryRestartMinUptimeSec ?? 180;
        const restartCool = perf.memoryRestartCooldownMs ?? 300000;
        const rssTicksNeed = Math.max(1, Number(perf.rssRestartConsecutiveTicks) || 3);
        const mu = process.memoryUsage();
        const rssMb = Math.round(mu.rss / 1024 / 1024);
        const heapMb = Math.round(mu.heapUsed / 1024 / 1024);
        if (rssRestartThr > 0) {
            if (rssMb >= rssRestartThr) {
                rssRestartStreak++;
            } else {
                rssRestartStreak = 0;
            }
        } else {
            rssRestartStreak = 0;
        }
        if (!isShuttingDown && process.uptime() >= minUp && Date.now() - lastMemoryRestartAt >= restartCool) {
            if (
                rssRestartThr > 0 &&
                rssRestartStreak >= rssTicksNeed &&
                rssMb >= rssRestartThr
            ) {
                rssRestartStreak = 0;
                lastMemoryRestartAt = Date.now();
                console.error(
                    `🔴 [MEMORY-RESTART] RSS ${rssMb}MB ≥ ${rssRestartThr}MB (${rssTicksNeed}x consecutive ticks) → graceful shutdown`
                );
                setImmediate(() => gracefulShutdown('MEMORY_RESTART'));
            } else if (heapRestartThr > 0 && heapMb >= heapRestartThr) {
                rssRestartStreak = 0;
                lastMemoryRestartAt = Date.now();
                console.error(
                    `🔴 [MEMORY-RESTART] heap ${heapMb}MB ≥ ${heapRestartThr}MB → graceful shutdown (RSS=${rssMb}MB)`
                );
                setImmediate(() => gracefulShutdown('MEMORY_RESTART_HEAP'));
            }
        }
    }, ramIntervalMs);

    /** Lonjakan heap bisa melewati ambang dalam <1 s saat Mark-Compact gagal — interval RAM (~12s) saja tidak cukup. */
    const heapWatchMs = Number(perf.memoryRestartHeapWatchdogMs) || 0;
    if (heapWatchMs > 0 && (perf.memoryRestartHeapMb ?? 0) > 0) {
        setInterval(() => {
            if (isShuttingDown) return;
            if (process.uptime() * 1000 < ramGraceMs) return;
            const heapThr = perf.memoryRestartHeapMb ?? 0;
            if (heapThr <= 0) return;
            const minUp = perf.memoryRestartMinUptimeSec ?? 180;
            const restartCool = perf.memoryRestartCooldownMs ?? 300000;
            if (process.uptime() < minUp) return;
            if (Date.now() - lastMemoryRestartAt < restartCool) return;
            const mu = process.memoryUsage();
            const heapMb = Math.round(mu.heapUsed / 1024 / 1024);
            const rssMb = Math.round(mu.rss / 1024 / 1024);
            if (heapMb >= heapThr) {
                rssRestartStreak = 0;
                lastMemoryRestartAt = Date.now();
                console.error(
                    `🔴 [MEMORY-RESTART:heap-watchdog] heap ${heapMb}MB ≥ ${heapThr}MB → graceful shutdown (RSS=${rssMb}MB)`
                );
                setImmediate(() => gracefulShutdown('MEMORY_RESTART_HEAP'));
            }
        }, heapWatchMs);
    }

    const perfLag = config.performance || {};
    const lagRestartMs = Number(perfLag.eventLoopLagRestartMs) || 0;
    if (lagRestartMs > 0) {
        const strikesNeed = Math.max(1, Number(perfLag.eventLoopLagRestartStrikes) || 3);
        const lagHist = monitorEventLoopDelay({ resolution: 20 });
        lagHist.enable();
        let lagStrikes = 0;
        setInterval(() => {
            if (isShuttingDown) return;
            const meanMs = lagHist.mean / 1e6;
            lagHist.reset();
            if (meanMs >= lagRestartMs) {
                lagStrikes++;
                console.warn(
                    `⚠️ [EVENT-LOOP-LAG] mean ${meanMs.toFixed(0)}ms ≥ ${lagRestartMs}ms (strike ${lagStrikes}/${strikesNeed})`
                );
                if (lagStrikes >= strikesNeed) {
                    lagStrikes = 0;
                    console.error(`🔴 [EVENT-LOOP-LAG] sustained → graceful shutdown`);
                    setImmediate(() => gracefulShutdown('EVENT_LOOP_LAG'));
                }
            } else {
                lagStrikes = 0;
            }
        }, 12000);
    }

    const allowSyncMs = Number(config.sessionWarmup?.connectedAllowlistSyncMs) || 0;
    if (allowSyncMs > 0 && isEnforceConnectedAllowlist()) {
        setInterval(() => {
            if (isShuttingDown) return;
            refreshConnectedAllowlistFromApi(true)
                .then(() => evictWrappersNotInConnectedAllowlist())
                .catch(() => {});
        }, allowSyncMs);
    }
}

// ✅ SMART: Memory optimization - limit active sessions di 1 port (prevent overload)
const MAX_SESSIONS_PER_PORT = 500; // Max 500 sessions per port (prevent memory overload)

// ============================================================================
// ✅ SIMPLIFIED: Single Port (7001) - Powerful & Efficient
// ============================================================================
const MAIN_PORT = 7001; // Single powerful port
/** Sinkron dengan folder sess_* di disk untuk port ini (bukan ++/-- runtime — itu bikin angka salah vs disk). */
let portUsageCount = 0;

/** Cache readdir `sessions/` — health sering di-poll tiap detik. */
let sessionFolderDiskCache = { n: -1, at: 0 };
const SESSION_FOLDER_DISK_CACHE_MS = 15000;

/** Hanya folder nama `sess_<userId>_<port>_<ts>` dengan port=7001; sess_* lain (format lawas) tidak dihitung. */
function countSessionFoldersForMainPort() {
    let sessionCount = 0;
    try {
        const sessionsDir = path.join(process.cwd(), 'sessions');
        if (fs.existsSync(sessionsDir)) {
            const sessionFolders = fs.readdirSync(sessionsDir).filter((f) => {
                const fullPath = path.join(sessionsDir, f);
                try {
                    return fs.statSync(fullPath).isDirectory() && f.startsWith('sess_');
                } catch {
                    return false;
                }
            });
            for (const sessionId of sessionFolders) {
                const portMatch = sessionId.match(/sess_\d+_(\d+)_/);
                if (portMatch) {
                    const port = parseInt(portMatch[1], 10);
                    if (port === MAIN_PORT) {
                        sessionCount++;
                    }
                }
            }
        }
    } catch (error) {
        console.error('Error counting sessions folder:', error.message);
    }
    return sessionCount;
}

function invalidateSessionFolderDiskCache() {
    sessionFolderDiskCache = { n: -1, at: 0 };
}

/** Jumlah folder session (port 7001) di disk; singkronkan portUsageCount saat miss cache. */
function getSessionFoldersOnDiskCached() {
    if (PORT != 7001 && PORT != '7001') {
        return null;
    }
    const now = Date.now();
    if (
        sessionFolderDiskCache.n >= 0 &&
        now - sessionFolderDiskCache.at < SESSION_FOLDER_DISK_CACHE_MS
    ) {
        return sessionFolderDiskCache.n;
    }
    const n = countSessionFoldersForMainPort();
    sessionFolderDiskCache = { n, at: now };
    portUsageCount = n;
    return n;
}

function syncPortCounter(quiet = false) {
    if (!quiet) {
        console.log('🔄 Syncing port counter with existing sessions...');
    }
    portUsageCount = countSessionFoldersForMainPort();
    sessionFolderDiskCache = { n: portUsageCount, at: Date.now() };
    if (!quiet && portUsageCount > 0) {
        console.log(`📊 Port ${MAIN_PORT}: ${portUsageCount} session folders on disk`);
    }
    if (!quiet) {
        console.log('✅ Port counter synced');
    }
}

function getOptimalPort() {
    return MAIN_PORT;
}

// ============================================================================
// BROADCAST ENDPOINT - Untuk menerima broadcast dari webhook atau port lain
// ============================================================================
app.post('/broadcast', (req, res) => {
    try {
        const apiKey = req.headers['x-broadcast-key'];
        const expectedKey = config.webhook.apiKey;
        
        // Validate API key
        if (apiKey !== expectedKey) {
            return res.status(401).json({ error: "1", msg: "Unauthorized" });
        }
        
        const body = req.body || {};
        const { event, userId } = body;

        if (String(process.env.BROADCAST_VERBOSE || '').trim() === '1') {
            console.log(`📡 Broadcast received: ${event} for user: ${userId}`);
        }

        if (!userId) {
            return res.status(400).json({ error: '1', msg: 'userId required' });
        }

        broadcastToUser(userId, {
            ...body,
            event,
            userId,
            timestamp: body.timestamp || Date.now(),
        });
        
        res.json({ 
            error: "0", 
            msg: "Broadcast successful"
        });
        
    } catch (error) {
        console.error('Broadcast error:', error);
        res.status(500).json({ error: "1", msg: error.message });
    }
});
app.post('/baileys/broadcast', (req, res) => {
    try {
        const apiKey = req.headers['x-broadcast-key'];
        const expectedKey = config.webhook.apiKey;
        
        // Validate API key
        if (apiKey !== expectedKey) {
            return res.status(401).json({ error: "1", msg: "Unauthorized" });
        }
        
        const body = req.body || {};
        const { event, userId } = body;

        if (String(process.env.BROADCAST_VERBOSE || '').trim() === '1') {
            console.log(`📡 Broadcast received: ${event} for user: ${userId}`);
        }

        if (!userId) {
            return res.status(400).json({ error: '1', msg: 'userId required' });
        }

        broadcastToUser(userId, {
            ...body,
            event,
            userId,
            timestamp: body.timestamp || Date.now(),
        });
        
        res.json({ 
            error: "0", 
            msg: "Broadcast successful"
        });
        
    } catch (error) {
        console.error('Broadcast error:', error);
        res.status(500).json({ error: "1", msg: error.message });
    }
});
app.post('/api/broadcast', (req, res) => {
    try {
        const apiKey = req.headers['x-broadcast-key'];
        const expectedKey = config.webhook.apiKey;
        
        // Validate API key
        if (apiKey !== expectedKey) {
            return res.status(401).json({ error: "1", msg: "Unauthorized" });
        }
        
        const body = req.body || {};
        const { event, userId } = body;

        if (String(process.env.BROADCAST_VERBOSE || '').trim() === '1') {
            console.log(`📡 Broadcast received: ${event} for user: ${userId}`);
        }

        if (!userId) {
            return res.status(400).json({ error: '1', msg: 'userId required' });
        }

        broadcastToUser(userId, {
            ...body,
            event,
            userId,
            timestamp: body.timestamp || Date.now(),
        });
        
        res.json({ 
            error: "0", 
            msg: "Broadcast successful"
        });
        
    } catch (error) {
        console.error('Broadcast error:', error);
        res.status(500).json({ error: "1", msg: error.message });
    }
});

// Internal broadcast endpoint (dari port lain)
app.post('/broadcast-internal', (req, res) => {
    const { userId, sessionId, data } = req.body;
    
    if (userId) {
        broadcastToUser(userId, data);
    } else if (sessionId) {
        broadcastToSession(sessionId, data);
    }
    
    res.json({ error: "0", msg: "Broadcast sent" });
});

// ============================================================================
// API ENDPOINTS
// ============================================================================

// Root — jangan expose katalog API ke publik
app.get('/', (req, res) => {
    res.status(404).type('text/plain').send('Not Found');
});

// Health check — memory dalam MB; sessions + redis untuk jq monitoring
// Catatan nginx shard: rewrite /e/baileys/health → upstream GET /baileys/health (bukan /health)
async function handleHealth(req, res) {
    try {
        /** Watchdog/cron: jangan panggil lLen + scan ribuan wrapper (bisa >2s → false restart loop). */
        const lightHealth =
            req.query.light === '1' ||
            req.query.light === 'true' ||
            req.query.ping === '1';
        if (lightHealth) {
            const memUsage = process.memoryUsage();
            const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
            const rssMB = Math.round(memUsage.rss / 1024 / 1024);
            const diskFolders =
                PORT === 7001 || PORT === '7001' ? getSessionFoldersOnDiskCached() : null;
            return res.status(200).json({
                status: 'healthy',
                light: true,
                timestamp: new Date().toISOString(),
                uptime: process.uptime(),
                memory: { heapUsed: heapUsedMB, rss: rssMB },
                redisConnected: !!(global.sharedRedis?.isReady),
                activeWrappers: activeWrappers.size,
                shardIsolation: getShardIsolationStats(4),
                /** Folder `sess_*` di disk untuk port 7001 (bukan counter ++/-- lawas) */
                sessionFoldersOnDisk: diskFolders,
                /** Alias ke sessionFoldersOnDisk — kompatibel script lama */
                portUsageCount: diskFolders
            });
        }

        const memUsage = process.memoryUsage();
        const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
        const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);
        const rssMB = Math.round(memUsage.rss / 1024 / 1024);
        const diskFoldersFull =
            PORT === 7001 || PORT === '7001' ? getSessionFoldersOnDiskCached() : null;

        // Shared Redis di-connect saat boot (ensureSharedRedis). Kalau di sini masih belum
        // ready berarti memang ada gangguan Redis — bangun ulang di latar supaya shard pulih
        // sendiri tanpa perlu ditembak restart oleh watchdog.
        if (!global.sharedRedis?.isReady) {
            ensureSharedRedis().catch(() => {});
        }

        const health = {
            status: 'healthy',
            timestamp: new Date().toISOString(),
            uptime: process.uptime(),
            /** Wrapper Baileys yang sedang di RAM (sama artinya dengan sessions.total setelah diisi) */
            activeSessions: activeWrappers.size,
            /** Folder session di disk (sess_* port 7001); cache ~15s — bisa >> RAM jika allowlist/evict */
            sessionFoldersOnDisk: diskFoldersFull,
            // Koneksi WS ke server ini (UI QR/pairing per user) — BUKAN jumlah socket WhatsApp Baileys per bot
            totalWsConnections:
                PORT == 7001
                    ? Array.from(sessionWebSockets.values()).reduce((sum, clients) => sum + clients.size, 0)
                    : 0,
            dashboardWebSocketClients:
                PORT == 7001
                    ? Array.from(sessionWebSockets.values()).reduce((sum, clients) => sum + clients.size, 0)
                    : 0,
            memory: {
                heapUsed: heapUsedMB,
                heapTotal: heapTotalMB,
                rss: rssMB,
                external: Math.round(memUsage.external / 1024 / 1024)
            },
            port: PORT,
            wsPort: PORT == 7001 ? WS_PORT : 'N/A (using 8001 via 7001)',
            portDistribution:
                diskFoldersFull != null ? { [MAIN_PORT]: diskFoldersFull } : null,
            redis: {
                connected: global.sharedRedis?.isReady || false,
                queueSize: 0,
                processingSize: 0
            },
            sendSlot: globalSendSlot.getStats(),
            shardIsolation: getShardIsolationStats(8),
            warnings: []
        };
        if (health.shardIsolation.foreign > 0) {
            health.warnings.push(
                `Foreign-port wrappers in RAM: ${health.shardIsolation.foreign} (should be 0)`
            );
            if (health.status === 'healthy') health.status = 'warning';
        }

        if (global.sharedRedis?.isReady) {
            try {
                health.redis.queueSize = (await global.sharedRedis.lLen('webhook:outbox')) || 0;
                health.redis.processingSize = (await global.sharedRedis.lLen('webhook:processing')) || 0;
            } catch (e) {
                health.redis.error = e.message;
                health.warnings.push('Redis queue check failed');
            }
        } else {
            // Redis lazy-init: baru connect saat ada session. Jangan alarm di shard idle.
            const sessNeed =
                Number(health.sessions?.total || 0) ||
                Number(health.activeSessions || 0) ||
                0;
            if (sessNeed > 0) {
                health.warnings.push('Redis not connected');
                if (health.status === 'healthy') health.status = 'warning';
            }
        }

        if (heapUsedMB > 4000) {
            health.status = 'warning';
            health.warnings.push(`High memory usage: ${heapUsedMB}MB`);
        }

        if (health.redis.queueSize > 1000) {
            health.status = health.status === 'warning' ? 'warning' : 'degraded';
            health.warnings.push(`Large webhook queue: ${health.redis.queueSize} items`);
        }

        if (health.redis.processingSize > 500) {
            health.status = health.status === 'warning' ? 'warning' : 'degraded';
            health.warnings.push(`Many items processing: ${health.redis.processingSize} items`);
        }

        let connectedCount = 0;
        let warmingCount = 0;
        for (const [, wrapper] of activeWrappers.entries()) {
            if (isHealthConnectedSession(wrapper)) {
                connectedCount++;
            } else if (isSessionWarmingInRam(wrapper)) {
                warmingCount++;
            }
        }
        const wrappersInRam = activeWrappers.size;
        const staleInRam = Math.max(0, wrappersInRam - connectedCount - warmingCount);
        health.sessions = {
            /** Total wrapper Baileys di memori (termasuk yang akan di-evict TTL) */
            total: wrappersInRam,
            /** WA WebSocket OPEN + user — yang benar-benar bisa kirim */
            connected: connectedCount,
            /** QR/pairing, reconnect, atau WS masih CONNECTING — jangan evict */
            warming: warmingCount,
            /** Mati di RAM menunggu TTL / fast-evict (WS closed, bukan pairing/reconnect) */
            staleInRam,
            /** Yang “penting” untuk kapasitas: connected + proses baru */
            liveOrConnecting: connectedCount + warmingCount,
            /** Belum WA-connected: warming + stale (sama seperti total - connected) */
            disconnected: warmingCount + staleInRam
        };

        const ss = health.sendSlot;
        const effCap = Math.max(1, ss?.effectiveMax ?? ss?.max ?? 1);
        const qCap = ss?.maxQueuedWaiters ?? 0;
        const perfCap = config.performance || {};
        health.capacity = {
            sendSlotMax: ss?.max ?? 0,
            sendSlotEffectiveMax: ss?.effectiveMax ?? ss?.max ?? 0,
            sendHeapPressure: ss?.heapPressure === true,
            sendRssPressure: ss?.rssPressure === true,
            heapUsedMb: ss?.heapUsedMb ?? 0,
            rssMb: ss?.rssMb ?? 0,
            sendInflight: ss?.inFlight ?? 0,
            sendQueued: ss?.queuedWaiters ?? 0,
            sendQueueCap: qCap,
            sendSlotBusyRatio: Math.min(1, (ss?.inFlight ?? 0) / effCap),
            webhookOutboxLen: health.redis.queueSize,
            resourceTier: perfCap.autoResourceTier,
            autoTotalMemMb: perfCap.autoTotalMemMb,
            autoCpuCount: perfCap.autoCpuCount,
            httpMaxConcurrentSendsConfigured: perfCap.httpMaxConcurrentSends
        };
        if (ss?.heapPressure === true) {
            health.warnings.push(
                `Send slot heap throttle: effectiveMax=${ss.effectiveMax} concurrent (SEND_SLOT_HEAP_SOFT_MB)`
            );
            if (health.status === 'healthy') health.status = 'warning';
        }
        if (ss?.rssPressure === true) {
            health.warnings.push(
                `Send slot RSS throttle: effectiveMax=${ss.effectiveMax} concurrent (SEND_SLOT_RSS_SOFT_MB; RSS rarely drops without process restart)`
            );
            if (health.status === 'healthy') health.status = 'warning';
        }
        if (qCap > 0 && (ss?.queuedWaiters ?? 0) > Math.floor(qCap * 0.85)) {
            health.warnings.push(
                `Send slot queue near cap: ${ss.queuedWaiters}/${qCap} (SEND_SLOT_MAX_QUEUED_WAITERS)`
            );
            if (health.status === 'healthy') health.status = 'warning';
        }

        const statusCode =
            health.status === 'healthy' ? 200 : health.status === 'warning' ? 200 : 503;

        res.status(statusCode).json(health);
    } catch (error) {
        res.status(503).json({
            status: 'error',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
}

app.get('/health', handleHealth);
app.get('/baileys/health', handleHealth);

// Stats
function handleStats(req, res) {
    let connected = 0;
    let warming = 0;
    for (const [, w] of activeWrappers.entries()) {
        if (isHealthConnectedSession(w)) connected++;
        else if (isSessionWarmingInRam(w)) warming++;
    }
    const totalWrappers = activeWrappers.size;
    const stats = {
        activeSessions: totalWrappers,
        sessionFoldersOnDisk:
            PORT === 7001 || PORT === '7001' ? getSessionFoldersOnDiskCached() : null,
        sessions: {
            totalInRam: totalWrappers,
            connected,
            warming,
            staleInRam: Math.max(0, totalWrappers - connected - warming),
            liveOrConnecting: connected + warming
        },
        totalWsConnections: PORT == 7001 ? Array.from(sessionWebSockets.values()).reduce((sum, clients) => sum + clients.size, 0) : 0,
        dashboardWebSocketClients:
            PORT == 7001 ? Array.from(sessionWebSockets.values()).reduce((sum, clients) => sum + clients.size, 0) : 0,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        timestamp: new Date().toISOString(),
        port: PORT
    };
    
    res.json({ error: "0", msg: stats });
}

app.get('/stats', handleStats);
app.get('/baileys/stats', handleStats);

/** E.1 — drain mode: tolak pair/send baru sebelum rolling restart shard. */
function handleShardDrain(req, res) {
    global.shardDrainMode = true;
    global.shardDrainSince = Date.now();
    const waitSec = Math.max(5, Math.min(120, Number(req.body?.wait_sec ?? req.query?.wait_sec) || 30));
    res.json({
        error: '0',
        msg: { draining: true, port: PORT, since: global.shardDrainSince, suggest_wait_sec: waitSec },
    });
}

function handleShardUndrain(req, res) {
    global.shardDrainMode = false;
    global.shardDrainSince = 0;
    res.json({ error: '0', msg: { draining: false, port: PORT } });
}

app.post('/drain', handleShardDrain);
app.post('/baileys/drain', handleShardDrain);
app.post('/undrain', handleShardUndrain);
app.post('/baileys/undrain', handleShardUndrain);
app.get('/drain-status', (req, res) => {
    res.json({
        error: '0',
        msg: { draining: isShardDraining(), since: global.shardDrainSince || null, port: PORT },
    });
});
app.get('/baileys/drain-status', (req, res) => {
    res.json({
        error: '0',
        msg: { draining: isShardDraining(), since: global.shardDrainSince || null, port: PORT },
    });
});

/** Flush pending SERVER_ACK via enqueueWebhook (session ack-live only). */
async function handleFlushPendingServerAck(req, res) {
    try {
        const maxPerSession = Math.max(1, Math.min(100, Number(req.body?.max_per_session ?? req.query?.max_per_session) || 30));
        let totalFlushed = 0;
        let sessions = 0;
        let skippedLive = 0;
        for (const [, wrapper] of activeWrappers.entries()) {
            if (!wrapper?.flushPendingServerAckViaWebhook) continue;
            sessions++;
            const r = await wrapper.flushPendingServerAckViaWebhook(maxPerSession).catch(() => ({ flushed: 0, skipped: 'error' }));
            if (r?.skipped === 'session_not_live') {
                skippedLive++;
                continue;
            }
            totalFlushed += Number(r?.flushed || 0);
        }
        res.json({
            error: '0',
            msg: {
                port: PORT,
                sessions_scanned: sessions,
                skipped_not_live: skippedLive,
                flushed: totalFlushed,
            },
        });
    } catch (e) {
        res.status(500).json({ error: '1', msg: e?.message || String(e) });
    }
}
app.post('/flush-pending-server-ack', handleFlushPendingServerAck);
app.post('/baileys/flush-pending-server-ack', handleFlushPendingServerAck);

function getWrapperSendReadiness(wrapper) {
    const real = wrapper?.getRealDeviceStatus?.() || {};
    const wsState = getBaileysWebSocketReadyState(wrapper?.sock);
    const realJid = typeof wrapper?.resolveAuthenticatedJid === 'function'
        ? wrapper.resolveAuthenticatedJid()
        : null;
    const hasIdentity = !!realJid;
    const socketOpen = isBaileysWebSocketOpen(wrapper?.sock);
    const pairingWaitScan = wrapper?.isPairingMode === true && !hasIdentity;
    const ready = socketOpen && wrapper?.isAuthenticated === true && hasIdentity && !pairingWaitScan;
    const reconnectAttempts = Number(wrapper?.reconnectAttempts ?? 0);
    const maxReconnectAttempts = Number(wrapper?.maxReconnectAttempts ?? 0);
    const terminalDisconnect = wrapper?.terminalDisconnect === true;

    let notReadyReason = null;
    if (!ready) {
        if (terminalDisconnect) notReadyReason = 'terminal_disconnect';
        else if (!wrapper?.sock) notReadyReason = 'no_socket';
        else if (!socketOpen) notReadyReason = `ws_not_open(wsState=${wsState})`;
        else if (pairingWaitScan || !hasIdentity) notReadyReason = pairingWaitScan ? 'pairing_wait_scan' : 'no_wa_identity';
        else if (wrapper?.isAuthenticated !== true) notReadyReason = 'not_authenticated';
        else notReadyReason = 'unknown';
    }

    const derivedConnectionState = (() => {
        if (ready) return 'open';
        if (terminalDisconnect) return 'connection closed';
        if (wrapper?.reconnectInFlight === true) return 'reconnecting';
        if (!socketOpen && maxReconnectAttempts > 0 && reconnectAttempts < maxReconnectAttempts) return 'reconnecting';
        if (!socketOpen && reconnectAttempts > 0 && maxReconnectAttempts > 0 && reconnectAttempts < maxReconnectAttempts) {
            return 'reconnecting';
        }
        return real.connectionState || 'unknown';
    })();

    return {
        ready,
        notReadyReason,
        reason: notReadyReason || (ready ? 'ready' : derivedConnectionState),
        socketOpen,
        wsState,
        hasIdentity,
        isAuthenticated: wrapper?.isAuthenticated === true,
        reconnectInFlight: wrapper?.reconnectInFlight === true,
        reconnectAttempts,
        maxReconnectAttempts,
        terminalDisconnect,
        connectionState: derivedConnectionState,
        // 2026-09-07: umur event terakhir + penanda tuli, supaya BE/laporan bisa membedakan socket OPEN
        // yang masih menerima event dari socket OPEN yang sudah bisu (sesi contoh: DEAF 15 menit
        // sebelum processor mulai blast, lalu bot dibunuh sebagai zombie). Murni tambahan field.
        lastEventAgeSec: (() => {
            const lastEvt = Math.max(Number(wrapper?.lastMessagesUpdateAt || 0), Number(wrapper?.lastEventAt || 0));
            return lastEvt > 0 ? Math.floor((Date.now() - lastEvt) / 1000) : null;
        })(),
        connectedAgeSec: wrapper?.connectedAt ? Math.floor((Date.now() - Number(wrapper.connectedAt)) / 1000) : null,
        reachoutTimeLock: wrapper?.reachoutTimeLock || null,
        messageCapping: wrapper?.messageCapping || null,
        deafStale: (() => {
            const staleMs = Math.max(60000, Math.min(600000, Number(process.env.DEAF_SESSION_STALE_MS) || 90000));
            const lastEvt = Math.max(Number(wrapper?.lastMessagesUpdateAt || 0), Number(wrapper?.lastEventAt || 0));
            const connectedAt = Number(wrapper?.connectedAt || 0);
            return ready && connectedAt > 0 && lastEvt > 0 && (Date.now() - connectedAt) > staleMs && (Date.now() - lastEvt) > staleMs;
        })(),
        details: real.details || {}
    };
}

async function handleBaileysSessionStatus(req, res) {
    const sessionId = String(req.query.sessionId || req.query.session || req.query.bot || '').trim();
    const shouldLoad = ['1', 'true', 'yes'].includes(String(req.query.load || '').toLowerCase());
    const waitMs = Math.max(0, Math.min(5000, Number(req.query.waitMs) || 0));

    if (!sessionId) {
        return res.json({ error: '1', msg: 'sessionId is required' });
    }

    const homePort = sessionIdHomePort(sessionId);
    if (IS_SHARD_HTTP_PORT && homePort != null && homePort !== normalizeToShardPort(Number(PORT))) {
        return res.json({
            error: '1',
            msg: `Wrong shard: ${sessionId} is for port ${homePort}; this worker is ${PORT}`,
            sessionId,
            homePort,
            ready: false,
            trace: 'session-status:home_port_mismatch'
        });
    }

    try {
        assertSessionIdBelongsToThisShard(sessionId);
        let wrapper = activeWrappers.get(sessionId);
        if (!wrapper && shouldLoad) {
            wrapper = await getOrLoadSession(sessionId);
        }

        const started = Date.now();
        let status = wrapper
            ? getWrapperSendReadiness(wrapper)
            : { ready: false, notReadyReason: 'wrapper_not_loaded', reason: 'wrapper_not_loaded' };
        while (!status.ready && waitMs > 0 && Date.now() - started < waitMs) {
            await new Promise((resolve) => setTimeout(resolve, 200));
            wrapper = activeWrappers.get(sessionId) || wrapper;
            status = wrapper
                ? getWrapperSendReadiness(wrapper)
                : { ready: false, notReadyReason: 'wrapper_not_loaded', reason: 'wrapper_not_loaded' };
        }

        return res.json({
            error: '0',
            msg: status.ready ? 'ready' : 'not_ready',
            sessionId,
            homePort,
            active: !!wrapper,
            waitedMs: Math.min(waitMs, Date.now() - started),
            ...status
        });
    } catch (error) {
        const msg = error?.message || String(error);
        return res.json({
            error: '1',
            msg,
            sessionId,
            homePort,
            ready: false,
            trace: 'session-status:getOrLoadSession_or_assert'
        });
    }
}

app.get('/baileys/session-status', handleBaileysSessionStatus);
app.get('/session-status', handleBaileysSessionStatus);

// Port Distribution Stats (simplified for single port)
app.get('/port-distribution', (req, res) => {
    const n =
        PORT === 7001 || PORT === '7001' ? getSessionFoldersOnDiskCached() : portUsageCount;
    res.json({
        error: "0",
        msg: "Port distribution stats",
        totalSessions: n,
        distribution: { [MAIN_PORT]: n },
        timestamp: new Date().toISOString()
    });
});

// Sync Port Counter
app.post('/sync-port-counter', (req, res) => {
    try {
        syncPortCounter(false);
        res.json({
            error: "0",
            msg: "Port counter synced successfully",
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.json({
            error: "1",
            msg: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// Pair endpoint
app.post('/pair', async (req, res) => {
    try {
        if (shardDrainReject(res)) return;
        const { phoneNumber, userId, botId, port } = req.body;
        
        if (!userId) {
            return res.json({ error: "1", msg: "userId is required" });
        }
        
        logPairPortFallbackIfNeeded(req, port);
        const actualPort = resolvePairServicePort(req, port);
        const sessionId = `sess_${userId}_${actualPort}_${Date.now()}`;
        
        console.log(`🔄 Creating session: ${sessionId}`);
        console.log(`   User: ${userId}, Phone: ${phoneNumber || 'QR Method'}, Port: ${actualPort} (shard PORT=${PORT})`);
        const isPairingMode = (() => {
            if (typeof phoneNumber === 'string') {
                return phoneNumber.trim().length > 0;
            }
            if (typeof phoneNumber === 'number') {
                return String(phoneNumber).trim().length > 0;
            }
            return false;
        })();
        
        // ✅ Create session directory first
        const sessionDir = path.join(process.cwd(), 'sessions', sessionId);
        if (!fs.existsSync(sessionDir)) {
            fs.mkdirSync(sessionDir, { recursive: true });
            console.log(`📁 Created session directory: ${sessionDir}`);
        }
        
        // Create new session
        await sessionManager.createSession(sessionId, phoneNumber);
        
        // Create wrapper
        const wrapper = new BaileysWrapper(sessionId, sessionManager, {
            onQR: async (qr) => {
                if (isPairingMode) {
                    console.warn(`⚠️ QR generated for ${sessionId} but pairing mode active - skipping QR webhook`);
                    return;
                }
                console.log(`📱 QR generated for ${sessionId}`);
                
                // Send to webhook with auth
                const payload = {
                    event: 'QR_CODE',
                    sessionId: sessionId,
                    bot: sessionId,
                    qr: qr,
                    user_id: userId,
                    port: actualPort
                };
                dispatchWebhook(payload, sessionId, 'QR_CODE');
            },
            
            onPairingCode: async (data) => {
                const {
                    code,
                    phoneNumber: normalizedPhone,
                    rawPhoneNumber,
                    appliedDefaultCountryCode,
                    defaultCountryCode
                } = data || {};

                console.log(`🔑 Pairing code generated for ${sessionId}: ${code}`);
                if (rawPhoneNumber && normalizedPhone && rawPhoneNumber !== normalizedPhone) {
                    console.log(`   Raw phone: ${rawPhoneNumber} -> Normalized: ${normalizedPhone}`);
                    if (appliedDefaultCountryCode) {
                        console.log(`   Applied default country code: ${defaultCountryCode}`);
                    }
                }
                
                const payload = {
                    event: 'PAIRING_CODE',
                    sessionId: sessionId,
                    bot: sessionId,
                    code,
                    phoneNumber: normalizedPhone || phoneNumber,
                    rawPhoneNumber: rawPhoneNumber || phoneNumber,
                    appliedDefaultCountryCode: appliedDefaultCountryCode || false,
                    defaultCountryCode: defaultCountryCode || null,
                    user_id: userId,
                    port: actualPort
                };
                dispatchWebhook(payload, sessionId, 'PAIRING_CODE');
            },
            
            onMessageTimer: async (payload) => {
                console.log(`⏱️ Message timer detected for ${sessionId}`);
                dispatchWebhook(payload, sessionId, 'MESSAGE_TIMER');
            },
            
            onConnected: async () => {
                console.log(`✅ WhatsApp connected: ${sessionId} (CONNECTED webhook handled by wrapper.js)`);
            },
            
            onDisconnected: async (reason) => onWrapperDisconnected(sessionId, reason),
            onBannedImmediate: async (reason) => onWrapperBannedImmediate(sessionId, reason),
        });
        
        // ✅ CRITICAL: Set userId dan port ke wrapper (diperlukan untuk webhook payload)
        wrapper.userId = userId;
        wrapper.port = actualPort;
        
        activeWrappers.set(sessionId, wrapper);
        // ✅ FIXED: Wrap initialize dengan try-catch untuk prevent crash
        try {
        await wrapper.initialize(phoneNumber);
        } catch (initError) {
            console.error(`❌ [INIT] Error initializing session ${sessionId}:`, initError.message);
            ramEvictSession(sessionId, 'init failed');
            return res.json({ error: "1", msg: `Failed to initialize: ${initError.message}` });
        }
        registerConnectedAllowlistLocal(sessionId);
        invalidateSessionFolderDiskCache();

        res.json({ 
            error: "0", 
            msg: sessionId,
            sessionId: sessionId,
            port: actualPort
        });
        
    } catch (error) {
        console.error('Pair error:', error);
        res.json({ error: "1", msg: error.message });
    }
});

// ============================================================================
// 📱 /baileys/pair - MIDDLEWARE COMPATIBILITY ENDPOINT
// ============================================================================
app.post('/baileys/pair', async (req, res) => {
    try {
        if (shardDrainReject(res)) return;
        const { phoneNumber, userId, botId, port } = req.body;
        
        if (!userId) {
            return res.json({ error: "1", msg: "userId is required" });
        }
        
        logPairPortFallbackIfNeeded(req, port);
        const actualPort = resolvePairServicePort(req, port);
        const sessionId = `sess_${userId}_${actualPort}_${Date.now()}`;
        
        console.log(`🔄 [BAILEYS/PAIR] Creating session: ${sessionId}`);
        console.log(`   User: ${userId}, Phone: ${phoneNumber || 'QR Method'}, Port: ${actualPort} (shard PORT=${PORT})`);
        const isPairingMode = (() => {
            if (typeof phoneNumber === 'string') {
                return phoneNumber.trim().length > 0;
            }
            if (typeof phoneNumber === 'number') {
                return String(phoneNumber).trim().length > 0;
            }
            return false;
        })();
        
        // Create session directory
        const sessionDir = path.join(process.cwd(), 'sessions', sessionId);
        if (!fs.existsSync(sessionDir)) {
            fs.mkdirSync(sessionDir, { recursive: true });
            console.log(`📁 Created session directory: ${sessionDir}`);
        }
        
        // Create new session
        await sessionManager.createSession(sessionId, phoneNumber);
        
        // Create wrapper
        const wrapper = new BaileysWrapper(sessionId, sessionManager, {
            onQR: async (qr) => {
                if (isPairingMode) {
                    console.warn(`⚠️ QR generated for ${sessionId} but pairing mode active - skipping QR webhook`);
                    return;
                }
                console.log(`📱 QR generated for ${sessionId}`);
                
                const qrPayload = {
                    event: 'QR_CODE',
                    sessionId: sessionId,
                    bot: sessionId,
                    qr: qr,
                    user_id: userId,
                    port: actualPort
                };
                dispatchWebhook(qrPayload, sessionId, 'QR_CODE');
            },
            
            onPairingCode: async (data) => {
                const {
                    code,
                    phoneNumber: normalizedPhone,
                    rawPhoneNumber,
                    appliedDefaultCountryCode,
                    defaultCountryCode
                } = data || {};

                console.log(`🔑 Pairing code: ${code} for ${sessionId}`);
                if (rawPhoneNumber && normalizedPhone && rawPhoneNumber !== normalizedPhone) {
                    console.log(`   Raw phone: ${rawPhoneNumber} -> Normalized: ${normalizedPhone}`);
                    if (appliedDefaultCountryCode) {
                        console.log(`   Applied default country code: ${defaultCountryCode}`);
                    }
                }
                
                const pairingPayload = {
                    event: 'PAIRING_CODE',
                    sessionId: sessionId,
                    bot: sessionId,
                    code,
                    phoneNumber: normalizedPhone || phoneNumber,
                    rawPhoneNumber: rawPhoneNumber || phoneNumber,
                    appliedDefaultCountryCode: appliedDefaultCountryCode || false,
                    defaultCountryCode: defaultCountryCode || null,
                    user_id: userId,
                    port: actualPort
                };
                dispatchWebhook(pairingPayload, sessionId, 'PAIRING_CODE');
            },
            
            onMessageTimer: async (payload) => {
                console.log(`⏱️ Message timer detected for ${sessionId}`);
                dispatchWebhook(payload, sessionId, 'MESSAGE_TIMER');
            },
            
            onConnected: async () => {
                console.log(`✅ WhatsApp connected: ${sessionId} (CONNECTED webhook handled by wrapper.js)`);
            },
            
            onDisconnected: async (reason) => onWrapperDisconnected(sessionId, reason),
            onBannedImmediate: async (reason) => onWrapperBannedImmediate(sessionId, reason),
            
            onMessageSent: (msg) => {
                broadcastToUser(userId, {
                    type: 'message_sent',
                    sessionId: sessionId,
                    message: msg
                });
            }
        });
        
        // ✅ CRITICAL: Set userId dan port ke wrapper (diperlukan untuk webhook payload)
        wrapper.userId = userId;
        wrapper.port = actualPort;
        
        if (isPairingMode) {
            await evictRivalPairingSessions(sessionId, phoneNumber);
        }

        // Initialize wrapper
        await wrapper.initialize(phoneNumber);
        
        // Store wrapper
        activeWrappers.set(sessionId, wrapper);
        registerConnectedAllowlistLocal(sessionId);
        invalidateSessionFolderDiskCache();

        res.json({ 
            error: "0", 
            msg: phoneNumber ? "Pairing code will be sent via webhook" : "QR code will be sent via webhook",
            session: sessionId,
            port: actualPort
        });
        
    } catch (error) {
        console.error('Error in /baileys/pair endpoint:', error.message);
        res.json({ error: "1", msg: error.message });
    }
});

// Alias /api/pair - sama seperti /pair (IMPROVED WITH WEBHOOK LOGGING)
app.post('/api/pair', async (req, res) => {
    try {
        const { phoneNumber, userId, botId, port } = req.body;
        
        if (!userId) {
            return res.json({ error: "1", msg: "userId is required" });
        }
        
        logPairPortFallbackIfNeeded(req, port);
        const actualPort = resolvePairServicePort(req, port);
        const sessionId = `sess_${userId}_${actualPort}_${Date.now()}`;
        
        console.log(`🔄 Creating session (API): ${sessionId}`);
        console.log(`   User: ${userId}, Phone: ${phoneNumber || 'QR Method'}, Port: ${actualPort} (shard PORT=${PORT})`);
        const isPairingMode = (() => {
            if (typeof phoneNumber === 'string') {
                return phoneNumber.trim().length > 0;
            }
            if (typeof phoneNumber === 'number') {
                return String(phoneNumber).trim().length > 0;
            }
            return false;
        })();
        
        // ✅ Create session directory first
        const sessionDir = path.join(process.cwd(), 'sessions', sessionId);
        if (!fs.existsSync(sessionDir)) {
            fs.mkdirSync(sessionDir, { recursive: true });
            console.log(`📁 Created session directory: ${sessionDir}`);
        }
        
        // Create new session
        await sessionManager.createSession(sessionId, phoneNumber);
        
        // Create wrapper
        const wrapper = new BaileysWrapper(sessionId, sessionManager, {
            onQR: async (qr) => {
                if (isPairingMode) {
                    console.log(`⚠️ QR generated for ${sessionId} (API) but pairing mode active - skipping QR webhook`);
                    return;
                }
                console.log(`📱 QR generated for ${sessionId}`);
                console.log(`📤 Sending QR_CODE to webhook (API)`);
                
                const qrPayload2 = {
                    event: 'QR_CODE',
                    sessionId: sessionId,
                    bot: sessionId,
                    qr: qr,
                    user_id: userId,
                    port: actualPort
                };
                dispatchWebhook(qrPayload2, sessionId, 'QR_CODE');
            },
            
            onPairingCode: async (data) => {
                const {
                    code,
                    phoneNumber: normalizedPhone,
                    rawPhoneNumber,
                    appliedDefaultCountryCode,
                    defaultCountryCode
                } = data || {};

                console.log(`🔑 Pairing code: ${code} for ${sessionId}`);
                console.log(`📤 Sending PAIRING_CODE to webhook (API)`);
                if (rawPhoneNumber && normalizedPhone && rawPhoneNumber !== normalizedPhone) {
                    console.log(`   Raw phone: ${rawPhoneNumber} -> Normalized: ${normalizedPhone}`);
                    if (appliedDefaultCountryCode) {
                        console.log(`   Applied default country code: ${defaultCountryCode}`);
                    }
                }
                
                const pairingPayload2 = {
                    event: 'PAIRING_CODE',
                    sessionId: sessionId,
                    bot: sessionId,
                    code,
                    phoneNumber: normalizedPhone || phoneNumber,
                    rawPhoneNumber: rawPhoneNumber || phoneNumber,
                    appliedDefaultCountryCode: appliedDefaultCountryCode || false,
                    defaultCountryCode: defaultCountryCode || null,
                    user_id: userId,
                    port: actualPort
                };
                dispatchWebhook(pairingPayload2, sessionId, 'PAIRING_CODE');
            },
            
            onMessageTimer: async (payload) => {
                console.log(`⏱️ Message timer detected for ${sessionId}`);
                dispatchWebhook(payload, sessionId, 'MESSAGE_TIMER');
            },
            
            onConnected: async () => {
                console.log(`✅ WhatsApp connected: ${sessionId} (CONNECTED webhook handled by wrapper.js)`);
            },
            
            onDisconnected: async (reason) => onWrapperDisconnected(sessionId, reason),
            onBannedImmediate: async (reason) => onWrapperBannedImmediate(sessionId, reason),
        });
        
        // ✅ CRITICAL: Set userId dan port ke wrapper (diperlukan untuk webhook payload)
        wrapper.userId = userId;
        wrapper.port = actualPort;
        
        // Store phone number reference
        wrapper.phoneNumber = phoneNumber;
        
        activeWrappers.set(sessionId, wrapper);
        // ✅ FIXED: Wrap initialize dengan try-catch untuk prevent crash
        try {
        await wrapper.initialize(phoneNumber);
        } catch (initError) {
            console.error(`❌ [INIT] Error initializing session ${sessionId}:`, initError.message);
            ramEvictSession(sessionId, 'init failed');
            return res.json({ error: "1", msg: `Failed to initialize: ${initError.message}` });
        }
        registerConnectedAllowlistLocal(sessionId);
        invalidateSessionFolderDiskCache();

        res.json({ 
            error: "0", 
            msg: sessionId,
            sessionId: sessionId,
            port: actualPort
        });
        
    } catch (error) {
        console.error('Pair error:', error);
        res.json({ error: "1", msg: error.message });
    }
});

// ============================================================================
// 📤 /send - UNIFIED SEND ENDPOINT (Main endpoint)
// ============================================================================
app.post('/send', async (req, res) => {
    try {
        if (shardDrainReject(res)) return;
        const { cmd, bot, recipient, message, footer, url, button, title, subtitle, imageUrl, image_url, image, caption, port, skip_webhook, skipWebhook, enable_callback, enableCallback, callback } = req.body;
        
        // ✅ OPTIMIZED: Minimal logging untuk performance (ringan & efisien)
        // ✅ FIX: Validation - tidak require message jika ada image/imageUrl (bisa pakai caption)
        const hasImage = !!(image || imageUrl || image_url);
        if (!bot || !recipient || (!message && !hasImage)) {
            return res.json({ error: "1", msg: 'Missing required fields: bot, recipient, and (message or image/imageUrl)' });
        }
        
        // ✅ NEW: Extract skipWebhook parameter (support both skip_webhook and skipWebhook)
        // ✅ Priority: Parameter > Config default
        const paramSkipWebhook = skip_webhook !== undefined || skipWebhook !== undefined;
        const paramValue = skip_webhook !== undefined ? skip_webhook : skipWebhook;
        const shouldSkipWebhook = paramSkipWebhook 
            ? (paramValue === true || paramValue === '1' || paramValue === 1)
            : (config?.validation?.skipWebhook === true); // Default dari config jika parameter tidak ada
        
        const shouldEnableCallback = resolveSendCallbackMode(req.body);
        
        // Get or auto-load session
        let wrapper;
        try {
            wrapper = await getOrLoadSession(bot);
        } catch (loadError) {
            // ✅ NEW: getOrLoadSession sudah kirim DISCONNECTED webhook sebelum throw error
            // Jadi kita hanya perlu return error response
            return res.json({ error: "1", msg: loadError.message });
        }
        
        // ✅ FIXED: Hapus check isAuthenticated di API - biarkan sendMessage() yang handle
        // ✅ ALASAN: sendMessage() sudah check isAuthenticated dan akan throw error jika tidak authenticated
        // ✅ Biarkan WhatsApp yang handle timeout/error sendiri (lebih natural)
        // ✅ Jika session masih bisa kirim (socket OPEN), biarkan kirim - WhatsApp akan return timeout/error jika tidak bisa
        // ✅ Error dari sendMessage() akan di-catch dan return dengan message dari sendMessage()
        
        // Validate phone number format
        if (recipient && !recipient.includes('@')) {
            const validation = validateWhatsAppNumber(recipient);
            if (!validation.valid) {
                return res.json({ error: "1", msg: validation.error });
            }
        }
        
        const recipientJid = recipient.includes('@') ? recipient : `${recipient}@s.whatsapp.net`;
        
        if (isRecipientCheckEnabled()) {
            // ✅ FIXED: Check dengan timeout dan non-blocking error handling
            // Check if phone number exists on WhatsApp
            try {
                // ✅ OPTIMIZED: Timeout check sangat cepat untuk prevent slowdown
                const checkPromise = wrapper.checkWhatsAppNumber(recipientJid);
                // ✅ OPTIMIZED: Reduce timeout ke 500ms (prevent delay variabel saat high frequency)
                const timeoutPromise = new Promise((resolve) => 
                    setTimeout(() => resolve(false), 500) // ✅ REDUCED: 500ms max (cukup untuk check cepat, prevent delay)
                );
                
                const existsOnWhatsApp = await Promise.race([checkPromise, timeoutPromise]);
                
                if (existsOnWhatsApp === false) {
                    // ✅ FIXED: Jangan langsung reject, bisa jadi timeout atau network issue
                    // Log warning tapi tetap proceed untuk prevent slowdown
                    setImmediate(() => console.warn('⚠️ [CHECK] WhatsApp number check returned false (might be timeout/network issue), proceeding anyway'));
                    // ✅ OPTIMIZED: Skip check untuk prevent slowdown, langsung send
                    // User bisa handle error dari response WhatsApp jika memang tidak valid
                }
            } catch (checkError) {
                // ✅ FIXED: Error handling sudah non-blocking di wrapper, tapi tetap catch untuk safety
                console.warn('⚠️ Could not verify WhatsApp number, proceeding anyway:', checkError.message);
                // Continue to send - tidak blocking
            }
        } else {
            console.debug('ℹ️  Skipping WhatsApp recipient check (disabled via config/env).');
        }
        // ✅ NEW: Jika enable_callback=true, generate temp ID dan return temp ID langsung
        // ✅ Jika enable_callback=false, tunggu sendMessage selesai dan return message ID
        if (shouldEnableCallback) {
            // ✅ CRITICAL: Generate temp ID dulu sebelum return response
            // ✅ Temp ID diperlukan untuk mapping ke real message ID nanti
            const tempMsgId = wrapper.generateTempMessageId();
            // ✅ GREP: [TEMP-MSGID-API-GENERATED]
            console.log(`[TEMP-MSGID-API-GENERATED] tempMsgId=${tempMsgId} enableCallback=true`);
            
            // ✅ FIX: Return temp ID langsung dengan flag jelas, kirim pesan di background (full background)
            if (!res.headersSent) {
                res.json({ 
                    error: "0", 
                    msg: tempMsgId,
                    ack_status: "temp",
                    is_temp: true
                }); // ✅ Return temp ID dengan flag jelas
            }
            
            // ✅ Kirim pesan di background (fire-and-forget)
            const sendOptions = { 
                skipWebhook: shouldSkipWebhook,
                enableCallback: shouldEnableCallback
            };
            
            setImmediate(async () => {
                try {
        let result;
                    // ✅ SMART AUTO DETECT: Support image (local file) dan imageUrl (URL)
                    const hasImage = !!(image || imageUrl || image_url);
                    const imgPath = image || imageUrl || image_url;
                    
                    if ((cmd === 'sendImageMessage' || cmd === 'sendImageButtonMessage' || (hasImage && url && button)) && hasImage && url && button) {
                        // Send image with button (support local file atau URL)
                        result = await wrapper.sendImageButtonMessage(recipientJid, imgPath, caption || message || '', title, subtitle, footer || '', url, button, sendOptions);
                    } else if (hasImage && !url && !button) {
                        // Send image only (no button)
                        result = await wrapper.sendMessage(recipientJid, imgPath, 'image', { ...sendOptions, caption: caption || message || '' });
                    } else if ((cmd === 'sendButtonMessage' || (url && button)) && url && button) {
                        result = await wrapper.sendButtonMessage(recipientJid, message, footer || '', url, button, title, subtitle, sendOptions);
                    } else {
                        result = await wrapper.sendMessage(recipientJid, message, 'text', sendOptions);
                    }
                    
                    // ✅ CRITICAL: Pass tempMsgId ke processSendMessageResult untuk mapping
                    if (result && wrapper.processSendMessageResult) {
                        const messageType = hasImage ? 'image' : (url && button ? 'button' : 'text');
                        const finalMessage = hasImage ? (caption || message || '') : message;
                        await wrapper.processSendMessageResult(result, recipientJid, finalMessage, messageType, sendOptions, shouldSkipWebhook, shouldEnableCallback, tempMsgId);
                    }
                    
                    // ✅ FIXED: Defensive check untuk prevent "Cannot read properties of undefined (reading 'attrs')"
                    const messageId = result?.key?.id;
                    if (messageId) {
                        setImmediate(() => console.log(`✅ Message sent via /send (background) - ID: ${messageId}, tempMsgId: ${tempMsgId}`));
                    }
                    // ✅ Message ID akan dikirim via webhook MESSAGE_SENT di background (sudah di-handle di wrapper.js)
                } catch (bgError) {
                    console.error('❌ Error sending message in background:', bgError.message);
                    // ✅ Error juga bisa dikirim via webhook jika diperlukan
                }
            });
        } else {
            // ✅ enable_callback=false: tunggu message ID asli dengan batas syncSendMaxWaitMs (bukan temp/pending)
            const sendOptions = { 
                skipWebhook: shouldSkipWebhook,
                enableCallback: shouldEnableCallback
            };
            const hasImage = !!(image || imageUrl || image_url);
            const imgPath = image || imageUrl || image_url;
            let sendPromise;
            if ((cmd === 'sendImageMessage' || cmd === 'sendImageButtonMessage' || (hasImage && url && button)) && hasImage && url && button) {
                sendPromise = wrapper.sendImageButtonMessage(recipientJid, imgPath, caption || message || '', title, subtitle, footer || '', url, button, sendOptions);
            } else if (hasImage && !url && !button) {
                sendPromise = wrapper.sendMessage(recipientJid, imgPath, 'image', { ...sendOptions, caption: caption || message || '' });
            } else if ((cmd === 'sendButtonMessage' || (url && button)) && url && button) {
                sendPromise = wrapper.sendButtonMessage(recipientJid, message, footer || '', url, button, title, subtitle, sendOptions);
            } else {
                sendPromise = wrapper.sendMessage(recipientJid, message, 'text', sendOptions);
            }
            try {
                const result = await sendPromise;
                const messageId = result?.key?.id;
                if (messageId) {
                    // registerOutboundMessageSync sudah di wrapper.sendMessage / processSendMessageResult
                    setImmediate(() => console.log(`✅ Message sent via /send - ID: ${messageId}`));
                }
                if (!res.headersSent) {
                    if (messageId) {
                        // ack_status=original = msg_id nyata dari sendMessage, BUKAN receipt WA (status≥2).
                        return res.json({
                            error: "0",
                            msg: messageId,
                            ack_status: "original",
                            is_temp: false,
                            receipt_pending: true
                        });
                    }
                    return res.json({ error: "1", msg: "Failed to get message ID from WhatsApp" });
                }
            } catch (error) {
                if (!res.headersSent) {
                    return res.json({
                        error: "1",
                        msg: error?.message || 'Send message failed'
                    });
                }
            }
        }
        
    } catch (error) {
        console.error('❌ Error in /send:', error.message);
        if (!res.headersSent) {
        res.json({ error: "1", msg: error.message });
        }
    }
});

// ============================================================================
// 📤 /api/send - UNIFIED SEND ENDPOINT (API alias)
// ============================================================================
app.post('/api/send', async (req, res) => {
    try {
        const { cmd, bot, recipient, message, footer, url, button, title, subtitle, imageUrl, image_url, image, caption, port, skip_webhook, skipWebhook, enable_callback, enableCallback, callback } = req.body;
        
        // ✅ FIX: Validation - tidak require message jika ada image/imageUrl (bisa pakai caption)
        const hasImage = !!(image || imageUrl || image_url);
        if (!bot || !recipient || (!message && !hasImage)) {
            return res.json({ error: "1", msg: 'Missing required fields: bot, recipient, and (message or image/imageUrl)' });
        }
        
        // ✅ NEW: Extract skipWebhook parameter (support both skip_webhook and skipWebhook)
        // ✅ Priority: Parameter > Config default
        const paramSkipWebhook = skip_webhook !== undefined || skipWebhook !== undefined;
        const paramValue = skip_webhook !== undefined ? skip_webhook : skipWebhook;
        const shouldSkipWebhook = paramSkipWebhook 
            ? (paramValue === true || paramValue === '1' || paramValue === 1)
            : (config?.validation?.skipWebhook === true); // Default dari config jika parameter tidak ada
        
        const shouldEnableCallback = resolveSendCallbackMode(req.body);
        
        // Get or auto-load session
        let wrapper;
        try {
            wrapper = await getOrLoadSession(bot);
        } catch (loadError) {
            // ✅ NEW: getOrLoadSession sudah kirim DISCONNECTED webhook sebelum throw error
            // Jadi kita hanya perlu return error response
            return res.json({ error: "1", msg: loadError.message });
        }
        
        // ✅ FIXED: Hapus check isAuthenticated di API - biarkan sendMessage() yang handle
        // ✅ ALASAN: sendMessage() sudah check isAuthenticated dan akan throw error jika tidak authenticated
        // ✅ Biarkan WhatsApp yang handle timeout/error sendiri (lebih natural)
        // ✅ Jika session masih bisa kirim (socket OPEN), biarkan kirim - WhatsApp akan return timeout/error jika tidak bisa
        // ✅ Error dari sendMessage() akan di-catch dan return dengan message dari sendMessage()
        
        // Validate phone number format
        if (recipient && !recipient.includes('@')) {
            const validation = validateWhatsAppNumber(recipient);
            if (!validation.valid) {
                return res.json({ error: "1", msg: validation.error });
            }
        }
        
        const recipientJid = recipient.includes('@') ? recipient : `${recipient}@s.whatsapp.net`;
        
        if (isRecipientCheckEnabled()) {
            // ✅ FIXED: Check dengan timeout dan non-blocking error handling
            try {
                // ✅ OPTIMIZED: Timeout check untuk prevent slowdown saat nomor diblokir
                const checkPromise = wrapper.checkWhatsAppNumber(recipientJid);
                const timeoutPromise = new Promise((resolve) => 
                    setTimeout(() => resolve(false), 2500) // 2.5 detik max
                );
                
                const existsOnWhatsApp = await Promise.race([checkPromise, timeoutPromise]);
                
                if (existsOnWhatsApp === false) {
                    // ✅ FIXED: Jangan langsung reject, bisa jadi timeout atau network issue
                    console.warn('⚠️ [CHECK] WhatsApp number check returned false (might be timeout/network issue), proceeding anyway');
                    // ✅ OPTIMIZED: Skip check untuk prevent slowdown, langsung send
                }
            } catch (checkError) {
                console.warn('⚠️ Could not verify WhatsApp number, proceeding anyway:', checkError.message);
            }
        } else {
            console.debug('ℹ️  Skipping WhatsApp recipient check (disabled via config/env).');
        }
        // ✅ NEW: Jika enable_callback=true, generate temp ID dan return temp ID langsung
        // ✅ Jika enable_callback=false, tunggu sendMessage selesai dan return message ID
        if (shouldEnableCallback) {
            // ✅ CRITICAL: Generate temp ID dulu sebelum return response
            const tempMsgId = wrapper.generateTempMessageId();
            // ✅ GREP: [TEMP-MSGID-API-GENERATED]
            console.log(`[TEMP-MSGID-API-GENERATED] tempMsgId=${tempMsgId} enableCallback=true`);
            
            // ✅ FIX: Return temp ID langsung dengan flag jelas, kirim pesan di background (full background)
            if (!res.headersSent) {
                res.json({ 
                    error: "0", 
                    msg: tempMsgId,
                    ack_status: "temp",
                    is_temp: true
                }); // ✅ Return temp ID dengan flag jelas
            }
            
            // ✅ Kirim pesan di background (fire-and-forget)
            // ✅ CRITICAL: Pass tempMsgId ke sendOptions agar wrapper.js bisa dapat tempMsgId yang sama
            const sendOptions = { 
                skipWebhook: shouldSkipWebhook,
                enableCallback: shouldEnableCallback,
                tempMsgId: tempMsgId // ✅ CRITICAL: Pass tempMsgId ke options
            };
            
            setImmediate(async () => {
                try {
        let result;
                    // ✅ SMART AUTO DETECT: Support image (local file) dan imageUrl (URL)
                    const hasImage = !!(image || imageUrl || image_url);
                    const imgPath = image || imageUrl || image_url;
                    
                    if ((cmd === 'sendImageMessage' || cmd === 'sendImageButtonMessage' || (hasImage && url && button)) && hasImage && url && button) {
                        // Send image with button (support local file atau URL)
                        result = await wrapper.sendImageButtonMessage(recipientJid, imgPath, caption || message || '', title, subtitle, footer || '', url, button, sendOptions);
                    } else if (hasImage && !url && !button) {
                        // Send image only (no button)
                        result = await wrapper.sendMessage(recipientJid, imgPath, 'image', { ...sendOptions, caption: caption || message || '' });
                    } else if ((cmd === 'sendButtonMessage' || (url && button)) && url && button) {
                        result = await wrapper.sendButtonMessage(recipientJid, message, footer || '', url, button, title, subtitle, sendOptions);
                    } else {
                        result = await wrapper.sendMessage(recipientJid, message, 'text', sendOptions);
                    }
                    
                    // ✅ NOTE: processSendMessageResult sudah dipanggil di wrapper.js sendMessage dengan tempMsgId dari options
                    
                    const messageId = result?.key?.id;
                    setImmediate(() => console.log(`✅ Message sent via /api/send (background) - ID: ${messageId}, tempMsgId: ${tempMsgId}`));
                    // ✅ Message ID akan dikirim via webhook MESSAGE_SENT di background (sudah di-handle di wrapper.js)
                } catch (bgError) {
                    console.error('❌ Error sending message in background:', bgError.message);
                    // ✅ Error juga bisa dikirim via webhook jika diperlukan
                }
            });
        } else {
            const sendOptions = { 
                skipWebhook: shouldSkipWebhook,
                enableCallback: shouldEnableCallback
            };
            const hasImage = !!(image || imageUrl || image_url);
            const imgPath = image || imageUrl || image_url;
            let sendPromise;
            if ((cmd === 'sendImageMessage' || cmd === 'sendImageButtonMessage' || (hasImage && url && button)) && hasImage && url && button) {
                sendPromise = wrapper.sendImageButtonMessage(recipientJid, imgPath, caption || message || '', title, subtitle, footer || '', url, button, sendOptions);
            } else if (hasImage && !url && !button) {
                sendPromise = wrapper.sendMessage(recipientJid, imgPath, 'image', { ...sendOptions, caption: caption || message || '' });
            } else if ((cmd === 'sendButtonMessage' || (url && button)) && url && button) {
                sendPromise = wrapper.sendButtonMessage(recipientJid, message, footer || '', url, button, title, subtitle, sendOptions);
            } else {
                sendPromise = wrapper.sendMessage(recipientJid, message, 'text', sendOptions);
            }
            let result;
            try {
                result = await sendPromise;
            } catch (error) {
                if (!res.headersSent) {
                    return res.json({ 
                        error: "1", 
                        msg: error.message || 'Send message failed' 
                    });
                }
                return;
            }
            const messageId = result?.key?.id;
            if (messageId) {
                setImmediate(() => console.log(`✅ Message sent via /api/send - ID: ${messageId}`));
            }
            if (!res.headersSent) {
                if (messageId) {
                    // ack_status=original = msg_id nyata, BUKAN receipt WA
                    res.json({ 
                        error: "0", 
                        msg: messageId,
                        ack_status: "original",
                        is_temp: false,
                        receipt_pending: true
                    });
                } else {
                    res.json({ 
                        error: "1", 
                        msg: "Failed to get message ID from WhatsApp"
                    });
                }
            }
        }
        
    } catch (error) {
        console.error('❌ Error in /api/send:', error.message);
        res.json({ error: "1", msg: error.message });
    }
});

// ============================================================================
// 🔄 AUTO-LOAD SESSION FROM DISK - Helper Function
// ============================================================================
async function getOrLoadSession(sessionId) {
    assertSessionIdBelongsToThisShard(sessionId);
    // ✅ CRITICAL: Check if session already in memory (instant return)
    let wrapper = activeWrappers.get(sessionId);
    if (wrapper) {
        return wrapper;
    }

    if (isEnforceConnectedAllowlist()) {
        await refreshConnectedAllowlistFromApi();
        assertSessionAllowedToLoad(sessionId);
    }
    
    // ✅ Satu promise per sessionId: `has` + `set` tanpa await di antaranya → tidak ada dua initialize paralel (race lama).
    if (!loadingSessions.has(sessionId)) {
        const created = (async () => {
    // Session not in memory, check if exists on disk
    const sessionsDir = path.join(process.cwd(), 'sessions');
    const sessionPath = path.join(sessionsDir, sessionId);
    const credsPath = path.join(sessionPath, 'creds.json');
    
    if (!fs.existsSync(credsPath)) {
        // ✅ NEW: Kirim DISCONNECTED webhook jika session tidak ditemukan
        // Extract userId dan port dari sessionId (sess_USERID_PORT_TIMESTAMP)
        const userIdMatch = sessionId.match(/sess_(\d+)_(\d+)_/);
        const userId = userIdMatch ? userIdMatch[1] : null;
        const port = userIdMatch ? parseInt(userIdMatch[2]) : null;
        
        // ✅ Kirim DISCONNECTED webhook untuk session yang tidak ditemukan
        setImmediate(async () => {
            try {
                const webhookPayload = {
                    event: 'DISCONNECTED',
                    sessionId: sessionId,
                    reason: 'SESSION_NOT_FOUND',
                    user_id: userId,
                    port: port,
                    reconnectAttempt: 0,
                    isLoggedOut: false,
                    canReconnect: false,
                    reason_detail: 'Session not found on disk - session may have been deleted or never created'
                };
                await dispatchWebhook(webhookPayload, sessionId, 'DISCONNECTED');
            } catch (err) {
                // Ignore webhook errors
                console.error(`❌ [WEBHOOK] Failed to send DISCONNECTED for not found session ${sessionId}:`, err.message);
            }
        });
        
        throw new Error(`Session not found: ${sessionId}`);
    }
    
    console.log(`📂 Auto-loading session from disk: ${sessionId}`);
    
    // Extract userId from sessionId (sess_USERID_PORT_TIMESTAMP)
    const userIdMatch = sessionId.match(/sess_(\d+)_/);
    const userId = userIdMatch ? userIdMatch[1] : null;
    
    // Create new wrapper with minimal callbacks
        const newWrapper = new BaileysWrapper(sessionId, sessionManager, {
        onQR: (qr) => console.log(`QR for ${sessionId}`),
        onConnected: async () => {
            console.log(`✅ Auto-loaded session connected: ${sessionId}`);
        },
        onDisconnected: async (reason) => {
                onWrapperDisconnected(sessionId, reason);
                loadingSessions.delete(sessionId);
        },
        onBannedImmediate: async (reason) => onWrapperBannedImmediate(sessionId, reason),
        onMessageSent: (msg) => {}
    });
    
        // ✅ FIXED: Wrap initialize dengan try-catch untuk prevent crash
        try {
            await newWrapper.initialize();
        } catch (initError) {
            console.error(`❌ [AUTO-LOAD] Error initializing session ${sessionId}:`, initError.message);
            ramEvictSession(sessionId, 'auto-load init failed');
            loadingSessions.delete(sessionId);
            throw new Error(`Failed to auto-load session: ${initError.message}`);
        }
    
    // Store in activeWrappers
        activeWrappers.set(sessionId, newWrapper);
    
        // ✅ CRITICAL: Remove dari loadingSessions setelah selesai
        loadingSessions.delete(sessionId);
        
        return newWrapper;
        })();
        loadingSessions.set(sessionId, created);
    }

    const loadingPromise = loadingSessions.get(sessionId);

    // ✅ Wait untuk loading selesai (bisa dari request ini atau request lain)
    try {
        wrapper = await loadingPromise;
    } catch (error) {
        // Loading gagal, cleanup dan throw error
        loadingSessions.delete(sessionId);
        throw error;
    }
    
    // ✅ FIXED: Non-blocking console.log (fire-and-forget)
    // ✅ OPTIMIZED: Minimal logging untuk performance
    
    // ✅ OPTIMIZED: Check connection status, return immediately if ready (instead of fixed 2s delay)
    if (wrapper.isAuthenticated && wrapper.sock?.user) {
        return wrapper;
    }
    
    // ✅ OPTIMIZED: Wait cukup untuk reconnect, tapi tidak terlalu lama (reduce delay variabel)
    // ✅ CRITICAL: Session bisa butuh waktu untuk authenticate, tapi 500ms sudah cukup untuk most cases
    let attempts = 10; // ✅ REDUCED: 10 attempts × 50ms = 500ms max (faster, prevent delay variabel)
    let lastConnectionState = null;
    
    while (attempts > 0) {
        // Check jika sudah authenticated
        if (wrapper.isAuthenticated && wrapper.sock?.user) {
            // ✅ OPTIMIZED: Minimal logging untuk performance
            return wrapper;
        }
        
        // ✅ NEW: Check connection state (byee-wamod: pakai .socket.readyState)
        const connectionState = getBaileysWebSocketReadyState(wrapper.sock);
        if (connectionState !== lastConnectionState) {
            lastConnectionState = connectionState;
            // 0 = CONNECTING, 1 = OPEN, 2 = CLOSING, 3 = CLOSED
            if (connectionState === 1) {
                // Socket sudah OPEN, tapi belum authenticated - wait lebih lama
                setImmediate(() => console.log(`⏳ Session ${sessionId} socket OPEN but not authenticated yet, waiting...`));
            } else if (connectionState === 0) {
                // Socket masih CONNECTING - normal, wait
                setImmediate(() => console.log(`⏳ Session ${sessionId} still connecting, waiting...`));
            } else if (connectionState === 3) {
                // Socket CLOSED - tidak akan connect, return error
                setImmediate(() => console.error(`❌ Session ${sessionId} socket CLOSED, cannot authenticate`));
                break;
            }
        }
        
        await new Promise(resolve => setTimeout(resolve, 50)); // ✅ REDUCED: 50ms per attempt (faster, lebih smooth)
        attempts--;
    }
    
    // ✅ FIXED: Return wrapper meskipun belum authenticated - biarkan endpoint handle retry
    // Jangan throw error di sini, biarkan endpoint check dan retry jika perlu
    if (!wrapper.isAuthenticated) {
        setImmediate(() =>
            console.warn(
                `⚠️ Session ${sessionId} not authenticated after short wait (~${10 * 50}ms poll), returning wrapper for send path to handle`
            )
        );
    }
    
    return wrapper;
}

// ============================================================================
// 📤 /baileys/send - UNIFIED SEND ENDPOINT (for processor compatibility)
// ============================================================================
app.post('/baileys/send', async (req, res) => {
    if (shardDrainReject(res)) return;
    const debugSend = config.logging?.enableConsoleLog === true;

    if (debugSend) {
        const preTimestamp = new Date().toISOString();
        console.error(`[ENDPOINT-DEBUG] ${preTimestamp} | Endpoint: /baileys/send | ACTION: Request arrived`);
        console.error(`[ENDPOINT-DEBUG] ${preTimestamp} | RequestMethod: ${req.method}`);
        console.error(`[ENDPOINT-DEBUG] ${preTimestamp} | ContentType: ${req.headers['content-type'] || 'none'}`);
        console.error(`[ENDPOINT-DEBUG] ${preTimestamp} | BodyType: ${typeof req.body}`);
        console.error(`[ENDPOINT-DEBUG] ${preTimestamp} | BodyKeys: ${req.body ? Object.keys(req.body).join(',') : 'null'}`);
    }

    try {
        const { cmd, bot, recipient, message, footer, url, button, title, subtitle, imageUrl, image_url, image, caption, port, skip_webhook, skipWebhook, enable_callback, enableCallback, callback } = req.body;

        if (debugSend) {
            const timestamp = new Date().toISOString();
            console.error(`[ENDPOINT-DEBUG] ${timestamp} | Endpoint: /baileys/send | ACTION: Request received`);
            console.error(`[ENDPOINT-DEBUG] ${timestamp} | Cmd: ${cmd || 'none'}`);
            console.error(`[ENDPOINT-DEBUG] ${timestamp} | Bot: ${bot || 'none'}`);
            console.error(`[ENDPOINT-DEBUG] ${timestamp} | Recipient: ${recipient || 'none'}`);
            console.error(`[ENDPOINT-DEBUG] ${timestamp} | RawBodyCheck: image=${!!image}, imageUrl=${!!imageUrl}, image_url=${!!image_url}`);
            console.error(`[ENDPOINT-DEBUG] ${timestamp} | HasImage: ${!!(image || imageUrl || image_url)}`);
            console.error(`[ENDPOINT-DEBUG] ${timestamp} | ImageUrl: ${imageUrl || image_url || image || 'none'}`);
            console.error(`[ENDPOINT-DEBUG] ${timestamp} | Url: ${url || 'none'}`);
            console.error(`[ENDPOINT-DEBUG] ${timestamp} | Button: ${button || 'none'}`);
        }

        // ✅ FIX: Validation - tidak require message jika ada image/imageUrl (bisa pakai caption)
        const hasImage = !!(image || imageUrl || image_url);
        if (!bot || !recipient || (!message && !hasImage)) {
            if (debugSend) {
                console.error(`[ENDPOINT-DEBUG] ${new Date().toISOString()} | ERROR: Missing required fields`);
            }
            return res.json({ error: "1", msg: 'Missing required fields: bot, recipient, and (message or image/imageUrl)' });
        }
        
        // ✅ NEW: Extract skipWebhook parameter (support both skip_webhook and skipWebhook)
        // ✅ Priority: Parameter > Config default
        const paramSkipWebhook = skip_webhook !== undefined || skipWebhook !== undefined;
        const paramValue = skip_webhook !== undefined ? skip_webhook : skipWebhook;
        const shouldSkipWebhook = paramSkipWebhook 
            ? (paramValue === true || paramValue === '1' || paramValue === 1)
            : (config?.validation?.skipWebhook === true); // Default dari config jika parameter tidak ada
        
        const shouldEnableCallback = resolveSendCallbackMode(req.body);
        
        // Get or auto-load session
        let wrapper;
        try {
            wrapper = await getOrLoadSession(bot);
        } catch (loadError) {
            // ✅ NEW: getOrLoadSession sudah kirim DISCONNECTED webhook sebelum throw error
            // Jadi kita hanya perlu return error response
            return res.json({ error: "1", msg: loadError.message });
        }
        
        // ✅ FIXED: Hapus check isAuthenticated di API - biarkan sendMessage() yang handle
        // ✅ ALASAN: sendMessage() sudah check isAuthenticated dan akan throw error jika tidak authenticated
        // ✅ Biarkan WhatsApp yang handle timeout/error sendiri (lebih natural)
        // ✅ Jika session masih bisa kirim (socket OPEN), biarkan kirim - WhatsApp akan return timeout/error jika tidak bisa
        // ✅ Error dari sendMessage() akan di-catch dan return dengan message dari sendMessage()
        
        // Validate phone number format
        if (recipient && !recipient.includes('@')) {
            const validation = validateWhatsAppNumber(recipient);
            if (!validation.valid) {
                return res.json({ error: "1", msg: validation.error });
            }
        }
        
        const recipientJid = recipient.includes('@') ? recipient : `${recipient}@s.whatsapp.net`;
        
        if (isRecipientCheckEnabled()) {
            // ✅ FIXED: Check dengan timeout dan non-blocking error handling
            try {
                // ✅ OPTIMIZED: Timeout check untuk prevent slowdown saat nomor diblokir
                const checkPromise = wrapper.checkWhatsAppNumber(recipientJid);
                const timeoutPromise = new Promise((resolve) => 
                    setTimeout(() => resolve(false), 2500) // 2.5 detik max
                );
                
                const existsOnWhatsApp = await Promise.race([checkPromise, timeoutPromise]);
                
                if (existsOnWhatsApp === false) {
                    // ✅ FIXED: Jangan langsung reject, bisa jadi timeout atau network issue
                    console.warn('⚠️ [CHECK] WhatsApp number check returned false (might be timeout/network issue), proceeding anyway');
                    // ✅ OPTIMIZED: Skip check untuk prevent slowdown, langsung send
                }
            } catch (checkError) {
                console.warn('⚠️ Could not verify WhatsApp number, proceeding anyway:', checkError.message);
            }
        } else {
            console.debug('ℹ️  Skipping WhatsApp recipient check (disabled via config/env).');
        }
        
        // ✅ NEW: Jika enable_callback=true, generate temp ID dan return temp ID langsung
        // ✅ Jika enable_callback=false, tunggu sendMessage selesai dan return message ID
        if (shouldEnableCallback) {
            // ✅ CRITICAL: Generate temp ID dulu sebelum return response
            const tempMsgId = wrapper.generateTempMessageId();
            // ✅ GREP: [TEMP-MSGID-API-GENERATED]
            console.log(`[TEMP-MSGID-API-GENERATED] tempMsgId=${tempMsgId} enableCallback=true`);
            
            // ✅ FIX: Return temp ID langsung dengan flag jelas, kirim pesan di background (full background)
            if (!res.headersSent) {
                res.json({ 
                    error: "0", 
                    msg: tempMsgId,
                    ack_status: "temp",
                    is_temp: true
                }); // ✅ Return temp ID dengan flag jelas
            }
            
            // ✅ Kirim pesan di background (fire-and-forget)
            // ✅ CRITICAL: Pass tempMsgId ke sendOptions agar wrapper.js bisa dapat tempMsgId yang sama
            const sendOptions = { 
                skipWebhook: shouldSkipWebhook,
                enableCallback: shouldEnableCallback,
                tempMsgId: tempMsgId // ✅ CRITICAL: Pass tempMsgId ke options
            };
            
            setImmediate(async () => {
                try {
        let result;
                    // ✅ SMART AUTO DETECT: Support image (local file) dan imageUrl (URL)
                    const hasImage = !!(image || imageUrl || image_url);
                    const imgPath = image || imageUrl || image_url;
                    
                    // ✅ OPTIMIZED: Minimal overhead untuk performance (ringan & efisien)
                    if ((cmd === 'sendImageMessage' || cmd === 'sendImageButtonMessage' || (hasImage && url && button)) && hasImage && url && button) {
                        result = await wrapper.sendImageButtonMessage(recipientJid, imgPath, caption || message || '', title, subtitle, footer || '', url, button, sendOptions);
                    } else if (hasImage && !url && !button) {
                        result = await wrapper.sendMessage(recipientJid, imgPath, 'image', { ...sendOptions, caption: caption || message || '' });
                    } else if ((cmd === 'sendButtonMessage' || (url && button)) && url && button) {
                        result = await wrapper.sendButtonMessage(recipientJid, message, footer || '', url, button, title, subtitle, sendOptions);
                    } else {
                        result = await wrapper.sendMessage(recipientJid, message, 'text', sendOptions);
                    }
                    
                    // ✅ NOTE: processSendMessageResult sudah dipanggil di wrapper.js sendMessage dengan tempMsgId dari options
                    
                    const messageId = result?.key?.id;
                    setImmediate(() => console.log(`✅ Message sent via /baileys/send (background) - ID: ${messageId}, tempMsgId: ${tempMsgId}`));
                    // ✅ Message ID akan dikirim via webhook MESSAGE_SENT di background (sudah di-handle di wrapper.js)
                } catch (bgError) {
                    console.error('❌ Error sending message in background:', bgError.message);
                    // ✅ Error juga bisa dikirim via webhook jika diperlukan
                }
            });
        } else {
            const sendOptions = { 
                skipWebhook: shouldSkipWebhook,
                enableCallback: shouldEnableCallback
            };
            const hasImage = !!(image || imageUrl || image_url);
            const imgPath = image || imageUrl || image_url;
            let sendPromise;
            if ((cmd === 'sendImageMessage' || cmd === 'sendImageButtonMessage' || (hasImage && url && button)) && hasImage && url && button) {
                sendPromise = wrapper.sendImageButtonMessage(recipientJid, imgPath, caption || message || '', title, subtitle, footer || '', url, button, sendOptions);
            } else if (hasImage && !url && !button) {
                sendPromise = wrapper.sendMessage(recipientJid, imgPath, 'image', { ...sendOptions, caption: caption || message || '' });
            } else if ((cmd === 'sendButtonMessage' || (url && button)) && url && button) {
                sendPromise = wrapper.sendButtonMessage(recipientJid, message, footer || '', url, button, title, subtitle, sendOptions);
            } else {
                sendPromise = wrapper.sendMessage(recipientJid, message, 'text', sendOptions);
            }
            let result;
            try {
                result = await sendPromise;
            } catch (error) {
                if (!res.headersSent) {
                    return res.json({ 
                        error: "1", 
                        msg: error.message || 'Send message failed' 
                    });
                }
                return;
            }
            const messageId = result?.key?.id;
            setImmediate(() => console.log(`✅ Message sent via /baileys/send - ID: ${messageId}`));
            if (!res.headersSent) {
                if (messageId) {
                    // ack_status=original = msg_id nyata, BUKAN receipt WA
                    res.json({ 
                        error: "0", 
                        msg: messageId,
                        ack_status: "original",
                        is_temp: false,
                        receipt_pending: true
                    });
                } else {
                    res.json({ 
                        error: "1", 
                        msg: "Failed to get message ID from WhatsApp"
                    });
                }
            }
        }
        
    } catch (error) {
        console.error('❌ Error in /baileys/send:', error.message);
        res.json({ error: "1", msg: error.message });
    }
});

// ============================================================================
// 🔘 /sendButtonMessage - BUTTON MESSAGE ENDPOINT
// ============================================================================
app.post('/sendButtonMessage', async (req, res) => {
    try {
        const { bot, recipient, message, url, footer, button, title, subtitle } = req.body;
        
        if (!bot || !recipient || !message) {
            return res.json({ error: "1", msg: 'Missing required fields' });
        }
        
        let wrapper = activeWrappers.get(bot);
        if (!wrapper) {
            // ✅ NEW: Coba load session dari disk dulu
            try {
                wrapper = await getOrLoadSession(bot);
            } catch (loadError) {
                // ✅ NEW: Jika session tidak ditemukan, kirim DISCONNECTED webhook
                // Extract userId dan port dari sessionId (sess_USERID_PORT_TIMESTAMP)
                const userIdMatch = bot.match(/sess_(\d+)_(\d+)_/);
                const userId = userIdMatch ? userIdMatch[1] : null;
                const port = userIdMatch ? parseInt(userIdMatch[2]) : null;
                
                // ✅ Kirim DISCONNECTED webhook untuk session yang tidak ditemukan
                setImmediate(async () => {
                    try {
                        const webhookPayload = {
                            event: 'DISCONNECTED',
                            sessionId: bot,
                            bot: bot, // Session ID sebagai fallback
                            reason: 'SESSION_NOT_FOUND',
                            user_id: userId,
                            port: port,
                            reconnectAttempt: 0,
                            isLoggedOut: false,
                            canReconnect: false,
                            reason_detail: 'Session not found in memory or on disk'
                        };
                        await dispatchWebhook(webhookPayload, bot, 'DISCONNECTED');
                    } catch (err) {
                        // Ignore webhook errors
                        console.error(`❌ [WEBHOOK] Failed to send DISCONNECTED for not found session ${bot}:`, err.message);
                    }
                });
                
                return res.json({ error: "1", msg: loadError.message || 'Session not found' });
            }
        }
        
        // ✅ FIXED: Hapus check isAuthenticated di API - biarkan sendButtonMessage() yang handle
        // ✅ ALASAN: sendButtonMessage() sudah check isAuthenticated dan akan throw error jika tidak authenticated
        // ✅ Biarkan WhatsApp yang handle timeout/error sendiri (lebih natural)
        // ✅ Jika session masih bisa kirim (socket OPEN), biarkan kirim - WhatsApp akan return timeout/error jika tidak bisa
        // ✅ Error dari sendButtonMessage() akan di-catch dan return dengan message dari sendButtonMessage()
        
        // Validate phone number format
        if (recipient && !recipient.includes('@')) {
            const validation = validateWhatsAppNumber(recipient);
            if (!validation.valid) {
                return res.json({ error: "1", msg: validation.error });
            }
        }
        
        const recipientJid = recipient.includes('@') ? recipient : `${recipient}@s.whatsapp.net`;
        
        if (isRecipientCheckEnabled()) {
            // Check if phone number exists on WhatsApp
            try {
                const existsOnWhatsApp = await wrapper.checkWhatsAppNumber(recipientJid);
                if (!existsOnWhatsApp) {
                    return res.json({ error: "1", msg: 'Phone number is not registered on WhatsApp' });
                }
            } catch (checkError) {
                console.error('Error checking WhatsApp number:', checkError.message);
                // If check fails, still try to send (might be network issue)
                // But log warning
                console.warn('⚠️ Could not verify WhatsApp number, proceeding anyway:', checkError.message);
            }
        } else {
            console.debug('ℹ️  Skipping WhatsApp recipient check (disabled via config/env).');
        }
        
        // ✅ REMOVED: Timeout wrapper dihapus - biarkan WhatsApp handle sendiri
        // ✅ Support title, subtitle untuk interactive messages
        const sendPromise = wrapper.sendButtonMessage(recipientJid, message, footer || '', url, button, title, subtitle);
        
        // ✅ NO TIMEOUT: Biarkan WhatsApp handle timeout sendiri
        let result;
        try {
            result = await sendPromise;
        } catch (error) {
            if (!res.headersSent) {
                return res.json({ 
                    error: "1", 
                    msg: error.message || 'Send message failed' 
                });
            }
            return;
        }
        
        res.json({ error: "0", msg: result?.key?.id });
        
    } catch (error) {
        console.error('Send error:', error);
        res.json({ error: "1", msg: error.message });
    }
});

// ============================================================================
// 🔘 /api/sendButtonMessage - BUTTON MESSAGE ENDPOINT (API alias)
// ============================================================================
app.post('/api/sendButtonMessage', async (req, res) => {
    try {
        const { bot, recipient, message, url, footer, button, title, subtitle } = req.body;
        
        if (!bot || !recipient || !message) {
            return res.json({ error: "1", msg: 'Missing required fields' });
        }
        
        let wrapper = activeWrappers.get(bot);
        if (!wrapper) {
            // ✅ NEW: Coba load session dari disk dulu
            try {
                wrapper = await getOrLoadSession(bot);
            } catch (loadError) {
                // ✅ NEW: Jika session tidak ditemukan, kirim DISCONNECTED webhook
                // Extract userId dan port dari sessionId (sess_USERID_PORT_TIMESTAMP)
                const userIdMatch = bot.match(/sess_(\d+)_(\d+)_/);
                const userId = userIdMatch ? userIdMatch[1] : null;
                const port = userIdMatch ? parseInt(userIdMatch[2]) : null;
                
                // ✅ Kirim DISCONNECTED webhook untuk session yang tidak ditemukan
                setImmediate(async () => {
                    try {
                        const webhookPayload = {
                            event: 'DISCONNECTED',
                            sessionId: bot,
                            bot: bot, // Session ID sebagai fallback
                            reason: 'SESSION_NOT_FOUND',
                            user_id: userId,
                            port: port,
                            reconnectAttempt: 0,
                            isLoggedOut: false,
                            canReconnect: false,
                            reason_detail: 'Session not found in memory or on disk'
                        };
                        await dispatchWebhook(webhookPayload, bot, 'DISCONNECTED');
                    } catch (err) {
                        // Ignore webhook errors
                        console.error(`❌ [WEBHOOK] Failed to send DISCONNECTED for not found session ${bot}:`, err.message);
                    }
                });
                
                return res.json({ error: "1", msg: loadError.message || 'Session not found' });
            }
        }
        
        // ✅ FIXED: Hapus check isAuthenticated di API - biarkan sendButtonMessage() yang handle
        // ✅ ALASAN: sendButtonMessage() sudah check isAuthenticated dan akan throw error jika tidak authenticated
        // ✅ Biarkan WhatsApp yang handle timeout/error sendiri (lebih natural)
        // ✅ Jika session masih bisa kirim (socket OPEN), biarkan kirim - WhatsApp akan return timeout/error jika tidak bisa
        // ✅ Error dari sendButtonMessage() akan di-catch dan return dengan message dari sendButtonMessage()
        
        // Validate phone number format
        if (recipient && !recipient.includes('@')) {
            const validation = validateWhatsAppNumber(recipient);
            if (!validation.valid) {
                return res.json({ error: "1", msg: validation.error });
            }
        }
        
        const recipientJid = recipient.includes('@') ? recipient : `${recipient}@s.whatsapp.net`;
        
        if (isRecipientCheckEnabled()) {
            // Check if phone number exists on WhatsApp
            try {
                const existsOnWhatsApp = await wrapper.checkWhatsAppNumber(recipientJid);
                if (!existsOnWhatsApp) {
                    return res.json({ error: "1", msg: 'Phone number is not registered on WhatsApp' });
                }
            } catch (checkError) {
                console.error('Error checking WhatsApp number:', checkError.message);
                // If check fails, still try to send (might be network issue)
                // But log warning
                console.warn('⚠️ Could not verify WhatsApp number, proceeding anyway:', checkError.message);
            }
        } else {
            console.debug('ℹ️  Skipping WhatsApp recipient check (disabled via config/env).');
        }
        
        // ✅ REMOVED: Timeout wrapper dihapus - biarkan WhatsApp handle sendiri
        // ✅ Support title, subtitle untuk interactive messages
        const sendPromise = wrapper.sendButtonMessage(recipientJid, message, footer || '', url, button, title, subtitle);
        
        // ✅ NO TIMEOUT: Biarkan WhatsApp handle timeout sendiri
        let result;
        try {
            result = await sendPromise;
        } catch (error) {
            if (!res.headersSent) {
                return res.json({ 
                    error: "1", 
                    msg: error.message || 'Send message failed' 
                });
            }
            return;
        }
        
        res.json({ error: "0", msg: result?.key?.id });
        
    } catch (error) {
        console.error('Send error:', error);
        res.json({ error: "1", msg: error.message });
    }
});

// ============================================================================
// 🔘 /baileys/sendButtonMessage - BUTTON MESSAGE ENDPOINT (Baileys alias)
// ============================================================================
app.post('/baileys/sendButtonMessage', async (req, res) => {
    try {
        const { bot, recipient, message, url, footer, button, title, subtitle } = req.body;
        
        if (!bot || !recipient || !message) {
            return res.json({ error: "1", msg: 'Missing required fields' });
        }
        
        let wrapper = activeWrappers.get(bot);
        if (!wrapper) {
            // ✅ NEW: Coba load session dari disk dulu
            try {
                wrapper = await getOrLoadSession(bot);
            } catch (loadError) {
                // ✅ NEW: Jika session tidak ditemukan, kirim DISCONNECTED webhook
                // Extract userId dan port dari sessionId (sess_USERID_PORT_TIMESTAMP)
                const userIdMatch = bot.match(/sess_(\d+)_(\d+)_/);
                const userId = userIdMatch ? userIdMatch[1] : null;
                const port = userIdMatch ? parseInt(userIdMatch[2]) : null;
                
                // ✅ Kirim DISCONNECTED webhook untuk session yang tidak ditemukan
                setImmediate(async () => {
                    try {
                        const webhookPayload = {
                            event: 'DISCONNECTED',
                            sessionId: bot,
                            bot: bot, // Session ID sebagai fallback
                            reason: 'SESSION_NOT_FOUND',
                            user_id: userId,
                            port: port,
                            reconnectAttempt: 0,
                            isLoggedOut: false,
                            canReconnect: false,
                            reason_detail: 'Session not found in memory or on disk'
                        };
                        await dispatchWebhook(webhookPayload, bot, 'DISCONNECTED');
                    } catch (err) {
                        // Ignore webhook errors
                        console.error(`❌ [WEBHOOK] Failed to send DISCONNECTED for not found session ${bot}:`, err.message);
                    }
                });
                
                return res.json({ error: "1", msg: loadError.message || 'Session not found' });
            }
        }
        
        // ✅ FIXED: Hapus check isAuthenticated di API - biarkan sendButtonMessage() yang handle
        // ✅ ALASAN: sendButtonMessage() sudah check isAuthenticated dan akan throw error jika tidak authenticated
        // ✅ Biarkan WhatsApp yang handle timeout/error sendiri (lebih natural)
        // ✅ Jika session masih bisa kirim (socket OPEN), biarkan kirim - WhatsApp akan return timeout/error jika tidak bisa
        // ✅ Error dari sendButtonMessage() akan di-catch dan return dengan message dari sendButtonMessage()
        
        // Validate phone number format
        if (recipient && !recipient.includes('@')) {
            const validation = validateWhatsAppNumber(recipient);
            if (!validation.valid) {
                return res.json({ error: "1", msg: validation.error });
            }
        }
        
        const recipientJid = recipient.includes('@') ? recipient : `${recipient}@s.whatsapp.net`;
        
        if (isRecipientCheckEnabled()) {
            // Check if phone number exists on WhatsApp
            try {
                const existsOnWhatsApp = await wrapper.checkWhatsAppNumber(recipientJid);
                if (!existsOnWhatsApp) {
                    return res.json({ error: "1", msg: 'Phone number is not registered on WhatsApp' });
                }
            } catch (checkError) {
                console.error('Error checking WhatsApp number:', checkError.message);
                // If check fails, still try to send (might be network issue)
                // But log warning
                console.warn('⚠️ Could not verify WhatsApp number, proceeding anyway:', checkError.message);
            }
        } else {
            console.debug('ℹ️  Skipping WhatsApp recipient check (disabled via config/env).');
        }
        
        // ✅ REMOVED: Timeout wrapper dihapus - biarkan WhatsApp handle sendiri
        // ✅ Support title, subtitle untuk interactive messages
        const sendPromise = wrapper.sendButtonMessage(recipientJid, message, footer || '', url, button, title, subtitle);
        
        // ✅ NO TIMEOUT: Biarkan WhatsApp handle timeout sendiri
        let result;
        try {
            result = await sendPromise;
        } catch (error) {
            if (!res.headersSent) {
                return res.json({ 
                    error: "1", 
                    msg: error.message || 'Send message failed' 
                });
            }
            return;
        }
        
        res.json({ error: "0", msg: result?.key?.id });
        
    } catch (error) {
        console.error('Send error:', error);
        res.json({ error: "1", msg: error.message });
    }
});

// Logout endpoint
app.post('/logout', async (req, res) => {
    try {
        const { sessionId } = req.body;
        if (!sessionId || typeof sessionId !== 'string' || sessionId.trim() === '') {
            return res.json({ error: "1", msg: "sessionId is required" });
        }
        
        const wrapper = activeWrappers.get(sessionId);
        
        if (wrapper) {
            // ? Wrapper exists - use logout method (handles folder cleanup)
            await wrapper.logout();
            ramEvictSession(sessionId, 'logout');
            console.log(`? Logged out and cleaned up: ${sessionId}`);
        } else {
            // ? Wrapper not found - force delete folder anyway
            console.log(`?? Wrapper not found, force deleting folder: ${sessionId}`);
            
            const sessionDir = path.join(process.cwd(), 'sessions', sessionId);
            if (fs.existsSync(sessionDir)) {
                fs.rmSync(sessionDir, { recursive: true, force: true });
                console.log(`??? Force deleted session folder: ${sessionId}`);
            }
            
            // Also remove from session manager
            await sessionManager.deleteSession(sessionId);
        }

        invalidateSessionFolderDiskCache();
        res.json({ error: "0", msg: "Logged out successfully", sessionId: sessionId });
        
    } catch (error) {
        console.error('Logout error:', error);
        res.json({ error: "1", msg: error.message });
    }
});

// Logout endpoint
app.post('/api/logout', async (req, res) => {
    try {
        const { sessionId } = req.body;
        if (!sessionId || typeof sessionId !== 'string' || sessionId.trim() === '') {
            return res.json({ error: "1", msg: "sessionId is required" });
        }
        
        const wrapper = activeWrappers.get(sessionId);
        
        if (wrapper) {
            // ? Wrapper exists - use logout method (handles folder cleanup)
            await wrapper.logout();
            ramEvictSession(sessionId, 'logout');
            console.log(`? Logged out and cleaned up: ${sessionId}`);
        } else {
            // ? Wrapper not found - force delete folder anyway
            console.log(`?? Wrapper not found, force deleting folder: ${sessionId}`);
            
            const sessionDir = path.join(process.cwd(), 'sessions', sessionId);
            if (fs.existsSync(sessionDir)) {
                fs.rmSync(sessionDir, { recursive: true, force: true });
                console.log(`??? Force deleted session folder: ${sessionId}`);
            }
            
            // Also remove from session manager
            await sessionManager.deleteSession(sessionId);
        }

        invalidateSessionFolderDiskCache();
        res.json({ error: "0", msg: "Logged out successfully", sessionId: sessionId });
        
    } catch (error) {
        console.error('Logout error:', error);
        res.json({ error: "1", msg: error.message });
    }
});

// Logout endpoint
app.post('/baileys/logout', async (req, res) => {
    try {
        const { sessionId } = req.body;
        if (!sessionId || typeof sessionId !== 'string' || sessionId.trim() === '') {
            return res.json({ error: "1", msg: "sessionId is required" });
        }
        
        const wrapper = activeWrappers.get(sessionId);
        
        if (wrapper) {
            // ? Wrapper exists - use logout method (handles folder cleanup)
            await wrapper.logout();
            ramEvictSession(sessionId, 'logout');
            console.log(`? Logged out and cleaned up: ${sessionId}`);
        } else {
            // ? Wrapper not found - force delete folder anyway
            console.log(`?? Wrapper not found, force deleting folder: ${sessionId}`);
            
            const sessionDir = path.join(process.cwd(), 'sessions', sessionId);
            if (fs.existsSync(sessionDir)) {
                fs.rmSync(sessionDir, { recursive: true, force: true });
                console.log(`??? Force deleted session folder: ${sessionId}`);
            }
            
            // Also remove from session manager
            await sessionManager.deleteSession(sessionId);
        }

        invalidateSessionFolderDiskCache();
        res.json({ error: "0", msg: "Logged out successfully", sessionId: sessionId });
        
    } catch (error) {
        console.error('Logout error:', error);
        res.json({ error: "1", msg: error.message });
    }
});

// ============================================================================
// ?? RECONNECT - Force reconnect a disconnected session
// ============================================================================
app.post('/baileys/reconnect', async (req, res) => {
    try {
        const { sessionId } = req.body;
        const hard = req.body?.hard === true || req.body?.hard === 1 || req.body?.hard === '1'
            || req.body?.recycle === true || req.body?.recycle === 1 || String(req.body?.reason || '') === 'processor_zombie';
        
        if (!sessionId) {
            return res.json({ error: "1", msg: "Session ID required" });
        }
        
        console.log(`?? Reconnect requested for session: ${sessionId}${hard ? ' hard=1' : ''}`);
        
        // Check if session already exists and is active
        if (activeWrappers.has(sessionId)) {
            const wrapper = activeWrappers.get(sessionId);

            // Forum #2491: sock.user masih ada tapi send timeout = zombie. Jangan "already connected".
            if (hard && typeof wrapper.forceHardSessionRecycle === 'function') {
                const ok = await wrapper.forceHardSessionRecycle(String(req.body?.reason || 'processor_zombie'));
                return res.json({
                    error: ok ? "0" : "1",
                    msg: ok ? "Hard recycle done" : "Hard recycle failed",
                    status: ok ? "RECONNECTING" : "FAILED",
                    recycled: true,
                });
            }
            
            // If already connected, just return success
            if (wrapper.sock && wrapper.sock.user) {
                return res.json({ 
                    error: "0", 
                    msg: "Session already connected",
                    status: "CONNECTED"
                });
            }
            
            // If exists but not connected, try to reconnect
            console.log(`?? Attempting to reconnect existing wrapper: ${sessionId}`);
            // ✅ FIXED: Wrap initialize dengan try-catch
            try {
            await wrapper.initialize();
            } catch (initError) {
                console.error(`❌ [RECONNECT] Error reconnecting session ${sessionId}:`, initError.message);
                return res.json({ 
                    error: "1", 
                    msg: `Reconnect failed: ${initError.message}`,
                    status: "FAILED"
                });
            }
            
            return res.json({ 
                error: "0", 
                msg: "Reconnect initiated",
                status: "RECONNECTING"
            });
        }
        
        // Session doesn't exist in memory, restore from disk
        const sessionsDir = path.join(process.cwd(), 'sessions');
        const sessionPath = path.join(sessionsDir, sessionId);
        const credsPath = path.join(sessionPath, 'creds.json');
        
        // Check if session files exist
        if (!fs.existsSync(credsPath)) {
            return res.json({ 
                error: "1", 
                msg: "Session credentials not found. Please add bot again."
            });
        }
        
        console.log(`?? Restoring session from disk: ${sessionId}`);
        
        // Extract userId from sessionId (sess_USERID_PORT_TIMESTAMP)
        const userIdMatch = sessionId.match(/sess_(\d+)_/);
        const userId = userIdMatch ? userIdMatch[1] : null;
        
        // Create new wrapper
        const wrapper = new BaileysWrapper(sessionId, sessionManager, {
            onQR: (qr) => console.log(`?? QR for ${sessionId}`),
            onPairingCode: (data) => {
                if (!data) return;
                const { code, phoneNumber: normalizedPhone, rawPhoneNumber } = data;
                console.log(`?? Pairing code for ${sessionId}: ${code}${normalizedPhone ? ` (normalized: ${normalizedPhone})` : ''}`);
                if (rawPhoneNumber && normalizedPhone && rawPhoneNumber !== normalizedPhone) {
                    console.log(`   Raw phone: ${rawPhoneNumber}`);
                }
            },
            onConnected: async () => {
                console.log(`? Reconnected session: ${sessionId} (CONNECTED webhook handled by wrapper.js)`);
            },
            onDisconnected: async (reason) => onWrapperDisconnected(sessionId, reason),
            onBannedImmediate: async (reason) => onWrapperBannedImmediate(sessionId, reason),
            onMessageSent: (msg) => {}
        });
        
        // ✅ FIXED: Wrap initialize dengan try-catch
        try {
        await wrapper.initialize();
        } catch (initError) {
            console.error(`❌ [RECONNECT] Error initializing session ${sessionId}:`, initError.message);
            return res.json({ 
                error: "1", 
                msg: `Reconnect failed: ${initError.message}`,
                status: "FAILED"
            });
        }
        
        // Store wrapper
        activeWrappers.set(sessionId, wrapper);
        registerConnectedAllowlistLocal(sessionId);
        
        res.json({ 
            error: "0", 
            msg: "Reconnect initiated successfully",
            status: "RECONNECTING"
        });
        
    } catch (error) {
        console.error('? Reconnect error:', error);
        res.json({ error: "1", msg: error.message });
    }
});


// ============================================================================
// �"� AUTO-RESTORE EXISTING SESSIONS ON STARTUP
// ============================================================================
/** Folder session Baileys: sess_{userId}_{port}_{timestamp...} */
function isLikelyBaileysSessionId(s) {
    if (typeof s !== 'string' || !s.startsWith('sess_')) return false;
    const p = s.split('_');
    return p.length >= 4;
}

function extractSessionIdsFromTextBlob(str) {
    const found = new Set();
    if (typeof str !== 'string' || !str.includes('sess_')) return found;
    const re = /\bsess_[A-Za-z0-9_]+\b/g;
    let m;
    while ((m = re.exec(str)) !== null) {
        if (isLikelyBaileysSessionId(m[0])) {
            found.add(m[0]);
        }
    }
    return found;
}

/**
 * Parser payload API connected sessions — banyak bentuk PHP (msg nested, error/0, rows, bot, JSON string).
 */
function extractSessionIdsFromWarmupApiPayload(data) {
    const out = new Set();
    const maxDepth = 14;

    const addOne = (v) => {
        if (typeof v !== 'string') return;
        const t = v.trim();
        if (isLikelyBaileysSessionId(t)) {
            out.add(t);
        }
    };

    const walk = (node, d) => {
        if (node == null || d > maxDepth) return;

        if (typeof node === 'string') {
            addOne(node);
            for (const x of extractSessionIdsFromTextBlob(node)) {
                out.add(x);
            }
            const t = node.trim();
            if ((t.startsWith('{') && t.endsWith('}')) || (t.startsWith('[') && t.endsWith(']'))) {
                try {
                    walk(JSON.parse(t), d + 1);
                } catch {
                    /* ignore */
                }
            }
            return;
        }

        if (Array.isArray(node)) {
            for (const item of node) {
                walk(item, d + 1);
            }
            return;
        }

        if (typeof node !== 'object') return;

        const idKeys = [
            'session_id',
            'sessionId',
            'session',
            'id',
            'bot',
            'bot_session',
            'folder',
            'session_folder',
            'dir',
            'path',
            'nama_sess',
            'sess_id',
            'whatsapp_session'
        ];
        for (const k of idKeys) {
            if (node[k] != null) {
                walk(node[k], d + 1);
            }
        }

        const arrayKeys = [
            'sessions',
            'data',
            'msg',
            'result',
            'rows',
            'items',
            'list',
            'records',
            'bots',
            'connected',
            'active',
            'payload',
            'values',
            'session_ids',
            'sessionIds',
            'connected_sessions',
            'active_sessions'
        ];
        for (const k of arrayKeys) {
            if (node[k] != null) {
                walk(node[k], d + 1);
            }
        }

        if (node.error === '0' || node.error === 0) {
            walk(node.msg, d + 1);
            walk(node.data, d + 1);
        }

        for (const v of Object.values(node)) {
            if (v != null && (typeof v === 'object' || typeof v === 'string')) {
                walk(v, d + 1);
            }
        }
    };

    if (!data) return [];
    walk(data, 0);
    return [...out];
}

/**
 * @returns {{ ok: true, ids: string[] } | { ok: false, ids: [], error: string }}
 */
async function fetchWarmupConnectedSessionIdsResult() {
    const sw = config.sessionWarmup || {};
    if (!sw.enabled || !sw.apiUrl) {
        return { ok: true, ids: [] };
    }
    const method = String(sw.apiMethod || 'get').toLowerCase();
    const timeout = sw.apiTimeoutMs ?? 20000;
    const headers = {
        'X-Broadcast-Key': sw.apiKey,
        'X-API-Key': sw.apiKey
    };
    try {
        let res;
        if (method === 'post') {
            res = await axios.post(sw.apiUrl, { api_key: sw.apiKey }, { headers, timeout });
        } else {
            res = await axios.get(sw.apiUrl, { params: { api_key: sw.apiKey }, headers, timeout });
        }
        const ids = extractSessionIdsFromWarmupApiPayload(res.data);
        console.log(`📡 sessionWarmup API: ${ids.length} session id(s) (connected)`);
        if (
            ids.length === 0 &&
            (process.env.RESTORE_WARMUP_DEBUG === '1' || process.env.RESTORE_WARMUP_DEBUG === 'true')
        ) {
            try {
                const preview =
                    typeof res.data === 'string'
                        ? res.data.slice(0, 600)
                        : JSON.stringify(res.data).slice(0, 800);
                console.warn(`⚠️ [WARMUP-DEBUG] 0 ids parsed. Preview: ${preview}`);
            } catch {
                console.warn(`⚠️ [WARMUP-DEBUG] 0 ids parsed (could not stringify body)`);
            }
        }
        return { ok: true, ids };
    } catch (e) {
        console.error(`❌ sessionWarmup API failed (${sw.apiUrl}):`, e.message);
        return { ok: false, ids: [], error: e.message || String(e) };
    }
}

async function fetchWarmupConnectedSessionIds() {
    const r = await fetchWarmupConnectedSessionIdsResult();
    return r.ok ? r.ids : [];
}

function collectNewScanRestoreSessionIds(sessionsDir) {
    const sw = config.sessionWarmup || {};
    if (!sw.includeNewScanOnRestore) {
        return [];
    }
    const windowMs = sw.newScanRestoreWindowMs ?? 72 * 60 * 60 * 1000;
    const now = Date.now();
    const found = [];
    if (!fs.existsSync(sessionsDir)) {
        return found;
    }
    for (const name of fs.readdirSync(sessionsDir)) {
        if (!name.startsWith('sess_')) continue;
        const fullPath = path.join(sessionsDir, name);
        if (!fs.statSync(fullPath).isDirectory()) continue;
        const credsPath = path.join(fullPath, 'creds.json');
        if (!fs.existsSync(credsPath)) continue;
        const mtime = fs.statSync(credsPath).mtimeMs;
        if (now - mtime <= windowMs) {
            found.push(name);
        }
    }
    console.log(`🆕 New-scan restore window: ${found.length} recent session(s) (creds mtime <= ${Math.round(windowMs / 3600000)}h)`);
    return found;
}

function buildRestoreSessionIdList(sessionsDir, apiIds, newIds) {
    const sw = config.sessionWarmup || {};
    const max = sw.maxSessions ?? 200;
    const mergeNew = sw.includeNewScanOnRestore === true;
    const seen = new Set();
    const ordered = [];
    const accept = (id) => {
        if (!id || seen.has(id)) return false;
        if (!sessionIdBelongsToThisShard(id)) return false;
        seen.add(id);
        return true;
    };
    for (const id of apiIds) {
        if (!accept(id)) continue;
        ordered.push(id);
        if (ordered.length >= max) break;
    }
    if (ordered.length < max && mergeNew) {
        for (const id of newIds) {
            if (!accept(id)) continue;
            ordered.push(id);
            if (ordered.length >= max) break;
        }
    }
    return ordered.filter((id) => {
        const credsPath = path.join(sessionsDir, id, 'creds.json');
        return fs.existsSync(credsPath);
    });
}

async function restoreExistingSessions() {
    try {
        const sessionsDir = path.join(process.cwd(), 'sessions');
        
        if (!fs.existsSync(sessionsDir)) {
            console.log('📁 No sessions directory found, skipping restore');
            return;
        }
        
        const apiIds = await fetchWarmupConnectedSessionIds();
        const apiAll = Array.isArray(apiIds) ? apiIds : [];
        const skipOtherShard = apiAll.filter((id) => !sessionIdBelongsToThisShard(id)).length;
        // 🔌 RECONCILE: api_connected tapi folder sesi hilang → mustahil restore.
        // Hanya session rumah shard ini — jangan spam DISCONNECTED untuk port tetangga.
        try {
            const phantomIds = apiAll.filter((id) => {
                if (!sessionIdBelongsToThisShard(id)) return false;
                try { return !fs.existsSync(path.join(sessionsDir, id)); } catch (_) { return false; }
            });
            if (phantomIds.length > 0) {
                console.log(`🔌 [WARMUP-RECONCILE] ${phantomIds.length} api-connected session(s) tanpa folder → emit DISCONNECTED`);
                let __ri = 0;
                for (const sessionId of phantomIds) {
                    const __m = String(sessionId).match(/sess_(\d+)_(\d+)_/);
                    const __uid = __m ? __m[1] : null;
                    const __port = __m ? parseInt(__m[2]) : null;
                    const __delay = (__ri++) * 60;
                    setTimeout(() => {
                        dispatchWebhook({
                            event: 'DISCONNECTED',
                            sessionId,
                            reason: 'SESSION_NOT_FOUND',
                            user_id: __uid,
                            port: __port,
                            reconnectAttempt: 0,
                            isLoggedOut: false,
                            canReconnect: false,
                            reason_detail: 'Warmup reconcile: api_connected but no session folder on disk'
                        }, sessionId, 'DISCONNECTED').catch(() => {});
                    }, __delay);
                }
            }
        } catch (reconErr) {
            console.error('❌ [WARMUP-RECONCILE]:', reconErr?.message || reconErr);
        }
        const newIds = collectNewScanRestoreSessionIds(sessionsDir);
        const sessionFolders = buildRestoreSessionIdList(sessionsDir, apiIds, newIds);
        
        if (sessionFolders.length === 0) {
            console.log('🔄 Restore: no candidates (API list empty/failed, no new-scan sessions). Skipping full disk load.');
            return;
        }
        
        const warmupSrc = config.sessionWarmup?.includeNewScanOnRestore ? 'API + new-scan window' : 'API connected only';
        const toRestore = sessionFolders.filter((sessionId) => {
            if (!sessionIdBelongsToThisShard(sessionId)) {
                return false;
            }
            const credsPath = path.join(sessionsDir, sessionId, 'creds.json');
            if (!fs.existsSync(credsPath)) {
                return false;
            }
            if (activeWrappers.has(sessionId)) {
                return false;
            }
            return true;
        });

        let configured = config.sessionWarmup?.restoreConcurrency;
        if (configured == null || configured === 0) {
            configured = 18;
        }
        const concurrency = Math.max(1, Math.min(Number(configured) || 1, toRestore.length || 1));
        const delayMs = config.sessionWarmup?.delayBetweenLoads ?? 80;
        const sw = config.sessionWarmup || {};
        const staggerBase = Math.max(0, Number(sw.restoreStartStaggerMs) || 0);
        const staggerCap = Math.max(staggerBase, Number(sw.restoreStaggerCapMs) || 8000);

        console.log(
            `🔒 [SHARD-RESTORE] port=${PORT} keep=${toRestore.length} skipOtherShard=${skipOtherShard} (${warmupSrc}, max ${config.sessionWarmup?.maxSessions ?? 200}, concurrency ${concurrency}, stagger ${staggerBase}ms cap ${staggerCap}ms)`
        );

        async function restorePoolMap(items, limit, fn) {
            if (items.length === 0) return;
            const n = Math.max(1, Math.min(limit, items.length));
            let cursor = 0;
            async function worker() {
                while (true) {
                    const i = cursor++;
                    if (i >= items.length) break;
                    const stagger = Math.min(staggerCap, staggerBase * i);
                    if (stagger > 0) {
                        await new Promise((r) => setTimeout(r, stagger));
                    }
                    try {
                        await fn(items[i]);
                    } catch (err) {
                        console.error(`❌ [RESTORE] ${items[i]}:`, err?.message || err);
                    }
                }
            }
            await Promise.all(Array.from({ length: n }, () => worker()));
        }

        await restorePoolMap(toRestore, concurrency, async (sessionId) => {
            if (!sessionIdBelongsToThisShard(sessionId)) {
                console.warn(`⏭️ [SHARD-RESTORE] skip foreign ${sessionId} on port ${PORT}`);
                return;
            }
            const credsPath = path.join(sessionsDir, sessionId, 'creds.json');
            if (!fs.existsSync(credsPath) || activeWrappers.has(sessionId)) {
                return;
            }

            console.log(`🔄 Restoring session: ${sessionId}`);

            const userIdMatch = sessionId.match(/sess_(\d+)_/);
            const userId = userIdMatch ? userIdMatch[1] : null;

            const wrapper = new BaileysWrapper(sessionId, sessionManager, {
                onQR: (qr) => console.log(`📱 QR for ${sessionId}`),
                onPairingCode: (data) => {
                    if (!data) return;
                    const { code, phoneNumber: normalizedPhone, rawPhoneNumber } = data;
                    console.log(`🔑 Pairing code for ${sessionId}: ${code}${normalizedPhone ? ` (normalized: ${normalizedPhone})` : ''}`);
                    if (rawPhoneNumber && normalizedPhone && rawPhoneNumber !== normalizedPhone) {
                        console.log(`   Raw phone: ${rawPhoneNumber}`);
                    }
                },
                onConnected: async () => {
                    console.log(`✅ Restored session connected: ${sessionId} (CONNECTED webhook handled by wrapper.js)`);
                },
                onDisconnected: async (reason) => onWrapperDisconnected(sessionId, reason),
                onBannedImmediate: async (reason) => onWrapperBannedImmediate(sessionId, reason),
                onMessageSent: (msg) => {}
            });

            try {
                await wrapper.initialize();
            } catch (initError) {
                console.error(`❌ [RESTORE] Error initializing session ${sessionId}:`, initError.message);
                if (loadingSessions.has(sessionId)) {
                    loadingSessions.delete(sessionId);
                }
                return;
            }

            activeWrappers.set(sessionId, wrapper);
            console.log(`✅ Session restored: ${sessionId}`);

            if (concurrency <= 1 && delayMs > 0) {
                await new Promise((resolve) => setTimeout(resolve, delayMs));
            }
        });
        
        const evicted = evictWrappersNotBelongingToThisShard();
        const iso = getShardIsolationStats();
        console.log(
            `✅ [SHARD-RESTORE] done port=${PORT} active=${activeWrappers.size} own=${iso.own} foreign=${iso.foreign} evictedForeign=${evicted}`
        );
        
    } catch (error) {
        console.error('❌ Error in session restore:', error.message);
    }
}

// ✅ FIX: Error handling untuk HTTP server (prevent crash saat port sudah digunakan)
server.on('error', (error) => {
    if (error.code === 'EADDRINUSE') {
        console.warn(`⚠️  HTTP port ${PORT} already in use - another instance may be running`);
        console.warn(`💡 To kill existing process: lsof -ti:${PORT} | xargs kill -9`);
        // ✅ Don't crash - process will continue but server won't start
        // ✅ Systemd will detect and restart properly
    } else {
        console.error(`❌ HTTP server error:`, error.message);
    }
});

// Start server
// Penting: bind ke IPv4 juga. Nginx proxy_pass pakai 127.0.0.1 (IPv4); kalau Node hanya listen di IPv6, akan 502/timeout.
server.listen(PORT, '0.0.0.0', async () => {
    console.log(`🚀 Baileys server running on port ${PORT}`);

    // Connect Redis sejak boot, jangan tunggu sesi pertama: /baileys/health harus
    // melaporkan status Redis yang sebenarnya sejak awal supaya watchdog tidak salah
    // menyimpulkan "redis dead" lalu me-restart shard yang sehat.
    ensureSharedRedis()
        .then((r) => console.log(r
            ? '✅ [SHARED-REDIS] Eager connect saat boot OK'
            : '⚠️ [SHARED-REDIS] Eager connect saat boot gagal — akan dicoba lagi'))
        .catch(() => {});
    if (WS_EXTERNAL) {
        console.log(`ℹ️  WS external hub — shard ${PORT} HTTP only (broadcast via Redis → baileys-ws-hub)`);
    } else if (PORT == 7001 || PORT == '7001') {
        if (wss) {
            console.log(`🔌 WebSocket server running on port ${WS_PORT} (SHARED for all instances)`);
        } else {
            console.log(`⚠️  WebSocket server not available (port ${WS_PORT} may be in use)`);
        }
    } else {
        console.log(`ℹ️  Using shared WebSocket on port ${WS_PORT} via port 7001`);
    }
    console.log(`📊 Health: http://localhost:${PORT}/health`);
    
    // Auto-restore per shard: hanya session rumah PORT ini (bukan global di 7001).
    if (IS_SHARD_HTTP_PORT) {
        const baseDelay = config.sessionWarmup?.restoreStartupDelayMs ?? 300;
        const shardOffset = Math.max(0, Number(PORT) - SHARD_PORT_MIN) * (Number(config.sessionWarmup?.restoreShardOffsetMs) || 250);
        const restoreStartupDelayMs = baseDelay + shardOffset;
        setTimeout(async () => {
            evictWrappersNotBelongingToThisShard();
            if (isEnforceConnectedAllowlist()) {
                await refreshConnectedAllowlistFromApi(true).catch(() => {});
            }
            console.log(`\n🔄 Starting auto-restore of existing sessions (port=${PORT} only)...`);
            restoreExistingSessions().then(() => {
                setTimeout(() => {
                    syncPortCounter();
                    evictWrappersNotBelongingToThisShard();
                    if (isEnforceConnectedAllowlist()) {
                        evictWrappersNotInConnectedAllowlist();
                    }
                }, 1500);
            });
        }, restoreStartupDelayMs);
    }
});

// ============================================================================
// ?? UTILITY: Re-assign Proxy to Connected Bots (for migration)
// ============================================================================
app.post('/admin/reassign-proxies', async (req, res) => {
    try {
        const { adminKey } = req.body;
        
        // Security check
        if (adminKey !== config.admin.proxyReassignKey) {
            return res.status(403).json({ error: "1", msg: 'Unauthorized' });
        }
        
        let reassigned = 0;
        let failed = 0;
        
        // Get all active wrappers
        for (const [sessionId, wrapper] of activeWrappers.entries()) {
            try {
                // Skip if already has proxy
                if (wrapper.proxyInfo) {
                    console.log(`?? Skipping ${sessionId} - already has proxy`);
                    continue;
                }
                
                // Fetch proxy with auth
                const proxyUrl = config.proxy.secondaryUrl;
                const proxyApiKey = config.proxy.apiKey;
                
                const proxyPayloadReq = {
                    passwordnya: proxyApiKey,
                    sessionId: sessionId
                };
                const proxyResponse = await axios.post(proxyUrl, proxyPayloadReq, {
                    headers: generateWebhookHeaders(sessionId, proxyPayloadReq),
                    timeout: config.proxy.timeoutMs
                });
                
                if (proxyResponse.data.error === "0" && proxyResponse.data.proxy) {
                    const proxyAddress = proxyResponse.data.proxy;
                    const proxyId = proxyResponse.data.proxyId;
                    
                    // Save to wrapper
                    wrapper.proxyInfo = {
                        address: proxyAddress,
                        proxyId: proxyId
                    };
                    
                    // Send to webhook to update database
                    const proxyPayload = {
                        event: 'PROXY_ASSIGNED',
                        sessionId: sessionId,
                        proxyId: proxyId,
                        proxyAddress: proxyAddress.replace(/:[^:@]*@/, ':***@')
                    };
                    dispatchWebhook(proxyPayload, sessionId, 'PROXY_ASSIGNED');
                    
                    reassigned++;
                    console.log(`? Proxy assigned to ${sessionId}: Proxy ID ${proxyId}`);
                } else {
                    failed++;
                    console.warn(`⚠️ Failed to get proxy for ${sessionId}`);
                }
                
                // Small delay to avoid overwhelming proxy API
                await new Promise(resolve => setTimeout(resolve, 100));
                
            } catch (error) {
                failed++;
                console.error(`? Error assigning proxy to ${sessionId}:`, error.message);
            }
        }
        
        res.json({ 
            error: "0", 
            msg: `Proxy reassignment complete`,
            reassigned: reassigned,
            failed: failed,
            total: activeWrappers.size
        });
        
    } catch (error) {
        console.error('Error in reassign-proxies:', error);
        res.status(500).json({ error: "1", msg: error.message });
    }
});

// Graceful shutdown (global.isShuttingDown + massDisconnectWindow sudah di init di awal file)
// ✅ isShuttingDown (module) dideklarasikan di atas — dipakai interval MEMORY_RESTART + handler ini

async function gracefulShutdown(signal) {
    if (isShuttingDown) {
        console.log(`⏭️  Shutdown already in progress, ignoring ${signal}`);
        return;
    }
    
    isShuttingDown = true;
    rssRestartStreak = 0;
    // ✅ CRITICAL: Set global flag SEBELUM disconnect events terjadi
    markRealShutdown();
    console.log(`🛑 [${signal}] Global shutdown flag set - preventing false DISCONNECTED webhooks`);
    console.log(`🛑 [${signal}] Graceful shutdown initiated...`);
    
    const shutdownTimeout = setTimeout(() => {
        console.error('❌ Shutdown timeout (45s) - forcing exit');
        process.exit(1);
    }, 45000);
    
    try {
        // ✅ STEP 1: Tutup socket saja — JANGAN sock.logout() (itu logout dari server WA & bikin scan QR lagi).
        // smartDisconnect + creds di disk = setelah systemd restart, restore bisa jalan normal.
        console.log(`📦 Disconnecting ${activeWrappers.size} active session(s) (preserve WhatsApp login)...`);
        const disconnectPromises = [];

        for (const [sessionId, wrapper] of activeWrappers.entries()) {
            try {
                console.log(`🔄 Smart-disconnect: ${sessionId}`);
                disconnectPromises.push(
                    Promise.race([
                        wrapper
                            .smartDisconnect({ force: true, reason: `${signal} shutdown` })
                            .catch((err) => {
                                console.warn(`⚠️  Error disconnecting session ${sessionId}:`, err.message);
                            }),
                        new Promise((resolve) => setTimeout(resolve, 5000))
                    ])
                );
            } catch (err) {
                console.warn(`⚠️  Error preparing disconnect for session ${sessionId}:`, err.message);
            }
        }

        await Promise.allSettled(disconnectPromises);
        activeWrappers.clear();
        loadingSessions.clear();
        console.log(`✅ All sessions disconnected (creds unchanged on disk)`);
        
        // ✅ STEP 2: Outbox sudah di-stop per session di smartDisconnect; wrapper map sudah dikosongkan.
        console.log(`💾 Session wrappers cleared (shared Redis / outbox tetap jalan di proses lain jika ada)`);
        
        // ✅ STEP 3: Clear all cleanup intervals
        if (wsCleanupInterval) {
            clearInterval(wsCleanupInterval);
            wsCleanupInterval = null;
            console.log('⏰ WebSocket cleanup interval cleared');
        }
        if (ramCleanupInterval) {
            clearInterval(ramCleanupInterval);
            ramCleanupInterval = null;
            console.log('⏰ RAM cleanup interval cleared');
        }
        if (memoryMonitorInterval) {
            clearInterval(memoryMonitorInterval);
            memoryMonitorInterval = null;
            console.log('⏰ Memory monitor interval cleared');
        }
        
        // ✅ STEP 4: Close all WebSocket connections
        if (wss) {
            console.log(`🔌 Closing WebSocket connections...`);
            sessionWebSockets.forEach((clients) => {
                clients.forEach((client) => {
                    // Clear ping intervals
                    if (client.pingInterval) {
                        clearInterval(client.pingInterval);
                    }
                    // Close WebSocket
                    if (client.ws) {
                        client.ws.close();
                    }
                });
            });
            wss.close(() => {
                console.log('✅ WebSocket server closed');
            });
        }
        
        // ✅ STEP 5: Close HTTP server
        server.close(() => {
            clearTimeout(shutdownTimeout);
            console.log('✅ HTTP server closed');
            console.log(`✅ [${signal}] Graceful shutdown completed`);
            process.exit(0);
        });
        
        // Force close after 10 seconds if server.close() doesn't callback
        setTimeout(() => {
            clearTimeout(shutdownTimeout);
            console.log('⚠️  Server close timeout, forcing exit');
            process.exit(0);
        }, 10000);
        
    } catch (error) {
        clearTimeout(shutdownTimeout);
        console.error(`❌ Error during graceful shutdown:`, error.message);
        process.exit(1);
    }
}

// ✅ FIX: Single handler untuk SIGINT
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// ✅ FIX: Tambahkan handler untuk SIGTERM (yang digunakan systemd)
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// ✅ CRITICAL: Set global flag saat process akan exit (untuk hard crash detection)
process.on('exit', (code) => {
    markRealShutdown();
    console.log(`🛑 Process exiting with code ${code}`);
});

// ✅ CRITICAL: Set flag di beforeExit (terpanggil sebelum exit, termasuk untuk hard crash)
process.on('beforeExit', (code) => {
    markRealShutdown();
    console.log(`🛑 [beforeExit] Global shutdown flag set (code: ${code})`);
});

// ✅ CRITICAL: Handle untuk OOM (Out of Memory) - set flag sebelum process mati
// Note: OOM handler tidak bisa prevent crash, tapi bisa set flag
if (process.memoryUsage) {
    // Monitor memory usage untuk detect potential OOM (satu interval proses; di-clear saat gracefulShutdown)
    memoryMonitorInterval = setInterval(() => {
        try {
            const mem = process.memoryUsage();
            const heapUsedMB = mem.heapUsed / 1024 / 1024;
            const heapTotalMB = mem.heapTotal / 1024 / 1024;
            const rssMB = mem.rss / 1024 / 1024;

            // Selaras cgroup + MEMORY_RESTART_* (heap graceful ~3000 + heap-watchdog) — sesuaikan jika ganti fleet-slice.
            if (rssMB > 3800 || heapUsedMB > 2600) {
                console.warn(
                    `⚠️ [MEMORY] High memory usage detected: RSS=${rssMB.toFixed(0)}MB, Heap=${heapUsedMB.toFixed(0)}MB/${heapTotalMB.toFixed(0)}MB`
                );

                if ((rssMB > 4300 || heapUsedMB > 3600) && typeof global !== 'undefined') {
                    // Lonjakan memori belum tentu berujung mati; GC bisa menurunkannya lagi.
                    // Pakai redaman berjangka supaya flag tidak menempel selamanya.
                    suppressDisconnectWebhooks(
                        `memori kritis RSS=${rssMB.toFixed(0)}MB heap=${heapUsedMB.toFixed(0)}MB`,
                        30000
                    );
                }
            }
        } catch (err) {
            // Ignore errors in memory monitoring
        }
    }, 30000);
}

// ---------------------------------------------------------------------------
// Global error handlers to keep process alive and log issues separately
// ---------------------------------------------------------------------------

const criticalErrorQueue = [];
let criticalErrorProcessing = false;

function logCriticalError(data) {
    // Ensure we always have structured data
    const payload = {
        event: data?.event || 'CRITICAL_ERROR',
        error: data?.error || data?.reason || 'Unknown error',
        stack: data?.stack,
        timestamp: data?.timestamp || new Date().toISOString(),
        additional: data
    };
    // Local logging split from webhook dispatch
    console.error('🚨 [CRITICAL LOG]', JSON.stringify(payload));

    criticalErrorQueue.push(payload);
    if (!criticalErrorProcessing) {
        criticalErrorProcessing = true;
        setImmediate(processCriticalErrorQueue);
    }
}

async function processCriticalErrorQueue() {
    while (criticalErrorQueue.length > 0) {
        const item = criticalErrorQueue.shift();
        try {
            await sendWebhookWithLogging(item, 'CRITICAL', item.event || 'CRITICAL_ERROR');
        } catch (err) {
            console.error('🚨 [CRITICAL] Failed to send critical error webhook:', err.message);
        }
    }
    criticalErrorProcessing = false;
}

// ✅ REMOVED: Duplicate handler - sudah ada di atas dengan prevent crash
// Handler di atas sudah handle uncaughtException dan unhandledRejection dengan prevent crash


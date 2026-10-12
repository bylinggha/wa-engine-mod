/**
 * Matikan noise ke stdout/journal (console.log/warn/info) — ringan di event loop &
 * hindari "blob data" dari Buffer di journald.
 *
 * Aktif jika salah satu:
 *   BAILEYS_LOG_QUIET=1 | true | yes
 *   QUIET_CONSOLE=true (kompatibel lama)
 *
 * Tag operasional (SEND-VERIFY, OUTBOUND-DEAF, DEGRADED, …) tetap lewat.
 *
 * console.error tetap jalan (startup fatal / exception). Opsional matikan:
 *   BAILEYS_LOG_ERRORS_SILENT=1
 *
 * Log LID ke journal (F.3.1) terpisah — default mati:
 *   LID_SEND_LOG=0  (prod) | LID_SEND_LOG=1 (triage sementara)
 *   Metrik Redis tanpa journal: LID_SEND_METRICS=1 (default)
 *
 * Env dari file .env: dotenv di-load di sini dulu (sebelum import lain di index.js).
 */
import dotenv from 'dotenv';

dotenv.config();

function truthy(v) {
    if (v == null || v === '') return false;
    const s = String(v).trim().toLowerCase();
    return s === '1' || s === 'true' || s === 'yes' || s === 'on';
}

const noop = () => {};

const quiet =
    truthy(process.env.BAILEYS_LOG_QUIET) ||
    String(process.env.QUIET_CONSOLE || '').trim().toLowerCase() === 'true';

const origLog = console.log.bind(console);
const origWarn = console.warn.bind(console);
const origError = console.error.bind(console);

const CRITICAL_MARKERS = [
    '[SEND-VERIFY]',
    '[OUTBOUND-DEAF]',
    '[HARD-RECYCLE]',
    '[DEAF-SESSION]',
    '[ON-WA-PROBE]',
    '[SESSION-MAX-AGE]',
    '[DEGRADED]',
    '[ECHO-VERIFY]',
    '[CACHE-STATS]',
    '[SHUTDOWN-FLAG]',
    '[SHARED-REDIS]',
    '[SHARED-REDIS-CONNECTED]',
    '[SHARD-ISOLATION]',
    // Tanpa penanda pairing di sini, seluruh jejaknya hilang dari journal sehingga keluhan
    // "Gagal menautkan perangkat" mustahil ditelusuri: tidak terlihat kapan kode dibuat,
    // kapan diganti, dan kenapa CONNECTED ditolak.
    '[PAIRING]',
    '[PAIR]',
    '[CONNECTED-GATE]',
    'buang lokal saja, tanpa sock.logout()',
    'CONNECTED gated',
    'CONNECTED webhook was SKIPPED',
    '[PAIR-401]',
    '[CREDS.UPDATE]',
    'PAIRING CODE GENERATED',
    'PAIRING_CONNECTED_TOO_EARLY',
    // Tanpa ini keberhasilan pair tidak terlihat sama sekali, sehingga rasio sukses
    // vs gagal mustahil diukur saat menelusuri keluhan pairing.
    'CONNECTED & AUTHENTICATED',
    '[AUTH]',
    '[AUTH-POLL]',
    // Versi WA Web menentukan apakah tautan perangkat bisa selesai: fetchLatestBaileysVersion
    // mengembalikan revisi basi sambil mengaku isLatest, dan WhatsApp menolak menuntaskan
    // pairing memakai revisi itu ("Couldn't link device"). Tanpa penanda ini jalur mana yang
    // dipakai tiap shard tidak terlihat sama sekali, jadi kegagalan pairing musiman akibat
    // fallback ke revisi basi mustahil dibuktikan.
    'WA Web version',
    'Latest version fetched',
    'Using latest version',
    'Failed to fetch latest WA web version',
    'Reusing last cached version',
    'Using dynamic Baileys version',
    'Failed to load byee-wamod',
    // Siklus hidup shard & pemulihan sesi (sekali per start, bukan spam): tanpa ini restart shard
    // tidak bisa ditelusuri — tidak terlihat berapa sesi diminta ke API, dipulihkan, atau gagal.
    'running on port',
    'auto-restore',
    'sessionWarmup',
    '[WARMUP',
    'Restore:',
    'Restoring session',
    'Session restored',
    'Restored session connected',
    '[RESTORE]',
    '[SHARD-RESTORE]',
    'New-scan restore',
    'Graceful shutdown',
    'Disconnecting ',
    'All sessions disconnected',
    '[UNHANDLED]',
    'MEMORY_RESTART',
    'EVENT_LOOP',
    'Max reconnect',
    '[LOGOUT-FROM-PHONE]',
    '[REACHOUT',
    '[MESSAGE-CAPPING',
    'REACHOUT_TIMELOCK',
    '[DRAIN',
    'draining',
    '📤🦻',
    '🦻 [',
    '🔄 [HARD-RECYCLE]',
];

function isOperationalLog(args) {
    for (const a of args) {
        if (typeof a !== 'string') continue;
        if (CRITICAL_MARKERS.some((m) => a.includes(m))) return true;
    }
    return false;
}

function isThumbnailDebugNoise(args) {
    for (const a of args) {
        if (typeof a === 'string' && a.includes('[THUMBNAIL-DEBUG]')) return true;
    }
    return false;
}

if (quiet) {
    console.log = (...args) => {
        if (isOperationalLog(args)) origLog(...args);
    };
    console.info = noop;
    console.debug = noop;
    console.trace = noop;
    console.warn = (...args) => {
        if (isOperationalLog(args)) origWarn(...args);
    };
}

if (truthy(process.env.BAILEYS_LOG_ERRORS_SILENT)) {
    // Tag operasional tetap diloloskan. Mematikan console.error sepenuhnya membuat kegagalan
    // Redis/pairing hilang total dari journal, dan insiden jadi tidak bisa ditelusuri.
    console.error = (...args) => {
        if (isOperationalLog(args)) origError(...args);
    };
} else if (quiet) {
    // byee-wamod memakai console.error untuk [THUMBNAIL-DEBUG] → banjiri journald
    console.error = (...args) => {
        if (isThumbnailDebugNoise(args)) return;
        if (isOperationalLog(args)) {
            origError(...args);
            return;
        }
        origError(...args);
    };
}

export default {};

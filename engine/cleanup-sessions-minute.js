#!/usr/bin/env node
/**
 * Cleanup session corrupt / orphan / banned — batch per menit (cron).
 * Rotasi scan supaya 150k+ folder tidak block event loop sekaligus.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import axios from 'axios';
import config from './config/config.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Cron: cd /opt/baileys → cwd/sessions OK. Fallback: sibling of server-rapih.
const SESSIONS_DIR = process.env.CLEANUP_SESSIONS_DIR
    || (fs.existsSync(path.join(process.cwd(), 'sessions'))
        ? path.join(process.cwd(), 'sessions')
        : path.join(__dirname, '..', 'sessions'));
const STATE_DIR = process.env.CLEANUP_MINUTE_STATE_DIR || '/opt/baileys/tmp/session-cleanup-state';
const STATE_FILE = path.join(STATE_DIR, 'cursor.json');

function envInt(name, def, min, max) {
    const raw = process.env[name];
    if (raw == null || String(raw).trim() === '') return def;
    const n = parseInt(raw, 10);
    if (!Number.isFinite(n)) return def;
    return Math.min(max, Math.max(min, n));
}

const BATCH = envInt('CLEANUP_MINUTE_BATCH', 4000, 200, 25000);
const ORPHAN_MINUTES = envInt('CLEANUP_ORPHAN_MINUTES', 1440, 60, 10080);
const MAX_DELETE = envInt('CLEANUP_MINUTE_MAX_DELETE', 500, 50, 10000);
// Minimum umur folder/creds sebelum BOLEH dihapus (default 24 jam = kemarin ke bawah).
const MIN_DELETE_AGE_MINUTES = envInt('CLEANUP_MIN_DELETE_AGE_MINUTES', 1440, 60, 10080);
// Session di disk yang TIDAK ada di api_connected — default MATI (0).
// Jangan 5 menit: pairing/reconnect race + creds.mtime bukan umur session.
const NOT_IN_API_MINUTES = envInt('CLEANUP_NOT_IN_API_MINUTES', 0, 0, 1440);
// Probe RAM shard sebelum hapus (pelindung "jangan hapus yang sedang jalan"). 1 = nyala.
const LIVE_PROBE = process.env.CLEANUP_LIVE_PROBE !== '0';
const LIVE_PROBE_TIMEOUT_MS = envInt('CLEANUP_LIVE_PROBE_TIMEOUT_MS', 2500, 500, 15000);
// Hitung saja, jangan hapus. Dipakai untuk mengukur dampak sebelum dinyalakan.
const DRY_RUN = process.env.CLEANUP_MINUTE_DRY_RUN === '1';

const BANNED_MARKERS = ['.banned', 'banned.json', 'session-banned.json', 'session-banned'];

function loadState() {
    try {
        if (fs.existsSync(STATE_FILE)) {
            return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
        }
    } catch { /* ignore */ }
    return { skip: 0 };
}

function saveState(st) {
    fs.mkdirSync(STATE_DIR, { recursive: true });
    fs.writeFileSync(STATE_FILE, JSON.stringify(st));
}

function isBannedDir(sessionPath) {
    for (const m of BANNED_MARKERS) {
        if (fs.existsSync(path.join(sessionPath, m))) return true;
    }
    return false;
}

function isCorruptCreds(credsPath) {
    if (!fs.existsSync(credsPath)) return { corrupt: false };
    try {
        const raw = fs.readFileSync(credsPath, 'utf8');
        if (!raw || !raw.trim()) return { corrupt: true, reason: 'empty creds' };
        let creds;
        try {
            creds = JSON.parse(raw);
        } catch {
            return { corrupt: true, reason: 'invalid json' };
        }
        if (!creds || typeof creds !== 'object') return { corrupt: true, reason: 'bad structure' };
        const meId = creds?.me?.id;
        if (!meId) return { corrupt: true, reason: 'no me.id' };
        if (String(meId).includes('sess_')) return { corrupt: true, reason: 'bad me.id' };
        if (!String(meId).includes('@')) return { corrupt: true, reason: 'bad jid' };
        if (creds.loggedOut === true || creds.wasLoggedOut === true) {
            return { corrupt: true, reason: 'logged out' };
        }
        return { corrupt: false };
    } catch (e) {
        return { corrupt: true, reason: e.message };
    }
}

async function fetchActiveIds() {
    const sw = config.sessionWarmup || {};
    if (!sw.apiUrl) return { ids: new Set(), apiOk: false };
    const apiKey = sw.apiKey || '';
    const headers = { 'X-Broadcast-Key': apiKey, 'X-API-Key': apiKey };
    try {
        const method = String(sw.apiMethod || 'get').toLowerCase();
        const res = method === 'post'
            ? await axios.post(sw.apiUrl, { api_key: apiKey }, { headers, timeout: sw.apiTimeoutMs ?? 15000 })
            : await axios.get(sw.apiUrl, { params: { api_key: apiKey }, headers, timeout: sw.apiTimeoutMs ?? 15000 });
        const blob = typeof res.data === 'string' ? res.data : JSON.stringify(res.data);
        const ids = new Set();
        const re = /\bsess_[A-Za-z0-9_]+\b/g;
        let m;
        while ((m = re.exec(blob)) !== null) {
            if (m[0].split('_').length >= 4) ids.add(m[0]);
        }
        return { ids, apiOk: true };
    } catch {
        return { ids: new Set(), apiOk: false };
    }
}

/**
 * Sesi yang sedang HIDUP di RAM shard tidak boleh dihapus, walaupun status DB-nya
 * belum CONNECTED (pair baru, RECONNECTING, webhook telat). API saja tidak cukup.
 * Endpoint session-status tanpa load=1 hanya membaca activeWrappers — tidak memuat sesi.
 * Kalau probe gagal/timeout: dianggap HIDUP supaya kesalahan jaringan tidak menghapus sesi.
 */
async function isLiveOnShard(sessionId) {
    if (!LIVE_PROBE) return false;
    const m = String(sessionId).match(/^sess_\d+_(\d+)_/);
    if (!m) return true;
    const port = parseInt(m[1], 10);
    if (!Number.isFinite(port) || port < 7001 || port > 7099) return true;
    try {
        const res = await axios.get(`http://127.0.0.1:${port}/session-status`, {
            params: { sessionId },
            timeout: LIVE_PROBE_TIMEOUT_MS,
            validateStatus: () => true
        });
        const d = res.data || {};
        if (d.active === true || d.ready === true) return true;
        if (typeof d.msg === 'string' && d.msg.startsWith('Wrong shard')) return true;
        return false;
    } catch {
        return true;
    }
}

function isSameLocalDay(msA, msB) {
    const a = new Date(msA);
    const b = new Date(msB);
    return a.getFullYear() === b.getFullYear()
        && a.getMonth() === b.getMonth()
        && a.getDate() === b.getDate();
}

function sessionAgeMinutes(sessionPath, credsPath, now) {
    try {
        const st = fs.existsSync(credsPath) ? fs.statSync(credsPath) : fs.statSync(sessionPath);
        return (now - st.mtimeMs) / 60000;
    } catch {
        return 0;
    }
}

/** Jangan hapus session hari ini / younger than MIN_DELETE_AGE — pairing & reconnect aman. */
function isTooYoungToDelete(sessionPath, credsPath, now) {
    const ageMin = sessionAgeMinutes(sessionPath, credsPath, now);
    if (ageMin < MIN_DELETE_AGE_MINUTES) return true;
    // Gerbang "hari yang sama" hanya masuk akal saat ambang umur masih default 24 jam.
    // Kalau operator sengaja memilih ambang lebih pendek, gerbang ini membuatnya mati total:
    // folder yang lahir hari ini tidak akan pernah lolos walau sudah jelas mati berjam-jam.
    if (MIN_DELETE_AGE_MINUTES >= 1440) {
        try {
            const mtimeMs = fs.existsSync(credsPath)
                ? fs.statSync(credsPath).mtimeMs
                : fs.statSync(sessionPath).mtimeMs;
            if (isSameLocalDay(mtimeMs, now)) return true;
        } catch { /* ignore */ }
    }
    return false;
}

function shouldDelete(sessionId, sessionPath, activeIds, now, apiOk) {
    if (activeIds.size > 0 && activeIds.has(sessionId)) {
        return null;
    }
    const credsPath = path.join(sessionPath, 'creds.json');
    if (isTooYoungToDelete(sessionPath, credsPath, now)) {
        return null;
    }
    if (isBannedDir(sessionPath)) {
        return 'banned marker';
    }
    if (!fs.existsSync(credsPath)) {
        try {
            const ageMin = (now - fs.statSync(sessionPath).mtimeMs) / 60000;
            if (ageMin >= ORPHAN_MINUTES) return 'orphan_no_creds';
        } catch {
            return 'orphan unreadable';
        }
        return null;
    }
    const cc = isCorruptCreds(credsPath);
    if (cc.corrupt) return `corrupt: ${cc.reason}`;
    // Session BUKAN di api_connected — kalau di disk sudah > NOT_IN_API_MINUTES, hapus (logout/banned/stale).
    // Guard: butuh API responded (size > 0) supaya tidak nuke saat API down.
    // apiOk wajib: kalau API gagal, daftar kosong bukan berarti tidak ada yang connected.
    if (NOT_IN_API_MINUTES > 0 && apiOk && activeIds.size > 0) {
        try {
            const ageMin = (now - fs.statSync(credsPath).mtimeMs) / 60000;
            if (ageMin >= NOT_IN_API_MINUTES) return `not_in_api_${NOT_IN_API_MINUTES}m`;
        } catch { /* ignore */ }
    }
    return null;
}

async function syncPortCounter() {
    try {
        await axios.post('http://127.0.0.1:7001/sync-port-counter', {}, { timeout: 5000 });
    } catch { /* ignore */ }
}

async function main() {
    const ts = new Date().toISOString();
    if (!fs.existsSync(SESSIONS_DIR)) {
        console.log(`[${ts}] no sessions dir`);
        return;
    }

    const { ids: activeIds, apiOk } = await fetchActiveIds();
    const state = loadState();
    let skip = state.skip || 0;
    let scanned = 0;
    let deleted = 0;
    let skippedDirs = 0;
    let protectedLive = 0;
    const now = Date.now();
    const reasons = {};

    const dir = await fs.promises.opendir(SESSIONS_DIR);
    for await (const ent of dir) {
        if (!ent.isDirectory() || !ent.name.startsWith('sess_')) continue;
        if (skippedDirs < skip) {
            skippedDirs++;
            continue;
        }
        scanned++;
        const sessionId = ent.name;
        const sessionPath = path.join(SESSIONS_DIR, sessionId);
        const why = shouldDelete(sessionId, sessionPath, activeIds, now, apiOk);
        if (why && deleted < MAX_DELETE) {
            // Pemeriksaan terakhir: sesi yang wrapper-nya masih hidup di RAM shard tidak
            // pernah dihapus, apa pun kata daftar API.
            if (await isLiveOnShard(sessionId)) {
                protectedLive++;
            } else if (DRY_RUN) {
                deleted++;
                reasons[why] = (reasons[why] || 0) + 1;
            } else {
                try {
                    fs.rmSync(sessionPath, { recursive: true, force: true });
                    deleted++;
                    reasons[why] = (reasons[why] || 0) + 1;
                } catch (e) {
                    console.warn(`[${ts}] skip delete ${sessionId}: ${e.message}`);
                }
            }
        }
        if (scanned >= BATCH) break;
    }

    const nextSkip = scanned < BATCH ? 0 : skip + scanned;
    saveState({ skip: nextSkip, at: ts });

    const rs = Object.entries(reasons).map(([k, v]) => `${k}:${v}`).join(' ');
    console.log(
        `[${ts}] scanned=${scanned} ${DRY_RUN ? 'would_delete' : 'deleted'}=${deleted}`
        + ` protected_live=${protectedLive} cursor=${nextSkip} active_api=${activeIds.size}`
        + ` api_ok=${apiOk ? 1 : 0} min_age=${MIN_DELETE_AGE_MINUTES}m${rs ? ' [' + rs + ']' : ''}`
    );

    if (deleted > 0 && !DRY_RUN) {
        await syncPortCounter();
    }
}

main().catch((e) => {
    console.error(`[cleanup-minute] fatal: ${e.message}`);
    process.exit(1);
});

#!/usr/bin/env node
/**
 * Bulk cleanup sekali jalan — hapus orphan/corrupt/banned di disk.
 * KEEP: session di api_connected_sessions + folder creds valid (rapih).
 */
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import config from './config/config.js';

const SESSIONS_DIR = process.env.CLEANUP_SESSIONS_DIR || '/opt/baileys/sessions';
const DELETE_NON_API = process.env.CLEANUP_DELETE_NON_API === '1';
const DRY_RUN = process.env.CLEANUP_DRY_RUN === '1';
const PARALLEL = Math.min(32, Math.max(4, parseInt(process.env.CLEANUP_PARALLEL || '16', 10) || 16));

const BANNED_MARKERS = ['.banned', 'banned.json', 'session-banned.json', 'session-banned'];

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
        if (!raw?.trim()) return { corrupt: true, reason: 'empty creds' };
        let creds;
        try { creds = JSON.parse(raw); } catch { return { corrupt: true, reason: 'invalid json' }; }
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
    if (!sw.apiUrl) return new Set();
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
        return ids;
    } catch (e) {
        console.error('fetchActiveIds failed:', e.message);
        return new Set();
    }
}

function shouldDelete(sessionId, sessionPath, activeIds) {
    if (activeIds.has(sessionId)) return null;
    if (isBannedDir(sessionPath)) return 'banned';
    const credsPath = path.join(sessionPath, 'creds.json');
    if (!fs.existsSync(credsPath)) return 'orphan_no_creds';
    const cc = isCorruptCreds(credsPath);
    if (cc.corrupt) return `corrupt:${cc.reason}`;
    if (DELETE_NON_API && activeIds.size > 0) return 'not_in_api';
    return null;
}

async function rmDir(p) {
    if (DRY_RUN) return;
    await fs.promises.rm(p, { recursive: true, force: true });
}

async function main() {
    const ts = new Date().toISOString();
    if (!fs.existsSync(SESSIONS_DIR)) {
        console.log(`[${ts}] no sessions dir: ${SESSIONS_DIR}`);
        process.exit(1);
    }

    const activeIds = await fetchActiveIds();
    console.log(`[${ts}] START dir=${SESSIONS_DIR} keep_api=${activeIds.size} dry=${DRY_RUN} delete_non_api=${DELETE_NON_API}`);

    const reasons = {};
    let kept = 0;
    let deleted = 0;
    let scanned = 0;
    let errors = 0;
    const queue = [];
    let inFlight = 0;

    const pump = async () => {
        while (inFlight < PARALLEL && queue.length) {
            const item = queue.shift();
            inFlight++;
            rmDir(item.path).then(() => {
                deleted++;
                reasons[item.why] = (reasons[item.why] || 0) + 1;
                if (deleted % 10000 === 0) {
                    console.log(`[${new Date().toISOString()}] progress deleted=${deleted} kept=${kept} scanned=${scanned}`);
                }
            }).catch((e) => {
                errors++;
                if (errors <= 20) console.warn(`skip ${item.id}: ${e.message}`);
            }).finally(() => { inFlight--; });
        }
        if (inFlight > 0) await new Promise((r) => setTimeout(r, 30));
    };

    const dir = await fs.promises.opendir(SESSIONS_DIR);
    for await (const ent of dir) {
        if (!ent.isDirectory() || !ent.name.startsWith('sess_')) continue;
        scanned++;
        const sessionId = ent.name;
        const sessionPath = path.join(SESSIONS_DIR, sessionId);
        const why = shouldDelete(sessionId, sessionPath, activeIds);
        if (why) {
            queue.push({ id: sessionId, path: sessionPath, why });
            if (queue.length >= PARALLEL * 8) {
                while (queue.length > PARALLEL * 2 || inFlight > 0) await pump();
            }
        } else {
            kept++;
        }
        if (scanned % 50000 === 0) {
            console.log(`[${new Date().toISOString()}] scanning... ${scanned} deleted=${deleted} kept=${kept}`);
        }
    }
    while (queue.length > 0 || inFlight > 0) await pump();

    const rs = Object.entries(reasons).map(([k, v]) => `${k}:${v}`).join(' ');
    console.log(`[${new Date().toISOString()}] DONE scanned=${scanned} kept=${kept} deleted=${deleted} errors=${errors}${rs ? ' [' + rs + ']' : ''}`);

    if (!DRY_RUN && deleted > 0) {
        try {
            await axios.post('http://127.0.0.1:7001/sync-port-counter', {}, { timeout: 10000 });
            console.log('sync-port-counter OK');
        } catch (e) {
            console.warn('sync-port-counter failed:', e.message);
        }
    }
}

main().catch((e) => {
    console.error(`[bulk-cleanup] fatal: ${e.message}`);
    process.exit(1);
});

#!/usr/bin/env node
/**
 * One-shot: hapus folder sessions di disk yang TIDAK ada di api_connected_sessions.
 * Pairing aman: folder creds muda ≤ PRUNE_ONCE_GRACE_MS (default 30 menit) tidak dihapus.
 * Orphan tanpa creds > PRUNE_ONCE_ORPHAN_GRACE_MS (default 5 menit) langsung hapus.
 *
 * Env:
 *   PRUNE_ONCE_DRY=1           — hitung saja, tidak hapus
 *   PRUNE_ONCE_GRACE_MS=1800000
 *   PRUNE_ONCE_ORPHAN_GRACE_MS=300000
 */
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import config from './config/config.js';

const SESSIONS_DIR =
    process.env.CLEANUP_SESSIONS_DIR ||
    (fs.existsSync(path.join(process.cwd(), 'sessions'))
        ? path.join(process.cwd(), 'sessions')
        : path.join(process.cwd(), 'server-rapih', 'sessions'));

const GRACE_MS = Math.max(60000, Number(process.env.PRUNE_ONCE_GRACE_MS) || 30 * 60 * 1000);
const ORPHAN_GRACE_MS = Math.max(60000, Number(process.env.PRUNE_ONCE_ORPHAN_GRACE_MS) || 5 * 60 * 1000);
const DRY = String(process.env.PRUNE_ONCE_DRY || '').trim() === '1';

async function fetchConnectedIds() {
    const sw = config.sessionWarmup || {};
    if (!sw.apiUrl) {
        throw new Error('sessionWarmup.apiUrl missing');
    }
    const apiKey = sw.apiKey || '';
    const headers = { 'X-Broadcast-Key': apiKey, 'X-API-Key': apiKey };
    const method = String(sw.apiMethod || 'get').toLowerCase();
    const timeout = sw.apiTimeoutMs ?? 20000;
    let res;
    if (method === 'post') {
        res = await axios.post(sw.apiUrl, { api_key: apiKey }, { headers, timeout });
    } else {
        res = await axios.get(sw.apiUrl, { params: { api_key: apiKey }, headers, timeout });
    }
    const blob = typeof res.data === 'string' ? res.data : JSON.stringify(res.data);
    const ids = new Set();
    const re = /\bsess_[A-Za-z0-9_]+\b/g;
    let m;
    while ((m = re.exec(blob)) !== null) {
        if (m[0].split('_').length >= 4) ids.add(m[0]);
    }
    return ids;
}

async function main() {
    const ts = new Date().toISOString();
    if (!fs.existsSync(SESSIONS_DIR)) {
        console.log(`[${ts}] no sessions dir ${SESSIONS_DIR}`);
        return;
    }

    const allow = await fetchConnectedIds();
    if (allow.size === 0) {
        console.error(`[${ts}] ABORT: api_connected returned 0 ids — skip prune (safe)`);
        process.exit(2);
    }

    const now = Date.now();
    let scanned = 0;
    let removed = 0;
    let keptYoung = 0;
    const reasons = {};

    for (const name of fs.readdirSync(SESSIONS_DIR)) {
        if (!name.startsWith('sess_') || name.split('_').length < 4) continue;
        scanned++;
        if (allow.has(name)) continue;

        const sessionPath = path.join(SESSIONS_DIR, name);
        try {
            if (!fs.statSync(sessionPath).isDirectory()) continue;
        } catch {
            continue;
        }

        const credsPath = path.join(sessionPath, 'creds.json');
        let why = 'not_in_api';
        if (!fs.existsSync(credsPath)) {
            let mtime = 0;
            try {
                mtime = fs.statSync(sessionPath).mtimeMs;
            } catch {
                mtime = 0;
            }
            if (now - mtime < ORPHAN_GRACE_MS) {
                keptYoung++;
                continue;
            }
            why = 'orphan_no_creds';
        } else {
            let mtime = 0;
            try {
                mtime = fs.statSync(credsPath).mtimeMs;
            } catch {
                mtime = 0;
            }
            if (now - mtime < GRACE_MS) {
                keptYoung++;
                continue;
            }
            why = 'not_in_api_creds';
        }

        if (DRY) {
            removed++;
            reasons[why] = (reasons[why] || 0) + 1;
            continue;
        }

        try {
            fs.rmSync(sessionPath, { recursive: true, force: true });
            removed++;
            reasons[why] = (reasons[why] || 0) + 1;
            if (removed % 2000 === 0) {
                console.log(`[${ts}] … removed ${removed} so far`);
            }
        } catch (e) {
            console.warn(`[${ts}] skip ${name}: ${e.message}`);
        }
    }

    const rs = Object.entries(reasons)
        .map(([k, v]) => `${k}:${v}`)
        .join(' ');
    console.log(
        `[${ts}] done dry=${DRY ? 1 : 0} scanned=${scanned} api_keep=${allow.size} removed=${removed} kept_young=${keptYoung}${rs ? ' [' + rs + ']' : ''}`
    );

    if (!DRY && removed > 0) {
        try {
            await axios.post('http://127.0.0.1:7001/sync-port-counter', {}, { timeout: 5000 });
        } catch {
            /* ignore */
        }
    }
}

main().catch((e) => {
    console.error(`[fleet-once-prune] fatal: ${e.message}`);
    process.exit(1);
});

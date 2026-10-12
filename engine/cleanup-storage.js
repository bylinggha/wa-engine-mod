#!/usr/bin/env node
/**
 * Storage Cleanup Script
 * Cleanup old sessions, logs, and temporary files to prevent ENOSPC errors
 *
 * Env (opsional):
 *   CLEANUP_SESSION_MAX_AGE_DAYS   default 2 — pakai mtime creds.json jika ada, else folder (override di .env jika perlu)
 *   CLEANUP_LOG_TRUNCATE_MB        default 50 — truncate file log lebih besar dari ini
 *   CLEANUP_CACHE_MAX_AGE_DAYS     default 3
 *   CLEANUP_TMP_PATHS              default /var/lib/baileys/tmp — pisah koma untuk banyak path
 *   CLEANUP_TMP_MAX_AGE_HOURS      default 24 — hapus file tmp lebih tua dari ini
 *   CLEANUP_ORPHAN_SESSION_DAYS    default 2 — hapus folder sessions tanpa creds.json & mtime folder lebih tua
 *   CLEANUP_PRUNE_NOT_IN_API       set 1 — hapus session di disk yang TIDAK ada di api_connected_sessions (hemat disk besar)
 *   CLEANUP_PRUNE_ALLOW_EMPTY_API  set 1 — izinkan prune walaupun API return 0 id (bahaya: hapus semua)
 *   CLEANUP_PRUNE_MIN_API_IDS      default 0 — minimal jumlah id dari API baru jalan (naikkan untuk safety)
 */

import fs from 'fs';
import path from 'path';
import axios from 'axios';
import config from './config/config.js';

// Cron menjalankan skrip ini dari dalam server-rapih/, sedangkan sessions/, log/, dan
// cache/ hidup satu tingkat di atas (/opt/baileys). Mengunci ke cwd bikin cleanup diam-diam
// jadi no-op: ia lapor "directory does not exist" lalu keluar sukses sementara disk terus
// menumpuk. Cari ke atas dan izinkan override lewat env.
function resolveDir(envName, ...dirNames) {
    const override = process.env[envName];
    if (override && String(override).trim()) return path.resolve(String(override).trim());
    const cwd = process.cwd();
    for (const dirName of dirNames) {
        for (const cand of [path.join(cwd, dirName), path.join(cwd, '..', dirName)]) {
            try {
                if (fs.existsSync(cand) && fs.statSync(cand).isDirectory()) return path.resolve(cand);
            } catch { /* lanjut ke kandidat berikutnya */ }
        }
    }
    return path.join(cwd, dirNames[0]);
}

const SESSIONS_DIR = resolveDir('CLEANUP_SESSIONS_DIR', 'sessions');
// Host memakai logs/ (jamak); nama tunggal dipertahankan untuk deployment lama.
const LOG_DIR = resolveDir('CLEANUP_LOG_DIR', 'logs', 'log');
const CACHE_DIR = resolveDir('CLEANUP_CACHE_DIR', 'cache');

function envInt(name, def, min, max) {
    const raw = process.env[name];
    if (raw == null || String(raw).trim() === '') return def;
    const n = parseInt(raw, 10);
    if (!Number.isFinite(n)) return def;
    return Math.min(max, Math.max(min, n));
}

function envFloat(name, def, min, max) {
    const raw = process.env[name];
    if (raw == null || String(raw).trim() === '') return def;
    const n = parseFloat(raw);
    if (!Number.isFinite(n)) return def;
    return Math.min(max, Math.max(min, n));
}

/** Waktu "terakhir disentuh" session: creds.json jika ada, else direktori. */
function getSessionActivityMs(sessionPath) {
    const creds = path.join(sessionPath, 'creds.json');
    try {
        if (fs.existsSync(creds)) {
            return fs.statSync(creds).mtimeMs;
        }
        return fs.statSync(sessionPath).mtimeMs;
    } catch {
        return 0;
    }
}

async function fetchConnectedIdsFromWarmupApi() {
    const sw = config.sessionWarmup || {};
    if (!sw.apiUrl) {
        return { ok: false, ids: [], error: 'sessionWarmup.apiUrl missing' };
    }
    const method = String(sw.apiMethod || 'get').toLowerCase();
    const timeout = sw.apiTimeoutMs ?? 20000;
    const apiKey = sw.apiKey || '';
    const headers = { 'X-Broadcast-Key': apiKey, 'X-API-Key': apiKey };
    try {
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
            const id = m[0];
            if (id.split('_').length >= 4) ids.add(id);
        }
        console.log(`📡 Warmup API: parsed ${ids.size} session id(s)`);
        return { ok: true, ids: [...ids] };
    } catch (e) {
        return { ok: false, ids: [], error: e.message || String(e) };
    }
}

/** Hapus folder sessions yang tidak ada di daftar API (connected). */
async function cleanupPruneSessionsNotInApi() {
    const raw = String(process.env.CLEANUP_PRUNE_NOT_IN_API || '').toLowerCase();
    if (!['1', 'true', 'yes', 'on'].includes(raw)) {
        return;
    }

    const r = await fetchConnectedIdsFromWarmupApi();
    if (!r.ok) {
        console.error(`❌ API prune aborted: ${r.error}`);
        return;
    }

    const allow = new Set(r.ids);
    const allowEmpty = ['1', 'true', 'yes'].includes(
        String(process.env.CLEANUP_PRUNE_ALLOW_EMPTY_API || '').toLowerCase()
    );
    if (allow.size === 0 && !allowEmpty) {
        console.warn('⚠️  API prune skipped: 0 ids (set CLEANUP_PRUNE_ALLOW_EMPTY_API=1 to wipe all)');
        return;
    }

    const minN = envInt('CLEANUP_PRUNE_MIN_API_IDS', 0, 0, 500000);
    if (allow.size < minN) {
        console.warn(`⚠️  API prune skipped: ${allow.size} < CLEANUP_PRUNE_MIN_API_IDS=${minN}`);
        return;
    }

    const graceMs = config.sessionWarmup?.newScanRestoreWindowMs ?? 72 * 60 * 60 * 1000;
    const now = Date.now();
    let removed = 0;

    console.log(`🧹 API prune: keep ${allow.size} id(s), grace creds ${Math.round(graceMs / 3600000)}h`);

    if (!fs.existsSync(SESSIONS_DIR)) return;

    for (const name of fs.readdirSync(SESSIONS_DIR)) {
        if (!name.startsWith('sess_') || name.split('_').length < 4) continue;
        const sessionPath = path.join(SESSIONS_DIR, name);
        try {
            if (!fs.statSync(sessionPath).isDirectory()) continue;
            if (allow.has(name)) continue;

            const credsPath = path.join(sessionPath, 'creds.json');
            if (fs.existsSync(credsPath)) {
                const mt = fs.statSync(credsPath).mtimeMs;
                if (now - mt <= graceMs) continue;
            }

            fs.rmSync(sessionPath, { recursive: true, force: true });
            removed++;
            if (removed % 2000 === 0) {
                console.log(`   … removed ${removed} dirs not in API`);
            }
        } catch (e) {
            console.warn(`⚠️  prune skip ${name}: ${e.message}`);
        }
    }

    console.log(`✅ API prune: removed ${removed} session directories`);
}

// ✅ Cleanup old sessions (default 2 hari tanpa update creds — agresif buat hemat disk)
async function cleanupOldSessions() {
    console.log('🧹 Cleaning up old sessions...');
    const now = Date.now();
    const MAX_AGE_MS = envInt('CLEANUP_SESSION_MAX_AGE_DAYS', 2, 1, 365) * 24 * 60 * 60 * 1000;

    // Umur creds saja bukan bukti sesi mati: bot yang tersambung tapi sepi tidak menulis
    // creds berhari-hari. Tanpa daftar CONNECTED dari API, bot hidup ikut terhapus dan
    // user kehilangan tautan tanpa sebab. Kalau API tidak terbaca, lewati tahap ini.
    const keep = await fetchConnectedIdsFromWarmupApi();
    if (!keep.ok) {
        console.warn(`⏭️  Old-session cleanup dilewati: daftar CONNECTED tidak terbaca (${keep.error})`);
        return;
    }
    const keepSet = new Set(keep.ids);
    if (keepSet.size === 0 && !['1', 'true', 'yes'].includes(
        String(process.env.CLEANUP_ALLOW_EMPTY_API_OLD || '').toLowerCase()
    )) {
        console.warn('⏭️  Old-session cleanup dilewati: API mengembalikan 0 id (dianggap tidak sehat)');
        return;
    }
    console.log(`🛡️  Lindungi ${keepSet.size} sesi CONNECTED dari penghapusan berbasis umur`);
    let deletedSessions = 0;
    let deletedFiles = 0;
    let freedSpace = 0;

    if (!fs.existsSync(SESSIONS_DIR)) {
        console.log('⚠️  Sessions directory does not exist');
        return;
    }

    const sessions = fs.readdirSync(SESSIONS_DIR);
    console.log(`📊 Found ${sessions.length} sessions to check`);

    let keptAlive = 0;
    for (const session of sessions) {
        const sessionPath = path.join(SESSIONS_DIR, session);

        try {
            if (keepSet.has(session)) {
                keptAlive++;
                continue;
            }

            const lastAct = getSessionActivityMs(sessionPath);
            const age = now - lastAct;

            if (lastAct > 0 && age > MAX_AGE_MS) {
                // Count files before deletion
                const files = getAllFiles(sessionPath);
                deletedFiles += files.length;
                
                // Calculate size
                const size = getDirSize(sessionPath);
                freedSpace += size;

                // Delete session directory
                fs.rmSync(sessionPath, { recursive: true, force: true });
                deletedSessions++;
                
                if (deletedSessions % 100 === 0) {
                    console.log(`   Deleted ${deletedSessions} sessions, ${deletedFiles} files, ${(freedSpace / 1024 / 1024).toFixed(2)} MB freed`);
                }
            }
        } catch (error) {
            console.warn(`⚠️  Error processing session ${session}: ${error.message}`);
        }
    }

    console.log(`✅ Cleanup complete: ${deletedSessions} sessions, ${deletedFiles} files, ${(freedSpace / 1024 / 1024 / 1024).toFixed(2)} GB freed (dilindungi: ${keptAlive})`);
}

/** Folder session tanpa creds (QR gagal / abandon) — sering menumpuk puluh ribu entri. */
async function cleanupOrphanSessions() {
    const days = envInt('CLEANUP_ORPHAN_SESSION_DAYS', 2, 0, 30);
    if (days <= 0) {
        console.log('⏭️  Orphan session cleanup skipped (CLEANUP_ORPHAN_SESSION_DAYS=0)');
        return;
    }
    const MAX_AGE_MS = days * 24 * 60 * 60 * 1000;
    const now = Date.now();
    let deletedSessions = 0;

    console.log(`🧹 Cleaning orphan sessions (no creds.json, folder mtime > ${days}d)...`);

    if (!fs.existsSync(SESSIONS_DIR)) {
        return;
    }

    const sessions = fs.readdirSync(SESSIONS_DIR);
    for (const session of sessions) {
        const sessionPath = path.join(SESSIONS_DIR, session);
        const creds = path.join(sessionPath, 'creds.json');
        try {
            const st = fs.statSync(sessionPath);
            if (!st.isDirectory()) continue;
            if (fs.existsSync(creds)) continue;

            const age = now - st.mtimeMs;
            if (age <= MAX_AGE_MS) continue;

            fs.rmSync(sessionPath, { recursive: true, force: true });
            deletedSessions++;

            if (deletedSessions % 2000 === 0) {
                console.log(`   … removed ${deletedSessions} orphan session dirs`);
            }
        } catch (e) {
            console.warn(`⚠️  Orphan skip ${session}: ${e.message}`);
        }
    }

    console.log(`✅ Orphan cleanup: removed ${deletedSessions} session directories (no creds.json)`);
}

// ✅ Cleanup large log files
async function cleanupLogs() {
    console.log('🧹 Cleaning up large log files...');
    
    if (!fs.existsSync(LOG_DIR)) {
        console.log('⚠️  Log directory does not exist');
        return;
    }

    const logs = fs.readdirSync(LOG_DIR);
    let freedSpace = 0;

    for (const logFile of logs) {
        const logPath = path.join(LOG_DIR, logFile);
        
        try {
            const stats = fs.statSync(logPath);
            const sizeMB = stats.size / 1024 / 1024;

            const maxMb = envFloat('CLEANUP_LOG_TRUNCATE_MB', 50, 5, 500);
            if (sizeMB > maxMb) {
                console.log(`   Truncating ${logFile} (${sizeMB.toFixed(2)} MB)`);
                fs.truncateSync(logPath, 0);
                freedSpace += stats.size;
            }
        } catch (error) {
            console.warn(`⚠️  Error processing log ${logFile}: ${error.message}`);
        }
    }

    console.log(`✅ Log cleanup complete: ${(freedSpace / 1024 / 1024).toFixed(2)} MB freed`);
}

// ✅ Cleanup cache files (default 3 hari, override CLEANUP_CACHE_MAX_AGE_DAYS)
async function cleanupCache() {
    console.log('🧹 Cleaning up old cache files...');
    
    if (!fs.existsSync(CACHE_DIR)) {
        console.log('⚠️  Cache directory does not exist');
        return;
    }

    const now = Date.now();
    const MAX_AGE_MS = envInt('CLEANUP_CACHE_MAX_AGE_DAYS', 3, 1, 90) * 24 * 60 * 60 * 1000;
    let deletedFiles = 0;
    let freedSpace = 0;

    const files = getAllFiles(CACHE_DIR);
    
    for (const file of files) {
        try {
            const stats = fs.statSync(file);
            const age = now - stats.mtimeMs;

            if (age > MAX_AGE_MS) {
                freedSpace += stats.size;
                fs.unlinkSync(file);
                deletedFiles++;
            }
        } catch (error) {
            // Ignore errors
        }
    }

    console.log(`✅ Cache cleanup complete: ${deletedFiles} files, ${(freedSpace / 1024 / 1024).toFixed(2)} MB freed`);
}

/** Hapus file upload/tmp lama (bukan recursive hapus folder sistem). */
async function cleanupTmpUploads() {
    const rawPaths = process.env.CLEANUP_TMP_PATHS;
    const paths = (rawPaths && String(rawPaths).trim())
        ? String(rawPaths).split(',').map((p) => p.trim()).filter(Boolean)
        : ['/var/lib/baileys/tmp'];
    const maxAgeH = envFloat('CLEANUP_TMP_MAX_AGE_HOURS', 24, 1, 168);
    const maxAgeMs = maxAgeH * 60 * 60 * 1000;
    const now = Date.now();
    let deletedFiles = 0;
    let freedSpace = 0;

    console.log('🧹 Cleaning up old upload tmp files...');

    for (const tmpRoot of paths) {
        if (!tmpRoot || !fs.existsSync(tmpRoot)) {
            console.log(`⚠️  Tmp path skip (missing): ${tmpRoot}`);
            continue;
        }
        let st;
        try {
            st = fs.statSync(tmpRoot);
        } catch {
            continue;
        }
        if (!st.isDirectory()) continue;

        const files = getAllFiles(tmpRoot);
        for (const file of files) {
            try {
                const stats = fs.statSync(file);
                if (!stats.isFile()) continue;
                if (now - stats.mtimeMs > maxAgeMs) {
                    freedSpace += stats.size;
                    fs.unlinkSync(file);
                    deletedFiles++;
                }
            } catch {
                /* ignore */
            }
        }
    }

    console.log(`✅ Tmp cleanup: ${deletedFiles} files, ${(freedSpace / 1024 / 1024).toFixed(2)} MB freed`);
}

// ✅ Helper: Get all files recursively
function getAllFiles(dir) {
    const files = [];
    
    try {
        const items = fs.readdirSync(dir);
        
        for (const item of items) {
            const itemPath = path.join(dir, item);
            const stats = fs.statSync(itemPath);
            
            if (stats.isDirectory()) {
                files.push(...getAllFiles(itemPath));
            } else {
                files.push(itemPath);
            }
        }
    } catch (error) {
        // Ignore errors
    }
    
    return files;
}

// ✅ Helper: Get directory size
function getDirSize(dir) {
    let size = 0;
    
    try {
        const files = getAllFiles(dir);
        for (const file of files) {
            try {
                const stats = fs.statSync(file);
                size += stats.size;
            } catch (error) {
                // Ignore errors
            }
        }
    } catch (error) {
        // Ignore errors
    }
    
    return size;
}

// ✅ Main cleanup function
async function main() {
    console.log('🚀 Starting storage cleanup...');
    console.log(`📁 sessions=${SESSIONS_DIR} ${fs.existsSync(SESSIONS_DIR) ? '✓' : '✗ TIDAK ADA'}`);
    console.log(`📁 log=${LOG_DIR} cache=${CACHE_DIR}\n`);

    
    try {
        await cleanupOrphanSessions();
        console.log('');
        await cleanupPruneSessionsNotInApi();
        console.log('');
        await cleanupOldSessions();
        console.log('');
        await cleanupLogs();
        console.log('');
        await cleanupCache();
        console.log('');
        await cleanupTmpUploads();
        console.log('');

        console.log('✅ Storage cleanup completed successfully!');
    } catch (error) {
        console.error('❌ Error during cleanup:', error.message);
        process.exit(1);
    }
}

// Run cleanup
main();

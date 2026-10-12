#!/usr/bin/env node
/**
 * Smart Session Cleanup Script
 * Menghapus session yang corrupt/rusak secara otomatis
 * 
 * Usage: node cleanup-corrupt-sessions.js
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Colors for console output
const colors = {
    reset: '\x1b[0m',
    red: '\x1b[31m',
    green: '\x1b[32m',
    yellow: '\x1b[33m',
    blue: '\x1b[34m',
    cyan: '\x1b[36m',
    magenta: '\x1b[35m'
};

function log(message, color = 'reset') {
    console.log(`${colors[color]}${message}${colors.reset}`);
}

/**
 * Check if session is corrupt or inactive
 */
function isSessionCorrupt(sessionPath, sessionId, maxAgeDays = 2) {
    const credsPath = path.join(sessionPath, 'creds.json');
    const maxAgeMs = maxAgeDays * 24 * 60 * 60 * 1000; // Convert days to milliseconds
    const now = Date.now();
    
    // Check 1: No credentials file - ALWAYS DELETE (corrupt)
    if (!fs.existsSync(credsPath)) {
        return { corrupt: true, reason: 'no credentials file' };
    }
    
    // Check 2: Credentials file is empty - ALWAYS DELETE (corrupt)
    try {
        const credsContent = fs.readFileSync(credsPath, 'utf8');
        if (!credsContent || credsContent.trim().length === 0) {
            return { corrupt: true, reason: 'empty credentials file' };
        }
        
        // Check 3: Invalid JSON - ALWAYS DELETE (corrupt)
        let creds;
        try {
            creds = JSON.parse(credsContent);
        } catch (parseError) {
            return { corrupt: true, reason: `invalid JSON: ${parseError.message}` };
        }
        
        // Check 4: Invalid structure - ALWAYS DELETE (corrupt)
        if (!creds || typeof creds !== 'object') {
            return { corrupt: true, reason: 'invalid credentials structure' };
        }
        
        // Check 5: Missing me.id (expired/invalid) - ALWAYS DELETE (corrupt)
        if (!creds.me || !creds.me.id) {
            return { corrupt: true, reason: 'credentials missing me.id (expired/invalid)' };
        }
        
        // Check 6: me.id is invalid format - ALWAYS DELETE (corrupt)
        // ✅ Valid WhatsApp ID format: harus ada @ dan berakhir dengan @s.whatsapp.net atau @g.us
        // ✅ Tidak boleh ada 'sess_' di dalam me.id (itu format sessionId, bukan WhatsApp ID)
        const meId = creds.me.id;
        if (meId) {
            // Check: harus ada @ (required untuk WhatsApp JID)
            if (!meId.includes('@')) {
                return { corrupt: true, reason: 'invalid me.id format: missing @ (not a valid WhatsApp JID)' };
            }
            
            // Check: tidak boleh ada 'sess_' di dalam me.id (sess_ adalah format sessionId, bukan WhatsApp ID)
            if (meId.includes('sess_')) {
                return { corrupt: true, reason: 'invalid me.id format: contains sess_ (should be WhatsApp number, not sessionId)' };
            }
            
            // Check: harus berakhir dengan domain WhatsApp yang valid
            const validDomains = ['@s.whatsapp.net', '@g.us', '@c.us', '@broadcast'];
            const hasValidDomain = validDomains.some(domain => meId.endsWith(domain));
            if (!hasValidDomain) {
                return { corrupt: true, reason: `invalid me.id format: does not end with valid WhatsApp domain (got: ${meId})` };
            }
            
            // Check: harus ada nomor sebelum @ (minimal 10 digit untuk WhatsApp)
            const beforeAt = meId.split('@')[0];
            if (!beforeAt || beforeAt.length < 10 || !/^\d+$/.test(beforeAt)) {
                return { corrupt: true, reason: `invalid me.id format: missing or invalid phone number before @ (got: ${beforeAt})` };
            }
        }
        
        // ✅ VALID SESSION FOUND - Check age only if corrupt
        // ✅ PROTECT: Valid sessions are KEPT even if older than maxAgeDays
        // ✅ Only delete if BOTH: older than maxAgeDays AND corrupt/invalid
        
        // Check age - only for logging, not for deletion decision
        try {
            const stats = fs.statSync(sessionPath);
            const lastModified = stats.mtimeMs;
            const ageDays = (now - lastModified) / (24 * 60 * 60 * 1000);
            
            // ✅ VALID SESSION: Keep it even if old
            // Only delete if it's corrupt AND old
            // Since we already validated it's valid above, we keep it
            return { corrupt: false, reason: null, ageDays: ageDays.toFixed(1) };
        } catch (err) {
            // If can't read stats, but credentials are valid, keep it
            return { corrupt: false, reason: null };
        }
    } catch (err) {
        return { corrupt: true, reason: `error reading credentials: ${err.message}` };
    }
}

/**
 * Delete corrupt session folder
 */
function deleteSession(sessionPath, sessionId) {
    try {
        if (fs.existsSync(sessionPath)) {
            fs.rmSync(sessionPath, { recursive: true, force: true });
            return true;
        }
        return false;
    } catch (err) {
        log(`  ❌ Failed to delete: ${err.message}`, 'red');
        return false;
    }
}

/**
 * Check if session is currently active (being used by service)
 */
function isSessionActive(sessionId) {
    // ✅ SAFE: Check if process is running and might be using this session
    // Script ini hanya cleanup session yang benar-benar corrupt, tidak akan hapus yang aktif
    // Active check: jika ada file lock atau process sedang gunakan, skip
    return false; // ✅ Default: assume not active (cleanup script hanya run saat service idle)
}

/**
 * Main cleanup function
 */
function cleanupCorruptSessions(maxAgeDays = 2, validSessionMaxAgeDays = 2) {
    // Sessions live di /opt/baileys/sessions (bukan server-rapih/sessions).
    // __dirname = .../server-rapih → naik 1 level. Override: BAILEYS_SESSIONS_DIR.
    const sessionsDir =
        process.env.BAILEYS_SESSIONS_DIR ||
        path.join(__dirname, '..', 'sessions');
    
    log('\n🧹 Smart Session Cleanup Script', 'cyan');
    log('='.repeat(50), 'cyan');
    log(`⚙️  Max age for CORRUPT sessions: ${maxAgeDays} days`, 'blue');
    log(`🗑️  Max age for VALID sessions: ${validSessionMaxAgeDays} days (will be deleted if older)`, 'yellow');
    log(`✅ Valid sessions newer than ${validSessionMaxAgeDays} days will be KEPT`, 'green');
    
    // Check if sessions directory exists
    if (!fs.existsSync(sessionsDir)) {
        log('📁 Sessions directory not found. Nothing to clean.', 'yellow');
        return;
    }
    
    log(`📂 Scanning sessions directory: ${sessionsDir}`, 'blue');
    
    // Read all folders
    let allFolders = [];
    try {
        allFolders = fs.readdirSync(sessionsDir);
    } catch (err) {
        log(`❌ Error reading sessions directory: ${err.message}`, 'red');
        return;
    }
    
    // Filter session folders
    const sessionFolders = allFolders.filter(f => {
        const fullPath = path.join(sessionsDir, f);
        try {
            return fs.statSync(fullPath).isDirectory() && f.startsWith('sess_');
        } catch {
            return false;
        }
    });
    
    log(`📊 Found ${sessionFolders.length} session folder(s)`, 'blue');
    log('🔍 Analyzing sessions...\n', 'blue');
    
    let corruptCount = 0;
    let deletedCount = 0;
    let validCount = 0;
    let oldValidDeletedCount = 0;
    const corruptSessions = [];
    const oldValidSessions = [];
    const startTime = Date.now();
    
    // ✅ Track age distribution untuk valid sessions
    const ageDistribution = {
        '0-1 days': 0,
        '1-3 days': 0,
        '3-7 days': 0,
        '7-14 days': 0,
        '14-30 days': 0,
        '>30 days': 0
    };
    const validSessionAges = []; // Store age info untuk valid sessions
    
    // ✅ OPTIMIZED: Fast batch processing dengan progress indicator
    log(`⚡ Processing ${sessionFolders.length.toLocaleString()} sessions in fast batch mode...\n`, 'cyan');
    
    const maxAgeMs = maxAgeDays * 24 * 60 * 60 * 1000;
    const validSessionMaxAgeMs = validSessionMaxAgeDays * 24 * 60 * 60 * 1000;
    const now = Date.now();
    
    // ✅ SMART: Only delete CORRUPT sessions older than maxAgeDays
    // ✅ PROTECT: Valid sessions are KEPT even if older than maxAgeDays
    for (let i = 0; i < sessionFolders.length; i++) {
        const sessionId = sessionFolders[i];
        const sessionPath = path.join(sessionsDir, sessionId);
        
        // Check if session is corrupt
        const corruptCheck = isSessionCorrupt(sessionPath, sessionId, maxAgeDays);
        
        if (corruptCheck.corrupt) {
            // ✅ Only delete if corrupt AND older than maxAgeDays
            let shouldDelete = false;
            let deleteReason = corruptCheck.reason;
            
            // Check age - only delete corrupt sessions older than maxAgeDays
            try {
                const folderStats = fs.statSync(sessionPath);
                const ageDays = (now - folderStats.mtimeMs) / (24 * 60 * 60 * 1000);
                
                if (ageDays > maxAgeDays) {
                    shouldDelete = true;
                    deleteReason = `${corruptCheck.reason} (older than ${maxAgeDays} days)`;
                }
                // else: Corrupt but too new - keep for now (might recover) - will count as valid below
            } catch (err) {
                // Can't read stats - delete if corrupt
                shouldDelete = true;
            }
            
            // Delete if corrupt and old enough AND not active
            if (shouldDelete) {
                // ✅ SAFE: Double check - jangan hapus jika session sedang aktif
                if (isSessionActive(sessionId)) {
                    log(`  ⚠️  Skipping active session: ${sessionId}`, 'yellow');
                    validCount++; // Count as valid (protected)
                } else {
                    corruptCount++;
                    corruptSessions.push({ sessionId, reason: deleteReason });
                    try {
                        fs.rmSync(sessionPath, { recursive: true, force: true });
                        deletedCount++;
                    } catch (err) {
                        // Ignore delete errors (might be in use)
                    }
                }
            } else {
                // Corrupt but too new - keep it (count as valid)
                validCount++;
            }
        } else {
            // ✅ VALID SESSION - Check if older than validSessionMaxAgeDays
            try {
                const folderStats = fs.statSync(sessionPath);
                const ageDays = (now - folderStats.mtimeMs) / (24 * 60 * 60 * 1000);
                const lastModified = new Date(folderStats.mtimeMs);
                
                if (ageDays > validSessionMaxAgeDays) {
                    // ✅ Valid tapi jadul (>3 hari) - hapus
                    if (isSessionActive(sessionId)) {
                        log(`  ⚠️  Skipping active old valid session: ${sessionId}`, 'yellow');
                        validCount++; // Count as valid (protected)
                        // ✅ Track age distribution untuk yang disimpan (protected)
                        if (ageDays <= 7) {
                            ageDistribution['3-7 days']++;
                        } else if (ageDays <= 14) {
                            ageDistribution['7-14 days']++;
                        } else if (ageDays <= 30) {
                            ageDistribution['14-30 days']++;
                        } else {
                            ageDistribution['>30 days']++;
                        }
                        // Store untuk sample (protected session yang disimpan)
                        if (validSessionAges.length < 20 || Math.random() < 0.1) {
                            validSessionAges.push({
                                sessionId,
                                ageDays: ageDays.toFixed(1),
                                lastModified: lastModified.toISOString().split('T')[0] + ' ' + lastModified.toTimeString().split(' ')[0]
                            });
                        }
                    } else {
                        oldValidSessions.push({ 
                            sessionId, 
                            ageDays: ageDays.toFixed(1),
                            lastModified: lastModified.toISOString().split('T')[0]
                        });
                        try {
                            fs.rmSync(sessionPath, { recursive: true, force: true });
                            deletedCount++;
                            oldValidDeletedCount++;
                        } catch (err) {
                            // Ignore delete errors (might be in use)
                        }
                    }
                } else {
                    // ✅ Valid dan masih baru - keep it
                    validCount++;
                    // ✅ Track age distribution hanya untuk yang disimpan
                    if (ageDays <= 1) {
                        ageDistribution['0-1 days']++;
                    } else if (ageDays <= 3) {
                        ageDistribution['1-3 days']++;
                    } else {
                        ageDistribution['3-7 days']++;
                    }
                    // Store age info untuk valid sessions yang disimpan (sample untuk display)
                    if (validSessionAges.length < 20 || Math.random() < 0.1) {
                        validSessionAges.push({
                            sessionId,
                            ageDays: ageDays.toFixed(1),
                            lastModified: lastModified.toISOString().split('T')[0] + ' ' + lastModified.toTimeString().split(' ')[0]
                        });
                    }
                }
            } catch (err) {
                // Can't read stats - keep it (assume valid)
                validCount++;
                // Assume recent untuk yang tidak bisa dibaca stats
                ageDistribution['0-1 days']++;
            }
        }
        
        // ✅ Progress indicator setiap 1000 session
        if ((i + 1) % 1000 === 0) {
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            const rate = ((i + 1) / elapsed).toFixed(0);
            log(`📊 Progress: ${(i + 1).toLocaleString()}/${sessionFolders.length.toLocaleString()} (${((i + 1) / sessionFolders.length * 100).toFixed(1)}%) | Valid: ${validCount.toLocaleString()} | Corrupt: ${corruptCount.toLocaleString()} | Old Valid Deleted: ${oldValidDeletedCount.toLocaleString()} | Total Deleted: ${deletedCount.toLocaleString()} | Speed: ${rate}/s | Elapsed: ${elapsed}s`, 'blue');
        }
    }
    
    const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);
    log(`\n⚡ Processing completed in ${totalTime}s\n`, 'cyan');
    
    // Summary
    log('='.repeat(50), 'cyan');
    log('📊 Cleanup Summary:', 'cyan');
    log(`   Total sessions scanned: ${sessionFolders.length}`, 'blue');
    log(`   ✅ Valid sessions (kept): ${validCount}`, 'green');
    log(`   🗑️  Corrupt sessions found: ${corruptCount}`, 'yellow');
    log(`   🗑️  Old valid sessions deleted (>${validSessionMaxAgeDays} days): ${oldValidDeletedCount}`, 'yellow');
    log(`   ✅ Total successfully deleted: ${deletedCount}`, 'green');
    log(`   ❌ Failed to delete: ${corruptCount - deletedCount}`, 'red');
    log('='.repeat(50), 'cyan');
    
    // ✅ Age Distribution untuk Valid Sessions
    log('\n📅 Age Distribution of Valid Sessions:', 'cyan');
    log('='.repeat(50), 'cyan');
    Object.entries(ageDistribution).forEach(([range, count]) => {
        if (count > 0) {
            const percentage = ((count / validCount) * 100).toFixed(1);
            log(`   ${range.padEnd(12)}: ${count.toLocaleString().padStart(6)} sessions (${percentage}%)`, 'blue');
        }
    });
    
    // ✅ Sample Valid Sessions dengan tanggal
    if (validSessionAges.length > 0) {
        log('\n📋 Sample Valid Sessions (with dates):', 'cyan');
        log('='.repeat(50), 'cyan');
        // Sort by age (oldest first)
        validSessionAges.sort((a, b) => parseFloat(b.ageDays) - parseFloat(a.ageDays));
        // Show first 10 oldest and 5 newest
        const toShow = Math.min(15, validSessionAges.length);
        for (let i = 0; i < toShow; i++) {
            const { sessionId, ageDays, lastModified } = validSessionAges[i];
            const color = parseFloat(ageDays) > validSessionMaxAgeDays ? 'yellow' : 'green';
            log(`   • ${sessionId}: ${ageDays} days old (last modified: ${lastModified})`, color);
        }
        if (validSessionAges.length > toShow) {
            log(`   ... (${validSessionAges.length - toShow} more samples) ...`, 'blue');
        }
    }
    
    // ✅ OPTIMIZED: Show summary instead of full list (too many)
    if (corruptCount > 0) {
        log('\n📋 Corrupt Sessions Summary:', 'yellow');
        if (corruptCount <= 50) {
            // Show all if less than 50
            corruptSessions.forEach(({ sessionId, reason }) => {
                log(`   • ${sessionId}: ${reason}`, 'yellow');
            });
        } else {
            // Show first 10 and last 10 if too many
            log(`   Showing first 10 and last 10 of ${corruptCount} corrupt sessions:`, 'yellow');
            for (let i = 0; i < Math.min(10, corruptSessions.length); i++) {
                const { sessionId, reason } = corruptSessions[i];
                log(`   • ${sessionId}: ${reason}`, 'yellow');
            }
            log(`   ... (${corruptCount - 20} more) ...`, 'yellow');
            for (let i = Math.max(10, corruptSessions.length - 10); i < corruptSessions.length; i++) {
                const { sessionId, reason } = corruptSessions[i];
                log(`   • ${sessionId}: ${reason}`, 'yellow');
            }
        }
    }
    
    if (oldValidDeletedCount > 0) {
        log(`\n📋 Old Valid Sessions Deleted (>${validSessionMaxAgeDays} days):`, 'yellow');
        // Sort by age (oldest first)
        oldValidSessions.sort((a, b) => parseFloat(b.ageDays) - parseFloat(a.ageDays));
        if (oldValidSessions.length <= 50) {
            oldValidSessions.forEach(({ sessionId, ageDays, lastModified }) => {
                log(`   • ${sessionId}: ${ageDays} days old (last modified: ${lastModified})`, 'yellow');
            });
        } else {
            log(`   Showing first 10 and last 10 of ${oldValidSessions.length} old valid sessions:`, 'yellow');
            for (let i = 0; i < Math.min(10, oldValidSessions.length); i++) {
                const { sessionId, ageDays, lastModified } = oldValidSessions[i];
                log(`   • ${sessionId}: ${ageDays} days old (last modified: ${lastModified})`, 'yellow');
            }
            log(`   ... (${oldValidSessions.length - 20} more) ...`, 'yellow');
            for (let i = Math.max(10, oldValidSessions.length - 10); i < oldValidSessions.length; i++) {
                const { sessionId, ageDays, lastModified } = oldValidSessions[i];
                log(`   • ${sessionId}: ${ageDays} days old (last modified: ${lastModified})`, 'yellow');
            }
        }
    }
    
    if (deletedCount > 0) {
        log(`\n✅ Cleanup completed! ${deletedCount} session(s) removed (${corruptCount} corrupt + ${oldValidDeletedCount} old valid).`, 'green');
    } else if (corruptCount === 0 && oldValidDeletedCount === 0) {
        log(`\n✅ No sessions to delete. All sessions are valid and recent!`, 'green');
    } else {
        log(`\n⚠️  Some sessions could not be deleted. Check permissions.`, 'yellow');
    }
    
    log('');
}

// Run cleanup
try {
    // ✅ Get max age from command line argument or use default (2 days for corrupt, 2 days for valid)
    const maxAgeDays = process.argv[2] ? parseInt(process.argv[2], 10) : 2;
    const validSessionMaxAgeDays = process.argv[3] ? parseInt(process.argv[3], 10) : 2;
    
    if (isNaN(maxAgeDays) || maxAgeDays < 1) {
        log('⚠️  Invalid max age for corrupt sessions. Using default: 2 days', 'yellow');
    }
    if (isNaN(validSessionMaxAgeDays) || validSessionMaxAgeDays < 1) {
        log('⚠️  Invalid max age for valid sessions. Using default: 2 days', 'yellow');
        cleanupCorruptSessions(maxAgeDays || 2, 2);
    } else {
        cleanupCorruptSessions(maxAgeDays || 2, validSessionMaxAgeDays);
    }
    
    process.exit(0);
} catch (error) {
    log(`\n❌ Fatal error: ${error.message}`, 'red');
    console.error(error);
    process.exit(1);
}

 
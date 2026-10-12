/**
 * Baileys API Routes - Fixed for app integration
 * Menambahkan routes yang compatible dengan App
 * 
 * @version 3.0.0
 * @date 2025-10-08
 */

import BaileysWrapper from './wrapper.js';

export function setupApiRoutes(app, sessionManager, activeWrappers, PORT, WS_PORT, broadcastToSession, sendEventToMainServer) {

    /**
     * Port di sessionId harus sama dengan shard yang dipakai & yang tersimpan di DB (bots.bot_port).
     * Sumber kebenaran: middleware/backend yang create/update bot — kirim `port` di body pair dan/atau `X-Session-Port`.
     * Baileys hanya membaca nilai itu + melekatkan ke `sess_{userId}_{port}_{ts}`; tidak menentukan load-balancing untuk DB.
     * Fleet 7 shard Node: 7001–7007. Port legacy / besar dilipat dengan mod 7.
     */
    const SHARD_PORT_MIN = 7001;
    const SHARD_PORT_MAX = 7007;
    const SHARD_COUNT = SHARD_PORT_MAX - SHARD_PORT_MIN + 1;

    /** 7001–7007: 7001 + ((n - 1) mod 7) untuk n ≥ 1 (slot 1–50, 7001+, legacy). */
    function normalizeToShardPort(n) {
        if (!Number.isFinite(n)) return null;
        const p = Math.floor(n);
        if (p < 1) return null;
        return SHARD_PORT_MIN + ((p - 1) % SHARD_COUNT);
    }

    const IS_SHARD_HTTP_PORT =
        Number.isFinite(PORT) && PORT >= SHARD_PORT_MIN && PORT <= SHARD_PORT_MAX;

    function resolvePairServicePort(req, bodyPort) {
        const here = normalizeToShardPort(Number(PORT)) ?? PORT;
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
                    `⚠️ pair: body.port/X-Session-Port (${claimed}) ≠ shard penerima (${here}) — sess_* memakai PORT=${here}`
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
    
    // ============================================================================
    // 🔌 PAIR ENDPOINT - Support both /pair and /api/pair
    // ============================================================================
    
    const pairHandler = async (req, res) => {
        try {
            const { phoneNumber, userId, botId, port } = req.body;
            
            // ✅ FIX: userId is required, phoneNumber can be null for QR method
            if (!userId) {
                return res.json({ error: "1", msg: "userId is required" });
            }
            
            const actualPort = resolvePairServicePort(req, port);
            const sessionId = `sess_${userId}_${actualPort}_${Date.now()}`;

            if (
                !IS_SHARD_HTTP_PORT &&
                actualPort === PORT &&
                (port === undefined || port === null || String(port).trim() === '') &&
                !req.headers['x-session-port'] &&
                !req.headers['X-Session-Port']
            ) {
                console.warn(`⚠️ pair: body.port & X-Session-Port kosong → pakai PORT proses ini (${PORT}). sessionId=${sessionId} — pastikan middleware/nginx kirim port shard yang benar.`);
            }
            
            console.log(`🔄 Creating session: ${sessionId}`);
            console.log(`   User: ${userId}`);
            console.log(`   Phone: ${phoneNumber || 'QR Method'}`);
            console.log(`   Port: ${actualPort}`);
            
            // Create new session
            await sessionManager.createSession(sessionId, phoneNumber);
            
            // Create and initialize Baileys wrapper
            const wrapper = new BaileysWrapper(sessionId, sessionManager, {
                onQR: (qr) => {
                    console.log(`📱 QR generated for session ${sessionId}`);
                    
                    // Broadcast QR via WebSocket
                    broadcastToSession(sessionId, {
                        type: 'qr_generated',
                        qr: qr,
                        sessionId: sessionId,
                        userId: userId,
                        botId: botId
                    });
                    
                    // Send to main server webhook
                    sendEventToMainServer('QR_GENERATED', {
                        sessionId: sessionId,
                        botId: botId,
                        userId: userId,
                        phoneNumber: phoneNumber,
                        qr: qr,
                        port: actualPort
                    });
                },
                
                onPairingCode: (code) => {
                    console.log(`🔑 Pairing code generated for session ${sessionId}: ${code}`);
                    
                    // Broadcast pairing code via WebSocket
                    broadcastToSession(sessionId, {
                        type: 'pairing_code_generated',
                        code: code,
                        sessionId: sessionId,
                        userId: userId,
                        botId: botId
                    });
                    
                    // Send to main server webhook
                    sendEventToMainServer('PAIRING_CODE_GENERATED', {
                        sessionId: sessionId,
                        botId: botId,
                        userId: userId,
                        phoneNumber: phoneNumber,
                        code: code,
                        port: actualPort
                    });
                },
                
                onConnected: async () => {
                    console.log(`✅ WhatsApp connected for session ${sessionId}`);
                    console.log(`🔍 DEBUG - wrapper.sock.user:`, wrapper.sock?.user);
                    
                    // Tunggu JID asli dari socket — jangan kirim sessionId sebagai @s.whatsapp.net (bikin webhook/DB "palsu connected")
                    let attempts = 40;
                    let botPhone = null;
                    
                    while (attempts > 0 && !botPhone) {
                        await new Promise(resolve => setTimeout(resolve, 500));
                        if (wrapper.sock?.user?.id) {
                            botPhone = wrapper.sock.user.id;
                            console.log(`✅ Got user ID: ${botPhone}`);
                            break;
                        }
                        attempts--;
                        if (attempts % SHARD_COUNT === 0) {
                            console.log(`⏳ Waiting for user JID... ${attempts} attempts left`);
                        }
                    }
                    
                    let actualPhone = botPhone || null;
                    if (!actualPhone && phoneNumber) {
                        const digits = String(phoneNumber).replace(/\D/g, '');
                        if (digits.length >= 10 && digits.length <= 15) {
                            actualPhone = `${digits}@s.whatsapp.net`;
                        }
                    }
                    
                    // Broadcast ke WS (lokal) tetap — tanpa JID pun UI bisa tahu socket open
                    broadcastToSession(sessionId, {
                        type: 'whatsapp_connected',
                        sessionId: sessionId,
                        userId: userId,
                        botId: botId,
                        connectionTime: new Date().toISOString(),
                        bot: actualPhone || undefined
                    });
                    
                    if (!actualPhone) {
                        console.warn(`⚠️ CONNECTED: belum ada JID untuk webhook (session ${sessionId}). Retry sekali ~5s — jangan pakai sessionId sebagai nomor WA.`);
                        setTimeout(() => {
                            const lateJid = wrapper.sock?.user?.id;
                            const digits = phoneNumber ? String(phoneNumber).replace(/\D/g, '') : '';
                            const latePhone = lateJid || (digits.length >= 10 && digits.length <= 15 ? `${digits}@s.whatsapp.net` : null);
                            if (!latePhone) {
                                console.error(`❌ CONNECTED webhook skipped: masih tanpa JID setelah retry (session ${sessionId})`);
                                return;
                            }
                            console.log(`📤 Deferred CONNECTED webhook bot: ${latePhone}`);
                            sendEventToMainServer('CONNECTED', {
                                bot: latePhone,
                                sessionId: sessionId,
                                botId: botId,
                                userId: userId,
                                phoneNumber: phoneNumber,
                                port: actualPort,
                                event: 'CONNECTED'
                            });
                        }, 5000);
                        return;
                    }
                    
                    console.log(`📤 Sending CONNECTED webhook with bot: ${actualPhone}`);
                    sendEventToMainServer('CONNECTED', {
                        bot: actualPhone,
                        sessionId: sessionId,
                        botId: botId,
                        userId: userId,
                        phoneNumber: phoneNumber,
                        port: actualPort,
                        event: 'CONNECTED'
                    });
                },
                
                onDisconnected: (reason) => {
                    console.log(`❌ WhatsApp disconnected for session ${sessionId}: ${reason}`);
                    
                    // Broadcast disconnection
                    broadcastToSession(sessionId, {
                        type: 'whatsapp_disconnected',
                        sessionId: sessionId,
                        userId: userId,
                        botId: botId,
                        reason: reason
                    });
                    
                    // DISCONNECTED lifecycle dikirim oleh wrapper.js setelah validasi reconnect/shutdown.
                    // Jangan kirim dari legacy route ini agar tidak ada double/false disconnect.
                }
            });
            
            activeWrappers.set(sessionId, wrapper);
            
            // Initialize connection
            await wrapper.initialize(phoneNumber);
            
            // ✅ FIX: Return sessionId directly in msg (compatible with App)
            res.json({ 
                error: "0", 
                msg: sessionId,  // App expects sessionId string here
                sessionId: sessionId,
                wsUrl: `ws://localhost:${WS_PORT}/ws?sessionId=${sessionId}&userId=${userId}`,
                port: actualPort
            });
            
        } catch (error) {
            console.error('❌ Pair error:', error);
            res.json({ error: "1", msg: error.message || "Failed to create session" });
        }
    };
    
    // ✅ FIX: Add both routes for compatibility
    app.post('/pair', pairHandler);
    app.post('/api/pair', pairHandler); // Alias untuk compatibility dengan App
    
    // ============================================================================
    // 📡 BROADCAST ENDPOINT - For webhook to broadcast events
    // ============================================================================
    
    app.post('/broadcast', (req, res) => {
        try {
            const apiKey = req.headers['x-broadcast-key'];
            const expectedKey = process.env.BROADCAST_KEY || '';
            
            // Validate API key
            if (apiKey !== expectedKey) {
                return res.status(401).json({ error: "1", msg: "Unauthorized" });
            }
            
            const { event, userId, botId, botNumber, status, messageId, timestamp } = req.body;
            
            console.log(`📡 Broadcasting event: ${event} for user: ${userId}`);
            
            // Broadcast ke semua WebSocket connections untuk user ini
            let broadcastCount = 0;
            sessionWebSockets.forEach((clients, sessionId) => {
                clients.forEach((client) => {
                    // Send to all clients for this user
                    if (client.ws.readyState === 1) { // OPEN
                        client.ws.send(JSON.stringify({
                            event: event,
                            userId: userId,
                            botId: botId,
                            botNumber: botNumber,
                            status: status,
                            messageId: messageId,
                            timestamp: timestamp || Date.now()
                        }));
                        broadcastCount++;
                    }
                });
            });
            
            console.log(`✅ Broadcast sent to ${broadcastCount} WebSocket clients`);
            
            res.json({ 
                error: "0", 
                msg: "Broadcast successful",
                broadcastCount: broadcastCount
            });
            
        } catch (error) {
            console.error('❌ Broadcast error:', error);
            res.status(500).json({ error: "1", msg: error.message });
        }
    });
    
    // ============================================================================
    // 📊 MONITORING ENDPOINTS
    // ============================================================================
    
    app.get('/health', (req, res) => {
        const health = {
            status: 'healthy',
            timestamp: new Date().toISOString(),
            uptime: process.uptime(),
            activeSessions: activeWrappers.size,
            totalWsConnections: Array.from(sessionWebSockets.values()).reduce((sum, clients) => sum + clients.size, 0),
            memory: process.memoryUsage(),
            port: PORT,
            wsPort: WS_PORT
        };
        
        res.json(health);
    });
    
    app.get('/stats', (req, res) => {
        const stats = {
            activeSessions: activeWrappers.size,
            totalWsConnections: Array.from(sessionWebSockets.values()).reduce((sum, clients) => sum + clients.size, 0),
            uptime: process.uptime(),
            memory: process.memoryUsage(),
            timestamp: new Date().toISOString()
        };
        
        res.json({ error: "0", msg: stats });
    });
}



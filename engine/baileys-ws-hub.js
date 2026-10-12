/**
 * Baileys WebSocket Hub — systemd terpisah (port 8001 /ws).
 * Dashboard admin WS + Redis fan-out broadcast. Shard 7001–7007 tidak pegang WS lagi.
 *
 * Env: WS_PORT (8001), WS_HUB_HTTP_PORT (8010), REDIS_URL, DASHBOARD_WS_TIMEOUT_MS
 */
import dotenv from 'dotenv';
import express from 'express';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import { createClient } from 'redis';
import config from './config/config.js';
import { createWsFleetGuard } from './ws-fleet-guard.js';

dotenv.config({ path: '/opt/baileys/.env' });

const WS_PORT = Number(process.env.WS_PORT) || 8001;
const HTTP_PORT = Number(process.env.WS_HUB_HTTP_PORT) || 8010;
const BROADCAST_REDIS_CHANNEL = process.env.BROADCAST_REDIS_CHANNEL || 'baileys:ws-broadcast';
const WS_PING_INTERVAL = 30000;
const WS_TIMEOUT_DURATION = Math.max(
    60000,
    Math.min(900000, Number(process.env.DASHBOARD_WS_TIMEOUT_MS) || 5 * 60 * 1000)
);
const WS_CLEANUP_INTERVAL = 2 * 60 * 1000;
const MAX_DASHBOARD_WS_PER_USER = Math.max(1, Math.min(20, Number(process.env.MAX_DASHBOARD_WS_PER_USER) || 5));

/** userId → Map<clientId, clientData> */
const sessionWebSockets = new Map();
let wss = null;
let wsCleanupInterval = null;
let redisPub = null;
let redisSub = null;

function broadcastToUserLocal(userId, data) {
    if (userId == null || String(userId).trim() === '') return 0;
    const userIdStr = String(userId);
    if (userIdStr === 'undefined' || userIdStr === 'null') return 0;
    if (!sessionWebSockets.has(userIdStr)) return 0;
    let sent = 0;
    sessionWebSockets.get(userIdStr).forEach((client) => {
        if (client.ws.readyState === 1) {
            client.ws.send(JSON.stringify(data));
            sent++;
        }
    });
    return sent;
}

function broadcastToSessionLocal(sessionId, data) {
    if (!sessionWebSockets.has(sessionId)) return 0;
    let sent = 0;
    sessionWebSockets.get(sessionId).forEach((client) => {
        if (client.ws.readyState === 1) {
            client.ws.send(JSON.stringify(data));
            sent++;
        }
    });
    return sent;
}

function startWebSocketServer() {
    const wsFleetGuard = createWsFleetGuard(config);
    console.log(
        `🔌 [WS-HUB] fleetIpLock=${wsFleetGuard.fleetIpLock ? 'on' : 'off'} port=${WS_PORT}`
    );
    wss = new WebSocketServer({
        port: WS_PORT,
        path: '/ws',
        verifyClient: wsFleetGuard.verifyClient,
    });

    wss.on('error', (err) => {
        console.error('❌ [WS-HUB] WebSocket server error:', err.message);
    });

    wss.on('connection', (ws, req) => {
        const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
        const sessionId = url.searchParams.get('sessionId');
        const userId = url.searchParams.get('userId');
        const page = url.searchParams.get('page') || 'unknown';
        const providedClientId = url.searchParams.get('clientId');
        const clientId = providedClientId
            ? `${providedClientId}_${page}_${Date.now()}`
            : `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        const userIdStr = String(userId);

        console.log(`🔌 [WS-HUB] connected ${clientId} user=${userIdStr} page=${page}`);

        if (!sessionWebSockets.has(userIdStr)) {
            sessionWebSockets.set(userIdStr, new Map());
        }
        const existing = sessionWebSockets.get(userIdStr);
        if (existing.size >= MAX_DASHBOARD_WS_PER_USER) {
            try {
                ws.close(1008, 'Too many dashboard connections');
            } catch {
                /* ignore */
            }
            return;
        }

        const clientData = {
            ws,
            sessionId,
            userId: userIdStr,
            clientId,
            page,
            connectedAt: new Date(),
            lastPing: Date.now(),
            lastPong: Date.now(),
            pingInterval: null,
        };
        existing.set(clientId, clientData);

        ws.send(
            JSON.stringify({
                type: 'connected',
                message: 'WebSocket connected to Baileys WS Hub',
                clientId,
                wsPort: WS_PORT,
                pingInterval: WS_PING_INTERVAL,
                timeout: WS_TIMEOUT_DURATION,
            })
        );

        clientData.pingInterval = setInterval(() => {
            try {
                if (ws.readyState !== 1) {
                    clearInterval(clientData.pingInterval);
                    return;
                }
                if (Date.now() - clientData.lastPong > WS_TIMEOUT_DURATION) {
                    clearInterval(clientData.pingInterval);
                    ws.terminate();
                    existing.delete(clientId);
                    if (existing.size === 0) sessionWebSockets.delete(userIdStr);
                    return;
                }
                clientData.lastPing = Date.now();
                ws.send(JSON.stringify({ type: 'ping', timestamp: Date.now() }));
            } catch {
                clearInterval(clientData.pingInterval);
            }
        }, WS_PING_INTERVAL);

        ws.on('message', (raw) => {
            try {
                const message = JSON.parse(raw);
                if (message.type === 'ping') {
                    clientData.lastPong = Date.now();
                    ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
                }
                if (message.type === 'pong') {
                    clientData.lastPong = Date.now();
                }
                if (message.type === 'subscribe_events') {
                    ws.send(JSON.stringify({ type: 'subscribed', events: message.events }));
                }
            } catch {
                /* ignore */
            }
        });

        ws.on('close', () => {
            if (clientData.pingInterval) clearInterval(clientData.pingInterval);
            existing.delete(clientId);
            if (existing.size === 0) sessionWebSockets.delete(userIdStr);
        });

        ws.on('error', (e) => console.error(`[WS-HUB] client ${clientId}:`, e.message));
    });

    wsCleanupInterval = setInterval(() => {
        const now = Date.now();
        sessionWebSockets.forEach((clients, uid) => {
            clients.forEach((client, cid) => {
                if (now - client.lastPong > WS_TIMEOUT_DURATION) {
                    if (client.pingInterval) clearInterval(client.pingInterval);
                    try {
                        client.ws.close();
                    } catch {
                        /* ignore */
                    }
                    clients.delete(cid);
                }
            });
            if (clients.size === 0) sessionWebSockets.delete(uid);
        });
    }, WS_CLEANUP_INTERVAL);

    console.log(`✅ [WS-HUB] WebSocket listening ws://0.0.0.0:${WS_PORT}/ws`);
}

async function startRedisSubscriber() {
    const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
    redisPub = createClient({ url: redisUrl });
    redisSub = redisPub.duplicate();
    redisPub.on('error', (e) => console.error('[WS-HUB] redis pub:', e.message));
    redisSub.on('error', (e) => console.error('[WS-HUB] redis sub:', e.message));
    await redisPub.connect();
    await redisSub.connect();
    await redisSub.subscribe(BROADCAST_REDIS_CHANNEL, (message) => {
        try {
            const o = JSON.parse(message);
            if (o.k === 'u' && o.id != null) {
                broadcastToUserLocal(o.id, o.d);
            } else if (o.k === 's' && o.id != null) {
                broadcastToSessionLocal(o.id, o.d);
            }
        } catch (e) {
            console.error('[WS-HUB] broadcast handle:', e.message);
        }
    });
    console.log(`✅ [WS-HUB] Redis subscribe ${BROADCAST_REDIS_CHANNEL}`);
}

function startHttpServer() {
    const app = express();
    app.use(express.json({ limit: '4mb' }));

    app.get('/health', (req, res) => {
        const light = req.query.light === '1' || req.query.ping === '1';
        let clients = 0;
        sessionWebSockets.forEach((m) => {
            clients += m.size;
        });
        res.json({
            status: 'healthy',
            service: 'baileys-ws-hub',
            light: !!light,
            wsPort: WS_PORT,
            dashboardWebSocketClients: clients,
            redisConnected: !!redisPub?.isReady,
            uptime: process.uptime(),
        });
    });

    app.post('/broadcast-internal', (req, res) => {
        const { userId, sessionId, data } = req.body || {};
        if (userId) broadcastToUserLocal(userId, data);
        else if (sessionId) broadcastToSessionLocal(sessionId, data);
        res.json({ error: '0', msg: 'Broadcast sent' });
    });

    const server = createServer(app);
    server.listen(HTTP_PORT, '0.0.0.0', () => {
        console.log(`✅ [WS-HUB] HTTP health/broadcast :${HTTP_PORT}`);
    });
}

async function main() {
    startWebSocketServer();
    await startRedisSubscriber();
    startHttpServer();
}

function shutdown(sig) {
    console.log(`🛑 [WS-HUB] ${sig} shutting down`);
    if (wsCleanupInterval) clearInterval(wsCleanupInterval);
    if (wss) wss.close();
    Promise.all([redisPub?.quit(), redisSub?.quit()]).finally(() => process.exit(0));
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

main().catch((e) => {
    console.error('❌ [WS-HUB] fatal:', e);
    process.exit(1);
});

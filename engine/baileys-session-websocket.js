import express from 'express';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import dotenv from 'dotenv';
import SessionManager from './session.js';
import BaileysWrapper from './enhanced-wrapper.js';
import axios from 'axios';
import fs from 'fs';
import path from 'path';

// Load environment variables
dotenv.config();

const app = express();
const server = createServer(app);
const PORT = process.env.PORT || 7001;
const WS_PORT = process.env.WS_PORT || 8001;

// WebSocket server untuk per-session real-time updates
const wss = new WebSocketServer({ 
    port: WS_PORT,
    path: '/ws'
});

// Middleware
app.use(express.json());
app.use(express.static('public'));

// Initialize session manager
const sessionManager = new SessionManager();

// Store active Baileys wrappers per session
const activeWrappers = new Map();

// Store WebSocket clients per session
const sessionWebSockets = new Map(); // sessionId -> Set of WebSocket connections

// WebSocket connection handler - PER SESSION
wss.on('connection', (ws, req) => {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const sessionId = url.searchParams.get('sessionId');
    const userId = url.searchParams.get('userId');
    const clientId = url.searchParams.get('clientId') || `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    console.log(`🔌 WebSocket client connected: ${clientId} for session: ${sessionId}, user: ${userId}`);
    
    // Store client by session
    if (!sessionWebSockets.has(sessionId)) {
        sessionWebSockets.set(sessionId, new Map());
    }
    
    sessionWebSockets.get(sessionId).set(clientId, {
        ws: ws,
        userId: userId,
        connectedAt: Date.now(),
        lastPing: Date.now()
    });
    
    // Send welcome message
    ws.send(JSON.stringify({
        type: 'connection_established',
        sessionId: sessionId,
        clientId: clientId,
        userId: userId,
        timestamp: new Date().toISOString(),
        message: 'Connected to Baileys Session WebSocket'
    }));
    
    // Handle client messages
    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            handleWebSocketMessage(ws, sessionId, userId, clientId, data);
        } catch (error) {
            console.error('Error parsing WebSocket message:', error);
        }
    });
    
    // Handle client disconnect
    ws.on('close', () => {
        console.log(`🔌 WebSocket client disconnected: ${clientId} for session: ${sessionId}`);
        
        if (sessionWebSockets.has(sessionId)) {
            sessionWebSockets.get(sessionId).delete(clientId);
            
            // Clean up empty session
            if (sessionWebSockets.get(sessionId).size === 0) {
                sessionWebSockets.delete(sessionId);
            }
        }
    });
    
    // Handle errors
    ws.on('error', (error) => {
        console.error(`WebSocket error for client ${clientId}, session ${sessionId}:`, error);
    });
    
    // Setup ping/pong for connection health
    ws.on('pong', () => {
        if (sessionWebSockets.has(sessionId) && sessionWebSockets.get(sessionId).has(clientId)) {
            sessionWebSockets.get(sessionId).get(clientId).lastPing = Date.now();
        }
    });
});

// Handle WebSocket messages
function handleWebSocketMessage(ws, sessionId, userId, clientId, data) {
    switch (data.type) {
        case 'ping':
            ws.send(JSON.stringify({ 
                type: 'pong', 
                clientId: clientId,
                timestamp: new Date().toISOString() 
            }));
            break;
            
        case 'subscribe_events':
            // Client subscribe to specific events for this session
            ws.send(JSON.stringify({ 
                type: 'events_subscribed', 
                sessionId: sessionId,
                clientId: clientId,
                events: data.events || ['all'],
                timestamp: new Date().toISOString() 
            }));
            break;
            
        case 'get_session_status':
            const wrapper = activeWrappers.get(sessionId);
            if (wrapper) {
                ws.send(JSON.stringify({
                    type: 'session_status_update',
                    sessionId: sessionId,
                    clientId: clientId,
                    status: wrapper.getStatus(),
                    timestamp: new Date().toISOString()
                }));
            }
            break;
            
        case 'request_qr':
            // Trigger QR generation for this session
            const sessionWrapper = activeWrappers.get(sessionId);
            if (sessionWrapper && !sessionWrapper.isAuthenticated) {
                // Force QR regeneration
                sessionWrapper.generateQR();
            }
            break;
            
        default:
            console.log(`Unknown WebSocket message type: ${data.type}`);
    }
}

// Broadcast to specific session WebSocket clients
function broadcastToSession(sessionId, message, excludeClientId = null) {
    if (!sessionWebSockets.has(sessionId)) {
        return;
    }
    
    const clients = sessionWebSockets.get(sessionId);
    const messageStr = JSON.stringify({
        ...message,
        sessionId: sessionId,
        timestamp: new Date().toISOString()
    });
    
    let sentCount = 0;
    clients.forEach((client, clientId) => {
        if (clientId !== excludeClientId && client.ws.readyState === 1) { // WebSocket.OPEN
            try {
                client.ws.send(messageStr);
                sentCount++;
            } catch (error) {
                console.error(`Failed to send message to client ${clientId}:`, error);
                // Remove dead connection
                clients.delete(clientId);
            }
        }
    });
    
    console.log(`📡 Broadcasted to ${sentCount} clients in session ${sessionId}: ${message.type}`);
    
    // Clean up empty session
    if (clients.size === 0) {
        sessionWebSockets.delete(sessionId);
    }
}

// Broadcast to specific user across all sessions
function broadcastToUser(userId, message) {
    let totalSent = 0;
    
    sessionWebSockets.forEach((clients, sessionId) => {
        clients.forEach((client, clientId) => {
            if (client.userId === userId && client.ws.readyState === 1) {
                try {
                    client.ws.send(JSON.stringify({
                        ...message,
                        sessionId: sessionId,
                        userId: userId,
                        timestamp: new Date().toISOString()
                    }));
                    totalSent++;
                } catch (error) {
                    console.error(`Failed to send message to user ${userId}, client ${clientId}:`, error);
                }
            }
        });
    });
    
    console.log(`📡 Broadcasted to ${totalSent} clients for user ${userId}: ${message.type}`);
}

// Enhanced /pair endpoint dengan session-specific WebSocket
app.post('/pair', async (req, res) => {
    try {
        const { phoneNumber, userId, botId } = req.body;
        
        if (!phoneNumber || !userId) {
            throw new Error('phoneNumber and userId are required');
        }
        
        // Create new session
        const sessionId = await sessionManager.createSession(phoneNumber);
        
        // Create and initialize Baileys wrapper dengan session-specific callbacks
        const wrapper = new BaileysWrapper(sessionId, sessionManager, {
            onQR: (qr) => {
                console.log(`📱 QR generated for session ${sessionId}, user ${userId}`);
                
                // Broadcast QR to ALL clients in this session
                broadcastToSession(sessionId, {
                    type: 'qr_generated',
                    qr: qr,
                    phoneNumber: phoneNumber,
                    userId: userId,
                    botId: botId
                });
                
                // Send event to main server
                sendEventToMainServer('QR_GENERATED', {
                    sessionId: sessionId,
                    botId: botId,
                    userId: userId,
                    phoneNumber: phoneNumber,
                    qrGenerated: true
                });
            },
            
            onConnected: () => {
                console.log(`✅ WhatsApp connected for session ${sessionId}, user ${userId}`);
                
                // Broadcast connection to ALL clients in this session
                broadcastToSession(sessionId, {
                    type: 'whatsapp_connected',
                    phoneNumber: phoneNumber,
                    userId: userId,
                    botId: botId,
                    connectionTime: new Date().toISOString()
                });
                
                // Send event to main server
                sendEventToMainServer('BOT_CONNECTED', {
                    sessionId: sessionId,
                    botId: botId,
                    userId: userId,
                    phoneNumber: phoneNumber,
                    port: PORT,
                    connectionTime: new Date().toISOString()
                });
            },
            
            onDisconnected: (reason) => {
                console.log(`❌ WhatsApp disconnected for session ${sessionId}, user ${userId}: ${reason}`);
                
                // Broadcast disconnection to ALL clients in this session
                broadcastToSession(sessionId, {
                    type: 'whatsapp_disconnected',
                    phoneNumber: phoneNumber,
                    userId: userId,
                    botId: botId,
                    reason: reason,
                    disconnectionTime: new Date().toISOString()
                });
                
                // Send event to main server
                sendEventToMainServer('BOT_DISCONNECTED', {
                    sessionId: sessionId,
                    botId: botId,
                    userId: userId,
                    phoneNumber: phoneNumber,
                    reason: reason,
                    disconnectionTime: new Date().toISOString()
                });
            },
            
            onMessageReceived: (message) => {
                console.log(`💬 Message received for session ${sessionId}, user ${userId}`);
                
                // Broadcast message to ALL clients in this session
                broadcastToSession(sessionId, {
                    type: 'message_received',
                    message: message,
                    phoneNumber: phoneNumber,
                    userId: userId,
                    botId: botId,
                    receivedTime: new Date().toISOString()
                });
                
                // Send event to main server
                sendEventToMainServer('MESSAGE_RECEIVED', {
                    sessionId: sessionId,
                    botId: botId,
                    userId: userId,
                    phoneNumber: phoneNumber,
                    message: message,
                    receivedTime: new Date().toISOString()
                });
            },
            
            onMessageSent: (message) => {
                console.log(`📤 Message sent for session ${sessionId}, user ${userId}`);
                
                // Broadcast message sent to ALL clients in this session
                broadcastToSession(sessionId, {
                    type: 'message_sent',
                    message: message,
                    phoneNumber: phoneNumber,
                    userId: userId,
                    botId: botId,
                    sentTime: new Date().toISOString()
                });
                
                // Send event to main server
                sendEventToMainServer('MESSAGE_SENT', {
                    sessionId: sessionId,
                    botId: botId,
                    userId: userId,
                    phoneNumber: phoneNumber,
                    message: message,
                    sentTime: new Date().toISOString()
                });
            },
            
            onTyping: (isTyping, from) => {
                // Broadcast typing status to ALL clients in this session
                broadcastToSession(sessionId, {
                    type: 'typing_status',
                    isTyping: isTyping,
                    from: from,
                    phoneNumber: phoneNumber,
                    userId: userId,
                    timestamp: new Date().toISOString()
                });
            },
            
            onPresenceUpdate: (presence) => {
                // Broadcast presence update to ALL clients in this session
                broadcastToSession(sessionId, {
                    type: 'presence_update',
                    presence: presence,
                    phoneNumber: phoneNumber,
                    userId: userId,
                    timestamp: new Date().toISOString()
                });
            }
        });
        
        activeWrappers.set(sessionId, wrapper);
        
        await wrapper.initialize(phoneNumber);

        // Response dengan WebSocket connection info
        const response = {
            sessionId: sessionId,
            wsUrl: `ws://localhost:${WS_PORT}/ws?sessionId=${sessionId}&userId=${userId}`,
            apiUrl: `http://localhost:${PORT}`,
            status: 'initializing'
        };

        res.json({ error: "0", msg: response });
        
    } catch (error) {
        console.error('Pair error:', error);
        res.json({ error: "1", msg: error.message || error });
    }
});

// Enhanced /send endpoint dengan session-specific feedback
app.post('/send', async (req, res) => {
    try {
        const { sessionId, to, message, messageType = 'text', userId } = req.body;
        
        if (!sessionId || !to || !message) {
            throw new Error('sessionId, to, and message are required');
        }
        
        const wrapper = activeWrappers.get(sessionId);
        if (!wrapper) {
            throw new Error('Session not found');
        }
        
        if (!wrapper.isAuthenticated) {
            throw new Error('WhatsApp not connected');
        }
        
        // Send message
        const result = await wrapper.sendMessage(to, message, messageType);
        
        // Broadcast message sent event to ALL clients in this session
        broadcastToSession(sessionId, {
            type: 'message_sending_complete',
            result: result,
            to: to,
            message: message,
            messageType: messageType,
            userId: userId,
            sentTime: new Date().toISOString()
        });
        
        res.json({ error: "0", msg: result });
        
    } catch (error) {
        console.error('Send error:', error);
        
        // Broadcast error to session clients
        const { sessionId } = req.body;
        if (sessionId) {
            broadcastToSession(sessionId, {
                type: 'message_send_error',
                error: error.message,
                userId: req.body.userId,
                timestamp: new Date().toISOString()
            });
        }
        
        res.json({ error: "1", msg: error.message || error });
    }
});

// Get session-specific QR code
app.get('/qr/:sessionId', async (req, res) => {
    try {
        const { sessionId } = req.params;
        const wrapper = activeWrappers.get(sessionId);
        
        if (!wrapper) {
            throw new Error('Session not found');
        }
        
        const qr = await wrapper.getQR();
        if (qr) {
            res.send(qr);
        } else {
            res.status(404).json({ error: "1", msg: "QR not available" });
        }
        
    } catch (error) {
        res.json({ error: "1", msg: error.message || error });
    }
});

// Get session-specific status
app.get('/status/:sessionId', async (req, res) => {
    try {
        const { sessionId } = req.params;
        const wrapper = activeWrappers.get(sessionId);
        
        if (!wrapper) {
            throw new Error('Session not found');
        }
        
        const status = wrapper.getStatus();
        const wsClients = sessionWebSockets.has(sessionId) ? sessionWebSockets.get(sessionId).size : 0;
        
        const response = {
            ...status,
            wsClients: wsClients,
            hasActiveConnections: wsClients > 0
        };
        
        res.json({ error: "0", msg: response });
        
    } catch (error) {
        res.json({ error: "1", msg: error.message || error });
    }
});

// Logout session
app.post('/logout/:sessionId', async (req, res) => {
    try {
        const { sessionId } = req.params;
        const wrapper = activeWrappers.get(sessionId);
        
        if (wrapper) {
            await wrapper.logout();
            activeWrappers.delete(sessionId);
            
            // Broadcast logout to ALL clients in this session
            broadcastToSession(sessionId, {
                type: 'session_logged_out',
                sessionId: sessionId,
                logoutTime: new Date().toISOString()
            });
            
            // Close all WebSocket connections for this session
            if (sessionWebSockets.has(sessionId)) {
                sessionWebSockets.get(sessionId).forEach((client) => {
                    if (client.ws.readyState === 1) {
                        client.ws.close();
                    }
                });
                sessionWebSockets.delete(sessionId);
            }
        }
        
        res.json({ error: "0", msg: 'Logged out successfully' });
        
    } catch (error) {
        res.json({ error: "1", msg: error.message || error });
    }
});

// Get all active sessions dengan WebSocket info
app.get('/sessions', (req, res) => {
    const sessions = [];
    
    activeWrappers.forEach((wrapper, sessionId) => {
        const wsClients = sessionWebSockets.has(sessionId) ? sessionWebSockets.get(sessionId).size : 0;
        
        sessions.push({
            sessionId: sessionId,
            status: wrapper.getStatus(),
            wsClients: wsClients,
            hasActiveConnections: wsClients > 0
        });
    });
    
    res.json({ error: "0", msg: sessions });
});

// Health check endpoint
app.get('/health', (req, res) => {
    const health = {
        status: 'healthy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        activeSessions: activeWrappers.size,
        totalWsConnections: Array.from(sessionWebSockets.values()).reduce((sum, clients) => sum + clients.size, 0),
        sessionWsConnections: Object.fromEntries(
            Array.from(sessionWebSockets.entries()).map(([sessionId, clients]) => [sessionId, clients.size])
        ),
        memory: process.memoryUsage(),
        port: PORT,
        wsPort: WS_PORT
    };
    
    res.json(health);
});

// Get server stats
app.get('/stats', (req, res) => {
    const stats = {
        activeSessions: activeWrappers.size,
        totalWsConnections: Array.from(sessionWebSockets.values()).reduce((sum, clients) => sum + clients.size, 0),
        totalMessages: Array.from(activeWrappers.values()).reduce((sum, wrapper) => sum + wrapper.messageCount, 0),
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        timestamp: new Date().toISOString(),
        sessionDetails: Array.from(sessionWebSockets.entries()).map(([sessionId, clients]) => ({
            sessionId: sessionId,
            wsClients: clients.size,
            clientIds: Array.from(clients.keys())
        }))
    };
    
    res.json({ error: "0", msg: stats });
});

// Send event to main server
async function sendEventToMainServer(eventType, data) {
    try {
        const eventData = {
            event_type: eventType,
            data: {
                ...data,
                port: PORT,
                wsPort: WS_PORT,
                timestamp: new Date().toISOString()
            },
            api_key: process.env.WEBHOOK_API_KEY || ''
        };
        
        const response = await axios.post(
            process.env.EVENT_WEBHOOK_URL || `${process.env.BE_BASE_URL || process.env.DEFAULT_BE_BASE || 'http://localhost'}/webhook_events.php`,
            eventData,
            {
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${process.env.WEBHOOK_API_KEY || ''}`
                },
                timeout: 5000
            }
        );
        
        console.log(`✅ Event sent to main server: ${eventType}`);
        
    } catch (error) {
        console.error(`❌ Failed to send event to main server: ${eventType}`, error.message);
    }
}

// Cleanup dead WebSocket connections
setInterval(() => {
    const now = Date.now();
    sessionWebSockets.forEach((clients, sessionId) => {
        clients.forEach((client, clientId) => {
            // Check if connection is dead (no ping for 2 minutes)
            if (now - client.lastPing > 120000) {
                console.log(`🧹 Cleaning up dead connection: ${clientId} in session ${sessionId}`);
                try {
                    client.ws.close();
                } catch (error) {
                    // Connection already closed
                }
                clients.delete(clientId);
            }
        });
        
        // Clean up empty session
        if (clients.size === 0) {
            sessionWebSockets.delete(sessionId);
        }
    });
}, 60000); // Check every minute

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('🛑 Shutting down Baileys Session WebSocket server...');
    
    // Close all WebSocket connections
    sessionWebSockets.forEach((clients) => {
        clients.forEach((client) => {
            client.ws.close();
        });
    });
    
    // Close server
    server.close(() => {
        console.log('✅ Server closed');
        process.exit(0);
    });
});

// Start server
server.listen(PORT, () => {
    console.log(`🚀 Baileys Session WebSocket server running on port ${PORT}`);
    console.log(`🔌 WebSocket server running on port ${WS_PORT}`);
    console.log(`📊 Health check: http://localhost:${PORT}/health`);
    console.log(`📈 Stats: http://localhost:${PORT}/stats`);
    console.log(`🔗 WebSocket URL: ws://localhost:${WS_PORT}/ws?sessionId=YOUR_SESSION_ID&userId=YOUR_USER_ID`);
});

import './quiet-console.js';
import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';
import Database from './database.js';
import config from './config/config.js';

let sessionHeapCleanupInterval = null;

class SessionManager {
    constructor() {
        this.sessions = new Map();
        this.database = new Database();
        this.getProxyUrl = config.proxy.enabled ? config.proxy.primaryUrl : null;
        this.getProxyApiKey = config.proxy.enabled ? config.proxy.apiKey : null;
        this.webhookUrl = config.webhook.url;
        this.port = config.server.httpPort;
        
        // JANGAN pakai GC periodik default: dengan --expose-gc (drop-in 32G) ini bikin pause global
        // tiap 30s → API & /health “kumat” / hank. GC terkontrol sudah ada di index (setelah RAM evict, dll).
        // Aktifkan hanya jika sengaja: SESSION_MANAGER_AUTO_GC_MS=300000 (ms), mis. 5 menit.
        const gcMsRaw = process.env.SESSION_MANAGER_AUTO_GC_MS;
        const gcMs = gcMsRaw != null && String(gcMsRaw).trim() !== '' ? Number(gcMsRaw) : 0;
        if (
            !sessionHeapCleanupInterval &&
            typeof global.gc === 'function' &&
            Number.isFinite(gcMs) &&
            gcMs >= 60000
        ) {
            sessionHeapCleanupInterval = setInterval(() => {
                try {
                    global.gc();
                    const memUsage = process.memoryUsage();
                    const heapUsedMB = (memUsage.heapUsed / 1024 / 1024).toFixed(2);
                    const heapTotalMB = (memUsage.heapTotal / 1024 / 1024).toFixed(2);
                    console.log(`🧹 [HEAP CLEAN] Heap: ${heapUsedMB}MB / ${heapTotalMB}MB`);
                } catch (err) {
                    console.warn(`⚠️ [HEAP CLEAN] Error during GC:`, err.message);
                }
            }, gcMs);
            console.log(`✅ SessionManager periodic GC enabled every ${gcMs}ms (SESSION_MANAGER_AUTO_GC_MS)`);
        }
    }

    generateSessionId() {
        return 'baileys_' + uuidv4();
    }

    async fetchUniqueProxy() {
        try {
            if (!this.getProxyUrl || !this.getProxyApiKey) {
                console.warn('Proxy configuration not provided - sessions will run without proxy');
                return null;
            }

            console.log('Fetching unique proxy for session...');
            
            const response = await axios.post(this.getProxyUrl, {
                passwordnya: this.getProxyApiKey
            }, {
                timeout: 10000,
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            if (response.data.error === "0" && response.data.proxy) {
                const proxyAddress = response.data.proxy;
                console.log(`Proxy fetched successfully: ${proxyAddress.replace(/:[^:@]*@/, ':***@')}`);
                return proxyAddress;
            } else {
                console.error('Failed to fetch proxy:', response.data);
                return null;
            }
        } catch (error) {
            console.error('Error fetching proxy:', error.message);
            return null;
        }
    }

    async pushWebhook(eventName, sessionId, data = null) {
        if (!this.webhookUrl) {
            console.warn('Webhook URL not configured - events will not be pushed');
            return;
        }

        try {
            const payload = {
                event: eventName,
                port: this.port,
                sessionId: sessionId,
                data: data || null
            };

            await axios.post(this.webhookUrl, payload, {
                timeout: 5000,
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            console.log(`Webhook pushed: ${eventName} for session ${sessionId}`);
        } catch (error) {
            console.error('Error pushing webhook:', error.message);
        }
    }

    async createSession(sessionIdOrPhoneNumber = null, phoneNumber = null) {
        const explicitSessionId = phoneNumber !== null;
        const sessionId = explicitSessionId ? sessionIdOrPhoneNumber : this.generateSessionId();
        const resolvedPhoneNumber = explicitSessionId ? phoneNumber : sessionIdOrPhoneNumber;
        
        await this.database.createSession(sessionId, resolvedPhoneNumber);
        
        this.sessions.set(sessionId, {
            id: sessionId,
            phoneNumber: resolvedPhoneNumber,
            isActive: false,
            socket: null
        });

        return sessionId;
    }

    getSession(sessionId) {
        return this.sessions.get(sessionId);
    }

    async updateSessionLogin(sessionId, isLoggedIn) {
        await this.database.updateSessionLogin(sessionId, isLoggedIn);
        
        const session = this.sessions.get(sessionId);
        if (session) {
            session.isActive = isLoggedIn;
        }
    }

    async deleteSession(sessionId) {
        const session = this.sessions.get(sessionId);
        if (session && session.socket) {
            try {
                await session.socket.logout();
            } catch (error) {
                console.error('Error logging out socket:', error.message);
            }
        }

        this.sessions.delete(sessionId);
        await this.database.deleteSession(sessionId);
    }

    async getActiveSessions() {
        return await this.database.getActiveSessions();
    }

    async getAllSessions() {
        return await this.database.getAllSessions();
    }

    async cleanupInactiveSessions(maxAgeHours = 24) {
        const allSessions = await this.database.getAllSessions();
        const now = Date.now();
        let cleaned = 0;
        
        for (const session of allSessions) {
            if (!session.isLoggedIn) {
                console.log(`🧹 Cleaning up inactive session: ${session.sessionId}`);
                await this.deleteSession(session.sessionId);
                cleaned++;
                continue;
            }
            
            if (session.createdAt) {
                const sessionAge = now - new Date(session.createdAt).getTime();
                const maxAge = maxAgeHours * 60 * 60 * 1000;
                
                if (sessionAge > maxAge && !session.isLoggedIn) {
                    console.log(`⏰ Cleaning up old session (${Math.floor(sessionAge/3600000)}h): ${session.sessionId}`);
                    await this.deleteSession(session.sessionId);
                    cleaned++;
                }
            }
        }
        
        if (cleaned > 0) {
            console.log(`✅ Cleaned up ${cleaned} inactive sessions`);
        }
        
        return cleaned;
    }

    close() {
        if (sessionHeapCleanupInterval) {
            clearInterval(sessionHeapCleanupInterval);
            sessionHeapCleanupInterval = null;
        }
        this.database.close();
    }
}

export default SessionManager;


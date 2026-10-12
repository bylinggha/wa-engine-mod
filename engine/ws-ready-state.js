/**
 * byee-wamod: `sock.ws` adalah WebSocketClient — WebSocket asli ada di `.socket`.
 * Cek `sock.ws.readyState` selalu undefined → health/send logic kira mati padahal hidup.
 * @param {object | null | undefined} sock — Baileys socket (makeWASocket)
 * @returns {number|undefined} 0–3 seperti WebSocket standar, atau undefined
 */
export function getBaileysWebSocketReadyState(sock) {
    try {
        const client = sock?.ws;
        if (client == null) {
            return undefined;
        }
        if (typeof client.readyState === 'number') {
            return client.readyState;
        }
        const raw = client.socket;
        if (raw != null && typeof raw.readyState === 'number') {
            return raw.readyState;
        }
        if (client.isOpen === true) {
            return 1;
        }
        if (client.isConnecting === true) {
            return 0;
        }
        if (client.isClosing === true) {
            return 2;
        }
        if (client.isClosed === true) {
            return 3;
        }
    } catch {
        /* ignore */
    }
    return undefined;
}

/** WebSocket OPEN (1), kompatibel byee-wamod + Baileys lama */
export function isBaileysWebSocketOpen(sock) {
    return getBaileysWebSocketReadyState(sock) === 1;
}

/** Filter JID noise — broadcast/status/newsletter (#1879) */
export function shouldIgnoreBaileysJid(jid) {
    if (!jid || typeof jid !== 'string') {
        return false;
    }
    if (jid === 'status@broadcast' || jid.endsWith('@broadcast')) {
        return true;
    }
    if (jid.endsWith('@newsletter')) {
        return true;
    }
    return false;
}

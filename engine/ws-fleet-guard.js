/**
 * Guard IP untuk layanan WebSocket terpisah (port WS_PORT / default 8001).
 * Bukan bagian Express HTTP shard 7001–7007 — hanya dipakai WebSocketServer di shard 7001.
 *
 * Env:
 *   WS_FLEET_IP_LOCK=1|0     — default 1 (hanya IP fleet)
 *   WS_ALLOWED_IPS_EXTRA=    — tambahan IP khusus WS (fallback: ALLOWED_IPS_EXTRA)
 */

function parseIpList(raw) {
    return String(raw || '')
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);
}

function fleetIpLockEnabled() {
    const v = String(process.env.WS_FLEET_IP_LOCK ?? '1').trim().toLowerCase();
    return !['0', 'false', 'off', 'no'].includes(v);
}

export function getWsClientIP(req) {
    const h = req?.headers || {};
    const remote = req?.socket?.remoteAddress || req?.connection?.remoteAddress || '';
    const remoteNorm = String(remote).replace(/^::ffff:/, '');
    // Koneksi dari nginx lib (127.0.0.1) — IP sudah dicek snippet lib-ws-fleet-allow; jangan pakai XFF browser via admin proxy
    if (remoteNorm === '127.0.0.1' || remoteNorm === '::1') {
        return remoteNorm;
    }
    return (
        h['cf-connecting-ip'] ||
        h['x-forwarded-for']?.split(',')[0]?.trim() ||
        remoteNorm ||
        ''
    );
}

/**
 * @param {import('./config/config.js').default} config
 */
export function createWsFleetGuard(config) {
    const base = config?.security?.allowedIPs || [];
    const extra = parseIpList(process.env.WS_ALLOWED_IPS_EXTRA || process.env.ALLOWED_IPS_EXTRA);
    const allowedIPs = [...base, ...extra];
    const fleetIpLock = fleetIpLockEnabled();

    function isWsClientAllowed(clientIP) {
        if (!fleetIpLock) return true;
        if (!clientIP) return false;
        const ip = String(clientIP).trim();
        return allowedIPs.some((allowed) => ip.includes(allowed));
    }

    function verifyClient(info, cb) {
        if (!fleetIpLock) {
            return cb(true);
        }
        const clientIP = getWsClientIP(info.req);
        if (isWsClientAllowed(clientIP)) {
            return cb(true);
        }
        console.warn(`🚫 [WS:${config?.server?.wsPort ?? 8001}] upgrade blocked from: ${clientIP}`);
        cb(false, 403, 'Forbidden');
    }

    return {
        allowedIPs,
        fleetIpLock,
        getWsClientIP,
        isWsClientAllowed,
        verifyClient,
    };
}

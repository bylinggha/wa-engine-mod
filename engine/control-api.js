/**
 * ═══════════════════════════════════════════════════════════════════════════
 * 🎮 Baileys Control API - Remote Management Server
 * ═══════════════════════════════════════════════════════════════════════════
 * Purpose: Provide HTTP API for remote control of Baileys instances
 * Port: 9000
 * Access: IP Whitelisted (env ALLOWED_IPS + localhost)
 * ═══════════════════════════════════════════════════════════════════════════
 */

import express from 'express';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);
const app = express();
const PORT = process.env.CONTROL_API_PORT || 9000;

// IP Whitelist - Main server only
const ALLOWED_IPS = [
    ...(process.env.ALLOWED_IPS || '').split(',').map((s) => s.trim()).filter(Boolean),  // dari env ALLOWED_IPS (dipisah koma)
    '127.0.0.1',       // Localhost
    '::1',             // IPv6 localhost
    '::ffff:127.0.0.1' // IPv6-mapped IPv4 localhost
];

// Middleware
app.use(express.json());

// IP Whitelist Middleware
app.use((req, res, next) => {
    const clientIP = req.ip || req.connection.remoteAddress;
    
    console.log(`📡 Request from IP: ${clientIP}`);
    
    // Check if IP is whitelisted
    const isAllowed = ALLOWED_IPS.some(allowedIP => clientIP.includes(allowedIP));
    
    if (!isAllowed) {
        console.log(`❌ Unauthorized access attempt from: ${clientIP}`);
        return res.status(403).json({
            error: '1',
            msg: 'Access denied: IP not whitelisted'
        });
    }
    
    next();
});

// Logging Middleware
app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.path} from ${req.ip}`);
    next();
});

/**
 * ═══════════════════════════════════════════════════════════════════════════
 * ROUTES
 * ═══════════════════════════════════════════════════════════════════════════
 */

/**
 * Health Check
 */
app.get('/health', (req, res) => {
    res.json({
        error: '0',
        msg: 'Control API is running',
        timestamp: Date.now(),
        uptime: process.uptime()
    });
});

/**
 * Execute Guardian Commands
 */
app.post('/execute', async (req, res) => {
    const { command, port, action } = req.body;
    
    console.log(`🎮 Execute command: ${command}`, req.body);
    
    try {
        let cmd = '';
        const guardianScript = '/opt/baileys/baileys-guardian.sh';
        
        switch (command) {
            case 'status':
                cmd = `${guardianScript} status`;
                break;
                
            case 'status-all':
                cmd = `${guardianScript} status-all`;
                break;
                
            case 'start-all':
                cmd = `${guardianScript} start-all`;
                break;
                
            case 'stop-all':
                cmd = `${guardianScript} stop-all`;
                break;
                
            case 'restart-all':
                cmd = `${guardianScript} restart-all`;
                break;
                
            case 'start-port':
                if (!port) {
                    return res.status(400).json({ error: '1', msg: 'Port required' });
                }
                cmd = `${guardianScript} start ${port}`;
                break;
                
            case 'stop-port':
                if (!port) {
                    return res.status(400).json({ error: '1', msg: 'Port required' });
                }
                cmd = `${guardianScript} stop ${port}`;
                break;
                
            case 'restart-port':
                if (!port) {
                    return res.status(400).json({ error: '1', msg: 'Port required' });
                }
                cmd = `${guardianScript} restart ${port}`;
                break;
                
            case 'kill-port':
                if (!port) {
                    return res.status(400).json({ error: '1', msg: 'Port required' });
                }
                cmd = `${guardianScript} kill ${port}`;
                break;
                
            case 'health-check':
                cmd = `${guardianScript} health`;
                break;
                
            case 'cleanup':
                cmd = `${guardianScript} cleanup`;
                break;
                
            case 'logs':
                if (!port) {
                    return res.status(400).json({ error: '1', msg: 'Port required' });
                }
                cmd = `tail -n 50 /opt/baileys/logs/baileys-${port}.log`;
                break;
                
            default:
                return res.status(400).json({
                    error: '1',
                    msg: 'Unknown command',
                    available: [
                        'status', 'status-all', 'start-all', 'stop-all', 'restart-all',
                        'start-port', 'stop-port', 'restart-port', 'kill-port',
                        'health-check', 'cleanup', 'logs'
                    ]
                });
        }
        
        console.log(`📝 Executing: ${cmd}`);
        
        const { stdout, stderr } = await execAsync(cmd, {
            timeout: 30000,  // 30 seconds timeout
            maxBuffer: 1024 * 1024 * 5  // 5MB buffer
        });
        
        res.json({
            error: '0',
            msg: 'Command executed successfully',
            output: stdout,
            stderr: stderr || null
        });
        
    } catch (error) {
        console.error('❌ Command execution error:', error);
        res.status(500).json({
            error: '1',
            msg: 'Command execution failed',
            details: error.message,
            stderr: error.stderr || null
        });
    }
});

/**
 * Get Port Statistics
 */
app.get('/stats/ports', async (req, res) => {
    try {
        const cmd = 'ps aux | grep "node /opt/baileys/index.js" | grep -v grep';
        const { stdout } = await execAsync(cmd);
        
        const lines = stdout.trim().split('\n').filter(line => line);
        const ports = [];
        
        for (const line of lines) {
            const match = line.match(/index\.js (\d+)/);
            if (match) {
                const port = match[1];
                const memMatch = line.match(/\s+(\d+\.\d+)\s+\d+\.\d+\s+/);
                const mem = memMatch ? memMatch[1] : '0.0';
                
                ports.push({
                    port: parseInt(port),
                    memory: parseFloat(mem),
                    running: true
                });
            }
        }
        
        res.json({
            error: '0',
            data: ports,
            total: ports.length
        });
        
    } catch (error) {
        res.json({
            error: '0',
            data: [],
            total: 0
        });
    }
});

/**
 * Get System Stats
 */
app.get('/stats/system', async (req, res) => {
    try {
        const [loadavg, mem, disk] = await Promise.all([
            execAsync('uptime | awk -F"load average:" \'{print $2}\''),
            execAsync('free -m | grep Mem | awk \'{print $2","$3","$4}\''),
            execAsync('df -h /opt/baileys | tail -1 | awk \'{print $2","$3","$4","$5}\'')
        ]);
        
        const memParts = mem.stdout.trim().split(',');
        const diskParts = disk.stdout.trim().split(',');
        
        res.json({
            error: '0',
            data: {
                loadavg: loadavg.stdout.trim(),
                memory: {
                    total: memParts[0] + 'MB',
                    used: memParts[1] + 'MB',
                    free: memParts[2] + 'MB'
                },
                disk: {
                    total: diskParts[0],
                    used: diskParts[1],
                    free: diskParts[2],
                    percent: diskParts[3]
                }
            }
        });
        
    } catch (error) {
        res.status(500).json({
            error: '1',
            msg: 'Failed to get system stats',
            details: error.message
        });
    }
});

/**
 * Restart Control API itself
 */
app.post('/restart-self', (req, res) => {
    res.json({
        error: '0',
        msg: 'Control API will restart in 2 seconds'
    });
    
    setTimeout(() => {
        console.log('🔄 Restarting Control API...');
        process.exit(0);  // Systemd will auto-restart
    }, 2000);
});

/**
 * 404 Handler
 */
app.use((req, res) => {
    res.status(404).json({
        error: '1',
        msg: 'Endpoint not found',
        available_endpoints: [
            'GET  /health',
            'POST /execute',
            'GET  /stats/ports',
            'GET  /stats/system',
            'POST /restart-self'
        ]
    });
});

/**
 * Error Handler
 */
app.use((err, req, res, next) => {
    console.error('❌ Unhandled error:', err);
    res.status(500).json({
        error: '1',
        msg: 'Internal server error',
        details: err.message
    });
});

/**
 * ═══════════════════════════════════════════════════════════════════════════
 * START SERVER
 * ═══════════════════════════════════════════════════════════════════════════
 */

app.listen(PORT, '0.0.0.0', () => {
    console.log('═══════════════════════════════════════════════════════════');
    console.log('🎮 Baileys Control API Started');
    console.log('═══════════════════════════════════════════════════════════');
    console.log(`📡 Port: ${PORT}`);
    console.log(`🔒 IP Whitelist: ${ALLOWED_IPS.join(', ')}`);
    console.log(`⏰ Started: ${new Date().toISOString()}`);
    console.log('═══════════════════════════════════════════════════════════');
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('🛑 SIGTERM received, shutting down gracefully...');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('🛑 SIGINT received, shutting down gracefully...');
    process.exit(0);
});


import sqlite3 from 'sqlite3';
import { promisify } from 'util';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSIONS_DB_PATH = path.join(__dirname, '..', 'sessions.db');

class Database {
    constructor() {
        this.db = new sqlite3.Database(SESSIONS_DB_PATH);
        this.initialized = false;
    }

    async init() {
        if (this.initialized) return;

        const run = promisify(this.db.run.bind(this.db));
        const get = promisify(this.db.get.bind(this.db));
        const all = promisify(this.db.all.bind(this.db));

        this.run = run;
        this.get = get;
        this.all = all;

        await this.run(`
            CREATE TABLE IF NOT EXISTS sessions (
                sessionId TEXT PRIMARY KEY,
                phoneNumber TEXT,
                isLoggedIn INTEGER DEFAULT 0,
                createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
                lastActivity DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `);

        this.initialized = true;
    }

    async createSession(sessionId, phoneNumber = null) {
        await this.init();
        await this.run(
            'INSERT OR REPLACE INTO sessions (sessionId, phoneNumber, isLoggedIn) VALUES (?, ?, 0)',
            [sessionId, phoneNumber]
        );
    }

    async updateSessionLogin(sessionId, isLoggedIn) {
        await this.init();
        await this.run(
            'UPDATE sessions SET isLoggedIn = ?, lastActivity = CURRENT_TIMESTAMP WHERE sessionId = ?',
            [isLoggedIn ? 1 : 0, sessionId]
        );
    }

    async getActiveSessions() {
        await this.init();
        return await this.all('SELECT * FROM sessions WHERE isLoggedIn = 1');
    }

    async deleteSession(sessionId) {
        await this.init();
        await this.run('DELETE FROM sessions WHERE sessionId = ?', [sessionId]);
    }

    async getAllSessions() {
        await this.init();
        return await this.all('SELECT * FROM sessions');
    }

    close() {
        this.db.close();
    }
}

export default Database;

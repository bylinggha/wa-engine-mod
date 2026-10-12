/**
 * Constants untuk seluruh aplikasi
 * Centralized constants untuk prevent magic numbers dan improve maintainability
 */

// ============================================================================
// TIMEOUT CONSTANTS
// ============================================================================
export const TIMEOUTS = {
    // Request timeouts
    REQUEST_TIMEOUT_MS: 15000, // 15 detik untuk endpoint umum
    SEND_ENDPOINT_TIMEOUT_MS: 15000, // 15 detik untuk /send endpoint
    INIT_TIMEOUT_MS: 3000, // 3 detik untuk initialize session
    LOAD_SESSION_TIMEOUT_MS: 2000, // 2 detik untuk load session
    QUICK_CHECK_TIMEOUT_MS: 1000, // 1 detik untuk quick check
    
    // WebSocket timeouts
    WS_PING_INTERVAL_MS: 30000, // 30 detik - kirim ping
    WS_TIMEOUT_DURATION_MS: 10 * 60 * 1000, // 10 menit - jika tidak ada pong, disconnect
    WS_CLEANUP_INTERVAL_MS: 2 * 60 * 1000, // 2 menit cleanup interval
    
    // Connection timeouts
    CONNECT_TIMEOUT_MS: 120000, // 120 detik untuk connect
    DEFAULT_QUERY_TIMEOUT_MS: 120000, // 120 detik untuk query
    KEEP_ALIVE_INTERVAL_MS: 30000, // 30 detik keep alive
    
    // Other timeouts
    PROXY_TIMEOUT_MS: 10000, // 10 detik untuk proxy fetch
    IMAGE_DOWNLOAD_TIMEOUT_MS: 5000, // 5 detik untuk download image
    PRESENCE_UPDATE_TIMEOUT_MS: 3000, // 3 detik untuk presence update
    WAIT_SOCKET_OPEN_TIMEOUT_MS: 5000, // 5 detik untuk wait socket open
    VERSION_FETCH_TIMEOUT_MS: 10000, // 10 detik untuk fetch version
    PAIRING_TIMEOUT_DURATION_MS: 5 * 60 * 1000, // 5 menit untuk pairing timeout
};

// ============================================================================
// RECONNECTION CONSTANTS
// ============================================================================
export const RECONNECTION = {
    MAX_ATTEMPTS: 5, // Max 5 attempts - jika gagal 5x, kemungkinan besar tidak bisa reconnect
    BASE_DELAY_MS: 1000, // 1 detik base delay - reconnect lebih cepat
    MIN_DELAY_MS: 1000, // Minimum 1 detik
    MAX_DELAY_MS: 3000, // Maximum 3 detik - reconnect lebih cepat
    QUICK_RECONNECT_WINDOW_MS: 5000, // 5 detik window untuk quick reconnect (temporary disconnect)
    RETRY_DELAY_MS: 500, // 500ms delay untuk retry
    COOLDOWN_MS: 2000, // 2 detik cooldown untuk reconnect
};

// ============================================================================
// SESSION MANAGEMENT CONSTANTS
// ============================================================================
export const SESSION = {
    CHECK_COOLDOWN_MS: 1000, // 1 detik cooldown untuk check (prevent spam check)
    CLEANUP_INTERVAL_MS: 10 * 60 * 1000, // 10 menit cleanup interval
    INACTIVE_THRESHOLD_MS: 30 * 60 * 1000, // 30 menit inactive = cleanup
    BATCH_SIZE: 5, // Batch size untuk load sessions
    BATCH_DELAY_MS: 100, // Delay antar batch
};

// ============================================================================
// CACHE CONSTANTS
// ============================================================================
export const CACHE = {
    // Sent messages cache
    SENT_MESSAGES_TTL_SEC: 7 * 24 * 60 * 60, // 7 hari TTL
    SENT_MESSAGE_IDS_MAX_SIZE: 200, // Max 200 entries
    
    // Image URL cache
    IMAGE_URL_CACHE_TTL_MS: 60 * 60 * 1000, // 1 jam TTL
    IMAGE_URL_CACHE_MAX_SIZE: 100, // Max 100 entries (~5KB memory max)
    
    // API check cache
    API_CHECK_CACHE_TTL_MS: 30000, // 30 detik cache TTL
    
    // WhatsApp number cache
    WHATSAPP_NUMBER_CACHE_TTL_MS: 5 * 60 * 1000, // 5 menit cache TTL
    WHATSAPP_NUMBER_CACHE_MAX_SIZE: 100, // Max 100 entries
    
    // Cache cleanup intervals
    CACHE_CLEANUP_INTERVAL_MS: 10 * 60 * 1000, // 10 menit cleanup interval
};

// ============================================================================
// MESSAGE CONSTANTS
// ============================================================================
export const MESSAGE = {
    MAX_SIZE_BYTES: 10 * 1024 * 1024, // Max 10MB per image
    MAX_PAYLOAD_SIZE_BYTES: 100 * 1024, // 100KB max payload size
    MIN_SEND_INTERVAL_MS: 100, // 100ms minimum (10 msg/sec per session)
    MAX_QUEUE_SIZE: 10, // Max 10 items untuk webhook queue
    MAX_CONCURRENT_PER_SESSION: 3, // Max 3 concurrent per session
    MAX_GLOBAL_CONCURRENT: 150, // Max 150 global concurrent
};

// ============================================================================
// PAIRING CONSTANTS
// ============================================================================
export const PAIRING = {
    CODE_COOLDOWN_MS: 60000, // 60 detik cooldown (sesuai best practice)
    MAX_QR_GENERATIONS_PER_HOUR: 10, // Max 10 QR per jam per session
};

// ============================================================================
// SERVER CONSTANTS
// ============================================================================
export const SERVER = {
    MAX_CONNECTIONS: 5000, // 5000 concurrent connections
    KEEP_ALIVE_TIMEOUT_MS: 30000, // 30 detik keep alive timeout
    RESPONSE_PREVIEW_LENGTH: 500, // 500 chars untuk response preview
};

// ============================================================================
// VALIDATION CONSTANTS
// ============================================================================
export const VALIDATION = {
    PHONE_MIN_LENGTH: 10, // Minimum 10 digits
    PHONE_MAX_LENGTH: 15, // Maximum 15 digits
    STATUS_CODE_CLIENT_ERROR_MIN: 400, // Min status code untuk client error
    STATUS_CODE_CLIENT_ERROR_MAX: 500, // Max status code untuk client error
    STATUS_CODE_SUCCESS_MIN: 200, // Min status code untuk success
    STATUS_CODE_SUCCESS_MAX: 300, // Max status code untuk success
};

// ============================================================================
// WORKER CONSTANTS
// ============================================================================
export const WORKER = {
    MAX_ERRORS: 10, // Max errors sebelum auto-stop worker
};

// ============================================================================
// ERROR MESSAGES
// ============================================================================
export const ERROR_MESSAGES = {
    SESSION_NOT_FOUND: (sessionId) => 
        `Connection closed: Session not found: ${sessionId}`,
    SESSION_DELETED: (sessionId) => 
        `Connection closed: Session not found: ${sessionId} (session has been deleted)`,
    CONNECTION_TIMEOUT: (timeout) => 
        `Connection closed: Session initialization timeout (${timeout}ms)`,
    CONNECTION_CLOSED: (details) => 
        `Connection closed: ${details}`,
    SESSION_INIT_FAILED: (sessionId, reason) => 
        `Connection closed: Session initialization failed: ${sessionId} - ${reason}`,
    WEBSOCKET_CLOSED: () => 
        `WebSocket CLOSED while waiting for OPEN`,
    PRESENCE_TIMEOUT: () => 
        `Presence timeout`,
    WAIT_SOCKET_OPEN_TIMEOUT: () => 
        `Wait for socket open timeout`,
};


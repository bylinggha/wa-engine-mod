/** DisconnectReason — byee-wamod jika ada, fallback whiskeysockets (shard tidak crash saat npm belum selesai). */
let DisconnectReason;

try {
    const bw = await import('byee-wamod');
    const pkg = bw.default ?? bw;
    DisconnectReason = pkg.DisconnectReason ?? bw.DisconnectReason;
} catch (_) {
    const ws = await import('@whiskeysockets/baileys');
    DisconnectReason = ws.DisconnectReason ?? ws.default?.DisconnectReason;
}

if (!DisconnectReason) {
    throw new Error('DisconnectReason unavailable (byee-wamod and @whiskeysockets/baileys)');
}

export { DisconnectReason };

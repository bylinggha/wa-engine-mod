# wa-engine-mod

Engine WhatsApp multi-sesi berbasis [Baileys](https://github.com/WhiskeySockets/Baileys).
Satu proses melayani ratusan sesi WhatsApp lewat HTTP API dan WebSocket. Pesan, status
koneksi, dan ACK dikirim balik ke backend melalui webhook.

Repo ini berisi dua bagian:

| Folder | Isi |
|---|---|
| `engine/` | Server engine (Node.js, ES module): HTTP API, WebSocket, pengelola sesi, antrean kirim, webhook |
| `lib/byee-wamod/` | Custom library WhatsApp — fork Baileys dengan tambalan lokal (pesan interaktif, kunci waktu, stabilitas) |

**📘 Tutorial lengkap — menyambungkan WhatsApp (kode pairing / QR), kirim pesan, webhook ACK, logout:
[`docs/TUTORIAL.md`](docs/TUTORIAL.md)**

## Persyaratan

- Node.js 20 atau lebih baru
- Redis (antrean, kunci, dan broadcast antar-shard)
- Backend yang menerima webhook dari engine

## Menjalankan

```bash
git clone git@github.com:bylinggha/wa-engine-mod.git
cd wa-engine-mod
npm install            # memasang dependensi + lib/byee-wamod lokal
cp .env.example .env   # lalu isi nilainya
npm run check          # cek sintaks semua berkas engine
npm start              # jalankan engine (satu shard)
```

Proses pendukung:

```bash
npm run hub       # baileys-ws-hub.js — hub broadcast WebSocket antar-shard
npm run control   # control-api.js — API kontrol jarak jauh (port 9000, IP dibatasi)
```

## Konfigurasi

Semua konfigurasi dibaca dari `.env`. Lihat `.env.example` untuk daftar lengkap. Yang wajib diisi:

| Variabel | Fungsi |
|---|---|
| `PORT` | Port HTTP API shard ini |
| `WEBHOOK_URL` / `DEFAULT_BE_BASE` | Alamat backend penerima webhook |
| `WEBHOOK_API_KEY` | Kunci rahasia antara engine dan backend. **Tidak ada nilai bawaan** — tanpa ini webhook ditolak |
| `ALLOWED_IPS` | IP backend yang boleh memanggil API (dipisah koma); localhost selalu diizinkan |
| `REDIS_URL` | Koneksi Redis |

`BROADCAST_KEY`, `WEBHOOK_SECRET`, dan `SESSION_WARMUP_API_KEY` memakai `WEBHOOK_API_KEY`
bila tidak diisi. `ADMIN_PROXY_REASSIGN_KEY` dan `GET_PROXY_API_KEY` tidak punya nilai bawaan.

### Multi-shard

Untuk banyak sesi, jalankan beberapa proses engine dengan `PORT` berbeda (mis. 7001–7007),
masing-masing dengan `.env.<port>` sendiri, di belakang reverse proxy yang memetakan sesi ke shard.
Semua shard memakai Redis yang sama.

## Struktur engine

| Berkas | Peran |
|---|---|
| `index.js` | Titik masuk: HTTP API, WebSocket, restore sesi, pemantau memori & event loop |
| `wrapper.js` | Pembungkus library WhatsApp: koneksi, QR / kode pairing, kirim pesan, ACK |
| `session.js` | Pengelola sesi (penyimpanan kredensial per sesi) |
| `api-routes.js` | Rute HTTP (kirim pesan, status sesi, broadcast) |
| `config.js`, `config/config.js` | Konfigurasi dari environment |
| `send-slot.js`, `webhook-slot.js` | Pembatas antrean kirim dan webhook |
| `baileys-ws-hub.js` | Hub broadcast WebSocket via Redis |
| `baileys-session-websocket.js` | WebSocket status per sesi untuk dasbor |
| `control-api.js` | API kontrol jarak jauh |
| `cleanup-*.js`, `session-cleanup-bulk-once.js`, `fleet-once-prune-sessions.js` | Pembersihan sesi & penyimpanan |
| `ws-fleet-guard.js`, `ws-ready-state.js`, `disconnect-reason.js`, `quiet-console.js`, `http-client.js`, `database.js`, `constants.js` | Utilitas |

## Library `byee-wamod`

`lib/byee-wamod` dipasang sebagai dependensi lokal (`"byee-wamod": "file:./lib/byee-wamod"`).
Wrapper memilih library lewat `BAILEYS_USE` (bawaan `byee-wamod`). Versi di repo ini sudah
memuat tambalan yang berjalan di produksi, jadi **jangan menimpanya** dengan versi dari npm
atau dari repo `byee-wamod` tanpa memindahkan tambalannya.

## Keamanan

- Repo ini tidak memuat `.env`, sesi WhatsApp (`sessions/`, `sessions.db`), kunci SSH, log, atau backup.
  Lihat `.gitignore`.
- Semua kunci rahasia wajib diisi lewat environment; kode tidak punya kunci bawaan.
- Batasi akses API dengan `ALLOWED_IPS` dan jalankan engine di belakang reverse proxy.

# Tutorial: dari nol sampai kirim pesan

Contoh di bawah memakai engine yang berjalan di `http://localhost:7001` (`PORT=7001`).
Semua request memakai JSON. Respons engine selalu berbentuk:

```json
{ "error": "0", "msg": "..." }   // sukses
{ "error": "1", "msg": "alasan gagal" }
```

Engine mengabarkan hal yang terjadi kemudian (QR, kode pairing, tersambung, ACK, putus)
lewat **webhook** ke backend Anda — bukan lewat respons HTTP. Jadi siapkan dulu endpoint webhook.

---

## 1. Siapkan `.env`

```bash
cp .env.example .env
```

Isi minimal:

```env
PORT=7001
WEBHOOK_URL=https://backend-anda.example/webhook.php
DEFAULT_BE_BASE=https://backend-anda.example
WEBHOOK_API_KEY=ganti-dengan-kunci-acak-panjang
ALLOWED_IPS=203.0.113.10          # IP backend Anda
REDIS_URL=redis://127.0.0.1:6379
```

Jalankan:

```bash
npm install
npm start
curl http://localhost:7001/health
```

---

## 2. Menyambungkan WhatsApp (connected)

Ada dua cara: **kode pairing** (masukkan kode 8 karakter di HP) atau **QR**.

### a. Kode pairing (disarankan)

```bash
curl -X POST http://localhost:7001/pair \
  -H 'Content-Type: application/json' \
  -d '{ "userId": 123, "phoneNumber": "6281234567890" }'
```

Respons langsung berisi ID sesi:

```json
{ "error": "0", "msg": "sess_123_7001_1760000000000", "sessionId": "sess_123_7001_1760000000000", "port": 7001 }
```

Simpan `sessionId` — dipakai untuk semua langkah berikutnya.

Beberapa detik kemudian webhook `PAIRING_CODE` masuk ke backend dengan kodenya. Tampilkan kode itu
ke pengguna, lalu di HP: **WhatsApp → Perangkat tertaut → Tautkan perangkat → Tautkan dengan nomor telepon**,
masukkan kodenya.

### b. QR

Kirim `/pair` **tanpa** `phoneNumber`:

```bash
curl -X POST http://localhost:7001/pair \
  -H 'Content-Type: application/json' \
  -d '{ "userId": 123 }'
```

Webhook `QR_CODE` masuk berisi string QR (`qr`). Ubah jadi gambar (mis. pustaka `qrcode`)
lalu scan dari **WhatsApp → Perangkat tertaut → Tautkan perangkat**.

### c. Tanda sudah tersambung

Begitu HP selesai menautkan, webhook **`CONNECTED`** masuk:

```json
{ "event": "CONNECTED", "sessionId": "sess_123_7001_1760000000000", "bot": "6281234567890", "user_id": 123, "port": 7001 }
```

Mulai saat itu sesi siap mengirim. Cek kapan saja:

```bash
curl 'http://localhost:7001/session-status?sessionId=sess_123_7001_1760000000000'
```

Bila kode/QR tidak dipakai, akan datang `PAIRING_TIMEOUT` / `QR_TIMEOUT` / `AUTH_TIMEOUT` —
ulangi langkah `/pair`.

---

## 3. Mengirim pesan

### Teks

```bash
curl -X POST http://localhost:7001/send \
  -H 'Content-Type: application/json' \
  -d '{
        "bot": "sess_123_7001_1760000000000",
        "recipient": "6289876543210",
        "message": "Halo dari wa-engine-mod"
      }'
```

### Gambar

```bash
curl -X POST http://localhost:7001/send \
  -H 'Content-Type: application/json' \
  -d '{
        "bot": "sess_123_7001_1760000000000",
        "recipient": "6289876543210",
        "imageUrl": "https://contoh.example/gambar.jpg",
        "caption": "Keterangan gambar"
      }'
```

### Pesan dengan tombol

```bash
curl -X POST http://localhost:7001/sendButtonMessage \
  -H 'Content-Type: application/json' \
  -d '{
        "bot": "sess_123_7001_1760000000000",
        "recipient": "6289876543210",
        "message": "Isi pesan",
        "footer": "Footer",
        "url": "https://contoh.example",
        "button": "Buka"
      }'
```

### Respons kirim

```json
{ "error": "0", "msg": "3EB0A1B2C3D4E5F6", "ack_status": "original", "is_temp": false, "receipt_pending": true }
```

- `msg` adalah ID pesan WhatsApp. Pada beban tinggi engine bisa membalas ID sementara
  (`"ack_status": "temp", "is_temp": true`) lalu mengirim di latar belakang.
- **ID pesan belum berarti pesan sampai.** Status sebenarnya datang lewat webhook (bagian 4).

Opsi tambahan di body `/send`:

| Field | Fungsi |
|---|---|
| `skip_webhook: 1` | Jangan kirim webhook untuk pesan ini |
| `enable_callback: 1` | Minta webhook status lengkap untuk pesan ini |

---

## 4. Webhook dari engine

Engine mengirim `POST` JSON ke `WEBHOOK_URL`. Setiap request membawa:

- `X-Broadcast-Key` — kunci API (diturunkan dari `WEBHOOK_API_KEY` per sesi dengan HMAC-SHA256)
- `X-Webhook-Signature` — HMAC-SHA256 dari body mentah

Verifikasi keduanya di backend sebelum memproses.

| Event | Kapan |
|---|---|
| `QR_CODE` | QR baru siap discan |
| `PAIRING_CODE` | Kode pairing siap; `PAIRING_CODE_ERROR` bila gagal dibuat |
| `CONNECTED` | WhatsApp tersambung — sesi siap kirim |
| `MESSAGE_SENT` | Pesan diterima engine dan dikirim |
| `SERVER_ACK` | Server WhatsApp menerima pesan (1 centang) |
| `DEVICE_SENT` / `DELIVERED` | Pesan sampai ke HP penerima (2 centang) |
| `READ` / `PLAYED` | Dibaca / diputar |
| `RECONNECTING` | Koneksi putus sementara, engine menyambung ulang |
| `DISCONNECTED` | Sesi terputus atau logout — perlu `/pair` ulang |
| `PAIRING_TIMEOUT`, `QR_TIMEOUT`, `AUTH_TIMEOUT` | Proses penautan habis waktu |
| `PROXY_ASSIGNED` | Sesi mendapat proxy |

Contoh `SERVER_ACK`:

```json
{ "event": "SERVER_ACK", "sessionId": "sess_123_7001_1760000000000", "bot": "6281234567890", "msg_id": "3EB0A1B2C3D4E5F6", "status": "server_ack" }
```

Cocokkan `msg_id` dengan ID dari respons `/send` untuk mengetahui nasib tiap pesan.

---

## 5. Logout & menyambung ulang

```bash
# Putuskan sesi dan hapus tautan perangkat
curl -X POST http://localhost:7001/logout \
  -H 'Content-Type: application/json' \
  -d '{ "sessionId": "sess_123_7001_1760000000000" }'

# Sambung ulang sesi yang masih tertaut
curl -X POST http://localhost:7001/baileys/reconnect \
  -H 'Content-Type: application/json' \
  -d '{ "sessionId": "sess_123_7001_1760000000000" }'
```

---

## 6. Endpoint lain

| Endpoint | Fungsi |
|---|---|
| `GET /health` | Status hidup engine |
| `GET /stats` | Jumlah sesi, memori, antrean |
| `GET /session-status?sessionId=...&load=1` | Status satu sesi (`load=1` memuat dari disk bila belum aktif) |
| `POST /drain` / `POST /undrain` / `GET /drain-status` | Kosongkan shard sebelum restart (tolak sesi baru) |
| `POST /broadcast` | Kirim event ke klien WebSocket dasbor (header `x-broadcast-key`) |

Setiap endpoint juga tersedia dengan awalan `/baileys/...` (dan sebagian `/api/...`) untuk
dipasang di belakang reverse proxy.

---

## Tips

- **Satu sesi = satu nomor WhatsApp.** Jangan pakai satu nomor di dua sesi; kode pairing lama saling membatalkan.
- **Format nomor**: kode negara tanpa `+` atau `0` di depan, mis. `6281234567890`.
- **Multi-shard**: `sessionId` memuat port shard (`sess_<user>_<port>_<waktu>`). Kirim request ke shard yang sesuai;
  shard yang salah membalas `Wrong shard`.
- Jaga jeda antar-kirim per nomor supaya akun tidak dibatasi WhatsApp.

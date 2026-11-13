# booking-service

Layanan **booking dan stok tiket** untuk Rusuh Booking Tiket (**BE-2**). Service membuat booking dengan hold stok atomik, memvalidasi data penonton, mengelola TTL pembayaran, dan mengirim perubahan stok melalui SSE.

- Arsitektur: Hexagonal (Ports & Adapters), `net/http` + PostgreSQL + Valkey/Redis + Kafka
- Port lokal: **8081**
- Access token antrian: header `X-Access-Token`
- Login user opsional: `Authorization: Bearer <authToken>`; bila valid, `userId` disimpan pada booking
- Payment token: header `X-Payment-Token`

## Prasyarat

- Go 1.26+
- PostgreSQL, Valkey-compatible Redis, dan Kafka dari repo `tools`
- `dukcapil-mock` tersedia di `:8085` untuk verifikasi data penonton
- Shared secrets lokal yang cocok dengan queue-service, payment-service, dan user-service

## Menjalankan

```bash
cd ../tools
make up
```

Jalankan `dukcapil-mock` di terminal lain bila validasi NIK diperlukan, lalu:

```bash
cd ../booking-service
APP_ENV=local APP_PORT=8081 DB_PASSWORD=password \
KAFKA_BROKERS=localhost:29092 \
ACCESS_TOKEN_SECRET=dev-access-token-secret \
PAYMENT_TOKEN_SECRET=dev-payment-token-secret \
USER_JWT_SECRET=dev-user-jwt-secret-change-in-production \
OTEL_ENABLED=false make run
```

Compose lokal memakai PostgreSQL `localhost:5432` (`rusuh_booking`, user `postgres`, password `password`), Redis `localhost:6379`, dan Kafka `localhost:29092`. Secret contoh hanya untuk development; production wajib memakai nilai dari secret manager dan menyamakan secret antar-service sesuai kontrak.

## Endpoint

| Method | Path | Akses | Keterangan |
|---|---|---|---|
| POST | `/api/booking/v1/create` | `X-Access-Token` | Membuat booking dan hold tiket; body berisi `eventId`, `email`, dan `items` beserta data attendee |
| GET | `/api/booking/v1/{bookingId}` | `X-Payment-Token` | Status booking untuk halaman pembayaran |
| POST | `/api/booking/v1/{bookingId}/cancel` | `X-Payment-Token` | Membatalkan booking yang cocok dengan token |
| GET | `/api/booking/v1/event/{eventId}/stock/stream` | `X-Access-Token` | SSE snapshot dan update stok kategori |
| GET | `/sys/ping` | Publik | Health check |

Contoh request booking:

```bash
curl -X POST http://localhost:8081/api/booking/v1/create \
  -H 'X-Access-Token: <queue-access-token>' \
  -H 'Authorization: Bearer <user-authToken>' \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"<event-uuid>","email":"budi@example.com","items":[{"categoryId":"<category-uuid>","attendees":[{"nik":"3273000000000001","nama":"Budi Santoso","tglLahir":"1990-01-01","namaIbu":"Siti Aminah"}]}]}'
```

`Authorization` bersifat opsional. `X-Access-Token` tetap wajib untuk masuk booking room; nilai `eventId` dari token dipakai untuk memastikan booking sesuai event admission. Maksimum tiga tiket per booking. Setiap attendee diverifikasi ke Dukcapil. Respons sukses berisi `bookingId`, status, total, waktu kedaluwarsa, dan payment token.

SSE mengirim snapshot awal dan event `stock` saat stok berubah, serta heartbeat berkala. Contoh koneksi:

```bash
curl -N http://localhost:8081/api/booking/v1/event/<event-uuid>/stock/stream \
  -H 'X-Access-Token: <queue-access-token>'
```

## Konfigurasi

| Env | Default lokal | Keterangan |
|---|---|---|
| `APP_ENV` / `APP_PORT` | `local` / `8081` | Cloud Run dapat mengisi `PORT` |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `json` | Logging terstruktur |
| `DB_HOST` / `DB_PORT` | `localhost` / `5432` | PostgreSQL bersama |
| `DB_USER` / `DB_PASSWORD` / `DB_NAME` | `postgres` / kosong / `rusuh_booking` | Compose lokal: password `password` |
| `DB_SSL_MODE` | `disable` | SSL mode PostgreSQL |
| `DB_MAX_OPEN_CONNS` / `DB_MAX_IDLE_CONNS` | `25` / `10` | Pool koneksi |
| `DB_CONN_TIMEOUT_SECONDS` | `5` | Timeout koneksi |
| `VALKEY_ADDR` / `VALKEY_PASSWORD` | `localhost:6379` / kosong | Pub/Sub dan cache lokal |
| `VALKEY_TLS` | `false` | TLS Valkey |
| `KAFKA_BROKERS` | `localhost:9092` | Compose lokal: `localhost:29092` |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` / `KAFKA_TLS` | kosong / kosong / `false` | Opsi autentikasi broker |
| `ACCESS_TOKEN_SECRET` | wajib diisi | Harus sama dengan queue-service |
| `PAYMENT_TOKEN_SECRET` | wajib diisi | Harus sama dengan payment-service |
| `USER_JWT_SECRET` | secret development | Verifikasi login user dari user-service; production wajib disamakan |
| `DUKCAPIL_BASE_URL` | `http://localhost:8085` | Endpoint dukcapil-mock |
| `DUKCAPIL_TIMEOUT_MS` / `DUKCAPIL_FAIL_OPEN` | `3000` / `true` | Timeout dan perilaku bila Dukcapil gagal |
| `EVENT_SERVICE_URL` | `http://localhost:8082` | Dipakai untuk integrasi event |
| `MIDTRANS_SERVER_KEY` / `MIDTRANS_ENV` | kosong / `sandbox` | Server key sandbox untuk proses pembayaran |
| `SSE_HEARTBEAT_SECONDS` | `30` | Nilai konfigurasi; handler SSE saat ini mengirim heartbeat tiap 30 detik |
| `OTEL_ENABLED` | aktif | Set `false` untuk menonaktifkan ekspor Cloud Trace saat lokal |
| `OTEL_SAMPLE_RATIO` | `1.0` | Rasio trace yang disimpan, antara `0` dan `1` |

## Pengembangan

```bash
make test
make test-race
make build
make golint
make scan       # gosec
make vuln       # govulncheck
```

Migrasi dan seed database bersama ada di repo `tools` (`docker/postgres/init`).

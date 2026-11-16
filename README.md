# Sistem Agregasi Peringatan Bencana BNPB - Kelompok pepeftw

Tugas Besar IF4031 Arsitektur Aplikasi Terdistribusi - Milestone 1.

Sistem ini mengambil data gempa dari BMKG dan data gunung api dari PVMBG, menyeragamkannya menjadi satu format `HazardEvent`, menyimpannya, lalu menyebarkannya ke beberapa layanan hilir (dashboard, notifikasi, audit) dan ke klien eksternal melalui API ber-JWT.

## Daftar Isi

1. [Arsitektur Singkat](#1-arsitektur-singkat)
2. [Prasyarat](#2-prasyarat)
3. [Menjalankan Sistem](#3-menjalankan-sistem)
4. [Konfigurasi `.env`](#4-konfigurasi-env)
5. [Port dan Endpoint](#5-port-dan-endpoint)
6. [Mencoba API](#6-mencoba-api)
7. [Demo per Problem](#7-demo-per-problem)
8. [Load Test](#8-load-test)
9. [Kafka](#9-kafka)
10. [Struktur Repositori](#10-struktur-repositori)
11. [Troubleshooting](#11-troubleshooting)
12. [Anggota Kelompok](#12-anggota-kelompok)

## 1. Arsitektur Singkat

```
bmkg-mock ─┐                         ┌─> dashboard-updater (SSE)
           ├─> aggregator ─> Kafka ──┼─> notifier
pvmbg-mock ┘   (poller, mapper,      └─> audit-log (profile extra)
                Postgres, outbox)
                    ▲
client-api ─────────┘  (REST internal, X-Internal-Key)
    ▲   │
    │   └─ Redis (rate limit) + denylist dari topic auth-revocations
klien (media, field-team, bnpb-ops) ── token dari auth (JWT EdDSA, Redis refresh token)
```

| Service | Fungsi |
|---|---|
| `bmkg-mock` | Simulasi API BMKG (gempa dan tsunami), autentikasi header `X-BMKG-Key`. |
| `pvmbg-mock` | Simulasi API PVMBG (laporan gunung api), autentikasi Bearer token, bisa diubah versi skemanya dan dibuat *outage*. |
| `aggregator` | *Poller* per sumber (*timeout* dan *circuit breaker* terpisah), *mapper* ke `HazardEvent`, simpan ke PostgreSQL, kirim ke Kafka lewat *transactional outbox*. |
| `auth` | Menerbitkan *access token* JWT (EdDSA, TTL 60 detik) dan *refresh token* berotasi; JWKS di `/.well-known/jwks.json`. |
| `client-api` | API untuk klien eksternal: `/hazards`, `/hazards/:id`, `/sources/status`, dengan *scope* dan *rate limit*. |
| `dashboard-updater` | Konsumen Kafka yang menampilkan bencana terbaru secara *real time* (SSE). |
| `notifier` | Konsumen Kafka yang bereaksi pada status `SIAGA` dan `AWAS`. |
| `audit-log` | Konsumen Kafka untuk pencatatan (profile `extra`, opsional). |

Database dan Redis tidak dibuka ke host. Masing-masing hanya bisa dijangkau dari jaringan internalnya (`store-net`, `auth-store-net`, `client-api-store-net`).

## 2. Prasyarat

- Docker Desktop (atau Docker Engine) dengan Docker Compose v2.
- `openssl` untuk membuat kunci JWT. Sudah tersedia di Git Bash, macOS, dan Linux.
- [Hurl](https://hurl.dev) untuk skrip verifikasi dan demo.
- `curl` untuk mencoba API secara manual.
- Pengguna Windows: jalankan semua perintah di **Git Bash**, bukan PowerShell atau CMD.

Go tidak perlu dipasang di laptop. Semua service di-*build* di dalam container.

## 3. Menjalankan Sistem

### Cara cepat (disarankan)

```bash
sh scripts/demo/setup.sh
```

Skrip ini:
1. menyalin `.env.example` menjadi `.env` jika `.env` belum ada,
2. membuat kunci Ed25519 di `secrets/` jika belum ada (`scripts/gen-auth-keys.sh`),
3. menjalankan `docker compose up -d --build`,
4. menunggu semua service sehat (`scripts/demo/00-health.hurl`).

Untuk mulai dari kondisi bersih (menghapus semua volume dan data):

```bash
sh scripts/demo/setup.sh --fresh
```

### Cara manual

```bash
cp .env.example .env              
sh scripts/gen-auth-keys.sh     
docker compose up -d --build
docker compose ps                 
```

Menyalakan juga `audit-log`:

```bash
docker compose --profile extra up -d --build
```

### Melihat log

```bash
docker compose logs -f aggregator
docker compose logs -f aggregator | grep hazard_saved
```

### Menghentikan

```bash
docker compose down        
docker compose down -v     
```

## 4. Konfigurasi `.env`

File `.env` **tidak** di-*commit*. Semua nilai contoh ada di `.env.example`. Untuk menjalankan secara lokal, nilai default sudah bisa dipakai. Ganti nilai rahasia sebelum dipakai di luar lingkungan pengembangan.

| Kelompok | Variabel penting | Keterangan |
|---|---|---|
| Database | `DB_USER`, `DB_PASSWORD`, `DB_NAME` | Wajib terisi; jika kosong, `aggregator-db` gagal start. |
| Kafka | `KAFKA_BROKERS`, `KAFKA_TOPIC_*`, `KAFKA_GROUP_ID_*` | Setiap konsumen wajib memakai *group id* sendiri. |
| API internal | `INTERNAL_API_KEY` | Dipakai `client-api` sebagai header `X-Internal-Key` ke aggregator. |
| BMKG | `BMKG_API_KEY`, `BMKG_DELAY_*`, `BMKG_TSUNAMI_RATE` | Kunci dipakai bersama oleh `bmkg-mock` dan aggregator. |
| PVMBG | `PVMBG_TOKEN_SECRET`, `PVMBG_TOKEN`, `PVMBG_ADMIN_KEY`, `PVMBG_DELAY_*`, `PVMBG_SCHEMA_VERSION` | Lihat catatan token di bawah. |
| Poller | `POLL_INTERVAL`, `BMKG_TIMEOUT`, `PVMBG_TIMEOUT`, `POLL_CURSOR_OVERLAP`, `BREAKER_FAILURES`, `BREAKER_OPEN_FOR`, `TSUNAMI_GRACE` | Interval, batas waktu, dan *circuit breaker* per sumber. |
| Auth | `JWT_ISSUER`, `JWT_AUDIENCE`, `ACCESS_TOKEN_TTL`, `REFRESH_TOKEN_TTL`, `MEDIA_SECRET`, `LAPANGAN_SECRET`, `BNPB_SECRET`, `REDIS_PASSWORD` | *Secret* untuk tiga klien: `media`, `field-team`, `bnpb-ops`. |
| Rate limit | `RATE_LIMIT_MAX`, `RATE_LIMIT_WINDOW_SECONDS` | Default 10 *request* per 60 detik per klien. |
| Batas beban | `MAX_CONCURRENT_REQUESTS` | Batas *request* bersamaan di `client-api` (default 200). Kelebihannya dijawab `429` dengan `Retry-After`. |
| Mock BMKG | `BMKG_EVENT_INTERVAL`, `BMKG_SEED_COUNT` | Laju event baru (default 5 detik) dan jumlah data awal (default 25). |
| Mock PVMBG | `PVMBG_REPORT_INTERVAL`, `PVMBG_SEED_COUNT`, `PVMBG_HANG_MAX` | Laju laporan baru, jumlah data awal, dan batas lama *hang* saat *outage*. |
| Lainnya | `POLLER_ENABLED`, `LOG_LEVEL` | Menyalakan *poller* aggregator, dan level log mock (`DEBUG`/`INFO`/`WARN`/`ERROR`). |

**Token PVMBG.** Jika `PVMBG_TOKEN_SECRET` diganti, buat ulang `PVMBG_TOKEN`:

```bash
docker compose run --rm pvmbg-mock ./pvmbg-mock gen-token bnpb-aggregator
```

Salin hasilnya ke `PVMBG_TOKEN` di `.env`, lalu jalankan ulang `docker compose up -d`.

## 5. Port dan Endpoint

| Service | Port host | Endpoint utama |
|---|---|---|
| `client-api` | 8080 | `GET /hazards`, `GET /hazards/:id`, `GET /sources/status`, `GET /health` |
| `bmkg-mock` | 8081 | `GET /seismic-events`, `GET /tsunami-warnings`, `GET /health` |
| `pvmbg-mock` | 8082 | `GET /volcanic-reports`, `POST /admin/outage`, `/admin/schema-version`, `GET /health` |
| `dashboard-updater` | 8083 | `GET /` (halaman dashboard), `GET /events` (SSE), `GET /api/hazards`, `GET /health` |
| `auth` | 8084 | `POST /token`, `GET /.well-known/jwks.json`, `GET /health` |
| `aggregator` (API internal) | 8085 | `GET /internal/hazards`, `GET /internal/sources/status`, `GET /health` |
| `kafka` | 9094 | Akses dari laptop untuk pengujian saja |

Spesifikasi API lengkap (OpenAPI dan AsyncAPI) ada di folder `api/`.

## 6. Mencoba API

Ambil *access token* (contoh klien `bnpb-ops`):

```bash
TOKEN=$(curl -s -X POST http://localhost:8084/token \
  -H "Content-Type: application/json" \
  -d "{\"grant_type\":\"client_credentials\",\"client_id\":\"bnpb-ops\",\"client_secret\":\"$(grep '^BNPB_SECRET=' .env | cut -d= -f2-)\"}" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
```

Panggil `client-api`:

```bash
curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:8080/hazards?limit=5"
curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:8080/hazards?type=VOLCANIC&limit=5"
curl -s -H "Authorization: Bearer $TOKEN" "http://localhost:8080/sources/status"
```

| Klien | `client_id` | *Scope* | Akses |
|---|---|---|---|
| Media | `media` | `hazard:read:summary` | Ringkasan saja; meminta field mentah menghasilkan `403 FORBIDDEN_FIELD`. |
| Tim lapangan | `field-team` | `hazard:read:raw` | Data lengkap. |
| BNPB | `bnpb-ops` | `hazard:read:raw` | Data lengkap. |

*Access token* berlaku 60 detik. Perbarui dengan `grant_type: refresh_token`. *Refresh token* lama otomatis tidak berlaku lagi setelah dipakai.

Dashboard dapat dibuka di <http://localhost:8083>.

## 7. Demo per Problem

Jalankan `sh scripts/demo/setup.sh` terlebih dahulu. Laporan HTML hasil Hurl tersimpan di `evidence/`.

| Problem | Perintah | Yang dibuktikan |
|---|---|---|
| P1: Perubahan skema PVMBG | `sh scripts/demo/run-p1.sh` | Skema PVMBG diganti dari v1 ke v2 saat sistem berjalan. Aggregator tetap menerima data tanpa restart; field baru (`confidence_level`) masuk ke `attributes`; data tidak valid masuk `rejected_records`. |
| P2: Sumber lambat/mati | `sh scripts/demo/run-p2.sh` | PVMBG dibuat *outage*; PVMBG menjadi `STALE` dengan *breaker* `OPEN`, BMKG tetap `OK`, lalu PVMBG pulih sendiri. Juga menguji *rate limit* (`429`). |
| P3: Autentikasi dan otorisasi | `sh scripts/demo/run-p3.sh` | Kredensial antarsumber ditolak, *scope* per klien, *refresh token* berotasi, pencabutan token lewat Kafka, dan tidak ada rahasia di riwayat git. |
| P4: Microservice dan penyimpanan fleksibel | Demo manual (lihat di bawah) | Satu service dapat di-*build* dan di-*deploy* ulang tanpa menghentikan service lain; field baru tersimpan di kolom JSONB `attributes` tanpa migrasi tabel. |
| P5: *Fan-out* / *pub-sub* | `sh scripts/demo/run-p5.sh` | Outbox relay mengirim event ke Kafka; dua konsumen menerima event yang sama; `notifier` dimatikan tanpa menghambat dashboard lalu menyusul; konsumen baru bisa ditambah tanpa mengubah aggregator. |

Beri jeda **60 detik** setelah `run-p2.sh` sebelum menjalankan `run-p3.sh`. Skrip P2 sengaja menghabiskan kuota *rate limit* klien `media`, yang juga dipakai P3.

**Demo manual P4** (jalankan `export MSYS_NO_PATHCONV=1` terlebih dahulu di Git Bash):

1. Setiap komponen berjalan sebagai container terpisah dengan Dockerfile sendiri:
```bash
docker compose --profile extra ps --format "table {{.Service}}\t{{.Image}}\t{{.Status}}"
ls cmd/*/Dockerfile
```

2. Satu service di-*rebuild* tanpa menyentuh service lain, dan sistem tetap melayani permintaan. Jalankan pengambilan token sampai `curl` dalam 60 detik:
```bash
SECRET=$(grep '^BNPB_SECRET=' .env | cut -d= -f2-); TOKEN=$(curl -s -X POST localhost:8084/token -H 'Content-Type: application/json' -d "{\"grant_type\":\"client_credentials\",\"client_id\":\"bnpb-ops\",\"client_secret\":\"$SECRET\"}" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
docker compose stop auth
curl -s -o /dev/null -w "GET /hazards -> HTTP %{http_code}\n" -H "Authorization: Bearer $TOKEN" "localhost:8080/hazards?limit=1"
docker compose up -d --build --no-deps auth
docker compose ps --format "table {{.Service}}\t{{.Status}}"
```
Hasil: HTTP 200 selama auth mati, dan hanya auth yang waktu "Up"-nya baru.

3. Record sebelum dan sesudah perubahan skema tersimpan berdampingan tanpa migrasi:
```bash
ls cmd/aggregator/migrations
ADMIN=$(grep '^PVMBG_ADMIN_KEY=' .env | cut -d= -f2-)
curl -s -X POST localhost:8082/admin/schema-version -H "X-Admin-Key: $ADMIN" -H 'Content-Type: application/json' -d '{"version":2}'
sleep 20
docker compose exec -T aggregator-db psql -U aat -d aggregator -c "(SELECT source_ref_id, COALESCE(attributes->>'confidence_level','NULL') AS confidence_level, ingested_at FROM hazard_events WHERE source='PVMBG' AND attributes ? 'confidence_level' ORDER BY ingested_at DESC LIMIT 4) UNION ALL (SELECT source_ref_id, 'NULL', ingested_at FROM hazard_events WHERE source='PVMBG' AND NOT attributes ? 'confidence_level' ORDER BY ingested_at DESC LIMIT 4) ORDER BY ingested_at;"
curl -s -X POST localhost:8082/admin/schema-version -H "X-Admin-Key: $ADMIN" -H 'Content-Type: application/json' -d '{"version":1}'
```
Hasil: baris dengan `confidence_level` NULL dan baris yang memiliki nilai tampil di tabel yang sama. Perintah terakhir mengembalikan PVMBG ke skema awal.

4. *Canonical store* hanya dapat dijangkau aggregator:
```bash
for s in client-api auth notifier aggregator; do printf '%-12s -> aggregator-db:5432  ' "$s"; docker compose exec -T $s sh -c 'nc -z -w 2 aggregator-db 5432 >/dev/null 2>&1 && echo REACHABLE || echo UNREACHABLE'; done
docker compose ps aggregator-db --format '{{.Ports}}'
```
Hasil: hanya aggregator yang `REACHABLE`, dan port 5432 tidak dipetakan ke host.

**Demo manual P1 (alternatif dari skrip):**

```bash
ADMIN=$(grep '^PVMBG_ADMIN_KEY=' .env | cut -d= -f2-)
curl -X POST -H "X-Admin-Key: $ADMIN" -H "Content-Type: application/json" \
  -d '{"version":2}' http://localhost:8082/admin/schema-version
sleep 15
docker compose logs --tail 20 aggregator | grep hazard_saved
```

## 8. Load Test

```bash
sh scripts/loadtest/run-load.sh
```

Menjalankan k6 di dalam container (profile `loadtest`):
- **Isolasi:** latensi kueri BMKG tetap rendah (p95 < 300 ms) saat PVMBG dibuat lambat 3 detik.
- **Beban:** 50 pengguna selama 60 detik.

Ringkasan hasil tersimpan di `evidence/p2-isolation.json` dan `evidence/p2-load.json`. Pengaturan normal dari `.env` dipulihkan otomatis setelah selesai.

## 9. Kafka

### Menjalankan Kafka saja

```bash
docker compose up -d kafka kafka-init
docker compose ps -a kafka-init
```

`kafka-init` berjalan sekali untuk membuat *topic*, lalu berhenti. Status yang benar adalah **Exited (0)**.

### Alamat broker

| Dari | Alamat |
|---|---|
| Container lain (service Go) | `kafka:9092` |
| Laptop (tool lokal) | `localhost:9094` |

Salah memakai alamat membuat koneksi menggantung.

### Topic

| Topic | Partisi | Key | Retensi | Producer | Consumer group |
|---|---|---|---|---|---|
| `hazard-events` | 3 | `hazard_id` | 7 hari | aggregator | `dashboard-updater`, `notifier`, `audit-log` |
| `auth-revocations` | 1 | tanpa key (isi pesan = `jti`) | 10 menit | auth | `client-api-revocations` |

### Smoke test

```bash
./scripts/kafka-smoke.sh
```

Skrip mengirim satu pesan ke *topic* `kafka-smoke` lalu membacanya lagi. Hasil yang benar adalah **PASS**. *Topic* ini terpisah dari *topic* utama.

### Library Go

Semua service memakai **github.com/twmb/franz-go**.

### Aturan konsumen

- Setiap konsumen memakai *group id* sendiri. *Group id* yang sama membuat pesan terbagi, bukan tersalin, sehingga *fan-out* gagal.
- Mulai membaca dari offset **earliest** agar konsumen baru membaca riwayat dan konsumen yang restart dapat menyusul.
- *Commit offset* **setelah** pesan selesai diproses (*at-least-once*).
- *Topic* tidak dibuat otomatis. *Topic* baru wajib ditambahkan ke `kafka-init`.

## 10. Struktur Repositori

```
api/                  Spesifikasi OpenAPI dan AsyncAPI
cmd/
  aggregator/         poller, mapper, penyimpanan, outbox relay, API internal
  auth/               penerbitan token dan JWKS
  client-api/         API klien eksternal
  bmkg-mock/          simulasi API BMKG
  pvmbg-mock/         simulasi API PVMBG
  dashboard-updater/  dashboard SSE
  notifier/           notifikasi SIAGA/AWAS
  audit-log/          pencatatan event
internal/shared/      helper bersama (pembacaan env)
scripts/
  demo/               setup dan skrip demo per Problem (Hurl)
  loadtest/           skenario k6
  gen-auth-keys.sh    pembuat kunci JWT
  kafka-smoke.sh      uji cepat Kafka
evidence/             hasil demo dan load test
secrets/              kunci JWT
docker-compose.yml
.env.example
```

## 11. Troubleshooting

| Gejala | Solusi |
|---|---|
| `The term ... is not recognized` | Terminal masih PowerShell. Jalankan perintah di Git Bash. |
| `sh: ...\r: not found` | Skrip ter-*checkout* dengan CRLF. Jalankan `git config core.autocrlf input`, lalu *clone* ulang. |
| `stat C:/Program Files/Git/...: no such file` | Jalankan `export MSYS_NO_PATHCONV=1` di Git Bash. |
| `port is already allocated` | Port 8080–8085 atau 9094 dipakai aplikasi lain. |
| `auth` atau `client-api` terus *restart* | Kunci di `secrets/` belum dibuat. Jalankan `sh scripts/gen-auth-keys.sh`. |
| Ada service yang mati setelah Docker *restart* | Jalankan `docker compose up -d`. |
| `run-p3.sh` gagal dengan `429` | Kuota *rate limit* `media` masih terpakai oleh `run-p2.sh`. Tunggu 60 detik lalu ulangi. |

## 12. Anggota Kelompok

Kelompok **pepeftw**

| No | Nama | NIM |
|---|---|---|
| 1 | Peter Wongsoredjo | 13523039 |
| 2 | Jessica Allen | 13523059 |
| 3 | David Bakti Lodianto | 13523083 |

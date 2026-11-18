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
13. [Milestone 2: Kubernetes](#13-milestone-2-kubernetes)

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
| `client-api` | API untuk klien eksternal: `/hazards`, `/hazards/:id`, `/sources/status`, `/unprocessed`, dengan *scope* dan *rate limit*. |
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
| Adaptasi PVMBG | `PVMBG_ADAPTATIONS` | Aturan adaptasi yang dideklarasikan untuk versi skema PVMBG yang *breaking*, dipisah koma (`rename:<lama>=<baru>`, `numeric_string:<field>`, `optional:<field>`). Kosong berarti tidak ada. Aturan yang tidak valid membuat aggregator gagal start. |
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
| `client-api` | 8080 | `GET /hazards`, `GET /hazards/:id`, `GET /sources/status`, `GET /unprocessed`, `POST /unprocessed/replay`, `GET /unprocessed/replay/:job_id`, `GET /health` |
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
| BNPB | `bnpb-ops` | `hazard:read:raw hazard:read:unprocessed hazard:replay:unprocessed` | Data lengkap, ditambah pesan gagal dan *replay*-nya (hanya klien ini). |

Klaim `scope` berisi daftar *scope* dipisah spasi. Klien tanpa *scope* yang dibutuhkan sebuah route mendapat `403 FORBIDDEN`.

*Access token* berlaku 60 detik. Perbarui dengan `grant_type: refresh_token`. *Refresh token* lama otomatis tidak berlaku lagi setelah dipakai.

Dashboard dapat dibuka di <http://localhost:8083>.

## 7. Demo per Problem

Jalankan `sh scripts/demo/setup.sh` terlebih dahulu. Laporan HTML hasil Hurl tersimpan di `evidence/`.

| Problem | Perintah | Yang dibuktikan |
|---|---|---|
| P1: Perubahan skema PVMBG | `sh scripts/demo/run-p1.sh` | Skema PVMBG diganti dari v1 ke v2 saat sistem berjalan. Aggregator tetap menerima data tanpa restart; field baru (`confidence_level`) masuk ke `attributes`; data tidak valid masuk `unprocessed_messages` sebagai `parked`. |
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

## 13. Milestone 2: Kubernetes

Mulai Milestone 2 seluruh komponen berjalan di Kubernetes pada namespace `sistem-informasi-kebencanaan-bnpb`. Penyimpanan kanonik aggregator memakai CockroachDB v24.2.4 (versi terakhir yang tidak memerlukan license key). Docker compose tetap tersedia untuk pengembangan cepat dengan CockroachDB single-node.

### Deploy dengan satu perintah

```bash
sh scripts/kind-up.sh
```

```bash
bash scripts/deploy.sh
```

`kind-up.sh` membuat cluster pengembangan kind (1 control plane dan 5 worker, satu worker cadangan untuk uji node baru). `deploy.sh` membuat Secret dari `.env`, membangun image, memuatnya ke kind, lalu menerapkan manifest bertahap (infra, init, migrate, apps) dan menjalankan health check.

| Opsi deploy.sh | Fungsi |
|---|---|
| `--overlay <nama>` | Overlay di `deploy/overlays` (bawaan `dev`) |
| `--registry <prefix>` | Push ke registry, misalnya `ghcr.io/<owner>/`, bukan kind |
| `--tag <tag> --no-build` | Pakai image yang sudah ada |
| `--only <service>` | Build ulang dan rollout satu service saja |
| `--fresh` | Hapus namespace beserta datanya lalu deploy dari awal |

### Deploy ke cluster VM (demo)

kind hanya untuk pengembangan. Demo berjalan di k3s pada VM dengan kernel terpisah. Langkah lengkap ada di [docs/vm-cluster.md](docs/vm-cluster.md).

```bash
bash scripts/deploy.sh --overlay vm --registry ghcr.io/koinen/
```

### Deklarasi topologi

| Node | Jenis | Host fisik | CPU host | RAM host | Peran |
|---|---|---|---|---|---|
| `node-a` | VM VirtualBox | (isi saat demo) | (isi) | (isi) | control plane dan worker |
| `node-b` | VM VirtualBox | (isi saat demo) | (isi) | (isi) | worker |
| `node-c` | VM VirtualBox | (isi saat demo) | (isi) | (isi) | worker |
| `node-d` | VM VirtualBox | (isi saat demo) | (isi) | (isi) | worker |

Setiap node menjalankan satu pod CockroachDB. Skenario mematikan node hanya dikenakan pada node worker non-control-plane.

### Struktur manifest

| Path | Isi |
|---|---|
| `deploy/base` | Topologi asli untuk cluster demo (CockroachDB 4, Kafka 3, aggregator 3) |
| `deploy/overlays/vm` | Cluster VM k3s (CockroachDB 4, aggregator 3, Kafka 1, image dari GHCR) |
| `deploy/overlays/dev` | Replika lebih sedikit untuk kind di laptop (CockroachDB 4, Kafka 1, aplikasi 1) |
| `deploy/kind/cluster.yaml` | Konfigurasi cluster kind dan port host 8080 sampai 8085 |
| `cmd/aggregator/migrations` | Skema SQL, dijalankan oleh Job `aggregator-migrate` (`./aggregator migrate`) |

Setiap pod memiliki label `role` (aggregator, store, broker, consumer, api, mock, sensor, other) serta `resources.requests` dan `resources.limits` sesuai Lampiran B. Kredensial berada di Secret `bnpb-secrets` dan `auth-keys`, konfigurasi di ConfigMap. NetworkPolicy memastikan CockroachDB hanya dapat dijangkau aggregator, `auth-store` hanya oleh auth, dan `client-api-store` hanya oleh client-api.

### Anggaran memori (requests)

| Komponen | Pod (dev) | Requests per pod |
|---|---|---|
| CockroachDB | 4 | 512Mi |
| Kafka | 1 | 512Mi |
| Redis auth-store, client-api-store | 2 | 512Mi |
| aggregator | 1 | 256Mi |
| auth, client-api | 2 | 128Mi |
| notifier, dashboard-updater | 2 | 128Mi |
| bmkg-mock, pvmbg-mock | 2 | 64Mi |

Pemakaian nyata cluster dev sekitar 5,5 GB, sehingga muat dalam batas WSL 8 GB.

### Menambah service baru

1. Buat folder `deploy/base/<service>/` berisi `configmap.yaml`, `deployment.yaml`, dan `service.yaml` mengikuti service yang sudah ada.
2. Pod template wajib memiliki label `app`, `role`, serta `bnpb.io/stage: apps`, memory requests dan limits, dan `enableServiceLinks: false`.
3. Image ditulis `bnpb/<service>:TAG`, lalu tambahkan nama service ke `SERVICES` di `scripts/deploy/images.sh`.
4. Daftarkan file baru di `deploy/base/kustomization.yaml`.
5. Migrasi SQL baru diberi nomor berikutnya di `cmd/aggregator/migrations` (nomor `004` dilewati karena leader election memakai Kubernetes Lease, `005` dipakai distribusi data P1).

### Pindah dari compose Milestone 1

Volume database compose lama berisi data Postgres. Jalankan sekali:

```bash
docker compose down -v
```

### P1: Distribusi data CockroachDB

Migrasi `005_p1_distribution.sql` memecah `hazard_events` menjadi 16 range berdasarkan `hazard_id` (UUIDv5, hasil hash dari `source:source_ref_id`) lalu menyebarkannya ke seluruh node. Index waktu dan primary key `outbox` memakai hash-sharded index agar penulisan berurutan tidak menumpuk pada satu range. Dengan 4 node CockroachDB dan replikasi 3, setiap node hanya memegang sekitar 75% data.

| Perintah | Fungsi |
|---|---|
| `bash scripts/p1/load-catalog.sh <katalog.csv> [workers]` | Memuat katalog gempa melalui `./aggregator load-catalog` di pod aggregator |
| `bash scripts/p1/distribution.sh` | Sebaran replika, leaseholder, dan byte `hazard_events` per node |
| `bash scripts/p1/node-load.sh [sampel] [jeda]` | Beban tulis per node selama pemuatan berlangsung |
| `bash scripts/p1/spike.sh [jumlah] [wilayah]` | Simulasi lonjakan gempa di satu wilayah sambil mengukur beban per node |
| `bash scripts/p1/spread.sh <prefix>` | Jumlah baris dengan prefix `source_ref_id` tertentu per node leaseholder, misalnya `catalog-spike` |
| `go run ./tools/catalog-gen -n 20000 > katalog.csv` | Katalog sintetis yang condong ke jalur megathrust Sunda |

Format katalog adalah CSV dengan header `event_id,time,latitude,longitude,depth_km,magnitude,place`. Waktu memakai ISO 8601 UTC. Setiap baris menjadi HazardEvent `BMKG` bertipe `SEISMIC` dengan `source_ref_id` `catalog-<event_id>`, sehingga pemuatan ulang tidak menggandakan data.

### P1: Pembacaan lintas shard dan Audit API

Data `hazard_events` dibagi menjadi 16 unit penyimpanan, `shard-0` sampai `shard-f`, menurut digit hex pertama `hazard_id`. Daftar hazard memakai cursor keyset `(ingested_at, hazard_id)` sehingga urutan tetap benar di seluruh halaman. Bila query cepat melewati 1,5 detik, aggregator membaca ke-16 shard secara paralel (2 detik per shard, satu snapshot `AS OF SYSTEM TIME`). Shard yang tidak menjawab dicantumkan pada `unavailable_units` dan respons diberi `partial: true`, bukan error 500.

| Endpoint | Fungsi |
|---|---|
| `GET /internal/audit/events?source&since&limit&cursor` | Seluruh HazardEvent, urut naik menurut `ingested_at` lalu `hazard_id` |
| `GET /internal/audit/checksum?source` | Jumlah record dan SHA-256 atas `source_ref_id` |
| `GET /internal/audit/location?hazard_id` | Node primary dan replica tempat HazardEvent disimpan |
| `GET /internal/audit/unprocessed?source&limit&cursor` | Pesan berstatus `retrying` atau `parked` |

Audit API memakai header `Authorization: Bearer <AUDIT_TOKEN>`. Token disimpan di `.env` lalu dimuat `deploy.sh` ke Secret `bnpb-secrets`. Kontraknya ada di `api/audit.yaml`.

```bash
bash scripts/p1/audit.sh checksum BMKG
```

Pesan gagal hanya bisa diperiksa BNPB Internal Ops. Klien `bnpb-ops` memanggil client-api; client-api meneruskannya ke API ops aggregator dengan `Authorization: Bearer <OPS_TOKEN>` (token ini hanya dipegang client-api, token audit tidak bisa memanggilnya). Hanya metadata pesan yang dikembalikan, tidak pernah *payload*. Kontraknya ada di `api/client-api.yaml` dan `api/ops.yaml`.

| client-api | Scope | Aggregator | Isi |
|---|---|---|---|
| `GET /unprocessed?source&since&limit&cursor` | `hazard:read:unprocessed` | `GET /internal/unprocessed` | Pesan berstatus `retrying` atau `parked` beserta `reason` |
| `POST /unprocessed/replay?source=PVMBG` | `hazard:replay:unprocessed` | `POST /internal/unprocessed/replay` | *Replay* pesan PVMBG yang `parked` (setelah aturan `PVMBG_ADAPTATIONS` dideklarasikan). `202 {job_id, status}`; jika masih ada job aktif untuk source itu, job yang sama dikembalikan. `requested_by` berisi subject JWT |
| `GET /unprocessed/replay/{job_id}` | `hazard:replay:unprocessed` | `GET /internal/unprocessed/replay/{job_id}` | Status job dan hitungan `selected`, `recovered`, `still_parked`, `handed_to_retry`, `not_replayable` |

```bash
curl -s -H "Authorization: Bearer $TOKEN" "localhost:8080/unprocessed?source=PVMBG&limit=5"
curl -s -X POST -H "Authorization: Bearer $TOKEN" "localhost:8080/unprocessed/replay?source=PVMBG"
```

| Variabel | Bawaan | Fungsi |
|---|---|---|
| `AUDIT_TOKEN` | kosong | Token Audit API, tanpa nilai ini seluruh permintaan audit ditolak |
| `OPS_TOKEN` | kosong | Token API ops (aggregator dan client-api). Tanpa nilai ini aggregator menolak seluruh permintaan ops dan route `/unprocessed` di client-api membalas `503` |
| `READ_FAST_TIMEOUT` | `1500ms` | Batas query cepat daftar hazard sebelum beralih ke pembacaan per shard |
| `READ_SHARD_TIMEOUT` | `2s` | Batas waktu tiap shard sebelum dianggap tidak terjangkau |
| `DB_POOL_MAX_CONNS` | `24` | Jumlah koneksi database per pod aggregator, cukup untuk 16 query shard paralel |

### P1: Node hilang, node baru, dan pemulihan

CockroachDB menyimpan 3 replika setiap range pada node yang berbeda. Bila satu node hilang, setiap range masih punya mayoritas sehingga baca dan tulis tetap berjalan. Setelah 75 detik node dianggap mati (`server.time_until_store_dead`) dan replika yang kurang dibuat ulang di node lain. Zona sistem (`meta`, `liveness`, `system`) juga memakai 3 replika agar status pemulihan bisa mencapai nol pada cluster 4 node.

| Skenario | Perintah |
|---|---|
| Lihat node, range kurang replika, dan range tidak tersedia | `bash scripts/p1/recovery-status.sh` |
| Tunggu sampai replikasi pulih penuh | `bash scripts/p1/recovery-status.sh --wait` |
| Simulasi node hilang beserta disknya (kind) | `bash scripts/p1/lose-node.sh <node>` |
| Node yang sama kembali dengan disk kosong | nyalakan lagi node itu lalu `bash scripts/p1/decommission-dead.sh` |
| Ganti node yang hilang permanen dengan node baru | `bash scripts/p1/replace-node.sh <node yang hilang>` |

`replace-node.sh` diperlukan karena volume local-path terikat pada node asalnya. Skrip menghapus objek node yang hilang, melepas PVC pod CockroachDB yang tinggal di sana sehingga pod pindah ke node kosong dengan disk baru, lalu men-decommission node CockroachDB yang mati. Pada VM, node dimatikan oleh Tester Agent atau dengan mematikan VM, lalu langkah pemulihannya sama.

### P1: Dashboard analitik

Service `analytics-dashboard` menyajikan halaman analitik di port `8086` (NodePort `30086`). Halaman memuat line chart jumlah kejadian per jenis bahaya dengan pilihan rentang waktu, jumlah per severity, dan tabel per wilayah. Angka dihitung aggregator lintas 16 unit penyimpanan pada satu snapshot (`GET /internal/analytics`), sehingga sama dengan hitungan langsung dari Audit API `events`.

| Kategori | Aturan |
|---|---|
| Gempa | `SEISMIC` tanpa peringatan tsunami |
| Tsunami | `SEISMIC` yang memiliki `attributes.tsunami_warnings` |
| Gunung berapi | `VOLCANIC` |
| Kebakaran hutan | `WILDFIRE` |
| Pesisir | `COASTAL` |

Bila sebagian unit tidak terjangkau, halaman tetap tampil, setiap angka diberi tanda `*`, dan banner menyebut unit yang hilang. Cluster kind yang dibuat sebelum port `8086` ditambahkan dapat memakai `kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/analytics-dashboard 8086:8080`.

### Observabilitas

| Komponen | Alamat | Isi |
|---|---|---|
| Grafana | `http://localhost:8087` | Dashboard "Layanan BNPB": service aktif, throughput, error rate, latensi p95 per service dan per route, pesan Kafka per detik, waktu proses pesan |
| Jaeger | `http://localhost:8088` | Jejak satu alur permintaan lintas service beserta durasi tiap langkah |
| Prometheus | `http://localhost:8089` | Metrik mentah dan status target scrape |

Setiap service Go memakai paket `internal/telemetry`: middleware HTTP yang mencatat metrik `bnpb_http_*` dan span, transport HTTP yang meneruskan header `traceparent`, serta pembungkus Kafka yang menulis `traceparent` ke header pesan. Outbox menyimpan `trace_parent` (migrasi `007`) sehingga satu jejak menyambung dari polling aggregator, mock BMKG atau PVMBG, penyimpanan, publikasi Kafka, sampai notifier, dashboard-updater, dan audit-log. Tracing aktif hanya bila `OTEL_EXPORTER_OTLP_ENDPOINT` terisi, sehingga docker compose tetap berjalan tanpa Jaeger.

| Jejak yang ditunjukkan | Cara mencarinya di Jaeger |
|---|---|
| Alur ingest | service `aggregator`, operation `poll BMKG` atau `poll PVMBG` |
| Alur baca klien | service `client-api`, operation `GET /hazards` |
| Dashboard analitik | service `analytics-dashboard`, operation `GET /api/analytics` |

Cluster kind yang dibuat sebelum port `8087` sampai `8089` ditambahkan dapat memakai `kubectl port-forward svc/grafana 8087:3000`, `svc/jaeger 8088:16686`, dan `svc/prometheus 8089:9090` pada namespace sistem.

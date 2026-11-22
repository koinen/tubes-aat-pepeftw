# Sistem Agregasi Peringatan Bencana BNPB - Kelompok pepeftw

Tugas Besar IF4031 Arsitektur Aplikasi Terdistribusi - Milestone 2.

Sistem ini mengambil data gempa dari BMKG, gunung api dari PVMBG, sensor pesisir dari Sensor Pantai, dan titik api dari feed satelit, menyeragamkannya menjadi satu format `HazardEvent`, menyimpannya di CockroachDB, lalu menyebarkannya lewat Kafka ke layanan hilir (dashboard, notifikasi, audit, Tim Lapangan) dan ke klien eksternal melalui API ber-JWT. Seluruh komponen berjalan di Kubernetes pada namespace `sistem-informasi-kebencanaan-bnpb`.

## Daftar Isi

1. [Arsitektur Singkat](#1-arsitektur-singkat)
2. [Prasyarat](#2-prasyarat)
3. [Menjalankan Sistem dari Awal](#3-menjalankan-sistem-dari-awal)
4. [Deklarasi Topologi](#4-deklarasi-topologi)
5. [Variabel Konfigurasi](#5-variabel-konfigurasi)
6. [Port dan Endpoint](#6-port-dan-endpoint)
7. [Memicu Skenario Pengujian](#7-memicu-skenario-pengujian)
8. [Detail P1: Penyimpanan Terdistribusi](#8-detail-p1-penyimpanan-terdistribusi)
9. [Observabilitas](#9-observabilitas)
10. [Mencoba API](#10-mencoba-api)
11. [Kafka](#11-kafka)
12. [Pengembangan](#12-pengembangan)
13. [Struktur Repositori](#13-struktur-repositori)
14. [Troubleshooting](#14-troubleshooting)
15. [Anggota Kelompok](#15-anggota-kelompok)
16. [Lampiran: Docker Compose](#16-lampiran-docker-compose)

## 1. Arsitektur Singkat

```
bmkg-mock ────┐                            ┌─> dashboard-updater (SSE)
pvmbg-mock ───┤                            ├─> notifier ─> field-team-receiver
sensor-pantai ┼─> aggregator ─> Kafka ─────┼─> audit-log
feed satelit ─┘   (leader election,        └─> analytics-dashboard (via aggregator)
                   CockroachDB 4 node, outbox)
                        ▲
client-api ─────────────┘  (REST internal, X-Internal-Key)
    ▲   │
    │   └─ Redis (rate limit) + denylist dari topic auth-revocations
klien (media, field-team, bnpb-ops) ── token dari auth (JWT EdDSA, Redis refresh token)
```

| Service | Fungsi |
|---|---|
| `aggregator` | Tiga replika. Hanya *leader* (Kubernetes Lease `aggregator-leader`, *fencing epoch*) yang menarik data BMKG, PVMBG, dan Sensor Pantai. Semua replika menyimpan ke CockroachDB dan mengirim ke Kafka lewat *transactional outbox*. Juga menjalankan konsumer feed satelit, Audit API, dan API ops. |
| `client-api` | API klien eksternal: `/hazards`, `/hazards/:id`, `/sources/status`, `/unprocessed`, `/field-reports`, dengan *scope*, *rate limit*, dan *load shedding* Media. |
| `auth` | *Access token* JWT (EdDSA, TTL 60 detik) dan *refresh token* berotasi, JWKS di `/.well-known/jwks.json`. |
| `dashboard-updater` | Dashboard *real time* (SSE). |
| `analytics-dashboard` | Dashboard analitik lintas 16 unit penyimpanan. |
| `notifier` | Bereaksi pada status `SIAGA` dan `AWAS`, mengirim peringatan ke `field-team-receiver`. |
| `field-team-receiver` | Penerima peringatan Tim Lapangan, mencatat tanda terima ke topic `field-team-receipts`. |
| `audit-log` | Pencatatan event. |
| `bmkg-mock`, `pvmbg-mock` | Simulasi sumber BMKG dan PVMBG, termasuk *outage*, versi skema, dan respons rusak. |
| `sensor-pantai` | Manifest asisten, dijalankan apa adanya. |
| CockroachDB, Kafka, Redis | Penyimpanan kanonik (4 node), *broker* (3 broker di VM), dan dua Redis terpisah untuk auth dan client-api. |
| Prometheus, Grafana, Jaeger | Metrik, dashboard, dan *tracing*. |

## 2. Prasyarat

| Alat | Dipakai untuk |
|---|---|
| Git Bash | Seluruh perintah di README ini. Di Windows jangan pakai PowerShell atau CMD. |
| Docker Desktop | Membangun image dan menjalankan kind |
| kubectl dan kind | Cluster pengembangan |
| Hurl | Health check dan skrip demo |
| Python 3 | Penilaian P5 (`p5_slo.py`) |
| VirtualBox 7, Vagrant | VM untuk cluster demo |
| Tailscale | Menghubungkan VM di laptop yang berbeda |
| Akun GHCR | Image untuk cluster VM |

Go tidak perlu dipasang. Semua service di-*build* di dalam container. k6 dijalankan dari image `grafana/k6`.

## 3. Menjalankan Sistem dari Awal

kind hanya untuk pengembangan. Demo dan pengujian M2 berjalan di k3s pada VM dengan kernel terpisah.

```
 kondisi awal (repo baru di-clone)
     │
     ├─ A. kind (laptop)     kind-up.sh ─> deploy.sh ─> health check
     │
     └─ B. VM (demo)         vagrant up ─> tailnet-join ─> kubeconfig ─> deploy.sh --overlay vm ─> health check
```

### A. Cluster pengembangan (kind)

1. Clone repo dan masuk ke foldernya.

```bash
git clone https://github.com/koinen/tubes-aat-pepeftw.git && cd tubes-aat-pepeftw
```

2. Buat cluster kind (1 control plane, 5 worker).

```bash
sh scripts/kind-up.sh
```

3. Deploy seluruh sistem. Bila `.env` belum ada, skrip menyalinnya dari `.env.example`. Kunci JWT di `secrets/` juga dibuat otomatis.

```bash
bash scripts/deploy.sh
```

4. Cek kesehatan.

```bash
hurl --test scripts/demo/00-health.hurl
```

### B. Cluster demo (VM k3s)

Langkah lengkap ada di [docs/vm-cluster.md](docs/vm-cluster.md). Ringkasnya:

1. Di laptop pemilik `node-a`, nyalakan VM. Provisioner berhenti dengan pesan `set K3S_TOKEN`, dan pesan ini memang diharapkan.

```bash
cd scripts/vm && BRIDGE="<nama adapter Wi-Fi>" vagrant up node-a
```

2. Pasang k3s server di `node-a` lewat Tailscale. Skrip mencetak IP tailnet `node-a`.

```bash
TS_AUTHKEY=<auth-key> bash scripts/vm/tailnet-join.sh node-a server
```

3. Ambil token join dari `node-a`.

```bash
cd scripts/vm && vagrant ssh node-a -c "sudo cat /var/lib/rancher/k3s/server/node-token"
```

4. Di laptop pemilik setiap worker, nyalakan VM lalu gabungkan ke cluster.

```bash
TS_AUTHKEY=<auth-key> K3S_TOKEN=<token> bash scripts/vm/tailnet-join.sh <node> agent <ip-tailnet-node-a>
```

5. Di laptop pemilik `node-a`, ambil kubeconfig lalu pastikan semua node berstatus Ready. Laptop lain memakai salinan berkas yang sama.

```bash
bash scripts/vm/fetch-kubeconfig.sh <ip-tailnet-node-a>
```

```bash
export KUBECONFIG="$(cygpath -m ~/.kube/bnpb-vm.yaml)" && kubectl get nodes -o wide
```

6. Sinkronkan jam setiap VM. CockroachDB menolak node yang jamnya bergeser.

```bash
cd scripts/vm && vagrant ssh <node> -c "sudo timedatectl set-ntp true && sudo systemctl restart systemd-timesyncd && sleep 12 && timedatectl show -p NTPSynchronized"
```

Hasil yang benar adalah `NTPSynchronized=yes`.

7. Login ke GHCR lalu deploy. Bila package GHCR privat, isi `GHCR_USER` dan `GHCR_TOKEN` di `.env`.

```bash
docker login ghcr.io
```

```bash
bash scripts/deploy.sh --overlay vm --registry ghcr.io/peterwongsoredjo/
```

8. Pasang Tester Agent dari spesifikasi.

```bash
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
```

9. Cek kesehatan lewat tunnel, atau buka NodePort langsung di `http://<ip-tailnet-node-a>:30080` sampai `30089`.

```bash
bash scripts/vm/tunnel.sh <ip-tailnet-node-a>
```

```bash
hurl --test scripts/demo/00-health.hurl
```

Untuk profil ujian Sensor Pantai, simpan kunci dari asisten di `secrets/pantai.key` lalu deploy dengan `--overlay vm-exam`.

### Opsi deploy.sh

| Opsi | Fungsi |
|---|---|
| `--overlay <nama>` | Overlay di `deploy/overlays` (bawaan `dev`) |
| `--registry <prefix>` | Push ke registry, misalnya `ghcr.io/peterwongsoredjo/`, bukan kind |
| `--tag <tag> --no-build` | Pakai image yang sudah ada |
| `--only <service>` | Build ulang dan rollout satu service saja |
| `--fresh` | Hapus namespace beserta datanya lalu deploy dari awal |
| `--health` | Jalankan health check Hurl pada cluster non-kind lewat tunnel |

### Menghentikan

| Cluster | Perintah |
|---|---|
| kind | `kind delete cluster --name bnpb-dev` |
| VM | `cd scripts/vm && vagrant halt <node>` di laptop pemilik node |

## 4. Deklarasi Topologi

Cluster demo adalah k3s `v1.37.2+k3s1` pada VM VirtualBox (Debian, kernel terpisah) yang dibuat dengan Vagrant dan dihubungkan lewat Tailscale.

| Node | VM | Host fisik | CPU host | RAM host | Peran |
|---|---|---|---|---|---|
| `node-a` | 2 vCPU, RAM 3 GB | Laptop Peter (ASUS VivoBook M1403QA) | AMD Ryzen 7 5800HS, 8 core 16 thread | 15,4 GB | control plane dan worker |
| `node-b` | 2 vCPU, RAM 2,5 GB | Laptop Allen (Lenovo IdeaPad Gaming 3) | AMD Ryzen 5 7535HS | 15,2 GB | worker |
| `node-d` | 2 vCPU, RAM 2,5 GB | Laptop Allen (Lenovo IdeaPad Gaming 3) | AMD Ryzen 5 7535HS | 15,2 GB | worker |
| `node-e` | 2 vCPU, RAM 2,5 GB | Laptop David (MSI Katana GF66 12UE) | Intel Core i7-12700H, 14 core 20 thread | 15,7 GB | worker |

`node-c` (laptop David) dipakai pada awal pengujian, lalu dimatikan dan dihapus pada uji kehilangan node P1. `node-e` menggantikannya. Skenario mematikan node hanya dikenakan pada worker, bukan `node-a`.

| Komponen | Replika (overlay `vm`) | Penempatan |
|---|---|---|
| CockroachDB | 4 | Satu pod per node, replikasi 3 |
| Kafka | 3 | Replikasi topic 3, `min.insync.replicas` 2 |
| aggregator | 3 | Disebar ke node berbeda (`topologySpreadConstraints`) |
| client-api, auth | 1 | Bebas |
| Tester Agent | DaemonSet | Satu pod per node |

### Struktur manifest

| Path | Isi |
|---|---|
| `deploy/base` | Topologi asli untuk cluster demo (CockroachDB 4, Kafka 3, aggregator 3) |
| `deploy/overlays/vm` | Cluster VM k3s (CockroachDB 4, Kafka 3, aggregator 3, image dari GHCR) |
| `deploy/overlays/vm-exam` | Overlay `vm` dengan Sensor Pantai pada profil ujian (butuh `secrets/pantai.key` dari asisten) |
| `deploy/overlays/dev` | Replika lebih sedikit untuk kind di laptop (CockroachDB 4, Kafka 1, aplikasi 1) |
| `deploy/base/sensor-pantai` | `manifest.yaml` asisten (salinan apa adanya), Service, dan patch namespace, resources, label stage |
| `deploy/kind/cluster.yaml` | Konfigurasi cluster kind dan port host 8080 sampai 8089 |
| `cmd/aggregator/migrations` | Skema SQL, dijalankan oleh Job `aggregator-migrate` (`./aggregator migrate`) |

Setiap pod memiliki label `role` (aggregator, store, broker, consumer, api, mock, sensor, other) serta `resources.requests` dan `resources.limits` sesuai Lampiran B. NetworkPolicy memastikan CockroachDB hanya dapat dijangkau aggregator, `auth-store` hanya oleh auth, `client-api-store` hanya oleh client-api, dan Sensor Pantai hanya oleh aggregator.

## 5. Variabel Konfigurasi

Semua nilai contoh ada di `.env.example`. File `.env` **tidak** di-*commit*. `deploy.sh` memuat `.env` ke Secret `bnpb-secrets`, kunci JWT ke Secret `auth-keys`, dan `PANTAI_AUTH_TOKEN` ke Secret `pantai-token`. Nilai yang tidak rahasia berada di ConfigMap tiap service (`deploy/base/<service>/configmap.yaml`). Ganti semua nilai rahasia sebelum dipakai di luar pengembangan.

### Rahasia dan kredensial (`.env`)

| Variabel | Fungsi |
|---|---|
| `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_PORT` | Kredensial dan port CockroachDB (bawaan `26257`) |
| `INTERNAL_API_KEY` | Header `X-Internal-Key` dari client-api ke aggregator |
| `AUDIT_TOKEN` | Token Audit API. Tanpa nilai ini seluruh permintaan audit ditolak. |
| `OPS_TOKEN` | Token API ops, hanya dipegang client-api. Tanpa nilai ini route `/unprocessed` membalas `503`. |
| `PANTAI_AUTH_TOKEN` | Token Sensor Pantai |
| `SATELLITE_TOKEN` | Token kelompok untuk feed satelit |
| `GHCR_USER`, `GHCR_TOKEN` | Pull image GHCR privat di cluster VM. Kosongkan bila package publik. |
| `BMKG_API_KEY` | Kunci BMKG, dipakai bersama oleh `bmkg-mock` dan aggregator |
| `PVMBG_TOKEN_SECRET`, `PVMBG_TOKEN`, `PVMBG_ADMIN_KEY` | Token PVMBG dan kunci admin mock. Lihat catatan token di bawah. |
| `REDIS_PASSWORD` | Password kedua Redis |
| `MEDIA_SECRET`, `LAPANGAN_SECRET`, `BNPB_SECRET` | *Secret* klien `media`, `field-team`, dan `bnpb-ops` |

### Perilaku sistem

| Kelompok | Variabel | Keterangan |
|---|---|---|
| Kafka | `KAFKA_BROKERS`, `KAFKA_TOPIC_*`, `KAFKA_GROUP_ID_*` | Setiap konsumen wajib memakai *group id* sendiri. |
| Poller | `POLLER_ENABLED`, `POLL_INTERVAL`, `BMKG_TIMEOUT`, `PVMBG_TIMEOUT`, `POLL_CURSOR_OVERLAP`, `BREAKER_FAILURES`, `BREAKER_OPEN_FOR`, `TSUNAMI_GRACE` | Interval, batas waktu, dan *circuit breaker* per sumber |
| Adaptasi PVMBG | `PVMBG_ADAPTATIONS` | Aturan untuk versi skema PVMBG yang *breaking*, dipisah koma (`rename:<lama>=<baru>`, `numeric_string:<field>`, `optional:<field>`). Aturan yang tidak valid membuat aggregator gagal start. |
| Sensor Pantai | `SENSOR_PANTAI_URL`, `SENSOR_PANTAI_POLL_INTERVAL`, `SENSOR_PANTAI_TIMEOUT` | ConfigMap aggregator (bawaan interval `1s`, batas waktu `2s`) |
| Feed satelit | `SATELLITE_ENABLED`, `SATELLITE_URL`, `SATELLITE_LATENESS`, `SATELLITE_BATCH_SIZE`, `SATELLITE_BATCH_WAIT`, `SATELLITE_IDLE_TIMEOUT` | ConfigMap aggregator. `SATELLITE_TOKEN` wajib bila `SATELLITE_ENABLED=true`. |
| Pembacaan P1 | `READ_FAST_TIMEOUT`, `READ_SHARD_TIMEOUT`, `DB_POOL_MAX_CONNS` | Bawaan `1500ms`, `2s`, dan `24` |
| Auth | `JWT_ISSUER`, `JWT_AUDIENCE`, `ACCESS_TOKEN_TTL`, `REFRESH_TOKEN_TTL` | TTL dalam detik |
| Rate limit | `RATE_LIMIT_MAX`, `RATE_LIMIT_WINDOW_SECONDS` | Bawaan 10 *request* per 60 detik per klien |
| Batas beban | `MAX_CONCURRENT_REQUESTS` | Batas *request* aktif per pod client-api (bawaan 200, minimal 2). Seperempat slot dikhususkan untuk Tim Lapangan dan BNPB Internal Ops. Bila tidak ada slot, API membalas `503 overloaded` dengan `Retry-After: 2`. |
| Mock BMKG | `BMKG_DELAY_MIN`, `BMKG_DELAY_MAX`, `BMKG_EVENT_INTERVAL`, `BMKG_SEED_COUNT`, `BMKG_TSUNAMI_RATE` | Latensi, laju event, dan data awal |
| Mock PVMBG | `PVMBG_DELAY_MIN`, `PVMBG_DELAY_MAX`, `PVMBG_REPORT_INTERVAL`, `PVMBG_SEED_COUNT`, `PVMBG_SCHEMA_VERSION`, `PVMBG_HANG_MAX` | Latensi, laju laporan, versi skema, dan batas *hang* saat *outage* |
| Tracing | `OTEL_EXPORTER_OTLP_ENDPOINT` | ConfigMap `common` (`http://jaeger:4318`). Kosong berarti *tracing* mati. |
| Log | `LOG_LEVEL` | `DEBUG`, `INFO`, `WARN`, atau `ERROR` |

**Token PVMBG.** Bila `PVMBG_TOKEN_SECRET` diganti, buat ulang `PVMBG_TOKEN`:

```bash
docker compose run --rm pvmbg-mock ./pvmbg-mock gen-token bnpb-aggregator
```

Salin hasilnya ke `PVMBG_TOKEN` di `.env`, lalu jalankan ulang `bash scripts/deploy.sh`.

## 6. Port dan Endpoint

| Service | kind (`localhost`) | NodePort VM | Endpoint utama |
|---|---|---|---|
| `client-api` | 8080 | 30080 | `GET /hazards`, `GET /hazards/:id`, `GET /sources/status`, `GET /unprocessed`, `POST /unprocessed/replay`, `GET /unprocessed/replay/:job_id`, `POST /field-reports`, `GET /health` |
| `bmkg-mock` | 8081 | 30081 | `GET /seismic-events`, `GET /tsunami-warnings`, `GET /health` |
| `pvmbg-mock` | 8082 | 30082 | `GET /volcanic-reports`, `POST /admin/outage`, `/admin/schema-version`, `/admin/corrupt`, `GET /health` |
| `dashboard-updater` | 8083 | 30083 | `GET /` (dashboard), `GET /events` (SSE), `GET /api/hazards`, `GET /health` |
| `auth` | 8084 | 30084 | `POST /token`, `GET /.well-known/jwks.json`, `GET /health` |
| `aggregator` | 8085 | 30085 | `/internal/hazards`, `/internal/sources/status`, `/internal/audit/*`, `/internal/unprocessed*`, `GET /health` |
| `analytics-dashboard` | 8086 | 30086 | `GET /` (dashboard analitik), `GET /api/analytics` |
| Grafana | 8087 | 30087 | Dashboard "Layanan BNPB" dan "Feed Satelit" |
| Jaeger | 8088 | 30088 | Jejak lintas service |
| Prometheus | 8089 | 30089 | Metrik mentah dan target scrape |

Spesifikasi API lengkap (OpenAPI dan AsyncAPI) ada di folder `api/`.

## 7. Memicu Skenario Pengujian

Semua perintah dijalankan dari root repo di Git Bash, dengan `kubectl` menunjuk ke cluster yang benar. Di cluster VM jalankan `export KUBECONFIG="$(cygpath -m ~/.kube/bnpb-vm.yaml)"` terlebih dahulu. Path harus berformat Windows (`C:/...`) karena `deploy.sh` mematikan konversi path Git Bash. Log hasil skrip M2 tersimpan di `evidence/` dan `docs/results/`.

### Tester Agent

| Aksi | Perintah |
|---|---|
| Daftar skenario | `kubectl exec -n tester-agent daemonset/tester-agent -- /app list` |
| Menjalankan skenario latihan | `kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <skenario>` |
| Log aksi | `kubectl logs -n tester-agent -l app=tester-agent` |

Skenario yang dipakai: `wipe-node`, `clock-skew`, `partition`, dan `kill-leader`.

### P1: Penyimpanan terdistribusi

| Butir | Skenario | Perintah |
|---|---|---|
| K1 | Muat katalog lalu lihat sebaran data per node | `bash scripts/p1/load-catalog.sh <katalog.csv>` lalu `bash scripts/p1/distribution.sh` |
| K2 | Beban tulis per node selama pemuatan | `bash scripts/p1/node-load.sh` |
| K2 | Lonjakan gempa di satu wilayah | `bash scripts/p1/spike.sh 5000 "Selat Sunda"` lalu `bash scripts/p1/spread.sh catalog-spike` |
| K3 | Pagination lintas shard | `curl -s -H "Authorization: Bearer $TOKEN" "localhost:8080/hazards?limit=50"` lalu ulangi dengan `cursor` dari respons |
| K4 | Node hilang beserta datanya | `kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice wipe-node`, pantau dengan `bash scripts/p1/recovery-status.sh --wait` |
| K4 | Bandingkan checksum sebelum dan sesudah | `bash scripts/p1/audit.sh checksum BMKG` |
| K5 | Node baru menggantikan node yang hilang | Join node baru (langkah 3.B.4), lalu `bash scripts/p1/replace-node.sh <node yang hilang>` |
| K6 | Pemulihan penuh | `bash scripts/p1/recovery-status.sh` sampai 0 range kurang replika dan 0 range tidak tersedia |
| K7 | Dashboard analitik | Buka port 8086 atau NodePort 30086, bandingkan dengan `bash scripts/p1/audit.sh` |

Katalog sintetis dibuat dengan `go run ./tools/catalog-gen -n 20000 > katalog.csv`. Detail skrip ada di [bagian 8](#8-detail-p1-penyimpanan-terdistribusi).

### P2: Dua nakhoda satu kapal

| Butir | Skenario | Perintah |
|---|---|---|
| Persiapan | Konteks, penempatan pod aggregator, Lease, dan epoch *fencing* | `bash scripts/demo/m2/p2.sh preflight` |
| K1 | Hanya leader yang menarik dan mengirim data | `bash scripts/demo/m2/p2.sh leader`, `bash scripts/demo/m2/p2.sh workers`, `bash scripts/demo/m2/p2.sh relay` |
| K1 | Tidak ada HazardEvent ganda | `bash scripts/demo/m2/p2.sh duplicates` |
| K2 | Leader mati, leader baru terpilih dalam 60 detik | `CONFIRM=yes bash scripts/demo/m2/p2.sh kill-leader`, atau `--hard` untuk hapus paksa tanpa melepas Lease |
| K3 | Jam node bergeser | `CONFIRM=yes bash scripts/demo/m2/p2.sh clock-skew clock-skew` |
| K4 | Leader membeku, leader lama ditolak oleh *fencing* | Jeda VM leader di VirtualBox (Pause, hanya di VM) lalu lanjutkan, kemudian `bash scripts/demo/m2/p2.sh fence-log` |

Alur lengkap ada di [scripts/demo/m2/P2-P4-RUNBOOK.md](scripts/demo/m2/P2-P4-RUNBOOK.md). Log tersimpan di `evidence/m2-p2/`.

### P3: Pesan beracun dan replay

| Butir | Skenario | Perintah |
|---|---|---|
| K3, K5 | Input rusak di setiap pintu masuk diparkir tanpa membuat service mati, hanya BNPB Internal Ops yang bisa membaca pesan gagal | `bash scripts/demo/m2/p3-poison.sh` |
| K4 | Pesan PVMBG yang diparkir di-*replay* setelah perubahan skema dideklarasikan | `bash scripts/demo/m2/p3-replay.sh` |
| K4 | Replay tetap selesai walau leader dimatikan di tengah job | `bash scripts/demo/m2/p3-replay.sh --kill-leader` |
| K6 | Laporan lapangan yang diterima `202` tidak hilang dan tersimpan tepat sekali | `bash scripts/demo/m2/p3-field-reports.sh` |

Di kind, arahkan skrip ke port host dengan `BASE_URL=http://localhost:8080 AUTH_URL=http://localhost:8084`. Bawaannya adalah NodePort `30080` dan `30084`.

### P4: Feed satelit

| Skenario | Perintah |
|---|---|
| Offset feed, jumlah deteksi, kebakaran, dan duplikat | `bash scripts/demo/m2/p4.sh status` |
| Feed diproses langsung (status tiap 5 detik) | `bash scripts/demo/m2/p4.sh watch 60` |
| Metrik satelit dari pod leader (lag, deteksi terlambat, latensi) | `bash scripts/demo/m2/p4.sh metrics` |
| Celah offset per pod aggregator | `bash scripts/demo/m2/p4.sh gaps` |
| Leader *crash* saat feed berjalan, hasil sama tanpa deteksi hilang atau ganda | `CONFIRM=yes bash scripts/demo/m2/p4.sh crash` |

Konsumer satelit hanya berjalan bila `SATELLITE_ENABLED=true` di ConfigMap aggregator. Jalankan `p4.sh status` dua kali sebelum `crash` untuk memastikan offset bertambah. Log tersimpan di `evidence/m2-p4/`.

### P5: Game day

P5 diukur di cluster VM. *Operator* menjalankan pengukuran, sedangkan gangguan diberikan oleh *injector* otomatis (`p5.sh inject-auto`) yang memilih rencana acak dan baru membukanya setelah selesai. Alur lengkap ada di [scripts/demo/m2/P5-RUNBOOK.md](scripts/demo/m2/P5-RUNBOOK.md). Satu putaran memakan waktu sekitar 80 menit.

```
 preflight (~10 min)
   └─> warm-up K3, K4, K5 (~25 min)
         └─> cleanup dan catatan awal (~5 min)
               └─> gameday-start: S = T0, sensor menyala, injector berjalan
                     └─> T1 = T0+30, game day selesai
                           └─> C = T1+2, collect
```

| Langkah | Perintah |
|---|---|
| Preflight | `scripts/demo/m2/p5.sh preflight` |
| Beban K3 dan K5 | `scripts/demo/m2/p5.sh k6 k3`, `scripts/demo/m2/p5.sh k6 k5` |
| Sampel K4 | `scripts/demo/m2/p5.sh sample 480` |
| Cleanup dan catatan awal | `CONFIRM=yes scripts/demo/m2/p5.sh cleanup-sensor-history`, `scripts/demo/m2/p5.sh offsets start`, `scripts/demo/m2/p5.sh clocks T0` |
| Mulai game day dengan injector otomatis | `CONFIRM=yes scripts/demo/m2/p5.sh gameday-start 30` |
| Mulai game day dengan injector manusia | `INJECT=hand CONFIRM=yes scripts/demo/m2/p5.sh gameday-start 30` |
| Gangguan manual | `CONFIRM=yes scripts/demo/m2/p5.sh pvmbg-outage on hang`, `CONFIRM=yes scripts/demo/m2/p5.sh fault-sensor on`, `/app run practice partition`, `/app run practice kill-leader`, atau mematikan VM |
| Akhir game day | `scripts/demo/m2/p5.sh clocks T1`, `scripts/demo/m2/p5.sh offsets end`, `scripts/demo/m2/p5.sh tester-logs` |
| Penilaian K1, K2, K8 | `scripts/demo/m2/p5.sh collect <T0> <T1>` |

Setiap putaran latihan memakai `PREFIX` baru yang diawali `practice`, misalnya `practice-r2-`, karena `inject-auto` menolak menimpa rencana lama. Putaran ujian memakai `PREFIX=exam-` dan overlay `vm-exam`.

### Bonus: node di host fisik berbeda

| Skenario | Perintah |
|---|---|
| Mesin dari beberapa laptop dalam satu tailnet | `tailscale status` |
| Putuskan jaringan nyata satu laptop | Matikan Wi-Fi laptop pemilik worker, pantau `kubectl get nodes` dan `bash scripts/p1/recovery-status.sh` |

## 8. Detail P1: Penyimpanan Terdistribusi

### Distribusi data CockroachDB

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

### Pembacaan lintas shard dan Audit API

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

### Node hilang, node baru, dan pemulihan

CockroachDB menyimpan 3 replika setiap range pada node yang berbeda. Bila satu node hilang, setiap range masih punya mayoritas sehingga baca dan tulis tetap berjalan. Setelah 75 detik node dianggap mati (`server.time_until_store_dead`) dan replika yang kurang dibuat ulang di node lain. Zona sistem (`meta`, `liveness`, `system`) juga memakai 3 replika agar status pemulihan bisa mencapai nol pada cluster 4 node.

| Skenario | Perintah |
|---|---|
| Lihat node, range kurang replika, dan range tidak tersedia | `bash scripts/p1/recovery-status.sh` |
| Tunggu sampai replikasi pulih penuh | `bash scripts/p1/recovery-status.sh --wait` |
| Simulasi node hilang beserta disknya (kind) | `bash scripts/p1/lose-node.sh <node>` |
| Node yang sama kembali dengan disk kosong | nyalakan lagi node itu lalu `bash scripts/p1/decommission-dead.sh` |
| Ganti node yang hilang permanen dengan node baru | `bash scripts/p1/replace-node.sh <node yang hilang>` |

`replace-node.sh` diperlukan karena volume local-path terikat pada node asalnya. Skrip menghapus objek node yang hilang, melepas PVC pod CockroachDB yang tinggal di sana sehingga pod pindah ke node kosong dengan disk baru, lalu men-decommission node CockroachDB yang mati. Pada VM, node dimatikan oleh Tester Agent atau dengan mematikan VM, lalu langkah pemulihannya sama.

### Dashboard analitik

Service `analytics-dashboard` menyajikan halaman analitik di port `8086` (NodePort `30086`). Halaman memuat line chart jumlah kejadian per jenis bahaya dengan pilihan rentang waktu, jumlah per severity, dan tabel per wilayah. Angka dihitung aggregator lintas 16 unit penyimpanan pada satu snapshot (`GET /internal/analytics`), sehingga sama dengan hitungan langsung dari Audit API `events`.

| Kategori | Aturan |
|---|---|
| Gempa | `SEISMIC` tanpa peringatan tsunami |
| Tsunami | `SEISMIC` yang memiliki `attributes.tsunami_warnings` |
| Gunung berapi | `VOLCANIC` |
| Kebakaran hutan | `WILDFIRE` |
| Pesisir | `COASTAL` |

Bila sebagian unit tidak terjangkau, halaman tetap tampil, setiap angka diberi tanda `*`, dan banner menyebut unit yang hilang. Cluster kind yang dibuat sebelum port `8086` ditambahkan dapat memakai `kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/analytics-dashboard 8086:8080`.

## 9. Observabilitas

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

## 10. Mencoba API

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
| Tim lapangan | `field-team` | `hazard:read:raw field-report:write` | Data lengkap, ditambah mengirim laporan lapangan (`POST /field-reports`, hanya klien ini). |
| BNPB | `bnpb-ops` | `hazard:read:raw hazard:read:unprocessed hazard:replay:unprocessed` | Data lengkap, ditambah pesan gagal dan *replay*-nya (hanya klien ini). |

Klaim `scope` berisi daftar *scope* dipisah spasi. Klien tanpa *scope* yang dibutuhkan sebuah route mendapat `403 FORBIDDEN`.

*Access token* berlaku 60 detik. Perbarui dengan `grant_type: refresh_token`. *Refresh token* lama otomatis tidak berlaku lagi setelah dipakai.

Dashboard dapat dibuka di <http://localhost:8083>.

## 11. Kafka

#### Menjalankan Kafka saja

```bash
docker compose up -d kafka kafka-init
docker compose ps -a kafka-init
```

`kafka-init` berjalan sekali untuk membuat *topic*, lalu berhenti. Status yang benar adalah **Exited (0)**.

#### Alamat broker

| Dari | Alamat |
|---|---|
| Container lain (service Go) | `kafka:9092` |
| Laptop (tool lokal) | `localhost:9094` |

Salah memakai alamat membuat koneksi menggantung.

#### Topic

| Topic | Partisi | Key | Retensi | Producer | Consumer group |
|---|---|---|---|---|---|
| `hazard-events` | 3 | `hazard_id` | 7 hari | aggregator | `dashboard-updater`, `notifier`, `audit-log` |
| `auth-revocations` | 1 | tanpa key (isi pesan = `jti`) | 10 menit | auth | `client-api-revocations` |

#### Smoke test

```bash
./scripts/kafka-smoke.sh
```

Skrip mengirim satu pesan ke *topic* `kafka-smoke` lalu membacanya lagi. Hasil yang benar adalah **PASS**. *Topic* ini terpisah dari *topic* utama.

#### Library Go

Semua service memakai **github.com/twmb/franz-go**.

#### Aturan konsumen

- Setiap konsumen memakai *group id* sendiri. *Group id* yang sama membuat pesan terbagi, bukan tersalin, sehingga *fan-out* gagal.
- Mulai membaca dari offset **earliest** agar konsumen baru membaca riwayat dan konsumen yang restart dapat menyusul.
- *Commit offset* **setelah** pesan selesai diproses (*at-least-once*).
- *Topic* tidak dibuat otomatis. *Topic* baru wajib ditambahkan ke `kafka-init`.

## 12. Pengembangan

### Anggaran memori (requests, overlay dev)

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

## 13. Struktur Repositori

```
api/                  Spesifikasi OpenAPI dan AsyncAPI (termasuk audit.yaml dan ops.yaml)
cmd/
  aggregator/         poller, leader election, mapper, penyimpanan, outbox relay, feed satelit, Audit API
  analytics-dashboard/ dashboard analitik
  auth/               penerbitan token dan JWKS
  client-api/         API klien eksternal
  field-team-receiver/ penerima peringatan Tim Lapangan
  bmkg-mock/          simulasi API BMKG
  pvmbg-mock/         simulasi API PVMBG
  dashboard-updater/  dashboard SSE
  notifier/           notifikasi SIAGA/AWAS
  audit-log/          pencatatan event
deploy/
  base/               manifest Kubernetes per service
  overlays/           dev, vm, vm-exam
  kind/               konfigurasi cluster kind
internal/             helper bersama (env, telemetry)
scripts/
  kind-up.sh          membuat cluster kind
  deploy.sh           deploy satu perintah
  deploy/             tahap deploy (secret, image, stage)
  vm/                 Vagrantfile, k3s, Tailscale, kubeconfig, tunnel
  p1/                 skrip uji P1
  demo/m2/            skrip uji P3 dan P5, runbook P5
  demo/               skrip demo Milestone 1 (Hurl)
  loadtest/           skenario k6 Milestone 1
tools/catalog-gen/    pembuat katalog gempa sintetis
docs/                 panduan cluster VM, bukti, hasil uji, tangkapan layar
secrets/              kunci JWT dan kunci Sensor Pantai (tidak di-commit)
docker-compose.yml
.env.example
```

## 14. Troubleshooting

| Gejala | Solusi |
|---|---|
| `The term ... is not recognized` | Terminal masih PowerShell. Jalankan perintah di Git Bash. |
| `sh: ...\r: not found` | Skrip ter-*checkout* dengan CRLF. Jalankan `git config core.autocrlf input`, lalu *clone* ulang. |
| `stat C:/Program Files/Git/...: no such file` | Jalankan `export MSYS_NO_PATHCONV=1` di Git Bash. |
| `port is already allocated` | Port 8080 sampai 8089 atau 9094 dipakai aplikasi lain. |
| `auth` atau `client-api` terus *restart* | Kunci di `secrets/` belum dibuat. Jalankan `sh scripts/gen-auth-keys.sh`. |
| Ada service yang mati setelah Docker *restart* | Jalankan `docker compose up -d`. |
| `dial tcp 127.0.0.1:...: connection refused` saat `kubectl` | `kubectl` masih menunjuk ke kind. Jalankan `export KUBECONFIG="$(cygpath -m ~/.kube/bnpb-vm.yaml)"`. |
| `dial tcp [::1]:8080` saat `deploy.sh` | `KUBECONFIG` berformat `~/...` tidak terbaca setelah `deploy.sh` mematikan konversi path. Pakai `export KUBECONFIG="$(cygpath -m ~/.kube/bnpb-vm.yaml)"`. |
| `vagrant up` diam lama tanpa keluaran | Vagrant menunggu pilihan adapter *bridged*. Set `BRIDGE="<nama adapter>"` sebelum `vagrant up`. |
| Node CockroachDB ditolak setelah VM dinyalakan ulang | Jam VM bergeser. Jalankan langkah sinkronisasi jam pada bagian 3.B.6. |
| Skrip P5 menolak berjalan di kind | Disengaja. P5 hanya diukur di cluster VM. |

## 15. Anggota Kelompok

Kelompok **pepeftw**

| No | Nama | NIM |
|---|---|---|
| 1 | Peter Wongsoredjo | 13523039 |
| 2 | Jessica Allen | 13523059 |
| 3 | David Bakti Lodianto | 13523083 |

## 16. Lampiran: Docker Compose

Docker compose tetap tersedia untuk pengembangan cepat dengan CockroachDB single-node. Compose tidak dipakai untuk pengujian Milestone 2.

| Aksi | Perintah |
|---|---|
| Menyalakan (membuat `.env` dan kunci bila belum ada) | `sh scripts/demo/setup.sh` |
| Mulai dari kondisi bersih | `sh scripts/demo/setup.sh --fresh` |
| Menyalakan juga `audit-log` | `docker compose --profile extra up -d --build` |
| Melihat log | `docker compose logs -f aggregator` |
| Menghentikan | `docker compose down` |
| Menghentikan dan menghapus data | `docker compose down -v` |

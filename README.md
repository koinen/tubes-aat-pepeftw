## Kafka

### 1. Jalanin Kafka

    docker compose up -d kafka kafka-init
    docker compose ps -a kafka-init

kafka-init jalan sekali untuk membuat topic, lalu berhenti. Status yang benar **Exited (0)**.

### 2. Alamat Broker

| Dari | Alamat |
|---|---|
| Container lain (service Go) | kafka:9092 |
| Laptop (tool lokal) | localhost:9094 |

Salah make alamat nanti nggantung.

### 3. Topic

| Topic | Partisi | Key | Retensi | Producer | Consumer group |
|---|---|---|---|---|---|
| hazard-events | 3 | hazard_id | 7 hari | aggregator | dashboard-updater, notifier, audit-log |
| auth-revocations | 1 | jti | 10 menit | auth | client-api-revocations |

### 4. Smoke Test

    ./scripts/kafka-smoke.sh

Script ngirim satu pesan ke topic kafka-smoke lalu dibaca lagi. Hasil yang benar adalah **PASS**. Topic ini kepisah.

### 5. Library Go

Semua service memakai **github.com/twmb/franz-go**. Jangan dicampur yang lain.

### 6. Aturan Consumer

- Setiap consumer memakai group id sendiri. Group id yang sama membuat pesan terbagi, bukan tersalin, sehingga fan-out gagal.
- Mulai membaca dari offset **earliest** agar consumer baru membaca riwayat dan consumer yang restart dapat menyusul.
- Commit offset **setelah** pesan selesai diproses (at-least-once).
- Topic tidak dibuat otomatis. Topic baru wajib ditambahkan ke kafka-init.

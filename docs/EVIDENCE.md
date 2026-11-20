# Bukti Milestone 2

Indeks seluruh tangkapan layar di `docs/screenshot` dan hasil teks di `docs/results`, satu kalimat per berkas. Kolom Butir merujuk ke checker M2: K1 sampai K14 untuk platform, dan K1 sampai K7 per problem.

Konvensi nama berkas: `<masalah>-<butir>-<isi>.png`. Nama pemilik hanya dipakai bila gambar menunjukkan laptop orang tertentu.

## Topologi saat pengambilan bukti

| Node | Laptop | Peran |
|---|---|---|
| node-a | Peter | control plane dan worker |
| node-b, node-d | Allen | worker |
| node-c | David | worker, dimatikan Tester Agent pada uji P1 K4 lalu dihapus |
| node-e | David | worker pengganti node-c pada uji P1 K5 |

Seluruh VM adalah VirtualBox dengan kernel terpisah, dibuat dengan Vagrant dan dihubungkan lewat Tailscale.

## Catatan penting saat membaca bukti

- Baris `000` dengan waktu 10 detik pada log probe adalah batas waktu curl 10 detik karena batas waktu baca per shard dinaikkan ke 10 detik, bukan error HTTP.
- Dataset P1 pada cluster VM adalah 5.000 baris katalog sintetis karena throughput tulis dibatasi latensi antar-node sekitar 54 ms, hasil 20.000 baris ada dari uji pada cluster kind.
- Konsumer satelit dimatikan selama uji P1 dan P2 karena mengulang jutaan event dan membebani store, sehingga kategori kebakaran hutan bernilai 0 pada dashboard.
- Rentang bawaan 30 hari pada dashboard tidak mencakup katalog historis yang dimulai tahun 2000, gunakan rentang Semua untuk membandingkan dengan Audit API.
- Pada P2 K4 pod leader lama dapat diganti Kubernetes setelah 20 detik tidak terjangkau, jadi bukti utamanya adalah leader_lost dan naiknya epoch fencing.
- Jam VM harus disinkronkan dengan NTP, CockroachDB menolak node yang jamnya bergeser.
- Migrasi 009 diperbaiki agar berjalan di CockroachDB (commit b039a49).

## Platform Kubernetes

| Gambar | Butir | Penjelasan |
|---|---|---|
| `k2-k3-kubectl-get-nodes.png` | K2, K3 | Hasil kubectl get nodes -o wide: empat node k3s dengan kernel dan OS terpisah, node-a sebagai control plane. |
| `k2-hypervisor-peter-node-a.png` | K2 | VirtualBox di laptop Peter menampilkan VM node-a yang berjalan. |
| `k2-hypervisor-allen-node-b-node-d.png` | K2 | VirtualBox di laptop Allen menampilkan VM node-b dan node-d yang berjalan. |
| `k2-hypervisor-david-node-c-node-e.png` | K2 | VirtualBox di laptop David menampilkan VM node-c dan node-e. |
| `k4-deploy-finished.png` | K4 | Deploy dengan satu perintah selesai sampai baris deployed tag. |
| `k9-crdb-admin-nodes.png` | K9, P1 | Admin UI CockroachDB menampilkan node-node hidup, satu per VM. |
| `k10-tester-agent-pods.png` | K10 | Tester Agent berjalan sebagai satu pod di setiap node. |
| `bonus-tailscale-status.png` | Bonus | tailscale status menampilkan mesin dari beberapa laptop pada satu jaringan Tailscale. |

## Observabilitas

| Gambar | Butir | Penjelasan |
|---|---|---|
| `obs-grafana-layanan.png` | Observabilitas | Dashboard Grafana Layanan BNPB: service aktif, throughput, error rate, latensi p95 per service dan per route, dan metrik Kafka. |
| `obs-grafana-satelit.png` | Observabilitas, P4 | Dashboard Grafana Feed Satelit dengan panel lag dan offset, sebagian panel kosong karena konsumer satelit sedang dimatikan. |
| `obs-prometheus-targets.png` | Observabilitas | Prometheus menampilkan 12 dari 12 target berstatus UP lengkap dengan label node. |
| `obs-jaeger-ingest-trace.png` | Observabilitas | Trace Jaeger satu siklus poll PVMBG lintas aggregator, pvmbg-mock, notifier, dan dashboard-updater beserta durasi tiap langkah. |
| `obs-jaeger-read-trace.png` | Observabilitas | Trace Jaeger GET /hazards lintas client-api dan aggregator selama 793 ms dengan tiga span. |

## P1 Penyimpanan terdistribusi

| Gambar | Butir | Penjelasan |
|---|---|---|
| `p1-crdb-admin-databases.png` | P1 | Admin UI CockroachDB halaman Databases untuk database aggregator. |
| `p1-k1-distribution.png` | P1 K1 | Sebaran replika dan byte hazard_events per node setelah katalog dimuat, tidak ada node yang memegang seluruh data. |
| `p1-k2-node-load.png` | P1 K2 | Beban tulis per detik di setiap node saat dan setelah pemuatan katalog. |
| `p1-crdb0-oomkilled.png` | P1 | crdb-0 terkena OOMKilled pada batas memori 1 Gi saat burst tulis 32 worker sementara tiga replika lain tetap berjalan. |
| `p1-k3-pagination.png` | P1 K3 | Dua halaman /hazards lewat client-api dengan cursor, keduanya berstatus 200. |
| `p1-k4-checksum-before.png` | P1 K4 | Checksum dasar BMKG dan PVMBG dari Audit API sebelum node dimatikan, partial false. |
| `p1-k4-node-c-notready.png` | P1 K4 | node-c berstatus NotReady setelah Tester Agent menjalankan wipe-node. |
| `p1-k4-probe-during-node-loss.png` | P1 K4 | Probe baca lintas shard tetap menjawab selama node hilang, baris 000 adalah batas waktu klien 10 detik bukan error HTTP. |
| `p1-k4-recovery.png` | P1 K4, K6 | Cluster pulih penuh dalam 236 detik dengan 0 range kurang replika dan 0 range tidak tersedia, node-c tetap mati. |
| `p1-k4-checksum-compare.png` | P1 K4 | Perbandingan checksum sebelum dan sesudah kehilangan node, PVMBG identik dan BMKG bertambah karena ingest terus berjalan. |
| `p1-k5-decommission-recovery.png` | P1 K5 | Node CockroachDB yang mati di-decommission lalu pemulihan dipantau, angka 73 dan 73 adalah potret sesaat sebelum rebalance selesai. |
| `p1-k5-recovery-clean.png` | P1 K5, K6 | Empat node aktif setelah node-e bergabung dengan 0 range kurang replika dan 0 range tidak tersedia. |
| `p1-k5-distribution.png` | P1 K5 | node-e yang baru bergabung dengan disk kosong sudah memegang 31 replika seperti node lain. |
| `p1-k7-dashboard-all-units.png` | P1 K7 | Dashboard analitik saat normal dengan keterangan seluruh 16 unit penyimpanan terjangkau. |
| `p1-k7-dashboard-aggregator-unreachable.png` | P1 K7 | Dashboard menandai bahwa aggregator tidak terjangkau dan angka yang tampil adalah hasil terakhir yang bisa sudah usang. |
| `p1-k7-dashboard-full-page.png` | P1 K7 | Halaman dashboard lengkap dengan grafik garis per jenis bahaya, sebaran severity, dan jumlah per wilayah. |
| `p1-k7-dashboard-semua.png` | P1 K7 | Dashboard pada rentang Semua dengan total 14.484 kejadian sepanjang waktu. |

## P2 Dua nakhoda satu kapal

| Gambar | Butir | Penjelasan |
|---|---|---|
| `p2-k1-upstream-requests.png` | P2 K1 | Mock BMKG dan PVMBG hanya menerima satu atau dua permintaan data walau ada tiga aggregator, jadi hanya leader yang menarik data. |
| `p2-k1-no-duplicates.png` | P2 K1 | Jumlah baris hazard_events 14.008 sama dengan jumlah referensi berbeda 14.008 sehingga tidak ada HazardEvent ganda. |
| `p2-k2-leader-before.png` | P2 K2 | Pemegang Lease aggregator-leader dan tiga pod aggregator sebelum failover. |
| `p2-k2-failover-3s.png` | P2 K2 | Pod leader dihapus dan leader baru terpilih dalam 3 detik, jauh di bawah batas 60 detik. |
| `p2-k2-after-failover.png` | P2 K2 | Kondisi sesudah failover dengan leader baru dan pod pengganti yang sedang dibuat. |
| `p2-k3-clock-skew.png` | P2 K3 | Tester Agent menggeser jam node 300 detik selama 30 detik lalu memulihkannya, satu leader tetap dan CockroachDB menolak timestamp dari node yang jamnya terlalu maju. |
| `p2-k4-freeze-allen-node-d-paused.png` | P2 K4 | VM node-d dijeda di VirtualBox laptop Allen untuk membekukan leader. |
| `p2-k4-freeze-david-node-e-paused.png` | P2 K4 | VM node-e dijeda di VirtualBox laptop David untuk membekukan leader. |
| `p2-k4-takeover-epoch5.png` | P2 K4 | Leader baru memperoleh lease dengan epoch fencing 5 dan dua instance lain mengakuinya sebagai leader. |
| `p2-k4-takeover-epoch6.png` | P2 K4 | Leader baru memperoleh lease dengan epoch fencing 6 setelah pembekuan berikutnya. |
| `p2-k4-leader-lost-epoch8.png` | P2 K4 | Leader lama gagal memperbarui lease lalu mencatat leader_lost sementara leader baru memakai epoch 8. |

## Bonus node pada host fisik berbeda

| Gambar | Butir | Penjelasan |
|---|---|---|
| `bonus-wifi-cut-david-node-e.png` | Bonus | Wi-Fi laptop David dimatikan sementara VM node-e tetap menyala. |
| `bonus-wifi-cut-probe-and-nodes.png` | Bonus | node-e berstatus NotReady sementara probe baca tetap menjawab selama pemutusan jaringan nyata. |
| `bonus-wifi-back-recovery.png` | Bonus | Setelah Wi-Fi kembali empat node aktif lagi dengan 0 range bermasalah. |

## Berkas di docs/results

| Berkas | Isi |
|---|---|
| `00-host-peter.txt` | CPU, RAM, dan OS laptop Peter untuk deklarasi topologi. |
| `01-nodes.txt` | kubectl get nodes -o wide awal, empat node Ready. |
| `09-ping-a-b.txt` | Ping 20 kali node-a ke node-b pada jaringan nyata untuk latensi antar-node. |
| `09-ping-a-c.txt` | Ping node-a ke node-c pada jaringan nyata. |
| `09-ping-hotspot.txt` | Ping node-a ke node-b lewat hotspot ponsel. |
| `k2-node-a-uname.txt` | Hostname, kernel, CPU, dan RAM node-a. |
| `k2-nodes-final.txt` | Daftar node awal sebelum node-c diganti. |
| `k3-taints.txt` | Taint kosong di semua node. |
| `p1-crdb0-oomkilled.txt` | Status pod crdb dan alasan terminasi OOMKilled exit 137 pada crdb-0. |
| `p1-k1-distribution.txt` | Sebaran replika, leaseholder, dan byte per node setelah katalog dimuat. |
| `p1-k1-load.txt` | Log pemuatan katalog 5.000 baris dengan 0 gagal. |
| `p1-k2-load.txt` | Sampel beban tulis per node. |
| `p1-k2-spike.txt` | Sampel beban saat lonjakan satu wilayah, dihentikan setelah beberapa sampel. |
| `p1-k3-audit-events.txt` | Halaman pertama Audit API events dengan next_cursor dan partial false. |
| `p1-k3-pagination.txt` | Dua halaman /hazards lewat client-api dengan status dan waktu. |
| `p1-k4-checksum-before.txt` | Checksum dan jumlah BMKG dan PVMBG sebelum node dimatikan. |
| `p1-k4-checksum-after.txt` | Checksum dan jumlah BMKG dan PVMBG sesudah pemulihan. |
| `p1-k4-probe.txt` | Log probe baca ke Audit API events selama uji kehilangan node. |
| `p1-k4-recovery.txt` | Pemantauan pemulihan sampai fully replicated setelah 236 detik. |
| `p1-k4-wipe-node-run.txt` | Keluaran Tester Agent untuk skenario wipe-node beserta ID permintaannya. |
| `p1-k5-decommission.txt` | Decommission node CockroachDB yang mati. |
| `p1-k5-distribution.txt` | Sebaran data setelah node-e bergabung. |
| `p1-k5-recovery.txt` | Pemantauan pemulihan setelah node pengganti bergabung. |
| `p1-k5-replace-node.txt` | Log replace-node.sh yang melepas disk lama, termasuk timeout karena node baru belum terdaftar. |
| `p1-k7-audit-bmkg.txt` | Jumlah dan checksum BMKG dari Audit API. |
| `p1-k7-audit-pvmbg.txt` | Jumlah dan checksum PVMBG dari Audit API. |
| `p1-k7-dashboard-api.txt` | Respons /api/analytics untuk rentang 2020 sampai 2027. |
| `p1-k7-match.txt` | Perbandingan dashboard rentang penuh dengan Audit API pada waktu yang sama. |
| `p1-k7-recheck.txt` | Pengecekan ulang dashboard terhadap SQL yang menjelaskan selisih akibat rentang 30 hari. |
| `p2-k1-upstream-requests.txt` | Jumlah permintaan ke mock BMKG dan PVMBG dalam 15 menit. |
| `p2-k2-failover.txt` | Waktu penghapusan pod leader dan nama leader baru. |
| `p2-k3-agent-logs.txt` | Log Tester Agent untuk skenario clock-skew. |
| `p2-k3-clock-skew.txt` | Keluaran perintah Tester Agent clock-skew. |
| `p2-k3-no-duplicates.txt` | Hitungan baris dan referensi berbeda pada hazard_events. |
| `p2-k4-freeze.txt` | Log aggregator dengan leader_acquired, leader_lost, dan epoch. |
| `p2-leader.txt` | Nama pod leader dari Lease. |
| `p2-wifi-back-recovery.txt` | Pemulihan setelah Wi-Fi kembali. |
| `p2-wifi-cut-nodes.txt` | kubectl get nodes -w selama pemutusan Wi-Fi dengan node-e NotReady. |
| `p2-wifi-cut-probe.txt` | Log probe baca selama pemutusan Wi-Fi. |

## Berkas di docs/results/k8s

| Berkas | Isi |
|---|---|
| `k1-get-all.txt` | Seluruh objek di namespace sistem-informasi-kebencanaan-bnpb. |
| `k2-nodes.txt` | Daftar node dengan kernel, OS, dan IP internal. |
| `k2-nodes-final-with-node-e.txt` | Daftar node akhir setelah node-e menggantikan node-c. |
| `k2-node-a-uname.txt` | Hostname, kernel, CPU, RAM node-a. |
| `k2-node-b-uname.txt` | Hostname, kernel, CPU, RAM node-b. |
| `k2-node-c-uname.txt` | Hostname, kernel, CPU, RAM node-c. |
| `k2-node-d-uname.txt` | Hostname, kernel, CPU, RAM node-d. |
| `k3-roles.txt` | Kolom ROLES dengan node-a sebagai control plane. |
| `k3-taints.txt` | Taint kosong sehingga node-a juga menjalankan beban kerja. |
| `k4-deploy.log` | Log deploy sampai deployed tag dan health check. |
| `k5-resources.txt` | Memory requests dan limits tiap pod yang sesuai Lampiran B. |
| `k6-secrets-configmaps.txt` | Nama Secret dan ConfigMap tanpa nilainya. |
| `k7-allow.txt` | Pod pemilik berhasil menjangkau crdb. |
| `k7-deny.txt` | Pod non-pemilik diblokir ke crdb, auth-store, dan client-api-store. |
| `k7-netpol.txt` | Daftar dan deskripsi NetworkPolicy. |
| `k8-roles.txt` | Pod beserta label role. |
| `k8-no-unlabeled.txt` | Tidak ada pod tanpa label role. |
| `k9-pv.txt` | Daftar PV dan PVC. |
| `k9-pv-affinity.txt` | PV terikat ke satu node lewat node affinity. |
| `k9-node-a-disk.txt` | Isi disk lokal node-a. |
| `k9-node-b-d-disk.txt` | Isi disk lokal node-b dan node-d. |
| `k9-node-c-disk.txt` | Isi disk lokal node-c. |
| `k10-apply.txt` | Hasil apply manifest Tester Agent. |
| `k10-ds.txt` | DaemonSet Tester Agent dengan satu pod per node. |
| `k10-digest.txt` | Digest image Tester Agent sama di semua node dengan ketentuan spesifikasi. |
| `k11-no-netpol.txt` | Tidak ada NetworkPolicy di namespace tester-agent. |
| `k11-psa.txt` | Label namespace tester-agent tanpa pembatasan Pod Security. |
| `k11-list.txt` | Daftar skenario latihan Tester Agent. |
| `k11-practice.log` | Log aksi Tester Agent untuk skenario wipe-node dan clock-skew. |
| `k13-placement.txt` | Penempatan pod per node pada deploy awal. |
| `k13-placement-final.txt` | Penempatan pod per node setelah node-e menggantikan node-c. |

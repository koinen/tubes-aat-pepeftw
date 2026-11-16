# Cluster VM untuk Demo Milestone 2

Panduan ini memindahkan sistem dari kind (pengembangan) ke cluster k3s di atas VM dengan kernel terpisah, sesuai ketentuan demo. Seluruh skrip demo dan pengujian tetap dipakai tanpa perubahan.

## 1. Topologi

| Node | Peran | RAM | vCPU | Disk |
|---|---|---|---|---|
| `node-a` | control plane sekaligus worker | 3 GB | 2 | 20 GB |
| `node-b` | worker | 2,5 GB | 2 | 20 GB |
| `node-c` | worker | 2,5 GB | 2 | 20 GB |
| `node-d` | worker | 2,5 GB | 2 | 20 GB |

Empat node diperlukan agar setiap node CockroachDB memegang sebagian data saja dan cluster tetap bertahan saat satu node hilang. Skenario mematikan node hanya dikenakan pada `node-b` sampai `node-d`.

VM boleh tersebar di beberapa laptop selama berada pada jaringan yang sama. Cara ini meringankan RAM tiap laptop dan memenuhi bonus node pada host fisik berbeda.

## 2. Persiapan laptop

1. Matikan Docker Desktop dan cluster kind selama demo VM berjalan agar RAM cukup.
2. Pasang VirtualBox 7. VirtualBox berjalan di atas Windows Hypervisor Platform bersama WSL2, sehingga sedikit lebih lambat.
3. Simpan disk VM di drive D, bukan C.
4. Gunakan ISO `debian-13.1.0-amd64-netinst.iso`.

## 3. Membuat VM

Untuk setiap node pada tabel di atas:

1. VirtualBox, New, nama `node-a` (dan seterusnya), tipe Linux Debian 64-bit, RAM dan vCPU sesuai tabel, disk dinamis 20 GB di drive D.
2. Settings, Network, Adapter 1: **Bridged Adapter** ke kartu Wi-Fi atau LAN yang dipakai bersama. Semua VM harus mendapat IP dari router yang sama.
3. Pasang Debian:
   - hostname sama dengan nama node (`node-a`, `node-b`, dan seterusnya), karena nama ini muncul di Audit API `location`.
   - buat user `bnpb`.
   - pada pemilihan software, pilih hanya **SSH server** dan **standard system utilities**, tanpa desktop.
4. Setelah boot, catat IP dengan `ip -4 addr` lalu pastikan laptop bisa `ssh bnpb@<ip>`.
5. Salin skrip VM ke setiap node dari folder repo:

```bash
scp scripts/vm/k3s-*.sh bnpb@<ip-node>:~
```

## 4. Memasang k3s

Di `node-a`:

```bash
sudo bash k3s-server.sh
```

Skrip mematikan swap, memasang k3s versi `v1.37.2+k3s1`, lalu mencetak perintah join lengkap dengan token.

Di `node-b`, `node-c`, dan `node-d`, jalankan perintah join yang dicetak tadi:

```bash
sudo bash k3s-agent.sh <ip-node-a> <token>
```

## 5. Deploy dari laptop

1. Ambil kubeconfig dan pakai pada terminal ini:

```bash
bash scripts/vm/fetch-kubeconfig.sh <ip-node-a>
```

```bash
export KUBECONFIG=~/.kube/bnpb-vm.yaml
```

2. Login ke GitHub Container Registry dengan token yang punya izin `write:packages`:

```bash
docker login ghcr.io
```

3. Bila package GHCR bersifat privat, isi `GHCR_USER` dan `GHCR_TOKEN` (izin `read:packages`) di `.env`. Bila package dibuat publik, biarkan kosong.

4. Deploy:

```bash
bash scripts/deploy.sh --overlay vm --registry ghcr.io/koinen/
```

5. Buka tunnel di terminal lain agar `localhost:8080` sampai `8085` mengarah ke cluster VM, lalu cek kesehatan:

```bash
bash scripts/vm/tunnel.sh <ip-node-a>
```

```bash
hurl --test scripts/demo/00-health.hurl
```

6. Pasang Tester Agent dengan perintah dari spesifikasi:

```bash
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
```

## 6. Uji ulang di cluster VM

| Uji | Perintah |
|---|---|
| Kesehatan dan regresi M1 | `hurl --test scripts/demo/00-health.hurl`, `sh scripts/demo/run-p1.sh`, `sh scripts/demo/run-p3.sh` |
| Muat katalog dan sebaran data | `bash scripts/p1/load-catalog.sh <katalog.csv>`, `bash scripts/p1/distribution.sh` |
| Lonjakan satu wilayah | `bash scripts/p1/spike.sh 5000 "Selat Sunda"` |
| Audit API | `bash scripts/p1/audit.sh checksum BMKG` |
| Status replikasi | `bash scripts/p1/recovery-status.sh` |
| Latihan Tester Agent | `kubectl exec -n tester-agent daemonset/tester-agent -- /app list` |

## 7. Node hilang dan node baru

| Langkah | Di VM |
|---|---|
| Node hilang beserta datanya | Tester Agent menjalankan skenario `wipe-node`, atau matikan VM worker lewat VirtualBox (Close, Power Off) |
| Pantau pemulihan | `bash scripts/p1/recovery-status.sh --wait` |
| Node yang sama kembali dengan disk kosong | nyalakan VM, jalankan `sudo systemctl stop k3s-agent`, `sudo rm -rf /var/lib/rancher/k3s/storage/*`, `sudo systemctl start k3s-agent`, lalu `bash scripts/p1/decommission-dead.sh` |
| Node baru | buat VM baru seperti langkah 3, join dengan `k3s-agent.sh`, lalu `bash scripts/p1/replace-node.sh <node yang hilang>` |

`scripts/p1/lose-node.sh` hanya untuk kind karena memakai `docker exec`.

## 8. Tiga broker Kafka

Overlay `vm` memakai satu broker agar muat di satu laptop. Bila VM tersebar di beberapa laptop, ubah di `deploy/overlays/vm/kustomization.yaml`:

1. `kafka` count menjadi `3`.
2. hapus baris `- path: kafka-config.yaml` sehingga replikasi topic kembali ke nilai base (RF 3, min.insync 2).

## 9. Yang belum teruji sebelum pertemuan

Skrip `k3s-server.sh`, `k3s-agent.sh`, `fetch-kubeconfig.sh`, dan `tunnel.sh` baru diperiksa sintaksnya, karena membutuhkan VM sungguhan. Push ke GHCR membutuhkan login tim. Manifest overlay `vm` sudah dirender dan divalidasi terhadap Kubernetes.

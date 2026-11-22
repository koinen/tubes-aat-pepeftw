# Cluster VM untuk Demo Milestone 2

Panduan ini memindahkan sistem dari kind (pengembangan) ke cluster k3s di atas VM dengan kernel terpisah, sesuai ketentuan demo. Seluruh skrip demo dan pengujian tetap dipakai tanpa perubahan.

## 1. Topologi

| Node | Peran | RAM | vCPU | Disk |
|---|---|---|---|---|
| `node-a` | control plane sekaligus worker | 3 GB | 2 | 20 GB |
| `node-b` | worker | 2,5 GB | 2 | 20 GB |
| `node-e` | worker | 2,5 GB | 2 | 20 GB |
| `node-d` | worker | 2,5 GB | 2 | 20 GB |

Empat node diperlukan agar setiap node CockroachDB memegang sebagian data saja dan cluster tetap bertahan saat satu node hilang. Skenario mematikan node hanya dikenakan pada worker (`node-b`, `node-d`, dan `node-e`). Pada pengujian P1, `node-c` dipakai lalu dihapus dan digantikan `node-e`.

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

Di `node-b`, `node-d`, dan `node-e`, jalankan perintah join yang dicetak tadi:

```bash
sudo bash k3s-agent.sh <ip-node-a> <token>
```

## 5. Deploy dari laptop

1. Ambil kubeconfig dan pakai pada terminal ini:

```bash
bash scripts/vm/fetch-kubeconfig.sh <ip-node-a>
```

```bash
export KUBECONFIG="$(cygpath -m ~/.kube/bnpb-vm.yaml)"
```

2. Login ke GitHub Container Registry dengan token yang punya izin `write:packages`:

```bash
docker login ghcr.io
```

3. Bila package GHCR bersifat privat, isi `GHCR_USER` dan `GHCR_TOKEN` (izin `read:packages`) di `.env`. Bila package dibuat publik, biarkan kosong.

4. Deploy:

```bash
bash scripts/deploy.sh --overlay vm --registry ghcr.io/peterwongsoredjo/
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

Overlay `vm` memakai tiga broker dengan replikasi topic dari base (RF 3, min.insync 2), karena VM tersebar di beberapa laptop. Bila seluruh VM dijalankan di satu laptop, ubah di `deploy/overlays/vm/kustomization.yaml`:

1. `kafka` count menjadi `1`.
2. tambahkan `- path: kafka-config.yaml` di bawah `patches:` sehingga replikasi topic turun ke 1.

## 9. Node di jaringan berbeda lewat Tailscale

Bila laptop tidak berada pada Wi-Fi yang sama, setiap VM bergabung ke satu tailnet Tailscale dan k3s memakai IP tailnet (`100.x.y.z`) pada interface `tailscale0`, bukan IP bridged.

Ada dua skrip:

- `tailnet-join.sh` dijalankan di laptop dari Git Bash. Skrip ini mengunggah `tailnet-node.sh` dan `k3s-*.sh` ke `/tmp` di VM dengan `vagrant upload`, lalu menjalankan `tailnet-node.sh` sebagai root lewat `vagrant ssh`.
- `tailnet-node.sh` berjalan di dalam VM. Skrip ini memasang Tailscale, masuk tailnet dengan auth key memakai hostname VM, menunggu IP tailnet, lalu sesuai peran: `vpn` berhenti di situ, `server` memasang k3s server di IP tailnet, `agent` memastikan API `node-a` di port 6443 terjangkau lalu join.

1. Pemilik tailnet membuat auth key di admin console Tailscale, Settings, Keys: **reusable**, bukan ephemeral. Bila Device approval aktif, centang juga **pre-approved**. Bagikan secara privat seperti token k3s. Setelah semua VM bergabung, matikan key expiry untuk VM di halaman Machines.
2. Di laptop pemilik `node-a`, nyalakan VM **tanpa** `K3S_TOKEN` di shell:

```bash
vagrant up node-a
```

Provisioner membuat user `bnpb` beserta kunci SSH, lalu berhenti dengan pesan `set K3S_TOKEN before vagrant up node-a`. Pesan ini memang diharapkan, karena k3s tidak boleh terpasang di IP bridged. Jangan pakai `--no-provision`, karena user `bnpb` dibutuhkan oleh `fetch-kubeconfig.sh` dan `tunnel.sh`. Lalu, dari folder repo:

```bash
TS_AUTHKEY=<key> bash scripts/vm/tailnet-join.sh node-a server
```

Skrip mencetak IP tailnet `node-a` (`node-a tailnet ip 100.x.y.z`). IP yang sama terlihat di halaman Machines pada admin console. Token join ada di `sudo cat /var/lib/rancher/k3s/server/node-token` pada `node-a`.

3. Di setiap worker:

```bash
TS_AUTHKEY=<key> K3S_TOKEN=<token> bash scripts/vm/tailnet-join.sh node-b agent <ip-tailnet-node-a>
```

4. Node cadangan untuk uji node baru cukup masuk tailnet dulu, lalu join dengan perintah langkah 3 saat diminta:

```bash
TS_AUTHKEY=<key> bash scripts/vm/tailnet-join.sh node-e vpn
```

5. Laptop yang menjalankan `kubectl` memasang Tailscale dan masuk ke tailnet yang sama. `fetch-kubeconfig.sh <ip-tailnet-node-a>` hanya berhasil dari laptop pemilik `node-a`, karena hanya kunci SSH-nya yang ada di user `bnpb`. Laptop lain memakai salinan `~/.kube/bnpb-vm.yaml` darinya, yang sudah menunjuk ke IP tailnet. `tunnel.sh` opsional di tailnet: NodePort bisa dibuka langsung di `http://<ip-tailnet-node-a>:30080` sampai `30089`.

Catatan:

- VM yang dibuat manual memakai `scripts/vm/tailnet-node.sh` langsung: salin bersama `k3s-*.sh` dengan `scp`, lalu jalankan `sudo TS_AUTHKEY=<key> bash tailnet-node.sh <vpn|server|agent> [ip-tailnet-node-a]`.
- `tailnet-join.sh` menyetel `MSYS_NO_PATHCONV=1` saat `vagrant upload`, karena Git Bash mengubah argumen `/tmp/...` menjadi path Windows sebelum diteruskan ke `vagrant.exe`.
- Jangan join lewat `vagrant provision`, karena `vagrant-provision.sh` memilih IP non-NAT pertama yang bisa saja IP bridged. Semua node wajib memakai IP tailnet.
- Cek jalur antar-node dengan `tailscale ping node-a` di dalam VM. Hasil `via DERP` berarti lalu lintas lewat relay sehingga latensi naik.
- Skenario Tester Agent yang menghambat jaringan antar-node perlu dicoba sekali di atas tailnet sebelum demo.

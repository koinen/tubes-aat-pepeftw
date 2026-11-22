# P2 and P4 demo runbook 

Shows P2 (three Aggregators, one leader, fencing) and P4 (satellite feed to WILDFIRE) on the VM cluster. All commands run from the repo root. Cluster-changing commands need `CONFIRM=yes` and write a log to `evidence/m2-p2/` or `evidence/m2-p4/`.

```bash
export KUBECONFIG=~/.kube/bnpb-vm.yaml        
```

## P2

| Step | Command | What to point at (criterion) |
|---|---|---|
| 1 | `scripts/demo/m2/p2.sh preflight` | Three aggregator pods on three different nodes, `readinessProbe`, endpoints of the Service (1) |
| 2 | `scripts/demo/m2/p2.sh leader` and `workers` | One leader; only its log has `poller_started` (2) |
| 3 | `scripts/demo/m2/p2.sh relay 10m` | `kafka_published` only on the leader pod (2) |
| 4 | `scripts/demo/m2/p2.sh duplicates` | rows = distinct_refs for every source (2) |
| 5 | `CONFIRM=yes scripts/demo/m2/p2.sh kill-leader` | Failover time <= 60 s, new leader, lease epoch +1, no duplicates (3) |
| 6 | `CONFIRM=yes scripts/demo/m2/p2.sh kill-leader --hard` | Same without releasing the lease (about 17 s expected) (3) |
| 7 | `kubectl exec -n tester-agent daemonset/tester-agent -- /app list`, then `CONFIRM=yes scripts/demo/m2/p2.sh clock-skew <scenario>` | Leader and epoch unchanged, rows keep growing, 0 duplicates (4) |
| 8 | `scripts/demo/m2/p2.sh fence-log 10m` | `stale leader epoch` rejections after a freeze or epoch bump (5) |
| 9 | k6 (see below) | Load test, scale 3 -> 5 -> 2, version change (A5) |

Load test: `scripts/loadtest/k6-p2.js` (`TARGET_URL`, `INTERNAL_KEY`, `VUS`, `DURATION`). Run it as a Job outside the component pods and scale the Deployment during the run: `kubectl -n $NS scale deploy/aggregator --replicas=5`, then `--replicas=2`. Change the version with `kubectl -n $NS set env deploy/aggregator APP_VERSION=<new value>`.

Node freeze (criterion 5) is done on the VM that hosts the leader (pause it in VirtualBox, wait for a new leader, resume it), with `leader` and `fence-log` before and after.

## P4

| Step | Command | What to point at (criterion) |
|---|---|---|
| 1 | `scripts/demo/m2/p4.sh status` | Offset, detections, WILDFIRE count, 0 duplicates (1, 3) |
| 2 | `scripts/demo/m2/p4.sh watch 120` | The feed being processed live for a few minutes (7) |
| 3 | `scripts/demo/m2/p4.sh metrics` | Lag, `bnpb_satellite_late_detections`, latency histogram (2, 4) |
| 4 | `CONFIRM=yes scripts/demo/m2/p4.sh crash` | Resume <= 60 s, offset advances, gaps 0, duplicates 0, fires do not drop (5, 8) |
| 5 | `scripts/demo/m2/p4.sh gaps 10m` | `satellite_offset_gap` = 0 on every pod (8) |

`crash` deletes the leader pod, so another pod takes over the single feed connection (the feed answers 429 to a second connection). The 60x surge (criterion 4) and the comparison with the reference result (criterion 6) belong to the exam session (`/v1/exam/start`, exam token from the TAs).

## Notes

- The scripts only read or delete aggregator pods. They never touch the feed token or other secrets.
- `p4.sh metrics` reads `/metrics` through the API server proxy. If the aggregator exposes metrics on another port or path, set `METRICS_PORT` / `METRICS_PATH`.
- Not run against the VM cluster yet when written; run `p2.sh preflight` and `p4.sh status` first (read-only) before the cluster-changing commands.
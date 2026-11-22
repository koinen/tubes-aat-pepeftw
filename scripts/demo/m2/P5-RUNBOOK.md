# P5 game day runbook

Measures P5 K1–K8 on the VM cluster. All commands run from the repo root. Every cluster-changing command needs `CONFIRM=yes` and writes a UTC line to `docs/results/p5/<prefix>timeline.txt`.

**Roles.** The *operator* runs measurement and watches dashboards. The *injector* runs faults during the game-day window and keeps their choice and timing hidden. By default the injector is `p5.sh inject-auto`, started by `gameday-start`. It draws a random plan, logs only the plan's sha256 at T0, and reveals the plan at the end. A teammate may inject by hand instead (section 3). Between T0 and T1 nobody touches the system except through the injector's faults.

**Labels.** Practice-profile runs use a `PREFIX` that starts with `practice` (default `practice-`). Use a new prefix for each run (e.g. `practice-r2-`): `inject-auto` refuses to overwrite a plan. For the exam window, use `PREFIX=exam-` and overlay `vm-exam`.

**Timeline.** Warm-up (K3–K5) runs first, with the sensor as deployed. Then cleanup. Then `gameday-start`, which starts the sensor and marks T0 at the same moment (S = T0). The practice profile has criticals at sim t = 1800, 5400 and 9000 s. `gameday-start` sets `PANTAI_SPEED=6`, so they land at **T0+5, T0+15 and T0+25 min**. At the manifest's speed of 60 they would land 30, 90 and 150 *seconds* after S. T1 = T0+30, C = T1+2.

## 0. Before the clock starts (~10 min)

```bash
export KUBECONFIG="$(cygpath -m ~/.kube/bnpb-vm.yaml)"   # copy from Peter's laptop
export NODE=<tailnet IP of node-a>            # NodePorts 30080/30084/30085/30082; default = kubeconfig host
export PREFIX=practice-r2-
scripts/demo/m2/p5.sh preflight               # context, nodes, pods, digests, sensor profile, iptables, endpoints
```

Check before going on:
- every BNPB pod is Running and the `tester-agent` DaemonSet has one pod per node;
- every node shows a path for `iptables`, not `MISSING`. Without it the Tester Agent's `partition` fails ("biner iptables tidak ditemukan di host"). Fix it on that VM with `sudo apt-get install -y iptables`;
- the sensor profile reads `PANTAI_PROFILE=practice` (or `exam`);
- the audit API and client-api lines show `200`;
- Grafana `http://$NODE:30087` shows the "Layanan BNPB" dashboard, and Jaeger `:30088` has traces.

## 1. Warm-up: K3, K4, K5 (~25 min)

Run one load at a time: `p5.sh k6` refuses to start while another k6 pod is running, and `sample` refuses to append to a file that already has data.

Raise the per-subject quota first; both k6 modes need it. It is restored before cleanup.

```bash
CONFIRM=yes scripts/demo/m2/p5.sh config RATE_LIMIT_MAX 100000
```

**K3 baseline, sensor healthy (5 min):**

```bash
scripts/demo/m2/p5.sh k6 k3
```

**K3 + K4, sensor faulted (5 min load, then 2 min recovery).** Use two terminals.

Terminal A, the K4 sampler (it runs for 8 minutes):

```bash
scripts/demo/m2/p5.sh sample 480
```

Terminal B:

```bash
FAULT_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
CONFIRM=yes scripts/demo/m2/p5.sh fault-sensor on
scripts/demo/m2/p5.sh k6 k3                    # 5 min with the sensor unreachable
CONFIRM=yes scripts/demo/m2/p5.sh fault-sensor off
sleep 120                                       # breaker half-open -> closed without intervention
scripts/demo/m2/p5.sh sensor-rate "$FAULT_AT"   # calls/min stay flat; breaker CLOSED -> OPEN -> recovers
```

- **K3 pass:** both k6 runs show `p(95)<300` for `source:BMKG` and `source:PVMBG`.
- **K4 pass:**
  - `c4-rate.txt` shows the call rate falling once the breaker opens;
  - `c4-fast.txt` shows fast answers (`time_total` well under the 2 s sensor timeout) with `STALE`/`OPEN`, then `OK`/`CLOSED` after recovery.

**K5 (about 5 min), only after the K3 runs have ended:**

```bash
DURATION=3m scripts/demo/m2/p5.sh k6 k5
```

Read the `responses{role:…,class:…}` lines in the k6 output:
- **Pass:** field `ok` ≈ all, and media has `shed_503 > 0`.
- **No `shed_503` at all:** production capacity was not exceeded at that VU count. Record that, then rerun capacity-scaled:

```bash
CONFIRM=yes scripts/demo/m2/p5.sh config MAX_CONCURRENT_REQUESTS 40
DURATION=3m scripts/demo/m2/p5.sh k6 k5
```

**Restore:**

```bash
CONFIRM=yes scripts/demo/m2/p5.sh config-restore
```

## 2. Cleanup and start records (~5 min)

```bash
CONFIRM=yes scripts/demo/m2/p5.sh cleanup-sensor-history
scripts/demo/m2/p5.sh offsets start
scripts/demo/m2/p5.sh clocks T0
```

The cleanup steps, in order:
1. Scale the aggregator and the sensor to 0. A poll cycle still running would otherwise write the old run's cursor after the wipe.
2. Delete the SENSOR_PANTAI rows, outbox rows, unprocessed messages, cursor and status.
3. Scale the aggregator back up.
4. Restart the notifier, which empties its seen-store.

The sensor stays stopped until T0.

## 3. Game-day window (T0 = S to T1 = T0+30)

```bash
CONFIRM=yes scripts/demo/m2/p5.sh gameday-start 30
```

This starts the sensor (S), prints when the three criticals are due, marks T0, and starts `inject-auto 30` in the background. The random plan has these parts:
- an **anchor** fault: a PVMBG outage, the sensor made unreachable, or a VM power-off (`KILL_VM`, default node-e). It starts at T0+1–3 and ends at T0+10–13, so it overlaps the first critical;
- one or two **more** faults, drawn from a PVMBG outage, the sensor made unreachable, a Tester Agent `partition` or `kill-leader`, or a VM power-off. They start between T0+4 and T1−6 and clear at least 1 min before T1;
- a powered-off VM is started again only after T1.

Each action is written to the timeline (`FAULT …`, or `FAILED …` if the action did not take effect). Do not read `<prefix>inject-plan.txt` or `<prefix>injector.log` before T1.

**Hand injection instead.** `INJECT=hand CONFIRM=yes scripts/demo/m2/p5.sh gameday-start 30` starts the sensor and marks T0 without the injector. The teammate then picks freely from the table below. Constraint: one fault is active from T0+3 to T0+10. Log each one with `scripts/demo/m2/p5.sh mark "FAULT ..."`.

| Fault | Command (injector) | Clears |
|---|---|---|
| Kill node B | on the host laptop: `vagrant halt -f node-b` (or VirtualBox power off) | bring it back only after T1 |
| Inter-node partition | `kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice partition` | self-heals |
| Kill aggregator leader | `kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice kill-leader` | Kubernetes restarts it |
| PVMBG outage | `CONFIRM=yes scripts/demo/m2/p5.sh pvmbg-outage on hang` | `... pvmbg-outage off` |
| Sensor unreachable | `CONFIRM=yes scripts/demo/m2/p5.sh fault-sensor on` | `... fault-sensor off` |

Clearing a fault the injector set is part of the scenario. The operator never intervenes. Killing the sensor's node restarts sensor time, so the criticals come later than printed; n is reported as observed.

**Operator during the window (K7).** Take screenshots into `docs/results/p5/<prefix>c7-*.png` and note the UTC time of each:
- the Grafana panels that show each fault: error rate and p95 per service, Kafka msgs/s, the Sensor Pantai readiness panel;
- `GET /sources/status` (`breaker_state`, `station_silence`);
- one Jaeger trace with its trace ID;
- `kubectl get pods -o wide` during and after the node loss.

At T1 (T0+30):

```bash
scripts/demo/m2/p5.sh clocks T1
scripts/demo/m2/p5.sh mark "T1 game day end"
```

## 4. Cutoff and scoring (C = T1+2 min)

Wait until C, then:

```bash
scripts/demo/m2/p5.sh offsets end
scripts/demo/m2/p5.sh tester-logs
# timeline.txt and digests.txt already exist from the steps above
scripts/demo/m2/p5.sh collect <T0> <T1>        # RFC 3339 UTC, e.g. 2026-11-22T10:25:00Z
```

`collect` writes three files:
- `c1-critical.txt`: per-ID table, good/late/missing, SLI, budget burn, p95, verdict;
- `c8-slo.json`;
- `c2-rejected.json`: parked SENSOR_PANTAI records with reasons.

The verdict is `INCONCLUSIVE` if any audit page was partial, n = 0, or an evidence file is missing.

## 5. Postmortem notes (fill in right after scoring)

For each fault from `timeline.txt` and `c6-tester-agent.log`, record:
- **Detection:** which panel, metric or log, and at what time;
- **Impact:** criticals late or missing, M1 latency, shed requests;
- **Mitigation:** which mechanism acted, e.g. breaker open, failover, retries, DLQ;
- **Recovery time.**

Then list what to fix. If the notifier's or the poller leader's node was killed, note the delivery gap: the notifier is a single replica.

## File map (`docs/results/p5/`, prefixed)

| File | Criterion |
|---|---|
| `preflight.txt`, `digests.txt` | setup, deployed revision |
| `timeline.txt` | K6 fault timeline (UTC) |
| `k6-k3-*.txt` | K3 healthy and faulted |
| `c4-fast.txt`, `c4-rate.txt` | K4 fast answers, request rate, recovery |
| `k6-k5-*.txt` | K5 shedding |
| `clocks-T0.txt`, `clocks-T1.txt` | clock offsets for the latency evidence |
| `offsets-start.txt`, `offsets-end.txt`, `raw/` | receipt range and raw inputs |
| `c1-critical.txt`, `c8-slo.json` | K1, K8 |
| `c2-rejected.json` | K2 |
| `c6-tester-agent.log` | K6 |
| `c7-*.png` | K7 |

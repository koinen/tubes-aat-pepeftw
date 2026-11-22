#!/usr/bin/env python3
"""C12 measurement helpers for P5: critical-alert SLO scoring, audit fetches, and K4 sampling.

Subcommands:
  fetch-audit        page /internal/audit/events for SENSOR_PANTAI into a JSONL file
  fetch-unprocessed  page /internal/audit/unprocessed for SENSOR_PANTAI into a JSON file
  score              join the audit cohort with field-team-receipts and print the SLO result
  sample             poll client-api endpoints every few seconds and log status and time_total
  sensor-rate        count Sensor Pantai HTTP calls per second from aggregator JSON logs (stdin)

Scoring rules are the C12 decisions in docs/design-decisions.md (P5 decision record).
Standard library only.
"""

import argparse
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

SLO_TARGET = 0.99
LATENCY_LIMIT_S = 5.0
P95_MIN_SAMPLES = 20

REQUIRED_EVIDENCE = [
    "offsets-start.txt",
    "offsets-end.txt",
    "clocks-T0.txt",
    "clocks-T1.txt",
    "timeline.txt",
    "digests.txt",
]


def parse_ts(value):
    """Parse an RFC 3339 timestamp into an aware UTC datetime, or None."""
    if not isinstance(value, str) or not value:
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # Python < 3.11 accepts at most 6 fractional digits; Go emits up to 9.
    if "." in s:
        head, rest = s.split(".", 1)
        digits = ""
        i = 0
        while i < len(rest) and rest[i].isdigit():
            digits += rest[i]
            i += 1
        s = head + "." + (digits[:6].ljust(6, "0")) + rest[i:]
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def fmt_ts(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def read_jsonl(path):
    out = []
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"warning: {path}:{n} is not JSON, skipped", file=sys.stderr)
    return out


# ---------------------------------------------------------------------------
# Scoring


def nearest_rank(values, q):
    ordered = sorted(values)
    k = max(1, math.ceil(q * len(ordered)))
    return ordered[k - 1]


def score(events, receipts, t0, t1, pages=None, parked=None, evidence_present=None):
    """Classify every distinct critical SENSOR_PANTAI hazard_id fetched inside [t0, t1].

    events: audit HazardEvent dicts. receipts: field-team-receipts values (dicts).
    pages: list of {"partial": bool, "unavailable_units": [...]} from the audit fetch.
    parked: list of parked SENSOR_PANTAI rows with decoded payloads (optional).
    evidence_present: {filename: bool} for REQUIRED_EVIDENCE (optional).
    """
    cohort = {}
    untimed_criticals = []
    for e in events:
        if e.get("source") != "SENSOR_PANTAI":
            continue
        attrs = e.get("attributes") or {}
        if attrs.get("critical") is not True:
            continue
        hid = e.get("hazard_id")
        fetched = parse_ts(attrs.get("fetched_at"))
        if fetched is None:
            untimed_criticals.append(hid)
            continue
        if t0 <= fetched <= t1 and hid not in cohort:
            cohort[hid] = {
                "hazard_id": hid,
                "source_ref_id": e.get("source_ref_id"),
                "station_id": attrs.get("station_id"),
                "verified": attrs.get("verified"),
                "fetched_at": fetched,
            }

    by_id = defaultdict(list)
    unparseable_receipts = 0
    for r in receipts:
        hid = r.get("hazard_id")
        rec = parse_ts(r.get("received_at"))
        if not hid or rec is None:
            unparseable_receipts += 1
            continue
        by_id[hid].append(rec)

    rows = []
    for hid, c in sorted(cohort.items(), key=lambda kv: kv[1]["fetched_at"]):
        got = sorted(by_id.get(hid, []))
        row = dict(c, receipts=len(got))
        if not got:
            row.update(cls="missing", received_at=None, latency_s=None)
        else:
            lat = (got[0] - c["fetched_at"]).total_seconds()
            row.update(
                cls="good" if lat <= LATENCY_LIMIT_S else "late",
                received_at=got[0],
                latency_s=lat,
            )
        rows.append(row)

    n = len(rows)
    counts = Counter(r["cls"] for r in rows)
    good, late, missing = counts["good"], counts["late"], counts["missing"]
    latencies = [r["latency_s"] for r in rows if r["latency_s"] is not None]
    duplicates = sum(max(0, r["receipts"] - 1) for r in rows)
    outside_ids = sorted(set(by_id) - set(cohort))
    negative = sum(1 for v in latencies if v < 0)

    if latencies:
        if len(latencies) >= P95_MIN_SAMPLES:
            p95, p95_method = nearest_rank(latencies, 0.95), "nearest-rank p95"
        else:
            p95, p95_method = max(latencies), f"max of {len(latencies)} samples (n < {P95_MIN_SAMPLES})"
    else:
        p95, p95_method = None, "no receipts"

    sli = good / n if n else None
    allowed_bad = (1 - SLO_TARGET) * n
    bad = late + missing
    burn = (bad / allowed_bad) if n else None

    pages = pages or []
    partial_pages = [p for p in pages if p.get("partial")]

    checks = [
        ("audit pages fetched", bool(pages)),
        ("every audit page partial=false", bool(pages) and not partial_pages),
        ("n >= 1", n >= 1),
        ("missing = 0", missing == 0),
        (f"SLI >= {SLO_TARGET:.0%}", sli is not None and sli >= SLO_TARGET),
    ]
    data_ok = all(ok for _, ok in checks[:2])
    evidence_present = evidence_present or {}
    for name in REQUIRED_EVIDENCE:
        checks.append((f"evidence file {name}", bool(evidence_present.get(name))))

    if not data_ok or n == 0 or not all(evidence_present.get(x) for x in REQUIRED_EVIDENCE):
        verdict = "INCONCLUSIVE"
    elif missing == 0 and sli >= SLO_TARGET:
        verdict = "PASS"
    else:
        verdict = "FAIL"

    parked = parked or []
    stored_refs = {e.get("source_ref_id") for e in events if e.get("source") == "SENSOR_PANTAI"}
    undeliverable = [
        p for p in parked if p.get("critical") is True and p.get("observation_id") not in stored_refs
    ]

    return {
        "window": {"t0": fmt_ts(t0), "t1": fmt_ts(t1)},
        "n": n,
        "good": good,
        "late": late,
        "missing": missing,
        "sli": sli,
        "slo_target": SLO_TARGET,
        "error_budget_allowed_bad": allowed_bad,
        "bad": bad,
        "budget_burn": burn,
        "p95_s": p95,
        "p95_method": p95_method,
        "duplicate_receipts": duplicates,
        "receipts_outside_cohort": len(outside_ids),
        "unparseable_receipts": unparseable_receipts,
        "negative_latencies": negative,
        "criticals_without_fetched_at": len(untimed_criticals),
        "audit_pages": len(pages),
        "partial_pages": len(partial_pages),
        "unavailable_units": sorted({u for p in partial_pages for u in p.get("unavailable_units", [])}),
        "undeliverable_parked_criticals": len(undeliverable),
        "checks": [{"check": c, "ok": ok} for c, ok in checks],
        "verdict": verdict,
        "rows": [
            {
                **{k: v for k, v in r.items() if k not in ("fetched_at", "received_at")},
                "fetched_at": fmt_ts(r["fetched_at"]),
                "received_at": fmt_ts(r["received_at"]) if r["received_at"] else None,
            }
            for r in rows
        ],
        "undeliverable": undeliverable,
    }


def render(result):
    out = []
    w = result["window"]
    out.append(f"Window (fetched_at)  : {w['t0']} .. {w['t1']}")
    out.append(f"Audit pages          : {result['audit_pages']} (partial: {result['partial_pages']}"
               + (f", units {','.join(result['unavailable_units'])}" if result["unavailable_units"] else "") + ")")
    out.append(f"Critical cohort n    : {result['n']}")
    out.append(f"  good (<= {LATENCY_LIMIT_S:.0f} s)      : {result['good']}")
    out.append(f"  late (>  {LATENCY_LIMIT_S:.0f} s)      : {result['late']}")
    out.append(f"  missing            : {result['missing']}   (0 lost <=> missing = 0)")
    sli = result["sli"]
    out.append(f"SLI                  : {'n/a' if sli is None else f'{sli:.4f}'} (target {result['slo_target']:.0%})")
    if result["n"]:
        out.append(f"Error budget         : {result['error_budget_allowed_bad']:.2f} bad events allowed, "
                   f"{result['bad']} used, burn {result['budget_burn']:.0%}")
    p95 = result["p95_s"]
    out.append(f"p95 latency          : {'n/a' if p95 is None else f'{p95:.3f} s'} [{result['p95_method']}]")
    out.append(f"Duplicate receipts   : {result['duplicate_receipts']}")
    out.append(f"Receipts outside cohort (distinct ids): {result['receipts_outside_cohort']}")
    out.append(f"Unparseable receipts : {result['unparseable_receipts']}")
    out.append(f"Negative latencies   : {result['negative_latencies']} (clock offset between nodes)")
    out.append(f"Criticals without fetched_at (not windowed): {result['criticals_without_fetched_at']}")
    out.append(f"Parked criticals with invalid identity (undeliverable by design, outside SLI): "
               f"{result['undeliverable_parked_criticals']}")
    out.append("")
    out.append("hazard_id                             source_ref_id   station                fetched_at                received_at               latency_s  class    receipts")
    for r in result["rows"]:
        lat = "-" if r["latency_s"] is None else f"{r['latency_s']:.3f}"
        out.append(f"{r['hazard_id']:<37} {str(r['source_ref_id']):<15} {str(r['station_id']):<22} "
                   f"{r['fetched_at']:<25} {str(r['received_at'] or '-'):<25} {lat:>9}  {r['cls']:<8} {r['receipts']}")
    out.append("")
    out.append("Checks:")
    for c in result["checks"]:
        out.append(f"  [{'x' if c['ok'] else ' '}] {c['check']}")
    out.append(f"VERDICT: {result['verdict']}")
    return "\n".join(out)


def decode_parked(rows):
    """Turn parked rows {message_id, reason, received_at, payload_hex} into observation dicts."""
    out = []
    for row in rows:
        try:
            payload = json.loads(bytes.fromhex(row.get("payload_hex", "")).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            payload = None
        items = []
        if isinstance(payload, dict) and isinstance(payload.get("items"), list):
            items = payload["items"]
        elif isinstance(payload, dict):
            items = [payload]
        if not items:
            out.append({"message_id": row.get("message_id"), "reason": row.get("reason"),
                        "critical": None, "observation_id": None})
        for it in items:
            if not isinstance(it, dict):
                continue
            out.append({
                "message_id": row.get("message_id"),
                "reason": row.get("reason"),
                "received_at": row.get("received_at"),
                "critical": it.get("critical"),
                "observation_id": it.get("observation_id"),
                "station_id": it.get("station_id"),
            })
    return out


# ---------------------------------------------------------------------------
# HTTP helpers


def http_get(url, token, timeout=30):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def cmd_fetch_audit(a):
    pages_meta = []
    total = 0
    cursor = None
    with open(a.out, "w", encoding="utf-8") as f:
        while True:
            q = {"source": "SENSOR_PANTAI", "since": a.since, "limit": "1000"}
            if cursor:
                q["cursor"] = cursor
            page = http_get(f"{a.base.rstrip('/')}/internal/audit/events?{urllib.parse.urlencode(q)}", a.token)
            for item in page.get("items", []):
                f.write(json.dumps(item, separators=(",", ":")) + "\n")
            total += len(page.get("items", []))
            pages_meta.append({"items": len(page.get("items", [])), "partial": page.get("partial"),
                               "unavailable_units": page.get("unavailable_units", [])})
            cursor = page.get("next_cursor")
            if not cursor:
                break
    with open(a.pages_out, "w", encoding="utf-8") as f:
        json.dump(pages_meta, f, indent=1)
    print(f"audit: {total} SENSOR_PANTAI events since {a.since} in {len(pages_meta)} pages, "
          f"partial pages: {sum(1 for p in pages_meta if p['partial'])}")


def cmd_fetch_unprocessed(a):
    items, cursor = [], None
    while True:
        q = {"source": "SENSOR_PANTAI", "limit": "1000"}
        if cursor:
            q["cursor"] = cursor
        page = http_get(f"{a.base.rstrip('/')}/internal/audit/unprocessed?{urllib.parse.urlencode(q)}", a.token)
        items.extend(page.get("items", []))
        cursor = page.get("next_cursor")
        if not cursor:
            break
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump({"source": "SENSOR_PANTAI", "fetched_at": fmt_ts(datetime.now(timezone.utc)),
                   "count": len(items), "reasons": Counter(i.get("reason", "").split(":")[0] for i in items),
                   "items": items}, f, indent=1)
    print(f"unprocessed: {len(items)} SENSOR_PANTAI items")


def cmd_score(a):
    t0, t1 = parse_ts(a.t0), parse_ts(a.t1)
    if t0 is None or t1 is None or t1 < t0:
        sys.exit("score: --t0/--t1 must be RFC 3339 UTC with t0 <= t1")
    events = read_jsonl(a.events)
    receipts = read_jsonl(a.receipts)
    pages = json.load(open(a.pages, encoding="utf-8")) if a.pages else []
    parked = decode_parked(read_jsonl(a.parked)) if a.parked else []
    evidence = {}
    if a.evidence_dir:
        for name in REQUIRED_EVIDENCE:
            p = os.path.join(a.evidence_dir, a.prefix + name)
            evidence[name] = os.path.isfile(p) and os.path.getsize(p) > 0
    result = score(events, receipts, t0, t1, pages=pages, parked=parked, evidence_present=evidence)
    text = render(result)
    print(text)
    if a.json_out:
        with open(a.json_out, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=1)
    sys.exit(0 if result["verdict"] == "PASS" else 1)


# ---------------------------------------------------------------------------
# K4 sampler


class TokenSource:
    def __init__(self, auth_base, client_id, secret):
        self.url = auth_base.rstrip("/") + "/token"
        self.client_id, self.secret = client_id, secret
        self.token, self.at = None, 0.0

    def get(self):
        if self.token is None or time.monotonic() - self.at > 45:
            body = json.dumps({"grant_type": "client_credentials", "client_id": self.client_id,
                               "client_secret": self.secret}).encode()
            req = urllib.request.Request(self.url, data=body, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                self.token = json.loads(resp.read())["access_token"]
            self.at = time.monotonic()
        return self.token


def timed_get(url, token, timeout):
    start = time.monotonic()
    try:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            code = resp.status
    except urllib.error.HTTPError as e:
        body, code = e.read(), e.code
    except Exception as e:  # timeout, refused
        return 0, time.monotonic() - start, str(e).encode()
    return code, time.monotonic() - start, body


def summarize_body(path, body):
    try:
        doc = json.loads(body)
    except ValueError:
        return body[:120].decode("utf-8", "replace")
    if not isinstance(doc, dict):
        return ""
    if "error" in doc:
        err = doc["error"] if isinstance(doc["error"], dict) else {}
        return f"error={err.get('code')} {str(err.get('message', ''))[:80]}"
    if "sources/status" in path:
        sp = (doc.get("data") or {}).get("SENSOR_PANTAI")
    else:
        sp = (doc.get("sources") or {}).get("SENSOR_PANTAI")
    parts = []
    if "sources/status" not in path:
        parts.append(f"items={len(doc.get('data') or [])} partial={doc.get('partial')}")
    if isinstance(sp, dict):
        keys = ("status", "breaker_state", "last_success_at", "stale_since")
        parts.append("SENSOR_PANTAI " + " ".join(f"{k}={sp.get(k)}" for k in keys if k in sp))
    return " ".join(parts)


def cmd_sample(a):
    tokens = TokenSource(a.auth, a.client_id, a.secret)
    paths = a.path or ["/sources/status", "/hazards?type=COASTAL&limit=1"]
    end = time.monotonic() + a.duration
    print("ts_utc\tpath\tstatus\ttime_total_s\tsummary", flush=True)
    while time.monotonic() < end:
        tick = time.monotonic()
        tok = tokens.get()
        for p in paths:
            code, dt, body = timed_get(a.base.rstrip("/") + p, tok, a.timeout)
            print(f"{fmt_ts(datetime.now(timezone.utc))}\t{p}\t{code}\t{dt:.3f}\t{summarize_body(p, body)}",
                  flush=True)
        time.sleep(max(0.0, a.interval - (time.monotonic() - tick)))


# ---------------------------------------------------------------------------
# K4 request rate from aggregator logs


def cmd_sensor_rate(a):
    per_sec = Counter()
    statuses = Counter()
    transitions = []
    last_state = None
    for raw in sys.stdin:
        line = raw.strip()
        # kubectl logs --prefix puts "[pod/name/container] " in front.
        if line.startswith("[") and "] " in line:
            line = line.split("] ", 1)[1]
        if not line.startswith("{"):
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("target") != "SENSOR_PANTAI":
            continue
        msg, ts = rec.get("msg"), rec.get("ts", "")
        if msg == "outbound_call":
            per_sec[ts[:19]] += 1  # truncate sub-second precision
            statuses[(rec.get("path", "").split("?")[0], rec.get("status"))] += 1
        elif msg == "poll_failed":
            st = rec.get("breaker_state")
            if st != last_state:
                transitions.append((ts, st, rec.get("error", "")[:100]))
                last_state = st
    per_min = Counter()
    for ts, c in per_sec.items():
        per_min[ts[:16]] += c
    print("Sensor Pantai HTTP calls per minute (outbound_call, all aggregator pods):")
    for m in sorted(per_min):
        print(f"  {m}Z  {per_min[m]:5d}  {'#' * min(per_min[m], 80)}")
    print("\nCalls by path and status:")
    for (path, st), c in sorted(statuses.items(), key=lambda kv: (-kv[1], str(kv[0]))):
        print(f"  {path:<24} {str(st):>4}  {c}")
    print("\nBreaker state on failed polls (changes only):")
    for ts, st, err in transitions:
        print(f"  {ts}  {st:<10} {err}")
    if per_sec:
        peak = max(per_sec.values())
        print(f"\nPeak calls in one second: {peak}; seconds with calls: {len(per_sec)}")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("fetch-audit")
    s.add_argument("--base", required=True, help="aggregator base URL, e.g. http://NODE:30085")
    s.add_argument("--token", default=os.environ.get("AUDIT_TOKEN"))
    s.add_argument("--since", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--pages-out", required=True)
    s.set_defaults(fn=cmd_fetch_audit)

    s = sub.add_parser("fetch-unprocessed")
    s.add_argument("--base", required=True)
    s.add_argument("--token", default=os.environ.get("AUDIT_TOKEN"))
    s.add_argument("--out", required=True)
    s.set_defaults(fn=cmd_fetch_unprocessed)

    s = sub.add_parser("score")
    s.add_argument("--events", required=True, help="JSONL of audit events")
    s.add_argument("--receipts", required=True, help="JSONL of field-team-receipts values")
    s.add_argument("--t0", required=True)
    s.add_argument("--t1", required=True)
    s.add_argument("--pages", help="JSON page metadata from fetch-audit")
    s.add_argument("--parked", help="JSONL rows {message_id,reason,received_at,payload_hex}")
    s.add_argument("--evidence-dir", help="directory holding the REQUIRED_EVIDENCE files")
    s.add_argument("--prefix", default="", help="file-name prefix inside --evidence-dir")
    s.add_argument("--json-out")
    s.set_defaults(fn=cmd_score)

    s = sub.add_parser("sample")
    s.add_argument("--base", required=True, help="client-api base URL, e.g. http://NODE:30080")
    s.add_argument("--auth", required=True, help="auth base URL, e.g. http://NODE:30084")
    s.add_argument("--client-id", default="bnpb-ops")
    s.add_argument("--secret", default=os.environ.get("BNPB_SECRET"))
    s.add_argument("--path", action="append", help="repeatable; default status + COASTAL list")
    s.add_argument("--duration", type=float, default=600)
    s.add_argument("--interval", type=float, default=2)
    s.add_argument("--timeout", type=float, default=10)
    s.set_defaults(fn=cmd_sample)

    s = sub.add_parser("sensor-rate")
    s.set_defaults(fn=cmd_sensor_rate)

    a = p.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()

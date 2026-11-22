"""Fixture tests for p5_slo. Run: python3 -I scripts/demo/m2/p5_slo_test.py"""

import io
import json
import os
import sys
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import p5_slo  # noqa: E402

T0 = datetime(2026, 11, 22, 10, 0, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 11, 22, 10, 20, 0, tzinfo=timezone.utc)
ALL_EVIDENCE = {name: True for name in p5_slo.REQUIRED_EVIDENCE}
CLEAN_PAGES = [{"partial": False, "unavailable_units": []}]


def event(hid, fetched_at, critical=True, source="SENSOR_PANTAI", ref=None):
    attrs = {"critical": critical, "station_id": "stn-a", "verified": True}
    if fetched_at is not None:
        attrs["fetched_at"] = fetched_at
    return {"hazard_id": hid, "source": source, "source_ref_id": ref or f"obs-{hid}", "attributes": attrs}


def receipt(hid, received_at, fetched_at=None):
    r = {"hazard_id": hid, "received_at": received_at}
    if fetched_at:
        r["fetched_at"] = fetched_at
    return r


def run(events, receipts, pages=CLEAN_PAGES, evidence=ALL_EVIDENCE, parked=None):
    return p5_slo.score(events, receipts, T0, T1, pages=pages, evidence_present=evidence, parked=parked)


class ScoreTest(unittest.TestCase):
    def test_classifies_good_late_missing_once_each(self):
        events = [
            event("a", "2026-11-22T10:05:00.000Z"),
            event("b", "2026-11-22T10:06:00.000Z"),
            event("c", "2026-11-22T10:07:00.000Z"),
        ]
        receipts = [
            receipt("a", "2026-11-22T10:05:01.200Z"),
            receipt("b", "2026-11-22T10:06:05.001Z"),
        ]
        r = run(events, receipts)
        self.assertEqual((r["n"], r["good"], r["late"], r["missing"]), (3, 1, 1, 1))
        self.assertEqual(r["bad"], 2)
        self.assertEqual(r["verdict"], "FAIL")

    def test_exactly_five_seconds_is_good(self):
        r = run([event("a", "2026-11-22T10:05:00.000Z")], [receipt("a", "2026-11-22T10:05:05.000Z")])
        self.assertEqual(r["good"], 1)
        self.assertEqual(r["verdict"], "PASS")

    def test_earliest_receipt_wins_and_duplicates_are_counted(self):
        events = [event("a", "2026-11-22T10:05:00.000Z")]
        receipts = [
            receipt("a", "2026-11-22T10:05:09.000Z"),
            receipt("a", "2026-11-22T10:05:00.800Z"),
            receipt("a", "2026-11-22T10:05:30.000Z"),
        ]
        r = run(events, receipts)
        self.assertEqual(r["good"], 1)
        self.assertAlmostEqual(r["rows"][0]["latency_s"], 0.8, places=3)
        self.assertEqual(r["duplicate_receipts"], 2)

    def test_latency_uses_audit_fetched_at_not_receipt_field(self):
        events = [event("a", "2026-11-22T10:05:00.000Z")]
        # A receipt without fetched_at, or with a wrong one, is still scored from the audit event.
        receipts = [receipt("a", "2026-11-22T10:05:02.000Z", fetched_at="2026-11-22T09:00:00Z")]
        r = run(events, receipts)
        self.assertAlmostEqual(r["rows"][0]["latency_s"], 2.0, places=3)

    def test_window_is_by_fetched_at_and_inclusive(self):
        events = [
            event("edge0", "2026-11-22T10:00:00.000Z"),
            event("edge1", "2026-11-22T10:20:00.000Z"),
            event("before", "2026-11-22T09:59:59.999Z"),
            event("after", "2026-11-22T10:20:00.001Z"),
        ]
        r = run(events, [])
        self.assertEqual({row["hazard_id"] for row in r["rows"]}, {"edge0", "edge1"})

    def test_ignores_non_critical_and_other_sources(self):
        events = [
            event("a", "2026-11-22T10:05:00.000Z", critical=False),
            event("b", "2026-11-22T10:05:00.000Z", source="BMKG"),
            event("c", "2026-11-22T10:05:00.000Z", critical="true"),
        ]
        r = run(events, [])
        self.assertEqual(r["n"], 0)
        self.assertEqual(r["verdict"], "INCONCLUSIVE")

    def test_receipts_outside_cohort_do_not_change_sli(self):
        events = [event("a", "2026-11-22T10:05:00.000Z")]
        receipts = [receipt("a", "2026-11-22T10:05:01.000Z"), receipt("zzz", "2026-11-22T10:05:01.000Z")]
        r = run(events, receipts)
        self.assertEqual(r["receipts_outside_cohort"], 1)
        self.assertEqual(r["sli"], 1.0)

    def test_partial_audit_page_is_inconclusive_even_if_all_good(self):
        events = [event("a", "2026-11-22T10:05:00.000Z")]
        receipts = [receipt("a", "2026-11-22T10:05:01.000Z")]
        pages = [{"partial": False}, {"partial": True, "unavailable_units": ["shard-3"]}]
        r = run(events, receipts, pages=pages)
        self.assertEqual(r["verdict"], "INCONCLUSIVE")
        self.assertEqual(r["unavailable_units"], ["shard-3"])

    def test_missing_evidence_file_is_inconclusive(self):
        events = [event("a", "2026-11-22T10:05:00.000Z")]
        receipts = [receipt("a", "2026-11-22T10:05:01.000Z")]
        evidence = dict(ALL_EVIDENCE, **{"clocks-T1.txt": False})
        self.assertEqual(run(events, receipts, evidence=evidence)["verdict"], "INCONCLUSIVE")

    def test_p95_is_max_below_twenty_samples_and_nearest_rank_above(self):
        few = [event(f"h{i}", "2026-11-22T10:05:00.000Z") for i in range(3)]
        few_r = [receipt(f"h{i}", f"2026-11-22T10:05:0{i + 1}.000Z") for i in range(3)]
        r = run(few, few_r)
        self.assertAlmostEqual(r["p95_s"], 3.0, places=3)
        self.assertIn("max of 3", r["p95_method"])

        many = [event(f"m{i:02d}", "2026-11-22T10:05:00.000Z") for i in range(40)]
        many_r = [receipt(f"m{i:02d}", f"2026-11-22T10:05:{i + 1:02d}.000Z") for i in range(40)]
        r = run(many, many_r)
        # nearest rank: ceil(0.95 * 40) = 38th smallest = 38 s
        self.assertAlmostEqual(r["p95_s"], 38.0, places=3)
        self.assertEqual(r["p95_method"], "nearest-rank p95")

    def test_budget_burn_relative_to_one_percent(self):
        events = [event(f"h{i:03d}", "2026-11-22T10:05:00.000Z") for i in range(200)]
        receipts = [receipt(f"h{i:03d}", "2026-11-22T10:05:01.000Z") for i in range(199)]
        r = run(events, receipts)
        self.assertAlmostEqual(r["error_budget_allowed_bad"], 2.0)
        self.assertAlmostEqual(r["budget_burn"], 0.5)
        self.assertEqual(r["verdict"], "FAIL")  # one missing breaks "0 lost" even within budget

    def test_nanosecond_timestamps_and_negative_latency(self):
        events = [event("a", "2026-11-22T10:05:00.500Z")]
        receipts = [receipt("a", "2026-11-22T10:05:00.123456789Z")]
        r = run(events, receipts)
        self.assertEqual(r["negative_latencies"], 1)
        self.assertEqual(r["good"], 1)

    def test_criticals_without_fetched_at_are_reported_not_windowed(self):
        r = run([event("a", None)], [])
        self.assertEqual(r["criticals_without_fetched_at"], 1)
        self.assertEqual(r["n"], 0)

    def test_undeliverable_parked_criticals_are_counted_outside_sli(self):
        events = [event("a", "2026-11-22T10:05:00.000Z", ref="obs-1")]
        receipts = [receipt("a", "2026-11-22T10:05:01.000Z")]
        parked = p5_slo.decode_parked([
            {"message_id": "m1", "reason": "contract_violation",
             "payload_hex": json.dumps({"observation_id": "obs-9", "critical": True,
                                        "station_id": "stn-unknown-99"}).encode().hex()},
            {"message_id": "m2", "reason": "contract_violation",  # unverified alert, also stored
             "payload_hex": json.dumps({"observation_id": "obs-1", "critical": True}).encode().hex()},
            {"message_id": "m3", "reason": "contract_violation", "payload_hex": "7b7b"},  # not JSON
        ])
        r = run(events, receipts, parked=parked)
        self.assertEqual(r["undeliverable_parked_criticals"], 1)
        self.assertEqual(r["verdict"], "PASS")

    def test_render_mentions_verdict_and_counts(self):
        r = run([event("a", "2026-11-22T10:05:00.000Z")], [])
        text = p5_slo.render(r)
        self.assertIn("missing            : 1", text)
        self.assertIn("VERDICT: FAIL", text)


class SensorRateTest(unittest.TestCase):
    def test_counts_only_sensor_outbound_calls_and_breaker_changes(self):
        lines = [
            '[pod/aggregator-x/aggregator] {"ts":"2026-11-22T10:00:01Z","msg":"outbound_call","target":"SENSOR_PANTAI","path":"/v1/observations?limit=1000","status":200}',
            '{"ts":"2026-11-22T10:00:01Z","msg":"outbound_call","target":"BMKG","path":"/x","status":200}',
            '{"ts":"2026-11-22T10:00:02Z","msg":"outbound_call","target":"SENSOR_PANTAI","path":"/v1/observations","status":0}',
            '{"ts":"2026-11-22T10:00:02Z","msg":"poll_failed","target":"SENSOR_PANTAI","breaker_state":"CLOSED","error":"timeout"}',
            '{"ts":"2026-11-22T10:00:03Z","msg":"poll_failed","target":"SENSOR_PANTAI","breaker_state":"OPEN","error":"timeout"}',
            '{"ts":"2026-11-22T10:00:04Z","msg":"poll_failed","target":"SENSOR_PANTAI","breaker_state":"OPEN","error":"timeout"}',
            "not json",
        ]
        old = sys.stdin
        sys.stdin = io.StringIO("\n".join(lines))
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                p5_slo.cmd_sensor_rate(None)
        finally:
            sys.stdin = old
        out = buf.getvalue()
        self.assertIn("2026-11-22T10:00Z      2", out)
        self.assertEqual(out.count("OPEN"), 1)
        self.assertIn("Peak calls in one second: 1", out)


if __name__ == "__main__":
    unittest.main()

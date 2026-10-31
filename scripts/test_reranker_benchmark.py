"""Offline latency-statistics and failure accounting, with no model calls."""
import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location("reranker_benchmark", Path(__file__).with_name("benchmark-reranker_service.py"))
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


class FakeService:
    def __init__(self, durations):
        self.durations = iter(durations)
        self.calls = 0

    def request(self, base, key, path, payload, timeout):
        self.calls += 1
        duration = next(self.durations)
        if isinstance(duration, Exception):
            raise duration
        return 200, [{"index": index, "score": .5} for index in range(len(payload["texts"]))], duration

    @staticmethod
    def checked_scores(rows, count):
        assert len(rows) == count
        return {row["index"]: row["score"] for row in rows}


class BenchmarkTests(unittest.TestCase):
    def measure(self, service, repeats=2, warmup=0):
        return benchmark.measure_candidate(service, "http://127.0.0.1", "unlogged-key", 10,
                                           "short_80", 1, repeats, warmup,
                                           {"model": "test-model", "revision": "frozen", "device": "cuda"})

    def test_percentiles_interpolate_twenty_observations(self):
        summary = benchmark.latency_summary(list(range(1, 21)), 20)
        self.assertTrue(summary["complete"])
        self.assertEqual(summary["samples"], 20)
        self.assertEqual(summary["p50_seconds"], 10.5)
        self.assertAlmostEqual(summary["p95_seconds"], 19.05)

    def test_warmup_is_excluded_and_single_sample_retains_existing_seconds(self):
        service = FakeService([90, .2])
        record = self.measure(service, repeats=1, warmup=1)
        self.assertEqual(record["status"], "passed")
        self.assertEqual(record["warmup_completed"], 1)
        self.assertEqual(record["samples_seconds"], [.2])
        self.assertEqual(record["seconds"], .2)
        self.assertEqual(record["p95_seconds"], .2)
        self.assertEqual(record["device"], "cuda")

    def test_timeout_is_not_a_success_sample_and_stops_remaining_requests(self):
        service = FakeService([.2, TimeoutError(), .3])
        record = self.measure(service, repeats=3)
        self.assertEqual(record["status"], "timed_out")
        self.assertEqual(record["failed_phase"], "measurement")
        self.assertFalse(record["complete"])
        self.assertEqual(record["samples_seconds"], [.2])
        self.assertIsNone(record["p50_seconds"])
        self.assertIsNone(record["p95_seconds"])
        self.assertEqual(service.calls, 2)

    def test_warmup_timeout_produces_no_success_sample(self):
        service = FakeService([TimeoutError(), .1])
        record = self.measure(service, warmup=1)
        self.assertEqual(record["status"], "timed_out")
        self.assertEqual(record["failed_phase"], "warmup")
        self.assertEqual(record["samples"], 0)
        self.assertEqual(record["warmup_completed"], 0)
        self.assertEqual(service.calls, 1)

    def test_nonfinite_duration_is_failed_not_scored(self):
        record = self.measure(FakeService([float("nan")]))
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["samples"], 0)
        self.assertIsNone(record["p95_seconds"])


if __name__ == "__main__":
    unittest.main()

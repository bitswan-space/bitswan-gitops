#!/usr/bin/env python3
"""
Bitswan GitOps API stress tester.

Hammers the gitops API with concurrent creates, deploys, lists, inspects,
and deletes to surface race conditions and hot paths under load.

Prerequisites:
    pip install httpx

Usage:
    python scripts/stress_test.py \
        --url http://172.18.0.5:8079 \
        --token $BITSWAN_GITOPS_SECRET

    # Or via env vars:
    BITSWAN_GITOPS_URL=http://172.18.0.5:8079 \
    BITSWAN_GITOPS_SECRET=<token> \
    python scripts/stress_test.py
"""

import argparse
import asyncio
import hashlib
import io
import os
import statistics
import tarfile
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

try:
    import httpx
except ImportError:
    raise SystemExit("httpx is required: pip install httpx")


# ── Git tree-hash helpers (mirrors app/utils.py) ──────────────────────────────


def _blob_hash(content: bytes) -> bytes:
    h = hashlib.sha1()
    h.update(f"blob {len(content)}\0".encode())
    h.update(content)
    return h.digest()


def _tree_hash_from_entries(entries: list[tuple[str, bool, bytes]]) -> bytes:
    """entries = [(name, is_dir, sha1_20_bytes), ...]"""
    entries = sorted(entries, key=lambda e: (e[0] + "/") if e[1] else e[0])
    body = bytearray()
    for name, is_dir, sha1 in entries:
        mode = b"040000" if is_dir else b"100644"
        body += mode + b" " + name.encode() + b"\0" + sha1
    header = f"tree {len(body)}\0".encode()
    return hashlib.sha1(header + bytes(body)).digest()


def compute_checksum(files: list[tuple[str, bytes]]) -> str:
    """
    Compute the git tree hash for a flat list of (filename, content) pairs,
    matching the algorithm the server uses after extracting the archive.
    """
    entries = [(name, False, _blob_hash(content)) for name, content in files]
    return _tree_hash_from_entries(entries).hex()


def build_tar_gz(files: list[tuple[str, bytes]]) -> bytes:
    """Pack (filename, content) pairs into an in-memory tar.gz archive."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, content in files:
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return buf.getvalue()


# ── Automation templates ──────────────────────────────────────────────────────


def _make_automation(template: str, pid: str) -> list[tuple[str, bytes]]:
    """Return a list of (filename, content) pairs for a named template."""
    toml = f'process-id = "{pid}"\n'.encode()

    if template == "sensor-pipeline":
        return [
            ("process.toml", toml),
            ("main.py", _SENSOR_MAIN.encode()),
            ("config.yaml", _SENSOR_CONFIG.encode()),
            ("requirements.txt", b"bspump>=0.6\npyyaml\nnumpy\n"),
            ("transform.py", _SENSOR_TRANSFORM.encode()),
        ]
    elif template == "alert-monitor":
        return [
            ("process.toml", toml),
            ("main.py", _ALERT_MAIN.encode()),
            ("rules.yaml", _ALERT_RULES.encode()),
            ("requirements.txt", b"bspump>=0.6\npyyaml\nprometheus-client\n"),
            ("filters.py", _ALERT_FILTERS.encode()),
        ]
    elif template == "data-aggregator":
        return [
            ("process.toml", toml),
            ("main.py", _AGG_MAIN.encode()),
            ("config.yaml", _AGG_CONFIG.encode()),
            ("requirements.txt", b"bspump>=0.6\npandas\npyyaml\n"),
            ("aggregator.py", _AGG_PROCESSOR.encode()),
            ("schema.json", _AGG_SCHEMA.encode()),
        ]
    raise ValueError(f"Unknown template: {template}")


TEMPLATES = ["sensor-pipeline", "alert-monitor", "data-aggregator"]

_SENSOR_MAIN = """\
import bspump
import bspump.kafka
import bspump.common
import logging
import yaml

L = logging.getLogger(__name__)

with open("config.yaml") as f:
    CONFIG = yaml.safe_load(f)


class SensorNormalisationProcessor(bspump.Processor):
    UNIT_FACTORS = {
        "temperature_c": 1.0,
        "temperature_f": lambda v: (v - 32) * 5 / 9,
        "pressure_pa": 1.0,
        "pressure_hpa": 100.0,
        "humidity_pct": 0.01,
    }

    def __init__(self, app, pipeline, *args, **kwargs):
        super().__init__(app, pipeline, *args, **kwargs)
        self._factors = self.UNIT_FACTORS

    def process(self, context, event):
        sensor_type = event.get("sensor_type", "")
        raw = event.get("value")
        if raw is None:
            return None
        factor = self._factors.get(sensor_type, 1.0)
        si = factor(raw) if callable(factor) else raw * factor
        return {**event, "value_si": si, "normalised": True}


class ThresholdAlertProcessor(bspump.Processor):
    def __init__(self, app, pipeline, *args, **kwargs):
        super().__init__(app, pipeline, *args, **kwargs)
        self._thresholds = CONFIG.get("thresholds", {})

    def process(self, context, event):
        sensor_type = event.get("sensor_type", "")
        value = event.get("value_si")
        bounds = self._thresholds.get(sensor_type)
        if bounds and value is not None:
            lo, hi = bounds.get("min"), bounds.get("max")
            if (lo is not None and value < lo) or (hi is not None and value > hi):
                event["alert"] = True
        return event


class SensorPipeline(bspump.Pipeline):
    def __init__(self, app, pipeline_id=None):
        super().__init__(app, pipeline_id)
        src  = bspump.kafka.KafkaSource(app, self, "KafkaSrc",
                   config={"topic": CONFIG["kafka"]["source_topic"]})
        norm = SensorNormalisationProcessor(app, self)
        thr  = ThresholdAlertProcessor(app, self)
        sink = bspump.kafka.KafkaSink(app, self, "KafkaSink",
                   config={"topic": CONFIG["kafka"]["sink_topic"]})
        self.build(src, norm, thr, sink)


class SensorApp(bspump.BSPumpApplication):
    def __init__(self):
        super().__init__()
        svc = self.get_service("bspump.PumpService")
        svc.add(SensorPipeline(self))


if __name__ == "__main__":
    SensorApp().run()
"""

_SENSOR_CONFIG = """\
kafka:
  source_topic: sensors.raw
  sink_topic: sensors.normalised
  brokers: kafka:9092
thresholds:
  temperature_c:
    min: -40.0
    max: 85.0
  pressure_pa:
    min: 10000.0
    max: 120000.0
  humidity_pct:
    min: 0.05
    max: 0.95
"""

_SENSOR_TRANSFORM = """\
import math


def celsius_to_kelvin(c: float) -> float:
    return c + 273.15


def fahrenheit_to_celsius(f: float) -> float:
    return (f - 32) * 5 / 9


def dew_point(temp_c: float, humidity: float) -> float:
    \"\"\"Magnus formula approximation.\"\"\"
    a, b = 17.27, 237.7
    gamma = (a * temp_c / (b + temp_c)) + math.log(humidity)
    return (b * gamma) / (a - gamma)


def vapour_pressure(temp_c: float) -> float:
    return 0.6108 * math.exp(17.27 * temp_c / (temp_c + 237.3))
"""

_ALERT_MAIN = """\
import bspump
import bspump.kafka
import logging
import yaml
import time

L = logging.getLogger(__name__)

with open("rules.yaml") as f:
    RULES = yaml.safe_load(f)["rules"]


class RuleEvaluator(bspump.Processor):
    \"\"\"Evaluate each event against the configured alert rules.\"\"\"

    def process(self, context, event):
        triggered = []
        for rule in RULES:
            field_val = event.get(rule["field"])
            if field_val is None:
                continue
            op = rule["op"]
            threshold = rule["value"]
            if op == "gt" and field_val > threshold:
                triggered.append(rule["name"])
            elif op == "lt" and field_val < threshold:
                triggered.append(rule["name"])
            elif op == "eq" and field_val == threshold:
                triggered.append(rule["name"])
        if triggered:
            event["alerts"] = triggered
            event["alert_ts"] = time.time()
        return event


class DeduplicationProcessor(bspump.Processor):
    WINDOW = 60  # seconds

    def __init__(self, app, pipeline, *args, **kwargs):
        super().__init__(app, pipeline, *args, **kwargs)
        self._seen: dict[str, float] = {}

    def process(self, context, event):
        alerts = event.get("alerts")
        if not alerts:
            return event
        key = "|".join(sorted(alerts)) + str(event.get("source", ""))
        now = time.time()
        last = self._seen.get(key, 0)
        if now - last < self.WINDOW:
            return None  # deduplicated
        self._seen[key] = now
        # evict stale entries
        self._seen = {k: v for k, v in self._seen.items() if now - v < self.WINDOW * 2}
        return event


class AlertMonitorPipeline(bspump.Pipeline):
    def __init__(self, app, pipeline_id=None):
        super().__init__(app, pipeline_id)
        src   = bspump.kafka.KafkaSource(app, self, "KafkaSrc", config={"topic": "metrics.raw"})
        rules = RuleEvaluator(app, self)
        dedup = DeduplicationProcessor(app, self)
        sink  = bspump.kafka.KafkaSink(app, self, "KafkaSink", config={"topic": "alerts.triggered"})
        self.build(src, rules, dedup, sink)


class AlertMonitorApp(bspump.BSPumpApplication):
    def __init__(self):
        super().__init__()
        svc = self.get_service("bspump.PumpService")
        svc.add(AlertMonitorPipeline(self))


if __name__ == "__main__":
    AlertMonitorApp().run()
"""

_ALERT_RULES = """\
rules:
  - name: high_cpu
    field: cpu_usage_pct
    op: gt
    value: 90.0
    severity: critical
  - name: low_memory
    field: memory_free_mb
    op: lt
    value: 256
    severity: warning
  - name: high_latency
    field: response_time_ms
    op: gt
    value: 2000
    severity: warning
  - name: error_rate_spike
    field: error_rate_pct
    op: gt
    value: 5.0
    severity: critical
  - name: disk_almost_full
    field: disk_free_pct
    op: lt
    value: 10.0
    severity: critical
"""

_ALERT_FILTERS = """\
from typing import Any


def severity_filter(event: dict, min_severity: str = "warning") -> bool:
    order = {"info": 0, "warning": 1, "critical": 2}
    event_sev = event.get("severity", "info")
    return order.get(event_sev, 0) >= order.get(min_severity, 0)


def source_allowlist(event: dict, allowed: set[str]) -> bool:
    return event.get("source") in allowed


def enriched(event: dict, host_map: dict[str, Any]) -> dict:
    source = event.get("source", "")
    meta = host_map.get(source, {})
    return {**event, "host_meta": meta}
"""

_AGG_MAIN = """\
import bspump
import bspump.kafka
import bspump.trigger
import logging
import yaml
from aggregator import RollingWindowAggregator, SummaryStatsProcessor

L = logging.getLogger(__name__)

with open("config.yaml") as f:
    CONFIG = yaml.safe_load(f)


class DataAggregatorPipeline(bspump.Pipeline):
    def __init__(self, app, pipeline_id=None):
        super().__init__(app, pipeline_id)
        window_secs = CONFIG.get("window_seconds", 60)
        src  = bspump.kafka.KafkaSource(app, self, "KafkaSrc",
                   config={"topic": CONFIG["kafka"]["source_topic"]})
        win  = RollingWindowAggregator(app, self, window_secs=window_secs)
        stats = SummaryStatsProcessor(app, self)
        sink = bspump.kafka.KafkaSink(app, self, "KafkaSink",
                   config={"topic": CONFIG["kafka"]["sink_topic"]})
        self.build(src, win, stats, sink)


class DataAggregatorApp(bspump.BSPumpApplication):
    def __init__(self):
        super().__init__()
        svc = self.get_service("bspump.PumpService")
        svc.add(DataAggregatorPipeline(self))


if __name__ == "__main__":
    DataAggregatorApp().run()
"""

_AGG_CONFIG = """\
kafka:
  source_topic: metrics.raw
  sink_topic: metrics.aggregated
  brokers: kafka:9092
window_seconds: 60
fields:
  - name: cpu_usage_pct
    aggregations: [mean, max, p95]
  - name: memory_used_mb
    aggregations: [mean, max]
  - name: request_count
    aggregations: [sum]
  - name: response_time_ms
    aggregations: [mean, p50, p95, p99]
"""

_AGG_PROCESSOR = """\
import collections
import statistics
import time
import bspump


class RollingWindowAggregator(bspump.Processor):
    def __init__(self, app, pipeline, window_secs=60, *args, **kwargs):
        super().__init__(app, pipeline, *args, **kwargs)
        self._window = window_secs
        self._buckets: dict[str, list[tuple[float, float]]] = collections.defaultdict(list)

    def _evict(self, now: float):
        cutoff = now - self._window
        self._buckets = {
            k: [(t, v) for t, v in vals if t >= cutoff]
            for k, vals in self._buckets.items()
        }

    def process(self, context, event):
        now = time.time()
        self._evict(now)
        for k, v in event.items():
            if isinstance(v, (int, float)):
                self._buckets[k].append((now, float(v)))
        return event


class SummaryStatsProcessor(bspump.Processor):
    PERCENTILES = [50, 95, 99]

    def process(self, context, event):
        summary = {}
        for field, samples in event.get("_window_samples", {}).items():
            vals = [v for _, v in samples]
            if not vals:
                continue
            summary[f"{field}.mean"] = statistics.mean(vals)
            summary[f"{field}.max"]  = max(vals)
            summary[f"{field}.min"]  = min(vals)
            if len(vals) >= 2:
                summary[f"{field}.stdev"] = statistics.stdev(vals)
            sorted_vals = sorted(vals)
            for p in self.PERCENTILES:
                idx = max(0, int(len(sorted_vals) * p / 100) - 1)
                summary[f"{field}.p{p}"] = sorted_vals[idx]
        return {**event, "summary": summary} if summary else event
"""

_AGG_SCHEMA = """\
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "AggregatedMetric",
  "type": "object",
  "required": ["source", "window_start", "window_end", "summary"],
  "properties": {
    "source":        {"type": "string"},
    "window_start":  {"type": "number"},
    "window_end":    {"type": "number"},
    "summary": {
      "type": "object",
      "additionalProperties": {"type": "number"}
    }
  }
}
"""


# ── Stats tracking ────────────────────────────────────────────────────────────


@dataclass
class Stats:
    label: str
    latencies: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def record(self, latency: float):
        self.latencies.append(latency)

    def error(self, msg: str):
        self.errors.append(msg)

    def report(self) -> str:
        n = len(self.latencies)
        if n == 0:
            return f"  {self.label}: 0 requests"
        sorted_l = sorted(self.latencies)
        p = lambda pct: sorted_l[max(0, int(n * pct / 100) - 1)]  # noqa: E731
        err_rate = 100 * len(self.errors) / (n + len(self.errors)) if self.errors else 0
        return (
            f"  {self.label}: n={n}  "
            f"mean={statistics.mean(sorted_l) * 1000:.0f}ms  "
            f"p50={p(50) * 1000:.0f}ms  "
            f"p95={p(95) * 1000:.0f}ms  "
            f"p99={p(99) * 1000:.0f}ms  "
            f"max={max(sorted_l) * 1000:.0f}ms  "
            f"errors={len(self.errors)} ({err_rate:.1f}%)"
        )


# ── HTTP helpers ──────────────────────────────────────────────────────────────


async def api_get(client: httpx.AsyncClient, path: str, stats: Stats, **kwargs):
    t0 = time.perf_counter()
    try:
        r = await client.get(path, **kwargs)
        stats.record(time.perf_counter() - t0)
        if r.status_code >= 400:
            stats.error(f"GET {path} → {r.status_code}: {r.text[:120]}")
        return r
    except Exception as exc:
        stats.error(f"GET {path} → {exc}")
        return None


async def api_post(client: httpx.AsyncClient, path: str, stats: Stats, **kwargs):
    t0 = time.perf_counter()
    try:
        r = await client.post(path, **kwargs)
        stats.record(time.perf_counter() - t0)
        if r.status_code >= 400:
            stats.error(f"POST {path} → {r.status_code}: {r.text[:120]}")
        return r
    except Exception as exc:
        stats.error(f"POST {path} → {exc}")
        return None


async def api_delete(client: httpx.AsyncClient, path: str, stats: Stats):
    t0 = time.perf_counter()
    try:
        r = await client.delete(path)
        stats.record(time.perf_counter() - t0)
        if r.status_code >= 400:
            stats.error(f"DELETE {path} → {r.status_code}: {r.text[:120]}")
        return r
    except Exception as exc:
        stats.error(f"DELETE {path} → {exc}")
        return None


# ── Test phases ───────────────────────────────────────────────────────────────


async def phase_rapid_list(
    client: httpx.AsyncClient, rounds: int, concurrency: int
) -> Stats:
    """Fire GET /automations/ as fast as possible."""
    print(
        f"\n[Phase 1] Rapid-fire GET /automations/ — {rounds} requests, concurrency {concurrency}"
    )
    stats = Stats("GET /automations/")
    sem = asyncio.Semaphore(concurrency)

    async def one():
        async with sem:
            await api_get(client, "/automations/", stats)

    t0 = time.perf_counter()
    await asyncio.gather(*[one() for _ in range(rounds)])
    elapsed = time.perf_counter() - t0
    print(f"  Done in {elapsed:.2f}s  ({rounds / elapsed:.1f} req/s)")
    return stats


async def phase_create(
    client: httpx.AsyncClient,
    deployment_ids: list[str],
    concurrency: int,
) -> tuple[Stats, dict[str, str]]:
    """Create automations concurrently; return stats and {deployment_id: checksum}."""
    print(
        f"\n[Phase 2] Creating {len(deployment_ids)} automations, concurrency {concurrency}"
    )
    stats = Stats("POST /automations/{id} (create)")
    sem = asyncio.Semaphore(concurrency)
    checksums: dict[str, str] = {}
    lock = asyncio.Lock()

    import random as _rnd

    async def create_one(dep_id: str, template: str):
        pid = str(uuid.uuid4())
        files = _make_automation(template, pid)
        checksum = compute_checksum(files)
        archive = build_tar_gz(files)
        async with sem:
            r = await api_post(
                client,
                f"/automations/{dep_id}",
                stats,
                files={"file": (f"{dep_id}.tar.gz", archive, "application/gzip")},
                data={"checksum": checksum, "relative_path": f"stress/{dep_id}"},
            )
        if r is not None and r.status_code < 400:
            async with lock:
                checksums[dep_id] = checksum
            print(f"  ✓ created {dep_id} ({template})")
        else:
            print(f"  ✗ failed  {dep_id}")

    templates = [TEMPLATES[i % len(TEMPLATES)] for i in range(len(deployment_ids))]
    await asyncio.gather(
        *[create_one(dep_id, tmpl) for dep_id, tmpl in zip(deployment_ids, templates)]
    )
    print(f"  Created {len(checksums)}/{len(deployment_ids)}")
    return stats, checksums


async def phase_deploy(
    client: httpx.AsyncClient,
    checksums: dict[str, str],
    concurrency: int,
) -> tuple[Stats, dict[str, str]]:
    """Fire deploy for each created automation; return task_ids."""
    print(
        f"\n[Phase 3] Deploying {len(checksums)} automations, concurrency {concurrency}"
    )
    stats = Stats("POST /automations/{id}/deploy")
    sem = asyncio.Semaphore(concurrency)
    task_ids: dict[str, str] = {}
    lock = asyncio.Lock()

    async def deploy_one(dep_id: str, checksum: str):
        async with sem:
            r = await api_post(
                client,
                f"/automations/{dep_id}/deploy",
                stats,
                data={
                    "checksum": checksum,
                    "stage": "dev",
                    "relative_path": f"stress/{dep_id}",
                },
            )
        if r is not None and r.status_code in (200, 202):
            body = r.json()
            task_id = body.get("task_id")
            if task_id:
                async with lock:
                    task_ids[dep_id] = task_id
            print(f"  ✓ deployed {dep_id}  task={task_id}")
        else:
            print(f"  ✗ failed   {dep_id}")

    await asyncio.gather(*[deploy_one(dep_id, cs) for dep_id, cs in checksums.items()])
    return stats, task_ids


async def phase_poll_deploy_status(
    client: httpx.AsyncClient,
    task_ids: dict[str, str],
    timeout: float = 60.0,
) -> Stats:
    """Poll deploy-status until all tasks complete or timeout."""
    print(
        f"\n[Phase 4] Polling deploy-status for {len(task_ids)} tasks (timeout {timeout}s)"
    )
    stats = Stats("GET /automations/deploy-status/{id}")
    pending = dict(task_ids)
    deadline = time.perf_counter() + timeout

    while pending and time.perf_counter() < deadline:
        for dep_id in list(pending):
            task_id = pending[dep_id]
            r = await api_get(client, f"/automations/deploy-status/{task_id}", stats)
            if r and r.status_code == 200:
                body = r.json()
                status = body.get("status")
                if status in ("completed", "failed"):
                    mark = "✓" if status == "completed" else "✗"
                    print(f"  {mark} {dep_id}: {status}")
                    pending.pop(dep_id)
        if pending:
            await asyncio.sleep(2)

    if pending:
        print(f"  ⚠ Timed out waiting for: {list(pending)}")
    return stats


async def phase_mixed_ops(
    client: httpx.AsyncClient,
    deployment_ids: list[str],
    rounds: int,
    concurrency: int,
) -> list[Stats]:
    """Mix of inspect, history, list, start, stop operations."""
    print(
        f"\n[Phase 5] Mixed ops — {rounds} rounds against {len(deployment_ids)} automations"
    )

    stat_list = Stats("GET /automations/ (mixed)")
    stat_inspect = Stats("GET /automations/{id}/inspect")
    stat_history = Stats("GET /automations/{id}/history")
    stat_stop = Stats("POST /automations/{id}/stop")
    stat_restart = Stats("POST /automations/{id}/restart")

    sem = asyncio.Semaphore(concurrency)

    import random as _rnd

    async def one_round(_):
        dep_id = _rnd.choice(deployment_ids)
        ops = _rnd.choices(
            ["list", "inspect", "history", "stop", "restart"],
            weights=[4, 3, 2, 1, 1],
            k=1,
        )[0]
        async with sem:
            if ops == "list":
                await api_get(client, "/automations/", stat_list)
            elif ops == "inspect":
                await api_get(client, f"/automations/{dep_id}/inspect", stat_inspect)
            elif ops == "history":
                await api_get(client, f"/automations/{dep_id}/history", stat_history)
            elif ops == "stop":
                await api_post(client, f"/automations/{dep_id}/stop", stat_stop)
            elif ops == "restart":
                await api_post(client, f"/automations/{dep_id}/restart", stat_restart)

    await asyncio.gather(*[one_round(i) for i in range(rounds)])
    return [stat_list, stat_inspect, stat_history, stat_stop, stat_restart]


async def phase_cleanup(
    client: httpx.AsyncClient,
    deployment_ids: list[str],
    concurrency: int,
) -> Stats:
    """Delete all test automations."""
    print(f"\n[Phase 6] Cleanup — deleting {len(deployment_ids)} automations")
    stats = Stats("DELETE /automations/{id}")
    sem = asyncio.Semaphore(concurrency)

    async def del_one(dep_id: str):
        async with sem:
            r = await api_delete(client, f"/automations/{dep_id}", stats)
        mark = "✓" if (r and r.status_code < 400) else "✗"
        print(f"  {mark} deleted {dep_id}")

    await asyncio.gather(*[del_one(d) for d in deployment_ids])
    return stats


async def phase_final_list(
    client: httpx.AsyncClient, rounds: int, concurrency: int
) -> Stats:
    """Rapid-fire list after all the churning."""
    print(f"\n[Phase 7] Final rapid-fire GET /automations/ — {rounds} requests")
    stats = Stats("GET /automations/ (final)")
    sem = asyncio.Semaphore(concurrency)

    async def one():
        async with sem:
            await api_get(client, "/automations/", stats)

    t0 = time.perf_counter()
    await asyncio.gather(*[one() for _ in range(rounds)])
    elapsed = time.perf_counter() - t0
    print(f"  Done in {elapsed:.2f}s  ({rounds / elapsed:.1f} req/s)")
    return stats


# ── Entrypoint ────────────────────────────────────────────────────────────────


async def run(args):
    url = args.url.rstrip("/")
    token = args.token
    conc = args.concurrency

    # Generate unique-per-run deployment IDs so parallel runs don't clash.
    run_id = uuid.uuid4().hex[:6]
    deployment_ids = [f"stress-{run_id}-{i:03d}" for i in range(args.automations)]

    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(
        base_url=url,
        headers=headers,
        timeout=httpx.Timeout(30.0),
    ) as client:
        # Smoke test
        r = await client.get("/automations/")
        if r.status_code == 401:
            raise SystemExit("Auth failed — check --token / BITSWAN_GITOPS_SECRET")
        print(f"Connected to {url}  (existing automations: {len(r.json())})")

        all_stats: list[Stats] = []

        # 1 — rapid list before anything
        all_stats.append(await phase_rapid_list(client, args.list_rounds, conc))

        # 2 — create
        s_create, checksums = await phase_create(client, deployment_ids, conc)
        all_stats.append(s_create)

        if not checksums:
            print("\nNo automations created — skipping deploy/ops/cleanup phases.")
        else:
            created_ids = list(checksums)

            # 3 — deploy (async 202, doesn't wait for Docker)
            s_deploy, task_ids = await phase_deploy(client, checksums, conc)
            all_stats.append(s_deploy)

            # 4 — poll deploy status
            if task_ids:
                all_stats.append(
                    await phase_poll_deploy_status(
                        client, task_ids, timeout=args.deploy_timeout
                    )
                )

            # 5 — mixed ops
            all_stats.extend(
                await phase_mixed_ops(client, created_ids, args.mixed_rounds, conc)
            )

            # 6 — cleanup
            all_stats.append(await phase_cleanup(client, created_ids, conc))

        # 7 — rapid list after
        all_stats.append(await phase_final_list(client, args.list_rounds, conc))

    print("\n" + "=" * 72)
    print("RESULTS")
    print("=" * 72)
    for s in all_stats:
        if s.latencies or s.errors:
            print(s.report())
            for e in s.errors[:5]:
                print(f"    error: {e}")
            if len(s.errors) > 5:
                print(f"    ... and {len(s.errors) - 5} more errors")
    print("=" * 72)


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--url", default=os.environ.get("BITSWAN_GITOPS_URL", "http://localhost:8079")
    )
    p.add_argument("--token", default=os.environ.get("BITSWAN_GITOPS_SECRET", ""))
    p.add_argument(
        "--concurrency", type=int, default=8, help="max concurrent requests (default 8)"
    )
    p.add_argument(
        "--automations",
        type=int,
        default=12,
        help="number of test automations to create (default 12)",
    )
    p.add_argument(
        "--list-rounds",
        type=int,
        default=60,
        help="rapid-fire list rounds (default 60)",
    )
    p.add_argument(
        "--mixed-rounds", type=int, default=80, help="mixed-ops rounds (default 80)"
    )
    p.add_argument(
        "--deploy-timeout",
        type=float,
        default=45.0,
        help="seconds to wait for deploys (default 45)",
    )
    args = p.parse_args()

    if not args.token:
        p.error("--token is required (or set BITSWAN_GITOPS_SECRET)")

    asyncio.run(run(args))


if __name__ == "__main__":
    main()

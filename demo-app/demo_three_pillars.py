#!/usr/bin/env python3
import json, logging, random, time, os
from logging.handlers import RotatingFileHandler
from datetime import datetime

# ----- OTEL Metrics -----
from opentelemetry import metrics, trace
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# ----- OTEL Traces -----
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ---------------------------
# Config
# ---------------------------
OTLP_ENDPOINT = "http://127.0.0.1:4317"  # Collector gRPC
LOG_PATH = "/var/tmp/demo_app.log"        # Collector filelog receiver tails this

# ---------------------------
# Resource
# ---------------------------
resource = Resource.create({
    "service.name": "demo-three-pillars",
    "service.instance.id": "host1"
})

# ---------------------------
# Metrics Setup
# ---------------------------
metric_exporter = OTLPMetricExporter(endpoint=OTLP_ENDPOINT, insecure=True)
metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=5000)
metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(metric_provider)
meter = metrics.get_meter("demo.meter")

# Global-ish state
packets_in_total = 0
packets_out_total = 0

# Observable totals
def packets_in_cb(options):
    return [Observation(packets_in_total, {"hostname": "host1"})]

def packets_out_cb(options):
    return [Observation(packets_out_total, {"hostname": "host1"})]

meter.create_observable_counter("packets_in_total", callbacks=[packets_in_cb], description="Total inbound packets")
meter.create_observable_counter("packets_out_total", callbacks=[packets_out_cb], description="Total outbound packets")

# Packet rate (pps)
last_rate_calc = time.time()
last_total = 0

def packet_rate_cb(options):
    global last_rate_calc, last_total
    now = time.time()
    elapsed = max(0.001, now - last_rate_calc)
    current = packets_in_total + packets_out_total
    rate = (current - last_total) / elapsed
    last_total = current
    last_rate_calc = now
    return [Observation(rate, {"hostname": "host1"})]

meter.create_observable_gauge("packet_rate", callbacks=[packet_rate_cb], description="Packets per second (approx)")

# Per-source/destination gauges
sources = [f"source{i}" for i in range(1, 6)]
destinations = [f"dest{i}" for i in range(1, 3)]
src_state = {s: {"packets": 0, "bytes": 0, "last": 0} for s in sources}
dst_state = {d: {"packets": 0, "bytes": 0} for d in destinations}

def src_gauge_cb(options):
    # update and report packets/bytes/last per source
    now = int(time.time())
    out = []
    for s in sources:
        add_pk = random.randint(50, 200)
        add_by = add_pk * random.randint(64, 1200)
        src_state[s]["packets"] += add_pk
        src_state[s]["bytes"] += add_by
        src_state[s]["last"] = now
        out.append(Observation(src_state[s]["packets"], {"hostname":"host1","src":s,"metric":"packets"}))
        out.append(Observation(src_state[s]["bytes"],   {"hostname":"host1","src":s,"metric":"bytes"}))
        out.append(Observation(src_state[s]["last"],    {"hostname":"host1","src":s,"metric":"last"}))
    return out

def dst_gauge_cb(options):
    out = []
    for d in destinations:
        add_pk = random.randint(40, 160)
        add_by = add_pk * random.randint(64, 1200)
        dst_state[d]["packets"] += add_pk
        dst_state[d]["bytes"] += add_by
        out.append(Observation(dst_state[d]["packets"], {"hostname":"host1","dst":d,"metric":"packets"}))
        out.append(Observation(dst_state[d]["bytes"],   {"hostname":"host1","dst":d,"metric":"bytes"}))
    return out

meter.create_observable_gauge("src_stats", callbacks=[src_gauge_cb], description="Per-source packets/bytes/last")
meter.create_observable_gauge("dst_stats", callbacks=[dst_gauge_cb], description="Per-destination packets/bytes")

# ---------------------------
# Tracing Setup
# ---------------------------
trace_provider = TracerProvider(resource=resource)
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer("demo.tracer")
span_exporter = OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True)
trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))

# ---------------------------
# Logging (JSON to file)
# ---------------------------
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logger = logging.getLogger("demo.logger")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=2)
handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(handler)

def log_json(record: dict):
    handler.acquire()
    try:
        logger.info(json.dumps(record, separators=(",", ":")))
    finally:
        handler.release()

# ---------------------------
# Main loop
# ---------------------------
print("Demo app running: metrics+traces via OTLP, logs => /var/tmp/demo_app.log  (every ~5-10s)")
while True:
    # Simulate work burst
    req_count = random.randint(3, 8)

    with tracer.start_as_current_span("process_batch") as root:
        root.set_attribute("hostname", "host1")
        batch_latency_ms = 0

        for _ in range(req_count):
            src = random.choice(sources)
            dst = random.choice(destinations)
            size = random.randint(200, 4000)
            status = random.choice(["ok", "ok", "ok", "error"])  # ~25% errors for demo

            t0 = time.time()
            with tracer.start_as_current_span("ingest"):
                time.sleep(random.random() * 0.02)
            with tracer.start_as_current_span("transform"):
                time.sleep(random.random() * 0.02)
            with tracer.start_as_current_span("store"):
                time.sleep(random.random() * 0.02)
            latency_ms = int((time.time() - t0) * 1000)
            batch_latency_ms += latency_ms

            # update packet totals
            packets_in_total  += size // 512
            packets_out_total += size // 512

            # correlate logs with current trace
            ctx = trace.get_current_span().get_span_context()
            trace_id = f"{ctx.trace_id:032x}" if ctx and ctx.trace_id else None

            log_json({
                "ts": datetime.utcnow().isoformat() + "Z",
                "level": "INFO" if status == "ok" else "ERROR",
                "hostname": "host1",
                "src": src,
                "dst": dst,
                "bytes": size,
                "latency_ms": latency_ms,
                "status": status,
                "trace_id": trace_id,
                "msg": "processed message"
            })

        root.set_attribute("batch.size", req_count)
        root.set_attribute("batch.latency_ms", batch_latency_ms)

    time.sleep(5)

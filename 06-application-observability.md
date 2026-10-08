# Module 6 — Application Observability (13% of the exam)

## Learning objectives

By the end of this module you will be able to:

- Identify the key producer and consumer metrics to monitor.
- Explain consumer lag and why it's the single most important consumer health signal.
- Use JMX to expose Kafka client metrics.
- Recognize common troubleshooting signals and what they usually indicate.

## Guided walkthrough

### 1. Why observability matters for a Kafka developer

The exam frames observability as an application-development concern, not a cluster-ops
concern: as a developer, you're responsible for making sure your producer/consumer/
Streams application exposes enough signal to detect and diagnose problems — without
relying on someone else digging through raw logs.

### 2. Key producer metrics

| Metric | What it tells you |
|---|---|
| `record-send-rate` | Throughput — messages sent per second |
| `record-error-rate` | Rate of failed sends (after all retries) |
| `request-latency-avg` | Average time for a produce request to complete |
| `compression-rate-avg` | Effectiveness of compression (see [Module 7](07-kafka-advanced.md)) |
| `batch-size-avg` | Average batch size — low values may indicate `linger.ms` is too low |
| `buffer-available-bytes` | Free producer buffer memory — approaching zero risks blocking `send()` |

### 3. Key consumer metrics

| Metric | What it tells you |
|---|---|
| `records-consumed-rate` | Throughput — records consumed per second |
| `records-lag` / `records-lag-max` | **Consumer lag** — how far behind the consumer is from the latest offset |
| `commit-latency-avg` | Time taken to commit offsets |
| `rebalance-rate-per-hour` | How often the consumer group is rebalancing |
| `fetch-latency-avg` | Time taken to fetch a batch of records from the broker |

**Consumer lag** is the most important signal: it's the difference between the latest
produced offset and the consumer's current committed offset for a partition. Rising lag
means the consumer can't keep up with production — a leading indicator of processing
backlog before it becomes a bigger operational problem.

### 4. Exposing metrics via JMX

Kafka clients expose metrics via JMX (Java Management Extensions) automatically. Each
metric lives under an MBean, for example:

```
kafka.producer:type=producer-metrics,client-id=<client-id>
kafka.consumer:type=consumer-fetch-manager-metrics,client-id=<client-id>
kafka.consumer:type=consumer-coordinator-metrics,client-id=<client-id>
```

In practice, these are usually scraped by a metrics agent (e.g., a JMX exporter feeding
Prometheus) and visualized on dashboards, rather than read directly — but you should
know these MBean names exist and what they expose, since the exam tests recognition of
this mechanism.

### 5. Broker-side signals worth recognizing

Even though the exam frames observability from the application side, a few broker-level
metrics are worth recognizing because they explain symptoms your client-side metrics
will show:

| Metric | What it tells you | Watch for |
|---|---|---|
| `UnderReplicatedPartitions` | Partitions where a follower has fallen behind the leader | `> 0` for extended periods |
| `OfflinePartitionsCount` | Partitions with no available leader | `> 0` (critical — affects producers/consumers immediately) |
| `ActiveControllerCount` | Number of active controllers in the cluster | Anything other than `1` (critical) |
| `RequestHandlerAvgIdlePercent` | Broker request-handling thread pool utilization | `< 20%` (broker is saturated) |
| `ProduceTotalTimeMs` / `FetchConsumerTotalTimeMs` | Broker-side produce/fetch request latency | Rising above baseline |

Like client metrics, these are exposed via JMX; common tooling choices include
Prometheus (often paired with Grafana dashboards), Datadog, New Relic, and the ELK
stack. Client-side metrics often reveal a problem (e.g., rising producer latency)
*before* broker-side metrics do, which is why the exam emphasizes instrumenting your
own application first.

### 6. Kafka cluster operations (awareness only)

The exam explicitly puts cluster administration out of scope (see
the [Introduction](README.md)), but recognizing that these operational activities
exist — and that they can explain transient client-side symptoms like a rebalance or a
latency spike — is useful context: rolling broker restarts, updating broker
configuration, rebalancing partitions across brokers, changing replication factor, and
adding/replacing/removing a broker. If your producer or consumer metrics show a sudden,
short-lived anomaly, checking whether a cluster operation was in progress is a
reasonable first troubleshooting step, even though performing that operation isn't an
exam objective for developers.

### 7. Application-level logging and metrics

Beyond client-library metrics, instrument your own application code with:

- **Structured logs** for produce/consume success, failure, retry, and skip events —
  avoid logging unbounded-cardinality fields (e.g., raw customer IDs) as metric labels;
  keep those in log messages instead.
- **Custom business metrics**, e.g., "orders processed per minute," alongside the
  Kafka-level metrics, so you can correlate business impact with Kafka-level health.

### 8. Common troubleshooting signals

| Symptom | Likely cause | Where to look |
|---|---|---|
| Rising consumer lag | Slow processing, too few consumer instances, GC pauses | `records-lag-max`, processing time per record |
| Frequent rebalances | `max.poll.interval.ms` too low for processing time, consumer crashes | `rebalance-rate-per-hour`, application logs around crash times |
| High producer `record-error-rate` | Broker unavailability, message too large, authorization failure | Producer exception logs, broker-side error responses |
| Producer blocking on `send()` | Buffer memory exhausted (`buffer.memory`) | `buffer-available-bytes` |
| Duplicate records downstream | At-least-once semantics without idempotent processing | Application-level dedupe logic, delivery semantics config (Module 2) |

## Self-check questions

1. What is consumer lag, and why is it considered the most important consumer health
   metric?
2. Name three producer metrics and what each one indicates.
3. Where do Kafka Java clients expose their metrics by default?
4. If you see a rising `rebalance-rate-per-hour`, what are two likely root causes to
   investigate?
5. Why should you avoid using raw customer or transaction IDs as metric labels?
6. What does a non-zero `OfflinePartitionsCount` mean, and why is it critical?
7. Why does the exam still expect awareness of cluster operations like rolling
   restarts, even though performing them is out of scope for developers?

## Answer key

1. Consumer lag is the gap between the latest produced offset and the consumer's
   committed offset; it's the earliest, clearest signal that a consumer can't keep up
   with production, before it turns into a bigger backlog problem.
2. Examples: `record-send-rate` (throughput), `record-error-rate` (failed sends after
   retries), `request-latency-avg` (produce request latency).
3. Via JMX MBeans (e.g., `kafka.producer:type=producer-metrics,client-id=<id>`).
4. `max.poll.interval.ms` set too low relative to actual processing time, or consumers
   crashing/restarting frequently.
5. Metric labels should have bounded cardinality; raw IDs create unbounded label sets
   that blow up storage/cardinality in metrics systems — put such details in structured
   logs instead.
6. It means at least one partition has no available leader — producers and consumers
   for that partition cannot proceed at all until a new leader is elected, making it a
   critical, immediate-impact signal.
7. Because a cluster operation in progress (e.g., a rolling restart or partition
   rebalance) is a common, benign explanation for a transient anomaly a developer will
   see in their own client-side metrics (a latency spike, a brief rebalance) — knowing
   these operations exist helps you correctly diagnose the symptom without needing to
   perform the operation yourself.

Continue to [Module 7 — Kafka Advanced](07-kafka-advanced.md).

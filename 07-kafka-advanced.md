# Module 7 — Kafka Advanced

This module summarizes advanced topic, producer, and consumer internals that extend
what you learned in Modules 1–2, sourced from Conduktor's "Kafka Topics Advanced",
"Kafka Producers Advanced", and "Kafka Consumers Advanced" reference series
(conduktor.io/kafka). These topics deepen your understanding of *why* certain producer
and consumer configuration choices behave the way they do — valuable for the tuning and
troubleshooting objectives across the exam blueprint.

## Learning objectives

By the end of this module you will be able to:

- Explain how partitions are physically stored as segments and indexes on disk.
- Choose replication factor and partition count using concrete sizing guidance.
- Explain the durability/availability trade-offs of `min.insync.replicas` and unclean
  leader election.
- Choose a compression algorithm and batching strategy based on throughput/latency needs.
- Explain how idempotent producers prevent duplicates, and their limitations.
- Explain the consumer poll/heartbeat thread model and how it drives rebalances.
- Explain incremental cooperative rebalancing and static group membership.

## Part A — Kafka Topics Advanced

### Segments and indexes

A partition is not one giant file — it's physically split into **segments**, each
capped by size (`segment.bytes`, default 1 GB) or age (`segment.ms`, default 7 days),
whichever comes first. Only one segment per partition — the **active** segment — is
open for writes at any time; a segment must be closed before it's eligible for deletion
or compaction.

For fast lookups, Kafka maintains two indexes per segment:

| Index | Maps | Used for |
|---|---|---|
| Offset index | Offset → byte position in the segment file | Fast lookup of a message by offset |
| Time index | Timestamp → nearest offset | Time-based seeking (e.g., "start consuming from 1 hour ago") |

**Operational implication:** a broker keeps an open file handle for every segment in
every partition, including inactive ones — very small segments (aggressive
`segment.bytes`/`segment.ms`) can exhaust OS file-handle limits.

### Replication factor and partition count sizing

**Replication factor** trades durability for storage/network cost:

- With replication factor `N`, you tolerate `N-1` broker failures without data loss.
- Production guidance: minimum 3, never 1. Use 5 only for the most critical data.
- Recommended starting point: replication factor 3 with `min.insync.replicas=2`.

**Partition count** trades parallelism for management overhead:

- Consumer parallelism within a group is capped at the number of partitions.
- Rough sizing: `partitions ≥ target throughput / single-partition throughput`.
- Starting guidance: 6–12 partitions for most new topics, scaling with observed load.
- Partitions **cannot be decreased** after creation, and changing partition count
  changes the `hash(key) % partitions` mapping — breaking existing per-key ordering
  and requiring care during any resize.

### min.insync.replicas and acks=all

`min.insync.replicas` sets the floor for how many in-sync replicas must acknowledge a
write for it to succeed when the producer uses `acks=all`. With replication factor 3:

| Configuration | Broker failures tolerated | Durability |
|---|---|---|
| `min.insync.replicas=1` | 2 | Low (default) |
| `min.insync.replicas=2` | 1 | Recommended for production |
| `min.insync.replicas=3` | 0 | Maximum, no fault tolerance |

Formula: with `acks=all`, replication factor `N`, and `min.insync.replicas=M`, you can
tolerate `N-M` broker failures while remaining **available for writes**. If fewer than
`M` replicas are in-sync, producers receive `NotEnoughReplicasException`.

### Unclean leader election

When a partition's leader fails and **no ISR member is available** to take over, Kafka
must choose between two behaviors, controlled by `unclean.leader.election.enable`:

| Setting | Behavior | Risk |
|---|---|---|
| `false` (default, recommended) | Wait for an ISR to come back online | Availability loss (topic temporarily unavailable) |
| `true` | Promote an out-of-sync replica to leader | Data loss and inconsistency |

Keep this disabled for financial transactions, order processing, and audit logs. It may
be reasonable to enable for metrics or log aggregation topics where availability
matters more than completeness.

### Log retention vs. log compaction

Two cleanup policies apply to closed segments:

- **`delete`** (default) — removes segments older than `retention.ms` or beyond
  `retention.bytes`. These are minimum guarantees, not hard limits — the active segment
  never counts toward the byte limit.
- **`compact`** — retains only the latest value per key; used for the internal
  `__consumer_offsets` topic and useful for "current state"/changelog-style topics (see
  Kafka Streams state stores in [Module 3](03-kafka-streams.md)). Compaction preserves
  ordering and never renumbers offsets — it just skips over removed ones. A `null`
  value for a key acts as a **tombstone**, deleting that key, and tombstones themselves
  are cleaned up after `log.cleaner.delete.retention.ms` (default 24 hours).

### Large messages

Kafka's default size limits are all 1 MB by default, chained across the pipeline:
producer `max.request.size`, broker `message.max.bytes`, topic `max.message.bytes`
(inherits from broker), and consumer `max.partition.fetch.bytes`. All of these must be
raised together to support larger messages. Prefer alternatives before raising limits:
compress the payload, store large payloads externally (e.g., object storage) and send
only a reference, or split large payloads into chunks.

### Topic naming conventions

Pick one consistent pattern (kebab-case, snake_case, or dot-hierarchy, e.g.
`domain.event-type.version`) and stick to it. Avoid environment names or versions baked
into topic names — use separate clusters per environment and Schema Registry for
schema versioning instead of topic-name versioning (`orders-v2`). Reserved: names
starting with `__` (internal topics).

## Part B — Kafka Producers Advanced

### acks revisited

| Setting | Waits for | Data loss risk |
|---|---|---|
| `acks=0` | Nothing (fire-and-forget) | High — no confirmation at all |
| `acks=1` | Leader only | Data loss if leader fails before replicating |
| `acks=all` | All in-sync replicas (per `min.insync.replicas`) | Lowest — only if all ISRs fail simultaneously |

Default changed in Kafka 3.0: `acks=1` for Kafka < 3.0, `acks=all` for Kafka ≥ 3.0.

### Retries and error classification

Kafka 3.0+ defaults to `retries=Integer.MAX_VALUE` with idempotency enabled. Retries
only help for **retryable** errors (`TimeoutException`, `NotEnoughReplicasException`,
`LeaderNotAvailableException`, `NetworkException`); **non-retryable** errors
(`RecordTooLargeException`, `SerializationException`, `AuthorizationException`) will
fail identically on every retry and must be handled explicitly by the application. The
overall time budget is governed by `delivery.timeout.ms`, which must be
`>= request.timeout.ms + (retries × retry.backoff.ms)`.

Retries can reorder messages unless you set `max.in.flight.requests.per.connection=1`,
or (preferred) enable `enable.idempotence=true`, which allows up to 5 in-flight
requests while preserving order.

### Idempotent producers

`enable.idempotence=true` (default since Kafka 3.0) prevents duplicate writes caused by
retries. Kafka assigns each producer a **Producer ID (PID)** and tags every message with
a **sequence number** per topic-partition; the broker discards messages with a
sequence number it has already seen. Idempotency automatically forces
`acks=all`, `retries=Integer.MAX_VALUE`, and
`max.in.flight.requests.per.connection<=5`.

**Limitation:** idempotency is scoped to a single producer session — if the producer
process restarts, it gets a new PID and sequence numbers reset to zero, so duplicates
across restarts are still possible. Cross-session, cross-partition exactly-once requires
Kafka transactions (as used by Kafka Streams' `EXACTLY_ONCE_V2`, see
[Module 3](03-kafka-streams.md)).

### Compression

| Algorithm | Compression ratio | Speed | CPU cost | Best for |
|---|---|---|---|---|
| GZIP | Highest (~75%) | Slowest | High | Network-constrained environments |
| ZSTD | High (~72%) | Medium | Medium | Balanced choice (recommended default, Kafka 2.1+) |
| LZ4 | Medium (~65%) | Very fast | Very low | High-throughput, low-latency scenarios |
| Snappy | Medium (~68%) | Very fast | Low | High-throughput scenarios |

Compression operates at the **batch** level — larger batches (bigger `batch.size`,
higher `linger.ms`) compress more effectively. Remember consumers also pay a CPU cost to
decompress.

### Batching

Batches are sent when **any** of these trigger: `batch.size` reached, `linger.ms`
elapsed, the producer buffer needs space, or an explicit `flush()`/close. Key
parameters:

- `batch.size` (default 16 KB) — target max bytes per batch, per partition.
- `linger.ms` (default 0) — artificial delay to let a batch fill up before sending.
- `buffer.memory` (default 32 MB) — total memory across all partitions' batches; when
  exhausted, `send()` blocks until `max.block.ms`, then throws `TimeoutException`.

High-throughput configurations favor larger `batch.size`/`linger.ms`; low-latency
configurations favor `linger.ms=0` and smaller batches.

### Partitioners

- **Keyed messages**: `partition = hash(key) % numPartitions` — guarantees same-key
  messages land on the same partition (ordering), but a very frequent key creates a "hot
  partition."
- **Keyless messages (Kafka 2.4+)**: the **sticky partitioner** fills one partition's
  batch before switching, improving batch efficiency versus the old round-robin
  behavior (pre-2.4), which sent one message at a time to each partition in turn and
  produced smaller, less efficient batches.
- Changing partition count changes the `hash(key) % partitions` mapping for all
  existing keys — plan partition counts up front rather than resizing frequently.

## Part C — Kafka Consumers Advanced

### Delivery semantics recap

See [Module 2](02-application-development.md) for the full comparison table
(at-most-once / at-least-once / exactly-once) — this module adds the mechanics behind
*why* consumers behave this way.

### Poll and heartbeat threads

A consumer maintains group membership via two logical threads:

- **Heartbeat thread** — sends periodic heartbeats (`heartbeat.interval.ms`, default
  3s) to the group coordinator. If heartbeats stop for longer than
  `session.timeout.ms` (default 45s), the consumer is considered dead and a rebalance
  is triggered.
- **Poll thread** — your application code calling `.poll()`. If `.poll()` isn't called
  within `max.poll.interval.ms` (default 5 minutes), the consumer is also considered
  failed and a rebalance is triggered — this is why **blocking or slow processing
  inside the poll loop is a common cause of unwanted rebalances** (tune
  `max.poll.records` down or move heavy processing to a worker thread instead).

### auto.offset.reset

Controls where a consumer starts when there's no valid committed offset (new consumer
group, or the committed offset has expired past retention):

| Value | Behavior | Risk |
|---|---|---|
| `earliest` | Start from the beginning of the partition | Duplicate reprocessing |
| `latest` (default) | Start from the end — only new messages | Can skip/lose messages produced while consumer was down |
| `none` | Throw an exception if no offset exists | Requires explicit handling, but prevents silent data loss/reprocessing |

### Manual partition assignment and offset control

`auto.offset.reset` only applies within the automatic consumer-group model
(`subscribe()`), where Kafka's group coordinator assigns partitions for you. Some use
cases — a replay tool, an admin utility, a single-instance consumer that must own
every partition deterministically — need **full manual control** over which
partitions are read and from which offset, bypassing group coordination entirely.

**1. Manual partition assignment with `assign()`**

`assign()` is the alternative to `subscribe()`: the application specifies the exact
partitions to read, and no group coordinator, no rebalancing, and no
`group.id`-based partition assignment is involved. This trades automatic scaling for
full determinism.

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ManualAssignmentConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        // group.id is not required for assign()-based consumption

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition0 = new TopicPartition("orders-created", 0);
            TopicPartition partition1 = new TopicPartition("orders-created", 1);
            List<TopicPartition> partitions = List.of(partition0, partition1);

            consumer.assign(partitions); // explicit partitions, no group coordination

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition=%d offset=%d key=%s%n",
                            record.partition(), record.offset(), record.key());
                }
            }
        }
    }
}
```

**2. Seeking to the beginning, end, or a specific offset**

Once partitions are assigned (via `assign()`, or after partitions are handed to you in
a `ConsumerRebalanceListener`), `seek*()` methods reposition the consumer before the
next `poll()`:

```java
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

public class SeekExamples {

    static void repositionConsumer(KafkaConsumer<String, String> consumer,
                                    TopicPartition partition0, TopicPartition partition1) {
        // Replay a single partition from the very beginning
        consumer.seekToBeginning(List.of(partition0));

        // Skip partition1 forward to the latest offset (only new data)
        consumer.seekToEnd(List.of(partition1));

        // Jump to an exact, known offset — e.g., resuming from an externally
        // stored checkpoint (a database row, a file) instead of Kafka's
        // __consumer_offsets topic
        consumer.seek(partition0, 4200L);
    }
}
```

`seekToBeginning`/`seekToEnd` are evaluated lazily — the actual offset is only
resolved on the next `poll()` call, not immediately when `seek*()` is invoked.

**3. Seeking by timestamp**

`offsetsForTimes()` uses each segment's **time index** (see [Part A](#part-a--kafka-topics-advanced))
to find the earliest offset at or after a given timestamp — useful for "replay the
last hour" or "start from where an incident began" scenarios:

```java
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class SeekByTimestampExample {

    static void seekToOneHourAgo(KafkaConsumer<String, String> consumer,
                                  TopicPartition partition) {
        long oneHourAgoMs = Instant.now().minusSeconds(3600).toEpochMilli();

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(partition, oneHourAgoMs);

        Map<TopicPartition, OffsetAndTimestamp> result =
                consumer.offsetsForTimes(timestampsToSearch);

        OffsetAndTimestamp offsetAndTimestamp = result.get(partition);
        if (offsetAndTimestamp != null) {
            consumer.seek(partition, offsetAndTimestamp.offset());
        } else {
            // No message exists at or after that timestamp on this partition
            consumer.seekToEnd(java.util.List.of(partition));
        }
    }
}
```

**4. Committing specific offsets manually**

`commitSync()`/`commitAsync()` normally commit the offsets of the last batch returned
by `poll()`. Passing an explicit `Map<TopicPartition, OffsetAndMetadata>` instead lets
an application commit a **precise** offset per partition — for example, committing
after each individual record instead of each batch, which tightens the at-least-once
reprocessing window from Module 2:

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ManualOffsetCommitConsumer {

    static void processWithPerRecordCommit(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> record : records) {
                process(record); // application logic — must be idempotent (Module 1)

                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                // +1: the offset to resume from is the *next* record to read
                OffsetAndMetadata nextOffset = new OffsetAndMetadata(record.offset() + 1);

                consumer.commitSync(Collections.singletonMap(partition, nextOffset));
            }
        }
    }

    private static void process(ConsumerRecord<String, String> record) {
        System.out.println("Processing offset " + record.offset());
    }
}
```

Committing per-record trades throughput (many small commits) for a tighter recovery
window — on restart, at most one record is reprocessed per partition instead of an
entire batch. This is a direct application of the at-least-once trade-off from
[Module 2](02-application-development.md#8-delivery-semantics-recap-developers-perspective):
finer-grained commits reduce (but never eliminate) duplicate reprocessing.

### Incremental cooperative rebalancing and static membership

Pre-Kafka 2.4 ("eager") rebalancing stopped **all** consumers in a group and reassigned
**all** partitions on every rebalance — even if only one consumer left or joined.
Kafka 2.4+'s **cooperative sticky assignor**
(`partition.assignment.strategy=CooperativeStickyAssignor`) only revokes the specific
partitions that need to move, letting unaffected consumers keep processing —
dramatically cutting rebalance-induced downtime.

**Static group membership** (`group.instance.id`) gives a consumer a stable identity
across restarts, so a planned restart (e.g., a rolling deployment) doesn't trigger a
rebalance at all — the consumer simply resumes with the same partition assignment when
it reconnects within `session.timeout.ms`.

## Self-check questions

1. Why does a broker risk running out of file handles if segments are configured too
   small or too short-lived?
2. With replication factor 3 and `min.insync.replicas=2`, how many broker failures can
   the cluster tolerate while remaining available for writes with `acks=all`?
3. Why is `unclean.leader.election.enable=true` dangerous, and when might it still be
   acceptable?
4. What's the key difference between log retention (`delete`) and log compaction
   (`compact`)?
5. Why does idempotency alone not guarantee exactly-once across a producer restart?
6. What's the practical benefit of the sticky partitioner over the old round-robin
   partitioner for keyless messages?
7. What two conditions can each independently trigger a consumer group rebalance due to
   perceived consumer failure?
8. How does static group membership reduce unnecessary rebalances during rolling
   deployments?
9. Why does `assign()` not require a `group.id`, and what capability do you lose
   compared to `subscribe()`?
10. Why are `seekToBeginning()`/`seekToEnd()` described as "lazy," and what does that
    mean for when the repositioning actually happens?
11. What does `offsetsForTimes()` return if no message exists at or after the
    requested timestamp on a partition?
12. Why does committing a specific offset after every record (instead of after every
    batch) shrink — but not eliminate — the at-least-once reprocessing window?

## Answer key

1. Kafka keeps an open file handle for every segment in every partition, including
   inactive ones; smaller/shorter segments mean more segment files exist at once,
   multiplying open file handles and risking OS-level "too many open files" errors.
2. `N - M` = `3 - 2` = 1 broker failure.
3. It promotes an out-of-sync replica to leader, causing data loss/inconsistency; it
   may be acceptable for topics like metrics or log aggregation where availability
   matters more than completeness, but should stay disabled for financial or audit data.
4. `delete` removes old segments based on age/size; `compact` keeps only the latest
   value per key indefinitely (subject to tombstone cleanup), useful for changelog/
   current-state topics.
5. A producer restart gets a new Producer ID and resets sequence numbers to zero, so
   duplicates across restarts are possible; true cross-session exactly-once requires
   Kafka transactions.
6. It fills one partition's batch completely before switching to the next, producing
   larger, more efficient batches than round-robin's one-message-per-partition cycling.
7. (a) No heartbeat received within `session.timeout.ms`; (b) `.poll()` not called
   within `max.poll.interval.ms`.
8. A statically-assigned consumer (`group.instance.id`) keeps its partition assignment
   across a restart as long as it reconnects within `session.timeout.ms`, so a planned
   restart doesn't trigger a group-wide rebalance.
9. `assign()` bypasses the group coordinator entirely — the application specifies
   exact partitions instead of Kafka assigning them — so there's no group to join and
   no `group.id`-based coordination; the trade-off is losing automatic scaling and
   rebalancing across multiple consumer instances.
10. The actual offset lookup and repositioning only happens on the next `poll()` call,
    not at the moment `seekToBeginning()`/`seekToEnd()` is invoked — so code that reads
    `position()` immediately after seeking, without polling first, won't see the new
    position yet.
11. It returns `null` for that partition (no matching `OffsetAndTimestamp`), which the
    application must handle explicitly — e.g., falling back to `seekToEnd()`.
12. Because on restart, the consumer resumes from the last *committed* offset — with
    per-record commits, at most one record can be reprocessed per partition; with
    per-batch commits, the entire batch since the last commit can be reprocessed.
    Duplicates are still possible either way because processing and committing are not
    a single atomic operation (this is the same at-least-once trade-off from Module 2,
    just with a smaller blast radius).

Continue to [Module 8 — Kafka Streams Advanced](08-kafka-streams-advanced.md).

---
*Sources: Conduktor, "Kafka Topics Advanced", "Kafka Producers Advanced", and "Kafka
Consumers Advanced" (conduktor.io/kafka), accessed 2026-08-31.*

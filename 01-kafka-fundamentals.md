# Module 1 — Apache Kafka Fundamentals (23% of the exam)

## Learning objectives

By the end of this module you will be able to:

- Describe Kafka's core architecture: brokers, topics, partitions, offsets, and replication.
- Explain the role of the leader/follower model and in-sync replicas (ISR).
- Describe Kafka's approach to data retention and cleanup policies.
- Identify the main Kafka security mechanisms (authentication, authorization, encryption).
- Recognize the CLI tools and APIs a developer uses day-to-day.
- Distinguish retryable vs. non-retryable errors.
- Explain the purpose of message headers.
- Describe, at a high level, what Kafka Connect does.

## Guided walkthrough

### 1. Core architecture

Apache Kafka is a distributed, append-only log system built around a few core concepts:

- **Broker** — a Kafka server that stores data and serves client requests. A **cluster**
  is a group of brokers working together.
- **Topic** — a named stream of records (e.g., `orders-created`). Topics are the unit a
  producer writes to and a consumer reads from.
- **Partition** — a topic is split into one or more partitions. Each partition is an
  ordered, immutable sequence of records, identified by an **offset** (a sequential ID
  unique within that partition).
- **Offset** — the position of a record within a partition. Kafka guarantees ordering
  only *within* a partition, not across partitions of the same topic.

Because ordering is only guaranteed within a partition, **partition count** is a key
design decision: it determines the maximum consumer parallelism (one partition can be
read by at most one consumer within a consumer group at a time) and how ordering
guarantees apply to your data (e.g., all events for the same customer should share the
same partition if you need per-customer ordering).

### 2. Replication and the leader/follower model

Each partition has:

- One **leader** replica — all reads and writes for that partition go through the leader.
- Zero or more **follower** replicas — they replicate data from the leader.

The set of replicas that are caught up with the leader is called the **in-sync replica
set (ISR)**. If the leader fails, Kafka elects a new leader from the ISR. This is a
"clean" election because, by definition, ISR members have all committed data.

Replication factor determines how many copies of the data exist across the cluster.
With replication factor `N`, the cluster can tolerate up to `N-1` broker failures
without losing data (assuming those replicas were in-sync).

> We cover ISR-related producer/durability tuning (`acks`, `min.insync.replicas`,
> unclean leader election) in depth in [Module 7 — Kafka Advanced](07-kafka-advanced.md),
> since those are producer-facing configuration decisions built on this foundation.

### 3. Data retention

Kafka does not keep data forever by default. Each topic has a **cleanup policy**:

- `delete` (default for regular topics) — deletes messages older than the retention
  period (`retention.ms`, default 7 days) or once a size threshold is exceeded
  (`retention.bytes`).
- `compact` — keeps only the latest value per key (used internally for
  `__consumer_offsets`, and useful for "current state" topics like a changelog).

Retention is applied at the **segment** level: a partition's log is physically split
into segment files, and only closed (non-active) segments are eligible for deletion or
compaction.

### 4. Security basics

A production Kafka cluster typically layers three security mechanisms:

- **Authentication** — verifying client identity (e.g., SASL/SCRAM, mTLS).
- **Authorization** — controlling what an authenticated client can do (ACLs on topics,
  consumer groups, etc.).
- **Encryption** — protecting data in transit (TLS) and, less commonly, at rest.

As a developer, the exam expects you to know *that* these mechanisms exist and *how* a
client is configured to use them (e.g., setting `security.protocol`,
`sasl.mechanism`, and credentials in client properties) rather than how to operate a
broker's security configuration.

### 5. Tools and APIs

Day-to-day developer tools include:

- **CLI tools**: `kafka-topics`, `kafka-console-producer`, `kafka-console-consumer`,
  `kafka-consumer-groups`, `kafka-configs`.
- **Client APIs**: Producer API, Consumer API, Admin API (programmatic topic/config
  management), Streams API, Connect API.

The **Admin API** is worth calling out specifically since the exam blueprint mentions it
directly — it lets an application create/delete topics, alter configurations, and
describe cluster metadata programmatically instead of via the CLI.

### 6. Message headers

Kafka records can carry **headers** — key/value metadata pairs separate from the
record's key and value. Headers are commonly used for cross-cutting concerns like
tracing IDs, content-type hints, or routing metadata, without polluting the message
payload itself.

### 7. Error handling: retryable vs. non-retryable

Client applications need to distinguish:

- **Retryable errors** — transient conditions where retrying the same request might
  succeed (e.g., a leader election in progress, a temporary network blip).
- **Non-retryable errors** — permanent failures where retrying won't help (e.g., a
  message that's too large, a serialization failure, an authorization failure).

Correctly classifying an error determines whether your application should retry,
dead-letter the message, or fail fast. We'll revisit this concretely with producer
retry configuration in [Module 2](02-application-development.md).

### 8. Kafka Connect (preview)

Kafka Connect is a framework for moving data **into** Kafka (via **source**
connectors) and **out of** Kafka (via **sink** connectors) without writing custom
producer/consumer code. It also supports **Change Data Capture (CDC)** patterns, where
a source connector streams database change events into Kafka topics. We dedicate
[Module 4](04-kafka-connect.md) to this topic.

## Self-check questions

1. What is the difference between a topic, a partition, and an offset?
2. If replication factor is 3, how many broker failures can the cluster tolerate without
   losing data (assuming those brokers held in-sync replicas)?
3. What's the difference between the `delete` and `compact` cleanup policies?
4. Name the three security mechanisms typically layered on a Kafka cluster.
5. Give an example of a retryable error and a non-retryable error.
6. What is a Kafka message header used for, and how does it differ from the message
   key/value?

## Answer key

1. A **topic** is a named stream; it's divided into **partitions**, each an ordered
   append-only log; an **offset** is the position of a record within one partition.
2. 2 broker failures (`N-1` = `3-1` = 2).
3. `delete` removes records older than a time/size threshold; `compact` retains only the
   most recent value per key, regardless of age.
4. Authentication, authorization, and encryption.
5. Retryable: `LeaderNotAvailableException` (leader election in progress). Non-retryable:
   `RecordTooLargeException` (message exceeds size limit — retrying the same request
   will always fail the same way).
6. Headers carry metadata (e.g., tracing IDs) separate from the key/value payload,
   letting cross-cutting concerns travel with the message without changing its
   business content.

Continue to [Module 2 — Apache Kafka Application Development](02-application-development.md).

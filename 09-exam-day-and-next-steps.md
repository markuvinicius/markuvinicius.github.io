# Module 9 — Exam Day and Next Steps

## Learning objectives

By the end of this module you will be able to:

- Review your knowledge against the full exam blueprint, weighted by section.
- Apply a strategy for each exam question format.
- Identify official Confluent resources to fill any remaining gaps.

## Final review checklist (by blueprint weight)

Use this as a final gate before scheduling the exam — if you can't confidently explain
each bullet without notes, revisit the linked module.

### Section 1 — Apache Kafka Fundamentals (23%) — [Module 1](01-kafka-fundamentals.md)

- [ ] Topics, partitions, offsets, and how ordering guarantees work.
- [ ] Leader/follower replication and ISR.
- [ ] Retention policies (`delete` vs. `compact`) at a conceptual level.
- [ ] Security mechanisms: authentication, authorization, encryption.
- [ ] CLI tools and the Admin API's purpose.
- [ ] Retryable vs. non-retryable errors.
- [ ] Message headers vs. key/value.

### Section 2 — Application Development (28%) — [Module 2](02-application-development.md)

- [ ] Producer and consumer configuration for durability vs. throughput.
- [ ] Message keying and its effect on ordering and partition assignment.
- [ ] Avro + Schema Registry, and schema compatibility modes (`BACKWARD`/`FORWARD`/`FULL`).
- [ ] At-most-once, at-least-once, and exactly-once semantics, and how each is achieved.
- [ ] Using the Admin API to manage topics programmatically.

### Section 3 — Kafka Streams (12%) — [Module 3](03-kafka-streams.md)

- [ ] Stateless vs. stateful DSL operators.
- [ ] State stores and changelog topics, and how they enable recovery/scaling.
- [ ] `EXACTLY_ONCE_V2` processing guarantee.
- [ ] Deployment packaging considerations (not cluster administration).

### Section 4 — Kafka Connect (15%) — [Module 4](04-kafka-connect.md)

- [ ] Source vs. sink connectors, and CDC as a source pattern.
- [ ] Converters and their relationship to Schema Registry.
- [ ] Troubleshooting: task vs. connector failures, dead-letter queues.

### Section 5 — Application Testing (8%) — [Module 5](05-application-testing.md)

- [ ] When to use mocks vs. `TopologyTestDriver` vs. real integration tests.
- [ ] What a regression test suite should cover (happy path, boundaries, failure paths).

### Section 6 — Application Observability (13%) — [Module 6](06-application-observability.md)

- [ ] Key producer/consumer metrics, especially consumer lag.
- [ ] JMX as the exposure mechanism for client metrics.
- [ ] Common troubleshooting signals and their likely root causes.

### Kafka Advanced — [Module 7](07-kafka-advanced.md)

- [ ] Segments and indexes; replication factor and partition count sizing.
- [ ] `min.insync.replicas`, unclean leader election trade-offs.
- [ ] Compression algorithm trade-offs, batching, idempotent producers.
- [ ] Poll/heartbeat thread model, incremental cooperative rebalancing, static membership.

### Kafka Streams Advanced — [Module 8](08-kafka-streams-advanced.md)

- [ ] When to choose the DSL, the Processor API, or a combination of both.
- [ ] DSL object types, transformation and join behavior, and co-partitioning requirements.
- [ ] `KTable.suppress()` emission and buffering trade-offs.
- [ ] Processor context, punctuators, and state-store selection.
- [ ] Testing complete topologies and processors with the Streams test utilities.

## Test-taking strategy by question format

- **Multiple-choice** — eliminate obviously wrong options first; watch for
  qualifiers like "always," "never," or "only" — Kafka behavior is often
  configuration-dependent, so absolute statements are frequently the distractor.
- **Multiple-response** — read the question stem carefully for how many answers are
  expected; don't assume a single-answer mindset.
- **Matching** — work through the items you're most confident about first, so
  elimination narrows down the harder pairs.
- **Build list** — mentally anchor on the first and last step first (often the easiest
  to identify), then fill in the middle sequence.

## Recommended official resources

The exam guide itself points to these Confluent learning paths to fill any remaining
gaps:

- Developing with Confluent (3-day instructor-led training)
- Confluent Fundamentals Accreditation
- Apache Kafka Fundamentals Learning Path
- Kafka Connect 101 Learning Path
- Designing Events and Event Streams
- Practical Event Modeling
- Kafka Streams 101
- Designing Event Driven Microservices

## Final reminder: what's out of scope

Don't spend study time on: Confluent RBAC/ksqlDB/CFK, system-specific connectors,
plugins/extensions, networking, cluster administration/deployment, or infrastructure
provisioning (see [Introduction](README.md)).

Good luck on the exam.

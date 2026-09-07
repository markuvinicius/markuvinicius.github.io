# Module 4 — Kafka Connect (15% of the exam)

## Learning objectives

By the end of this module you will be able to:

- Explain the purpose of Kafka Connect and its source/sink connector model.
- Describe Change Data Capture (CDC) and how it's typically implemented with Connect.
- Configure and deploy a connector (conceptually, via a JSON configuration).
- Explain converters and their relationship to Schema Registry.
- Troubleshoot common connector failure modes.

## Guided walkthrough

### 1. Why Kafka Connect exists

Without Connect, moving data between Kafka and an external system (a database, a
search index, cloud storage) requires hand-written producer/consumer code for every
integration. Kafka Connect standardizes this with a plugin model:

![Flowchart: source systems (database, S3, APIs) feed source connectors, which write to Kafka topics, which sink connectors read and deliver to sink systems (Elasticsearch, data warehouse, S3).](https://www.conduktor.io/assets/kafka/diagrams/kafka-connect-cli-tutorial.svg)
*Image credit: Conduktor (conduktor.io)*

- **Source connectors** — pull data from an external system and produce it to a Kafka
  topic (e.g., streaming rows from a database). Examples: Debezium, JDBC, S3, MongoDB.
- **Sink connectors** — consume data from a Kafka topic and write it to an external
  system (e.g., writing records to Elasticsearch). Examples: Elasticsearch, S3, JDBC,
  HDFS, Splunk.

Pre-built connectors for most common systems are published on
[Confluent Hub](https://www.confluent.io/hub/), so a developer usually configures an
existing connector rather than writing one from scratch.

Connect runs as its own cluster of **workers**, separate from the Kafka brokers. Workers
distribute connector tasks among themselves for scalability and fault tolerance — this
distribution and the underlying worker cluster operations are out of scope for the
exam (see [Module 0](00-introduction.md)); what matters for a developer is how to
**configure and use** connectors.

Connect workers run in one of two modes:

| Mode | Fault tolerance | Typical use |
|---|---|---|
| Standalone | None — single worker process | Development, testing, one-off/simple tasks |
| Distributed | Tasks rebalance across multiple workers | Production, high availability |

In standalone mode, a connector is configured with a local `.properties` file and
started directly (`connect-standalone worker.properties connector.properties`) — useful
for learning a connector's configuration options before deploying it via the REST API
in distributed mode.

### 2. Change Data Capture (CDC)

CDC is a source-connector pattern that streams **row-level changes** (inserts, updates,
deletes) from a database's transaction/replication log into Kafka topics, rather than
periodically polling and re-querying tables. This gives:

- Near real-time propagation of changes.
- A complete history of change events (not just current state), useful for rebuilding
  downstream state or auditing.
- Lower load on the source database compared to repeated polling queries.

### 3. Connector configuration (conceptual)

A connector is configured with a JSON document describing its class and settings. For
example, a sink connector configuration (submitted via the Connect REST API) might look
like:

```json
{
  "name": "orders-elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "3",
    "topics": "orders-created",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081",
    "connection.url": "http://elasticsearch:9200"
  }
}
```

Key configuration concepts:

- **`tasks.max`** — the maximum parallelism for this connector; Connect distributes
  partitions (source) or topic-partitions (sink) across up to this many tasks.
- **Converters** — translate between Kafka's byte format and Connect's internal data
  format. The **`AvroConverter`** integrates with Schema Registry, so sink connectors
  can deserialize Avro records the same way a regular consumer would (see
  [Module 2](02-application-development.md)).

### 4. Source vs. sink vs. CDC — summary

| Type | Direction | Example use case |
|---|---|---|
| Source | External system → Kafka | Streaming files, message queues, or API polling into a topic |
| Sink | Kafka → External system | Writing events to a data warehouse or search index |
| CDC (a source pattern) | Database → Kafka | Streaming row-level database changes as events |

### 5. Troubleshooting connectors

Common failure categories a developer should recognize:

- **Serialization/converter mismatches** — the connector's configured converter doesn't
  match how data was actually produced (e.g., configuring `AvroConverter` for a topic
  produced with JSON), causing deserialization exceptions.
- **Dead Letter Queue (DLQ)** — sink connectors can be configured with
  `errors.tolerance=all` and a DLQ topic, routing records that fail to process instead
  of stopping the connector.
- **Task failures vs. connector failures** — an individual task can fail (e.g., due to a
  bad record) while the connector itself keeps running; check task status via the
  Connect REST API (`GET /connectors/{name}/status`) to see which failed and why.
- **Schema evolution issues** — a sink connector may reject records if the schema
  changed in a way incompatible with the connector's expectations — a good reason to
  keep to `BACKWARD`/`FULL` compatibility (see [Module 2](02-application-development.md)).

## Self-check questions

1. What's the difference between a source connector and a sink connector?
2. Why is CDC typically preferred over periodic polling queries for database
   integration?
3. What role does a converter play in a connector's configuration, and how does
   `AvroConverter` relate to Schema Registry?
4. What does `tasks.max` control, and what does it *not* guarantee?
5. What's one way to prevent a single bad record from stopping an entire sink connector?
6. When would you use standalone mode versus distributed mode for running a connector?

## Answer key

1. A source connector pulls data from an external system into Kafka; a sink connector
   consumes data from Kafka and writes it to an external system.
2. CDC captures every row-level change from the transaction log in near real time with
   lower source-database load, versus polling which re-queries tables periodically and
   can miss or duplicate changes between polls.
3. A converter translates between Kafka's byte format and Connect's internal record
   format; `AvroConverter` uses Schema Registry to serialize/deserialize Avro records,
   the same way `KafkaAvroSerializer`/`KafkaAvroDeserializer` do for regular
   producers/consumers.
4. `tasks.max` sets the upper bound on parallel tasks for a connector; it does not
   guarantee that many tasks will actually run (e.g., a source connector might have
   fewer things to parallelize than `tasks.max` allows).
5. Configure `errors.tolerance=all` with a dead-letter queue topic, so failing records
   are routed aside instead of halting the connector.
6. Standalone mode for development, testing, or a single simple task — it runs on one
   worker with no fault tolerance. Distributed mode for production, where multiple
   workers share and rebalance connector tasks for high availability.

Continue to [Module 5 — Application Testing](05-application-testing.md).

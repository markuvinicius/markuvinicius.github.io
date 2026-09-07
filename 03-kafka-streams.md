# Module 3 — Apache Kafka Streams (12% of the exam)

## Learning objectives

By the end of this module you will be able to:

- Add the Kafka Streams dependency to a Maven project alongside Schema Registry support.
- Explain the difference between the DSL's stateless and stateful operations.
- Build a simple topology using `KStream`/`KTable` with Avro records.
- Explain what a state store and changelog topic are used for.
- Configure exactly-once processing in a Streams application.
- Describe deployment considerations for Streams apps (packaging, not cluster admin).

## Guided walkthrough

### 1. What Kafka Streams is (and isn't)

Kafka Streams is a **library**, not a separate cluster. There's no scheduler to
submit jobs to — you add `kafka-streams` as a dependency, write a topology, and your
own application becomes the stream processor:

![A Kafka Streams application reads records from topics in a single Kafka cluster, processes them, and writes the results back into the same cluster, where other apps, other Kafka Streams applications, and dashboards or databases consume them.](https://www.conduktor.io/assets/kafka-streams/diagrams/kafka-streams-dataflow.svg)
*Image credit: Conduktor (conduktor.io)*

Because it's just a library, a Streams application scales exactly the way a
[consumer group](02-application-development.md) does: run more instances, and
Kafka's consumer group protocol spreads partitions across them — no resource
manager, no separate cluster to operate. The trade-off is ownership: your process
now owns the memory, local disk state, restarts, and rebalances that a
Streams application depends on.

Compared to hand-rolling the same logic with the plain consumer/producer API, the
library gives you, for free:

- **Stateful operations** — aggregations, counts, and joins backed by a local store
  that survives restarts.
- **Event-time windowing** — grouping records by when they happened, not when they
  arrived.
- **Exactly-once processing** — the read-process-write cycle made atomic with one
  config flag.
- **Automatic scaling and fault tolerance** — partitions, tasks, and state move
  between instances without you writing rebalance code.

### 2. Maven setup for Kafka Streams

Building on [Module 2](02-application-development.md)'s `pom.xml`, add the
`kafka-streams` and Avro Streams SerDe dependencies:

```xml
<dependencies>
    <!-- ...kafka-clients, avro, kafka-avro-serializer as in Module 2... -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-streams</artifactId>
        <version>${kafka.version}</version>
    </dependency>
    <dependency>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-streams-avro-serde</artifactId>
        <version>${confluent.version}</version>
    </dependency>
</dependencies>
```

### 3. Stateless vs. stateful operations

The Streams DSL provides two broad categories of operators:

| Category | Examples | Characteristic |
|---|---|---|
| Stateless | `filter`, `map`, `mapValues`, `flatMap`, `branch` | Each record is processed independently; no memory of previous records |
| Stateful | `aggregate`, `count`, `reduce`, `join`, windowed operations | Requires a **state store** to remember previous records (e.g., a running total) |

Stateful operations are backed by a local state store (RocksDB by default) that is
**backed up to a changelog topic** in Kafka, so state can be rebuilt if an instance
restarts or moves.

### 4. Building a topology

Continuing the `orders-created` example, this topology counts orders per customer using
a windowed aggregation:

```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import com.example.events.OrderCreated;

import java.time.Duration;
import java.util.Properties;

public class OrderCountStreamsApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-count-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", "http://localhost:8081");
        // Exactly-once processing for this Streams app (Kafka 2.5+)
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        SpecificAvroSerde<OrderCreated> orderSerde = new SpecificAvroSerde<>();
        orderSerde.configure(
                java.util.Map.of("schema.registry.url", "http://localhost:8081"), false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, OrderCreated> orders =
                builder.stream("orders-created", Consumed.with(Serdes.String(), orderSerde));

        KTable<Windowed<String>, Long> orderCountsByCustomer = orders
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30)))
                .count(Materialized.as("order-counts-store"));

        orderCountsByCustomer.toStream()
                .map((windowedKey, count) ->
                        KeyValue.pair(windowedKey.key(), count))
                .to("order-counts-by-customer", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
```

Key points illustrated here:

- **`groupByKey()` + `windowedBy()` + `count()`** — a stateful aggregation; Kafka Streams
  creates a state store (`order-counts-store`) and a backing changelog topic
  automatically.
- **`TimeWindows.ofSizeAndGrace(...)`** — a 5-minute tumbling window with a 30-second
  grace period for late-arriving records.
- **`EXACTLY_ONCE_V2`** — wraps state updates and output production in a Kafka
  transaction, so a failure and restart doesn't double-count or lose updates.

### 5. Architecture: sub-topologies, tasks, and stream threads

A topology you build with the DSL compiles into a runtime hierarchy:

![One Kafka Streams topology compiles into sub-topologies, tasks (one per partition), stream threads, and JVM instances.](https://www.conduktor.io/assets/kafka-streams/diagrams/topology-task-thread-hierarchy.svg)
*Image credit: Conduktor (conduktor.io)*

```
Topology              one graph you define
  └─ Sub-topology     a connected chunk, split at repartition boundaries
       └─ Task        one per partition of the sub-topology's input
            └─ runs inside a Stream Thread
                 └─ inside your JVM process (one of N instances)
```

Key facts that matter for the exam's tuning and scaling objectives:

- **Sub-topologies** are where the graph is cut — whenever a re-key forces data to be
  shuffled across the network (e.g., `selectKey` followed by `groupByKey`), Kafka
  Streams splits the graph and routes data through an internal **repartition topic**.
- **Tasks are the fixed unit of parallelism** — one task per partition of a
  sub-topology's input. The maximum parallelism of the whole application is the
  largest partition count among its sub-topologies; instances or threads beyond that
  ceiling sit idle.
- **Stream threads** (`num.stream.threads`) are *who* runs the tasks — Kafka Streams
  distributes all tasks across all threads across all instances. Adding threads only
  helps up to the number of available tasks.
- Under the hood, a Streams app **is** an ordinary consumer group named after
  `application.id` — the same `max.poll.interval.ms`/`session.timeout.ms` levers from
  [Module 2](02-application-development.md) govern it, and it shows up in
  `kafka-consumer-groups` output like any other consumer group.

### 6. State stores and changelog topics

- A **state store** holds the intermediate state (e.g., running counts) needed for
  stateful operators, typically backed by RocksDB on local disk.
- A **changelog topic** is a Kafka topic (usually compacted) that Streams writes to
  whenever the state store changes. If an instance fails, another instance (or the same
  instance restarting) can **replay the changelog** to rebuild the state store, rather
  than reprocessing the entire input topic from the beginning.
- Repartition topics (from sub-topology boundaries) and changelog topics (from state
  stores) are both created automatically, prefixed by your `application.id`. Naming
  them explicitly (e.g., `Materialized.as("order-counts-store")` as used above) keeps
  these internal topic names stable across topology changes — an unnamed store gets a
  positional name that can silently shift, and orphan existing state, if you insert or
  reorder an operator later.

This is why Streams applications can scale out and recover without needing shared
network storage.

### 7. Deployment considerations

The exam expects you to know how a Streams application is *packaged and deployed*, not
how to administer a cluster:

- Package the app as a runnable JAR (e.g., via the Maven Shade or Assembly plugin) and
  run it as a normal Java process — a Streams app is a client application, not a broker
  component.
- **Docker/Kubernetes**: each Streams application instance runs as a container; scaling
  out means running more instances (up to the partition count of the input topics —
  each partition is processed by exactly one Streams task at a time within an
  application ID).
- `num.stream.threads` controls how many processing threads a single instance uses.
- State stores should be backed by a persistent volume in containerized deployments if
  you want faster recovery (avoiding a full changelog replay on restart), though this
  isn't strictly required since the changelog topic is the source of truth.

### 8. When not to use Kafka Streams

Recognizing the boundaries is as testable as knowing the features:

- **Not a JVM shop** — Kafka Streams is Java/Scala only; a non-JVM team should use a
  plain consumer or a different stream-processing engine.
- **Sources aren't Kafka** — Kafka Streams only reads and writes Kafka; pulling from a
  database, a queue, and an HTTP API into one job is outside its scope.
- **A one-line stateless filter** — a single `filter` with no state is sometimes just a
  plain [consumer](02-application-development.md) with a few lines of code; don't add
  a framework for it.
- **Large local state you don't want to own** — large RocksDB-backed state means slow
  restores and rebalances that can stall tasks for minutes while state rebuilds; if you
  don't want to own that operational burden, a managed processing cluster may fit
  better.

## Self-check questions

1. Why is Kafka Streams described as "a library, not a cluster," and what responsibility
   does that shift onto your team compared to a separate processing cluster?
2. What's the difference between a stateless and a stateful DSL operator? Give one
   example of each.
3. What determines where a topology is split into sub-topologies, and why does that
   split matter for performance?
4. What is a changelog topic, and why does it matter for scaling a Streams app?
5. What does setting `PROCESSING_GUARANTEE_CONFIG` to `EXACTLY_ONCE_V2` change about how
   the topology behaves?
6. In a containerized deployment, what determines the maximum useful number of Streams
   application instances for a given topology?
7. Give one scenario where Kafka Streams would be the wrong tool for the job.

## Answer key

1. Because it's a JAR added to your app rather than a separate system you submit jobs
   to; your team owns its memory, local disk state, restarts, and rebalances, whereas a
   separate cluster (e.g., a managed engine) would carry that operational burden for
   you.
2. Stateless operators (e.g., `filter`, `mapValues`) process each record independently.
   Stateful operators (e.g., `count`, `aggregate`, `join`) need to remember information
   across records, backed by a state store.
3. A re-key that forces data to be shuffled across the network (e.g., `selectKey`
   followed by `groupByKey`) — records need to land on the correct partition before a
   stateful operator can group them, so Kafka Streams routes them through an internal
   repartition topic at that boundary, splitting the graph.
4. A changelog topic is where Kafka Streams durably records every change to a state
   store. It lets a new or restarted instance rebuild state by replaying the changelog
   instead of reprocessing all input data, which is essential for fast recovery and
   horizontal scaling.
5. It wraps state store updates and output topic writes in a Kafka transaction, so
   either all effects of processing a record are visible, or none are — preventing
   duplicate or lost updates on failure/restart.
6. The number of input topic partitions — each partition maps to one Streams task, and
   a task runs on exactly one instance at a time, so instances beyond the partition
   count sit idle.
7. Examples: a non-JVM team needing a Python/Go stream processor; a job that needs to
   read directly from a database or an HTTP API rather than Kafka topics; or a single
   stateless filter that doesn't warrant the framework's overhead.

Continue to [Module 4 — Kafka Connect](04-kafka-connect.md).

# Module 8 — Kafka Streams Advanced

## Learning objectives

By the end of this module you will be able to:

- Choose between the Kafka Streams DSL and Processor API for a processing task.
- Recognize the main DSL objects and choose appropriate transformations, joins, and aggregations.
- Control when table updates are emitted and understand the buffering trade-offs.
- Build a Processor API topology with an attached state store and a punctuator.
- Choose among common state-store types and test Streams topologies with `TopologyTestDriver`.

## Prerequisites

This chapter builds on [Module 3 — Apache Kafka Streams](03-kafka-streams.md). It assumes
that you understand the basic topology, stream/table duality, state stores, event time,
windowing, and changelog topics introduced there. The examples use the Kafka Streams
4.3 Java API and primitive SerDes; use your application's configured SerDes for other
record types.

## 1. DSL or Processor API?

The **Streams DSL** is the higher-level, declarative API. You describe transformations
such as filtering, grouping, joining, and aggregating, and Kafka Streams builds the
processor topology. It is the best starting point for most applications because common
stream-processing patterns need little custom code.

The **Processor API** is the lower-level API for defining custom record-by-record
processing. A processor receives a `Record`, can read or update attached state stores,
inspect record and task metadata through its context, schedule punctuation, and forward
records to downstream processors. It is useful when the DSL does not express the required
logic clearly or when code needs control over details such as metadata or emission timing.

These APIs are not mutually exclusive. A DSL topology can invoke custom processors at a
specific point, so keep the surrounding flow declarative and use a custom processor only
for the operation that needs it.

| Choose the DSL when... | Choose the Processor API when... |
|---|---|
| The logic fits common transformations, joins, or aggregations. | The operation needs custom per-record control or logic not provided by the DSL. |
| You want topology intent to be easy to scan and compose. | You need processor context, custom store access, or scheduled punctuation. |
| Kafka Streams should manage the details of common stateful operations. | You need to define how state is read, updated, or emitted. |

Both APIs still run as Kafka Streams topologies and use the same tasks, state-store
recovery, and application configuration described in Module 3.

## 2. DSL objects and operations

The DSL's object types describe the meaning of the data as it moves through a topology.
Choosing the right type is important because it determines how later operations interpret
records.

| Object | Meaning | Common next operations |
|---|---|---|
| `KStream<K, V>` | An event stream: each record is an event, including repeated keys. | `filter`, `mapValues`, `selectKey`, `branch`, `merge`, `groupByKey`, joins, `to` |
| `KTable<K, V>` | A changelog of the latest value per key; a null value is a tombstone that deletes the key. | `filter`, `mapValues`, table joins, `toStream` |
| `GlobalKTable<K, V>` | A full local copy of a table's input topic on each application instance. | Lookups when joining from a `KStream` |
| `KGroupedStream<K, V>` | A stream grouped by key, ready for aggregation or windowing. | `count`, `reduce`, `aggregate`, `windowedBy` |
| `KGroupedTable<K, V>` | A table grouped by a derived key, ready for aggregation. | `count`, `reduce`, `aggregate` |
| `TimeWindowedKStream` / `SessionWindowedKStream` | A grouped stream split into time windows or sessions. | Windowed `count`, `reduce`, and `aggregate` |

### Transformations

Stateless transformations include `filter`/`filterNot`, `map`/`mapValues`,
`flatMap`/`flatMapValues`, `selectKey`, `branch`, and `merge`. The value-only variants
preserve the key; this can avoid unnecessary repartitioning. Changing keys and then
grouping or joining may require Kafka Streams to repartition records so that records
with the same key reach the same task.

Stateful transformations include aggregations and joins. A stream aggregation starts
with records grouped by key and produces a `KTable` of current results. Use `count` for
a record count, `reduce` when the aggregate has the same type as its input, and
`aggregate` when the aggregate needs an initializer or a different result type. A table
aggregation must account for both the old and new values when a row changes; Kafka
Streams uses adders and subtractors to keep the result correct.

### Joins

The join method follows the meaning of its inputs:

| Inputs | Result and typical use |
|---|---|
| `KStream` + `KStream` | A windowed event-to-event join; records must be co-partitioned. |
| `KStream` + `KTable` | A lookup of the table's current value for each stream record; inputs must be co-partitioned. |
| `KStream` + `GlobalKTable` | A lookup against a full local table copy; no co-partitioning is required, but the table is replicated to each instance and has no event-time synchronization with the stream. |
| `KTable` + `KTable` | An updating table result; equi-joins require co-partitioned inputs. Foreign-key joins are also available. |

A stream-stream join needs a time window to bound the records retained for matching. Table
joins are non-windowed and update their result when either table changes. For ordinary
key-based joins, matching records must have compatible key types, partition counts, and
partitioning strategies. Kafka Streams can detect some partition-count mismatches, but
it cannot prove that different producers used compatible partitioners.

### DSL example: grouped count, table join, and controlled emission

Assume `purchase-events` is keyed by customer ID with a `Long` amount as its value, and
`customer-names` is a keyed changelog of customer names. The example counts purchases
per customer, enriches each count with the current name, and limits updates downstream
for each key.

```java
import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

StreamsBuilder builder = new StreamsBuilder();

KStream<String, Long> purchases = builder.stream(
        "purchase-events",
        Consumed.with(Serdes.String(), Serdes.Long()));

KTable<String, String> customerNames = builder.table(
        "customer-names",
        Consumed.with(Serdes.String(), Serdes.String()),
        Materialized.as("customer-names-store"));

KTable<String, Long> purchaseCounts = purchases
        .filter((customerId, amount) -> customerId != null && amount != null && amount > 0)
        .mapValues(amount -> 1L)
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
        .count(Materialized.as("purchase-counts-store"));

KTable<String, String> customerSummaries = purchaseCounts.leftJoin(
        customerNames,
        (count, name) -> (name == null ? "unknown" : name) + ":" + count);

customerSummaries
        .suppress(Suppressed.untilTimeLimit(
                Duration.ofMinutes(1),
                Suppressed.BufferConfig.maxBytes(1_000_000L).emitEarlyWhenFull()))
        .toStream()
        .to("customer-purchase-counts");
```

The aggregation produces a `KTable`, so later counts replace earlier values for that
customer in downstream table operations. The join enriches each updated count from the
current `customerNames` table. The two table inputs to this join must be co-partitioned.

`Suppressed.untilTimeLimit` delays updates for each key for up to the configured time.
Its buffer holds the latest table updates while they are suppressed. This example caps
the buffer at 1 MB and emits early when it fills, so the time limit is not an absolute
minimum interval under memory pressure. A shutdown-on-full policy can enforce the delay
but makes buffer sizing and recovery operationally important. Suppression is different
from the record cache: caching may coalesce intermediate updates for efficiency, while
suppression expresses an application-level emission policy. Suppression requires
buffering and should be sized deliberately. For a windowed table that should emit only
a final result, use `Suppressed.untilWindowCloses(...)` with an appropriate buffer
policy; the window's grace period determines when the result can be considered final.

## 3. Processor API

The modern Processor API represents input and output types explicitly. A processor
implements `Processor<KIn, VIn, KOut, VOut>` and receives each input as a `Record<KIn,
VIn>`. Its lifecycle is:

- `init(context)` runs when Kafka Streams initializes the processor for a task. Get
  attached stores and schedule punctuators here.
- `process(record)` runs for each input record. Read or update state and forward zero,
  one, or more output records as needed.
- `close()` releases resources created by the processor. State stores are managed by
  Kafka Streams and should not be closed by the processor.

`ProcessorContext<KOut, VOut>` provides access to attached state stores, record metadata
such as topic/partition/offset, application and task details, forwarding, punctuation
scheduling, and commit requests. `Record` is immutable; methods such as `withValue()`
create a shallow copy that retains the other record fields. Do not share mutable
processor instances: provide a supplier that creates a fresh instance for each task.

Use `FixedKeyProcessor` (typically through the DSL's `processValues`) when a processor
must preserve its input key. Use `Processor` (through `process`) when changing the key or
other record-level fields is part of the logic. A processor attached to a DSL topology
is a custom step, not a replacement for DSL operators that already express the same
behavior clearly.

## 4. State-store choices

Applications normally describe stores with a `StoreBuilder`; Kafka Streams creates and
associates their task-local instances. Built-in store families include:

| Store family | Use |
|---|---|
| Key-value | Look up or update state by key; commonly used for counters, deduplication, and rolling state. |
| Window | Keep values indexed by key and time window, commonly for stream-stream joins or time-windowed aggregates. |
| Session | Keep values for key-based sessions whose start and end depend on activity gaps. |
| Versioned key-value | Keep multiple timestamped versions per key for as-of lookups and timestamp-based table semantics. |

Persistent stores use RocksDB on local disk and are a common choice when state can exceed
heap memory. In-memory stores keep data in the process heap and are appropriate only when
the data volume fits the available memory and the store's operational trade-offs are
acceptable. Kafka Streams can back up supported stores to changelog topics by default;
disabling logging removes that store's changelog-based recovery and standby-replica
support. Custom stores are possible, but require implementing the store and its builder,
including restoration behavior.

Choose the store API to match the access pattern (`KeyValueStore`, `WindowStore`,
`SessionStore`, or a versioned store). Close iterators returned by stores, preferably
with try-with-resources. Store names and serialization formats are part of the
application's state contract, so preserve them carefully when evolving a topology.

## 5. Punctuators

A **punctuator** is a callback scheduled through `ProcessorContext.schedule()`. It can
emit or update state periodically without waiting for a specific input record. The
punctuation type determines the clock:

- `STREAM_TIME` advances as records are processed. It is suitable for event-time work,
  but does not advance while input is idle, so its punctuator will not fire during an
  idle period.
- `WALL_CLOCK_TIME` follows elapsed wall-clock time. It can fire without new input, but
  is not event-time aligned and scheduling is best-effort rather than a real-time
  guarantee.

Punctuation callbacks run within the stream task. Keep them bounded, avoid blocking
calls, and avoid scanning an unbounded store on every tick. Close any iterators they
open. Use the wall clock only for behavior that genuinely needs elapsed machine time;
use stream time for event-time semantics.

### Processor API example: state store and punctuator

This processor counts page-view records per key in a persistent key-value store and
emits the current local counts every 30 seconds of wall-clock time. The periodic scan is
kept small for demonstration; production code should avoid repeatedly scanning a large
store and should emit only the state that needs to be published.

```java
import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

final class PageViewCounter implements Processor<String, String, String, Long> {
    private static final String STORE_NAME = "page-view-counts";

    private KeyValueStore<String, Long> counts;
    private ProcessorContext<String, Long> context;

    @Override
    public void init(ProcessorContext<String, Long> context) {
        this.context = context;
        counts = context.getStateStore(STORE_NAME);

        context.schedule(Duration.ofSeconds(30), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            try (KeyValueIterator<String, Long> entries = counts.all()) {
                while (entries.hasNext()) {
                                        KeyValue<String, Long> entry = entries.next();
                    context.forward(new Record<>(entry.key, entry.value, timestamp));
                }
            }
        });
    }

    @Override
    public void process(Record<String, String> record) {
        if (record.key() == null || record.value() == null) {
            return;
        }

        Long currentCount = counts.get(record.key());
        counts.put(record.key(), currentCount == null ? 1L : currentCount + 1L);
    }

    @Override
    public void close() {
    }
}
```

Create the store builder and attach the processor to a source and sink in the topology
builder:

```java
StoreBuilder<KeyValueStore<String, Long>> countsStore = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("page-view-counts"),
        Serdes.String(),
        Serdes.Long());

Topology topology = new Topology();
topology.addSource(
                "page-view-source",
                Serdes.String().deserializer(),
                Serdes.String().deserializer(),
                "page-views")
        .addProcessor("page-view-counter", PageViewCounter::new, "page-view-source")
        .addStateStore(countsStore, "page-view-counter")
        .addSink(
                "count-sink",
                "page-view-counts-output",
                Serdes.String().serializer(),
                Serdes.Long().serializer(),
                "page-view-counter");
```

The store is built once as topology metadata and Kafka Streams creates its local task
instances. The same store name must be used when attaching the store and retrieving it
from the processor. The supplier `PageViewCounter::new` creates independent processor
instances. The count output is a periodic snapshot, not one output per input event.

## 6. Testing DSL and Processor API topologies

`kafka-streams-test-utils` provides `TopologyTestDriver` for testing a topology without
a broker. It accepts topologies built with either the DSL or the Processor API, lets a
test pipe records into input topics, captures output records, and exposes state stores.
Use deterministic input timestamps for event-time behavior; advance the driver's
wall-clock time explicitly to test wall-clock punctuators. Close the driver after each
test.

For example, a test of the `PageViewCounter` can assert that input updates the store
without immediately producing output, then advances the test clock and observes the
scheduled snapshot:

```java
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "page-view-counter-test");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
    TestInputTopic<String, String> input = driver.createInputTopic(
            "page-views", Serdes.String().serializer(), Serdes.String().serializer());
    TestOutputTopic<String, Long> output = driver.createOutputTopic(
            "page-view-counts-output", Serdes.String().deserializer(), Serdes.Long().deserializer());

    input.pipeInput("article-42", "view");
        assertTrue(output.isEmpty());

    driver.advanceWallClockTime(Duration.ofSeconds(30));
        assertEquals(new KeyValue<>("article-42", 1L), output.readKeyValue());
}
```

In a real test suite, use the test framework's assertion methods rather than Java's
`assert` keyword unless assertions are explicitly enabled. Test the DSL example by
piping purchase events and customer-table updates through its input topics, then
asserting the emitted output and (where relevant) materialized state. For suppression,
include several updates for the same key and verify the expected output timing, buffer
policy, and behavior when the buffer limit is reached.

Use `MockProcessorContext` when testing a processor in isolation: it captures forwarded
records and scheduled punctuators, and can register an in-memory store. Use
`TopologyTestDriver` when the test needs to verify the complete topology wiring,
serialization, store attachment, or automatic punctuation. Neither replaces an
integration test when broker behavior, topic configuration, or end-to-end transactions
are part of the requirement.

## Self-check questions

1. When is the DSL a better fit than the Processor API, and when should a custom
   processor be embedded in a DSL topology?
2. How does a `KStream` differ from a `KTable`, and how is a `GlobalKTable` different
   from a partitioned `KTable`?
3. Which join types are windowed, and which joins require co-partitioned inputs?
4. What does `KTable.suppress()` control, and what trade-off does its buffer policy
   introduce?
5. Which responsibilities belong in `init`, `process`, and `close` for a Processor API
   processor?
6. How do stream-time and wall-clock punctuators differ when the input is idle?
7. Name two built-in state-store families and a use case for each.
8. When would you use `MockProcessorContext` versus `TopologyTestDriver`?

## Answer key

1. The DSL is preferable for common transformations, joins, and aggregations because
   it makes intent concise and lets Streams manage standard operator behavior. Embed a
   custom processor when one operation needs custom per-record logic, metadata access,
   store access, or punctuation that is awkward to express in the DSL.
2. A `KStream` treats each record as an event, while a `KTable` treats each keyed record
   as an update to the latest value (and a null value as a delete). A `GlobalKTable`
   copies all input partitions to every application instance for lookup joins; it uses
   more local storage and network traffic and has no event-time synchronization with a
   stream join.
3. Stream-stream joins are windowed. Key-based stream-stream, stream-table, and
   table-table joins require compatible partitioning; a stream-global-table lookup does
   not require co-partitioning. Table-table foreign-key joins also do not require the
   inputs to be pre-partitioned together.
4. Suppression delays table updates according to a time or window-closing policy. It
   buffers updates; a bounded buffer may emit early or shut down when full, depending on
   its configured policy, so memory limits affect emission guarantees.
5. `init` obtains stores and schedules callbacks, `process` handles each input record,
   and `close` releases processor-owned resources. Kafka Streams manages the store
   lifecycle.
6. Stream time advances only as records are processed, so an idle input does not trigger
   stream-time punctuation. Wall-clock punctuation can fire while idle, but it follows
   elapsed processing-host time rather than event time.
7. Examples: a key-value store for counts by key, a window store for time-bounded joins,
   a session store for inactivity-separated sessions, or a versioned key-value store
   for timestamped lookups.
8. `MockProcessorContext` isolates processor behavior and captures forwards and
   scheduled punctuators. `TopologyTestDriver` exercises complete DSL or Processor API
   topologies with input/output topics, state stores, and controllable clocks.

## References

- [Kafka Streams DSL](https://kafka.apache.org/43/streams/developer-guide/dsl-api/)
- [Kafka Streams Processor API](https://kafka.apache.org/43/streams/developer-guide/processor-api/)
- [Testing a Kafka Streams application](https://kafka.apache.org/43/streams/developer-guide/testing/)

Continue to [Module 9 — Exam Day and Next Steps](09-exam-day-and-next-steps.md).

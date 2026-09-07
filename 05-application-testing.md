# Module 5 — Application Testing (8% of the exam)

## Learning objectives

By the end of this module you will be able to:

- Choose an appropriate testing strategy for producers, consumers, and Streams topologies.
- Explain the difference between unit testing with mocks and integration testing with an
  embedded/dockerized broker.
- Use `TopologyTestDriver` to test a Kafka Streams topology without a running cluster.
- Recognize what a good regression test for a Kafka application covers.

## Guided walkthrough

### 1. Levels of testing

| Level | Tooling | What it validates |
|---|---|---|
| Unit test | Mocked producer/consumer (`MockProducer`, `MockConsumer`) | Business logic in isolation from Kafka |
| Streams topology test | `TopologyTestDriver` | Topology logic (DSL operators, state stores) without a broker |
| Integration test | Embedded Kafka (e.g., Testcontainers running real Kafka) | Real serialization, real broker behavior, real Schema Registry interaction |

A common mistake is over-relying on mocks: mocking away serialization or partitioning
behavior can hide real bugs (e.g., an Avro schema incompatibility, or a partitioner
misconfiguration). Prefer integration tests whenever mocking would hide such
serialization/Kafka-specific behavior.

### 2. Testing a producer with `MockProducer`

`MockProducer` records what was sent without needing a real broker — useful for
verifying your business logic calls `send()` with the correct topic, key, and value.

```java
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OrderProducerServiceTest {

    @Test
    void sendsOrderKeyedByCustomerId() {
        MockProducer<String, String> mockProducer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        OrderProducerService service = new OrderProducerService(mockProducer);
        service.publishOrder("ord-123", "cust-456", "{\"amount\":99.9}");

        ProducerRecord<String, String> sent = mockProducer.history().get(0);
        assertEquals("orders-created", sent.topic());
        assertEquals("cust-456", sent.key()); // verifies keying strategy from Module 2
    }
}
```

### 3. Testing a Streams topology with `TopologyTestDriver`

`TopologyTestDriver` runs your actual topology definition in-memory, including state
stores, without needing a running Kafka cluster or Schema Registry — ideal for fast,
deterministic tests of Streams logic from [Module 3](03-kafka-streams.md).

```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Properties;

class OrderCountTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> input;
    private TestOutputTopic<String, Long> output;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("orders-created", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .count()
                .toStream()
                .to("order-counts-by-customer", Produced.with(Serdes.String(), Serdes.Long()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(builder.build(), props);
        input = testDriver.createInputTopic("orders-created",
                Serdes.String().serializer(), Serdes.String().serializer());
        output = testDriver.createOutputTopic("order-counts-by-customer",
                Serdes.String().deserializer(), Serdes.Long().deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void countsOrdersPerCustomer() {
        input.pipeInput("cust-456", "order-1");
        input.pipeInput("cust-456", "order-2");

        TestRecord<String, Long> result = output.readRecord();
        assertEquals("cust-456", result.key());
        assertEquals(1L, result.value());
    }
}
```

### 4. The caching gap: why tests can see different results than production

`TopologyTestDriver` is synchronous and has **no record cache** — it commits after
every input record. Production Kafka Streams, however, buffers updates to the same key
in a record cache (`cache.max.bytes.buffering` / `statestore.cache.max.bytes`) and only
forwards a downstream update when the cache flushes or `commit.interval.ms` fires. A
key updated 100 times between flushes emits **one** coalesced downstream update in
production.

This means a windowed `count` fed three records for the same key emits three running
values (`1`, `2`, `3`) under `TopologyTestDriver`, but production may collapse those
into a single emitted `3`. **This is not a bug** — it's the cost of the driver being
deterministic and synchronous. The practical rule: **assert on final, converged
values** (read the whole output to a map; check the last value per key), not on every
intermediate emission — a test asserting on intermediate values is testing timing
behavior the cache hides in production, and will not reflect what a downstream
consumer actually sees.

### 5. Testing windowed operations and `suppress()`

`TopologyTestDriver` gives you full control over record timestamps, and windowed logic
needs it: a window only closes when **stream time** (the maximum record timestamp a
task has seen) passes the window end plus the grace period. The driver never advances
stream time on its own — only piping a record with a later timestamp moves it forward.

This is exactly where testing `suppress(untilWindowCloses(...))` goes wrong: piping
three records into an open window and reading the output immediately gives you
**nothing** — not because `suppress` is broken, but because stream time never advanced
past the window boundary. The fix is to pipe one more record timestamped beyond the
window + grace period to force the flush:

```java
Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
// three events inside a 1-minute tumbling window (grace = 0)
input.pipeInput("cust-456", "order-1", t0);
input.pipeInput("cust-456", "order-2", t0.plusSeconds(10));
input.pipeInput("cust-456", "order-3", t0.plusSeconds(20));

assertTrue(output.isEmpty(), "suppress holds the result until the window closes");

// advance stream time past window end (+ grace) to flush the suppressed result
input.pipeInput("cust-456", "order-4", t0.plusSeconds(120));

TestRecord<String, Long> finalCount = output.readRecord();
assertEquals(3L, finalCount.value()); // orders 1-3; order-4 lands in the next window
```

The same technique — deliberately stepping stream time forward — is how you test grace
periods and anything that fires on window close.

### 6. What `TopologyTestDriver` won't catch, and snapshotting the topology

The test driver validates your topology's *logic*. It does not validate things that
depend on a real cluster:

- **Missing source topics** — the driver creates input/output topics on demand; in
  production, reading from a topic that doesn't exist is a startup failure the driver
  never reproduces.
- **Real serde and Schema Registry behavior** — a mock registry isn't the real wire
  format or real subject-compatibility rules.
- **Rebalancing, restore, and standby replicas** — there's no consumer group, so slow
  rebalances and state restore time (the failures that actually page you in
  production) are invisible.
- **The caching/commit timing** described above.

For those, run a handful of integration tests against a real, ephemeral broker (see
next section), and keep the bulk of your coverage in the fast `TopologyTestDriver`
tier.

A separate, high-value test is a **topology snapshot**: `Topology.describe()` renders
the full wiring, including autogenerated internal names for unnamed stores/repartition
topics. Since those names are positional (inserting or reordering an operator can shift
them and orphan existing state — see [Module 3](03-kafka-streams.md)), committing the
`describe()` output to a file and asserting against it in CI turns a risky topology
change into a visible code-review diff instead of a 3 a.m. state-restore failure:

```java
@Test
void topologyMatchesSnapshot() throws Exception {
    String actual = OrderCountStreamsApp.buildTopology().describe().toString();
    String expected = Files.readString(Path.of("src/test/resources/topology.txt"));
    assertEquals(expected, actual); // fails the build if any internal name moved
}
```

### 7. Integration testing with a real broker

For behavior that mocks can't safely stand in for — real serialization with Schema
Registry, real partition assignment, real consumer group rebalancing — run tests
against a real, ephemeral Kafka cluster (e.g., via Testcontainers' Kafka module). This
catches issues such as:

- Schema compatibility violations that only surface when actually calling Schema
  Registry.
- Partitioner/keying behavior under realistic partition counts.
- Consumer group rebalance behavior when multiple consumer instances are involved.

### 8. What a good regression test covers

For a bug fix or new feature, a solid Kafka test suite covers:

- **Happy path** — correct message flows through as expected.
- **Boundary conditions** — empty batches, single-record batches, maximum message size.
- **Invalid input** — malformed or unexpected schema versions.
- **Failure paths** — consumer restart after a crash (does at-least-once processing
  behave correctly on reprocessing?), broker unavailability during a send.

## Self-check questions

1. When should you prefer an integration test over a unit test with mocks, for a Kafka
   application?
2. What does `TopologyTestDriver` let you test without needing a running Kafka cluster?
3. Why is `MockProducer` insufficient for validating Schema Registry compatibility
   issues?
4. Give two failure-path scenarios worth testing for an at-least-once consumer.
5. Why might a `TopologyTestDriver` test show three intermediate aggregation values
   where production emits only one final value — and what should your assertion focus
   on instead?
6. Why does piping three records into an open time window and immediately reading the
   output return nothing when testing `suppress()`?
7. Name two things `TopologyTestDriver` cannot validate, that require a real broker.
8. What risk does a topology snapshot test (`Topology.describe()`) protect against?

## Answer key

1. When the behavior under test depends on real serialization, real partitioning, or
   real consumer group coordination — things a mock would either skip or fake
   incorrectly.
2. The Streams DSL topology logic itself — including stateful operators and state
   stores — in-memory and without a broker or Schema Registry.
3. `MockProducer` doesn't perform real serialization against a real Schema Registry, so
   it can't catch schema compatibility violations; that requires an integration test
   with a real Schema Registry instance.
4. (a) Consumer crash after processing but before committing (should safely reprocess
   without corrupting state — verifies idempotency); (b) consumer group rebalance mid
   processing (partitions should reassign without message loss).
5. `TopologyTestDriver` has no record cache and commits after every input record, so it
   emits one update per input; production coalesces updates to the same key in its
   record cache and only flushes on `commit.interval.ms` or cache limits. Assertions
   should check final, converged values (e.g., the last value per key), not every
   intermediate emission.
6. A window only closes when stream time (the latest record timestamp seen) passes the
   window end plus grace period, and the test driver never advances stream time on its
   own — only piping a record with a later timestamp does. Three records within the
   window don't push stream time past the boundary, so the window (and any `suppress`)
   never flushes.
7. Examples: real serde/Schema Registry wire-format behavior (including compatibility
   rule enforcement), and consumer group rebalancing/state restore behavior (there's no
   real consumer group in the test driver).
8. It protects against silently shifting the positional, autogenerated names of
   internal stores, changelog topics, or repartition topics when an operator is
   inserted or reordered — a shift that can orphan existing production state on
   deployment.

Continue to [Module 6 — Application Observability](06-application-observability.md).

Continue to [Module 6 — Application Observability](06-application-observability.md).

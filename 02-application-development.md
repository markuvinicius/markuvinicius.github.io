# Module 2 — Apache Kafka Application Development (28% of the exam)

This is the highest-weighted section of the exam. It's also the most hands-on:
producer/consumer APIs, serialization, schema management, and delivery semantics.

## Learning objectives

By the end of this module you will be able to:

- Set up a Java + Maven project with the Kafka clients and Confluent Schema Registry
  dependencies.
- Write a producer and consumer using the Java client, with proper configuration.
- Explain message keying and key hashing, and why they matter for ordering.
- Use Avro with Schema Registry, and explain schema evolution compatibility modes.
- Explain the difference between at-least-once, at-most-once, and exactly-once
  processing semantics from the client's perspective.
- Use the Kafka Admin API to manage topics programmatically.

## Guided walkthrough

### 1. Producer fundamentals, revisited

A producer sends messages to a topic; the broker then routes each message to a
partition. Every Kafka message has the same anatomy, regardless of language or
client library:

![Diagram showing how Kafka Producers structure a message created by the Apache Kafka Producer.](https://www.conduktor.io/assets/kafka/Kafka-Producers-3.png)
*Image credit: Conduktor (conduktor.io)*

- **Key** — optional; may be `null`, a string, a number, or any object, serialized to
  bytes.
- **Value** — the message payload, also serialized to bytes (also optionally `null`,
  e.g., as a compaction tombstone — see [Module 7](07-kafka-advanced.md)).
- **Compression type** — `none`, `gzip`, `lz4`, `snappy`, or `zstd` (see
  [Module 7](07-kafka-advanced.md)).
- **Headers** — optional key/value metadata pairs, commonly used for tracing.
- **Partition + offset** — assigned once the message is accepted; together with the
  topic name, this triplet uniquely identifies the message.
- **Timestamp** — set by the producer or the broker.

Before the key and value reach the broker, a **serializer** converts your
programmatic object into a byte array — this is why the producer configuration
specifies a key serializer and a value serializer independently:

![Message serialization diagram showing how Apache Kafka Producers integer and string serializers work.](https://www.conduktor.io/assets/kafka/Kafka-Producers-4.png)
*Image credit: Conduktor (conduktor.io)*

| Format | Strength | Requires |
|---|---|---|
| String/JSON | Flexible, easy to debug | No built-in schema |
| Avro | Compact, strong schema evolution support | Schema Registry |
| Protobuf | High performance, cross-language | Schema Registry |

Once serialized, the producer's **partitioner** decides which partition a message
lands in, commonly using the message key:

![Kafka Producers use default partitioning logic to assign Kafka messages to the appropriate Apache Kafka Partition.](https://www.conduktor.io/assets/kafka/Kafka-Producers-5.png)
*Image credit: Conduktor (conduktor.io)*

The default partitioner hashes the key with the **murmur2** algorithm:

```
targetPartition = Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1)
```

This is **key hashing** — deterministically mapping a key to a partition so that all
messages sharing that key always land on the same partition (giving per-key
ordering). A practical decision guide for when to use a key:

| Use case | Key choice |
|---|---|
| Order processing | Order ID — keep all events for an order together |
| User activity tracking | User ID — maintain per-user event sequence |
| IoT sensor data | Device ID — preserve per-device ordering |
| Log aggregation | No key — maximize throughput/spread |
| Metrics collection | No key — even distribution |

**Rule of thumb:** use a key when you need ordering guarantees for related messages;
skip it when maximum throughput and even distribution matter more.

### 2. Consumer fundamentals, revisited

Consumers implement a **pull model**: rather than brokers pushing data, consumers
request it by calling `.poll()`, giving each consumer control over its own
consumption rate (natural backpressure, no risk of a slow consumer overwhelming
itself):

![Sequence diagram of the Kafka consumer pull model: the consumer calls poll() to request messages, the broker returns a batch, the consumer processes it, then polls again for the next batch.](https://www.conduktor.io/assets/kafka/diagrams/kafka-consumers.svg)
*Image credit: Conduktor (conduktor.io)*

A consumer always reads a partition from lower to higher offsets — it cannot read
backwards — and by default only consumes data produced **after** it first connects
(reading historical data requires explicitly seeking, e.g., via
`auto.offset.reset=earliest`, see [Module 7](07-kafka-advanced.md)).

Just as producers serialize, consumers must **deserialize** using the exact format
the producer used to serialize — a `StringSerializer`-produced message must be read
with a `StringDeserializer`, an integer key needs an `IntegerDeserializer`, and so on:

![Kafka Consumers must use the same format for deserialization that was used by the producer when serializing the message.](https://www.conduktor.io/assets/kafka/Kafka-Consumers-2.png)
*Image credit: Conduktor (conduktor.io)*

A message that doesn't match the expected format is called a **poison pill** — it can
crash a consumer or feed bad data downstream if not handled deliberately. Common
strategies:

| Strategy | When to use |
|---|---|
| Fail fast | Development, testing |
| Log and skip | Non-critical data, metrics |
| Dead letter queue | Production, when the bad record must be recoverable |
| Schema validation at the producer | Best — prevents bad data from ever being produced |

For horizontal scalability, consumers are organized into **consumer groups**
(`group.id`). Kafka assigns each partition to exactly one consumer within a group,
though one consumer can own multiple partitions:

![Apache Kafka Consumer Group diagram showing how a consumer group reads messages from a Kafka topic with 5 partitions.](https://www.conduktor.io/assets/kafka/Consumer-Group-reading-from-topic-with-5-partitions.png)
*Image credit: Conduktor (conduktor.io)*

Scaling has a hard ceiling — **the maximum number of active consumers in a group
equals the number of partitions**; extra consumers sit idle:

| Partitions | Consumers | Result |
|---|---|---|
| 3 | 1 | One consumer handles all 3 partitions |
| 3 | 2 | Partitions split 2/1 across consumers |
| 3 | 3 | Each consumer handles exactly 1 partition (optimal) |
| 3 | 5 | 3 active, 2 permanently idle |

Multiple independent applications (each with a distinct `group.id`) can consume the
same topic simultaneously without interfering with each other — Kafka tracks
progress per group, not per topic.

### 3. Project setup: Maven + Kafka + Schema Registry

A typical Maven `pom.xml` for a Kafka Java application using Avro and Confluent Schema
Registry needs three dependency groups: the Kafka clients, Avro, and the Confluent
serializers — plus the Confluent Maven repository (Confluent's serializers aren't on
Maven Central) and the Avro code-generation plugin.

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>kafka-ccdak-tutorial</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <kafka.version>3.7.0</kafka.version>
        <confluent.version>7.6.0</confluent.version>
        <avro.version>1.11.3</avro.version>
    </properties>

    <repositories>
        <repository>
            <id>confluent</id>
            <url>https://packages.confluent.io/maven/</url>
        </repository>
    </repositories>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${kafka.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro</artifactId>
            <version>${avro.version}</version>
        </dependency>
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-avro-serializer</artifactId>
            <version>${confluent.version}</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.avro</groupId>
                <artifactId>avro-maven-plugin</artifactId>
                <version>${avro.version}</version>
                <executions>
                    <execution>
                        <phase>generate-sources</phase>
                        <goals><goal>schema</goal></goals>
                        <configuration>
                            <sourceDirectory>${project.basedir}/src/main/avro</sourceDirectory>
                            <outputDirectory>${project.basedir}/src/main/java</outputDirectory>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

### 4. Data modeling with Avro and Schema Registry

Define the record shape as an Avro schema (`src/main/avro/OrderCreated.avsc`):

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.example.events",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "customerId", "type": "string" },
    { "name": "amount", "type": "double" },
    { "name": "createdAt", "type": "long", "logicalType": "timestamp-millis" }
  ]
}
```

Running `mvn generate-sources` generates a Java class `OrderCreated` from this schema.
Schema Registry stores this schema centrally and enforces **compatibility rules** as the
schema evolves (see below), decoupling producer and consumer schema versions.

### 5. Producer: configuration and code

Key producer configuration decisions map directly to exam objectives around tuning and
durability:

```java
import org.apache.kafka.clients.producer.*;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import com.example.events.OrderCreated;

import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");

        // Durability / correctness
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);

        // Throughput tuning
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_768);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        try (Producer<String, OrderCreated> producer = new KafkaProducer<>(props)) {
            OrderCreated event = OrderCreated.newBuilder()
                    .setOrderId("ord-123")
                    .setCustomerId("cust-456")   // used as the message key below
                    .setAmount(99.90)
                    .setCreatedAt(System.currentTimeMillis())
                    .build();

            // Keying by customerId ensures all events for the same customer
            // land on the same partition, preserving per-customer ordering.
            ProducerRecord<String, OrderCreated> record =
                    new ProducerRecord<>("orders-created", event.getCustomerId(), event);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    // Distinguish retryable vs. non-retryable per Module 1
                    System.err.println("Failed to send: " + exception.getMessage());
                } else {
                    System.out.printf("Sent to partition %d at offset %d%n",
                            metadata.partition(), metadata.offset());
                }
            });
        }
    }
}
```

**Why the key matters:** the default partitioner computes
`partition = hash(key) % numPartitions`. Choosing `customerId` as the key guarantees
every order for a given customer is appended, in order, to the same partition —
which is what gives you per-key ordering guarantees in Kafka. Messages without a key
use the sticky partitioner (Kafka 2.4+), which batches efficiently but gives no
ordering guarantee across keys.

### 6. Consumer: configuration and code

```java
import org.apache.kafka.clients.consumer.*;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import com.example.events.OrderCreated;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-processing-service");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        // At-least-once: commit only after successful processing
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, OrderCreated> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("orders-created"));

            while (true) {
                ConsumerRecords<String, OrderCreated> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, OrderCreated> record : records) {
                    process(record.value()); // must be idempotent — see Module 1
                }

                if (!records.isEmpty()) {
                    consumer.commitSync(); // commit after processing = at-least-once
                }
            }
        }
    }

    private static void process(OrderCreated event) {
        System.out.println("Processing order " + event.getOrderId());
    }
}
```

### 7. Schema evolution and compatibility

Schema Registry enforces a **compatibility mode** per subject (topic-schema binding),
the most common being:

| Mode | Allows | Typical use |
|---|---|---|
| `BACKWARD` | New schema can read data written with the previous schema | Default; safe for evolving consumers first |
| `FORWARD` | Old schema can read data written with the new schema | Safe when producers upgrade first |
| `FULL` | Both backward and forward compatible | Strictest, safest for shared topics |
| `NONE` | No compatibility checks | Rare; only for isolated/internal use |

The most common backward-compatible change is **adding an optional field with a
default value** (e.g., adding `discountCode` with `"default": null`). Removing a
required field or changing a field's type are classic **breaking** changes.

### 8. Delivery semantics recap (developer's perspective)

| Semantic | How it's achieved | Trade-off |
|---|---|---|
| At-most-once | Commit offset before processing | Risk of message loss on failure |
| At-least-once | Commit offset after processing | Risk of duplicate processing on restart — **make processing idempotent** |
| Exactly-once | Kafka transactions (producer + consumer offsets in the same transaction), or Kafka Streams `EXACTLY_ONCE_V2` | Only guaranteed for Kafka-to-Kafka pipelines; requires idempotent producers and transactional consumers |

### 9. The Admin API

The Admin API lets applications manage topics and configuration programmatically,
instead of shelling out to CLI tools:

```java
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicAdmin {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic topic = new NewTopic("orders-created", 6, (short) 3)
                    .configs(Collections.singletonMap("min.insync.replicas", "2"));

            admin.createTopics(Collections.singletonList(topic)).all().get();

            ConfigResource resource =
                    new ConfigResource(ConfigResource.Type.TOPIC, "orders-created");
            var config = admin.describeConfigs(Collections.singletonList(resource))
                    .all().get().get(resource);
            System.out.println(config);
        }
    }
}
```

## Self-check questions

1. What are the six components of a Kafka message's anatomy?
2. What formula does the default partitioner use to route a keyed message to a
   partition?
3. If a topic has 4 partitions and a consumer group has 6 consumer instances, how many
   consumers will be idle?
4. What is a "poison pill," and what's the best strategy to prevent one from ever
   reaching a consumer?
5. Why does the Maven `pom.xml` need the Confluent Maven repository in addition to
   Maven Central?
6. Why did the producer example key messages by `customerId` instead of leaving the key
   null?
7. What's the risk of setting `enable.auto.commit=true` in the consumer, and how does
   the example above avoid it?
8. Which Schema Registry compatibility mode should you choose if consumers and
   producers upgrade independently and in any order?
9. What does the Admin API example's `min.insync.replicas` configuration interact with
   on the producer side to achieve strong durability?

## Answer key

1. Key, value, compression type, headers, partition + offset, and timestamp.
2. `targetPartition = Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1)` — the
   key's hash deterministically maps it to the same partition every time.
3. 2 idle consumers — the maximum number of active consumers in a group equals the
   partition count (4), so the remaining 2 of the 6 consumers get no partitions.
4. A poison pill is a message that doesn't match the format a consumer expects to
   deserialize, potentially crashing it or corrupting downstream data. The best
   prevention is schema validation at the producer (e.g., via Schema Registry) so a
   bad message is rejected before it's ever produced.
5. Confluent's serializers (`kafka-avro-serializer`, etc.) are published to Confluent's
   own Maven repository, not to Maven Central.
6. To guarantee ordering per customer: `hash(customerId) % partitions` always routes
   the same customer's events to the same partition.
7. Auto-commit can commit offsets on a timer regardless of whether processing actually
   completed, risking message loss (at-most-once) if the app crashes after commit but
   before finishing processing. The example disables auto-commit and calls
   `commitSync()` only after successful processing (at-least-once).
8. `FULL` compatibility — it guarantees both backward and forward compatibility.
9. `acks=all` on the producer — the write is only acknowledged once
   `min.insync.replicas` replicas have the data, giving a durability guarantee tied to
   this topic configuration.

Continue to [Module 3 — Apache Kafka Streams](03-kafka-streams.md).

# Introduction: Preparing for the Confluent Certified Developer for Apache Kafka (CCDAK) Exam

## Learning objectives

By the end of this module you will be able to:

- Explain what the CCDAK exam validates and who it is designed for.
- Describe the exam's question formats and how they are scored.
- Map the exam blueprint's six sections to a study plan ordered by weight.
- Identify what is explicitly **out of scope** so you don't waste study time.

## Who this tutorial is for

This tutorial is written for developers who already have hands-on experience with Kafka
(producers, consumers, basic topic operations) but have not gone deep into tuning,
Kafka Streams, Kafka Connect, testing, or observability. If you can write a basic
producer/consumer in Java today, you're in the right place.

## What the exam validates

CCDAK certifies that you can:

- Connect to a secured Apache Kafka cluster.
- Produce and consume from topics, and model Kafka datasets correctly.
- Define topic configurations (partitions, compression, replication).
- Use the Kafka Admin API.
- Deploy, test, and tune Kafka client applications.
- Write Kafka Streams applications and understand serialization/deserialization (SerDe).
- Monitor applications and implement processing semantics (exactly-once / at-least-once).
- Configure and deploy Kafka Connect connectors (source, sink, CDC).
- Troubleshoot Kafka applications and work with the CLI.

## Exam blueprint — study roadmap by weight

The exam is organized into six weighted sections. Use this table to prioritize your
study time — spend roughly proportional effort to each section's weight.

| # | Section | Weight | Tutorial module |
|---|---|---|---|
| 1 | Apache Kafka Fundamentals | 23% | [Module 1](01-kafka-fundamentals.md) |
| 2 | Apache Kafka Application Development | 28% | [Module 2](02-application-development.md) |
| 3 | Apache Kafka Streams | 12% | [Module 3](03-kafka-streams.md) |
| 4 | Kafka Connect | 15% | [Module 4](04-kafka-connect.md) |
| 5 | Application Testing | 8% | [Module 5](05-application-testing.md) |
| 6 | Application Observability | 13% | [Module 6](06-application-observability.md) |
| 7 | Advanced Topics | Extra | [Module 7](07-kafka-advanced.md) |
| 8 | Kafka Streams Advanced | Extra | [Module 8](08-kafka-streams-advanced.md) |
| 9 | Exam Day Preparation | Extra | [Module 9](09-exam-day-and-next-steps.md) |

Together, Sections 1 and 2 make up more than half the exam (51%) — mastering core
architecture, producers, and consumers pays off the most.

## Exam question formats

- **Multiple-choice** — select the one best answer.
- **Multiple-response** — select more than one correct option.
- **Matching** — match items in a list to the correct counterpart.
- **Build list** — arrange items into the correct order (e.g., steps of a process).

Distractors (wrong answers) are designed to be plausible to someone with partial
knowledge, so precise understanding — not just familiarity — matters.

## Out of scope

The exam explicitly does **not** cover:

- Confluent-specific components: RBAC, ksqlDB, Confluent for Kubernetes (CFK).
- System-specific Kafka Connect connectors (e.g., a specific JDBC driver's quirks).
- Plugins and extensions.
- Networking configuration.
- Administering and deploying a Kafka cluster (broker ops, cluster setup).
- Infrastructure provisioning.

This tutorial mirrors that scope: it focuses on **developer-facing** concepts (how a
client application connects to, produces to, consumes from, and processes Kafka data),
not on operating a cluster.

## How to use this tutorial

Each module follows the same structure:

1. **Learning objectives** — what you'll be able to do.
2. **Guided walkthrough** — a narrative explanation building on the previous module.
3. **Self-check questions** — validate your understanding before moving on.
4. **Answer key** — check your self-check answers.

Work through the modules in order — later modules assume you understand concepts from
earlier ones (e.g., Module 3 on Kafka Streams assumes you understand consumer groups
from Module 2).

Continue to [Module 1 — Apache Kafka Fundamentals](01-kafka-fundamentals.md).

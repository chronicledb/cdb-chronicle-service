# cdb-chronicle-service

A gRPC service that acts as a write-ahead log gateway, enforcing strict per-database transaction ordering before durably writing to Kafka.

## Overview

`cdb-chronicle-service` sits between clients and a Kafka log. Clients submit transactions with a sequence number, and the service guarantees that transactions are committed in strict order — a transaction is only accepted if its sequence number is exactly one greater than the last committed sequence number for that database.

```
Client → gRPC → cdb-chronicle-service → Kafka
```

On startup, the service recovers the last committed sequence number for every database directly from Kafka, so no external state store is required.

## Prerequisites

- Java 21+
- Maven
- A running Kafka broker

## Configuration

The service is configured via environment variables:

| Variable | Description |
|---|---|
| `CHRONICLE_SERVICE_PORT` | Port the gRPC server listens on |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers (e.g. `localhost:9092`) |

## Running

```bash
export CHRONICLE_SERVICE_PORT=9090
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

mvn package
java -jar target/cdb-chronicle-service.jar
```

## API

Defined in `chronicle.proto`.

### `AppendTx`

Appends a transaction to the log for a given database.

**Request**

| Field | Type | Description                             |
|---|---|-----------------------------------------|
| `cdb_id` | string | The chronicle database identifier       |
| `seq_num` | int64 | The sequence number of this transaction |
| `tx` | string | The transaction payload                 |

**Response**

| Field | Type | Description |
|---|---|---|
| `committed_seq_num` | int64 | The sequence number that was committed |

**Error codes**

| Code | Meaning |
|---|---|
| `ABORTED` | Sequence number mismatch — retry with the correct sequence number |
| `INTERNAL` | Kafka write failed — retry later |

**Example usage**

```java
final ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", 9090)
        .usePlaintext()
        .build();

final ChronicleServiceGrpc.ChronicleServiceBlockingStub stub =
        ChronicleServiceGrpc.newBlockingStub(channel);

final AppendTxRequest request = AppendTxRequest.newBuilder()
        .setCdbId("my-database")
        .setSeqNum(1)
        .setTx("{\"op\": \"insert\", \"key\": \"foo\"}")
        .build();

final AppendTxResponse response = stub.appendTx(request);
System.out.println("Committed seq num: " + response.getCommittedSeqNum());
```

## Sequence Number Semantics

Each database (`cdb_id`) has its own independent sequence number starting at 1. The service rejects any transaction whose sequence number is not exactly `lastCommittedSeqNum + 1`. On `ABORTED`, the client should fetch the current committed sequence number and retry accordingly.

## Architecture

- **Stripe locking** — `cdb_id`s are hashed across 1024 `ReentrantLock` stripes, allowing unrelated databases to commit in parallel while serializing concurrent writes to the same database
- **Synchronous Kafka writes** — transactions are written with `acks=all` and idempotence enabled before the client is acknowledged, ensuring durability
- **Startup recovery** — on boot, the service reads the last record from each Kafka topic partition to rebuild in-memory sequence number state before accepting traffic

## Running Tests

```bash
mvn test
```

Tests use plain Java stubs with no external dependencies — no Kafka broker or mocking framework required.
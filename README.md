# Distributed Runner

A Java library for distributing work across threads with automatic retry, backpressure, and optional result accumulation. Supports batch processing, interactive workflows, and continuous streaming patterns.

## Installation

Add the JitPack repository and dependency to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.github.galkahana</groupId>
        <artifactId>distributed-runner-java</artifactId>
        <version>v1.0</version>
    </dependency>
</dependencies>
```

## Use Cases

- [**Partitioned Parallel Processing**](#1-partitioned-parallel-processing) — Split data into partitions, process each in parallel (side-effect-only)
- [**Batch Processing with Accumulation**](#2-batch-processing-with-accumulation) — Process a list of items, accumulate results into a single value
- [**Non-blocking Interactive Workflow**](#3-non-blocking-interactive-workflow) — Submit tasks, monitor progress, cancel early if needed
- [**Streaming Event Processing**](#4-streaming-event-processing) — Continuously ingest and process events with backpressure

### 1. Partitioned Parallel Processing

Use `SimplifiedMapReduce` to split work into partitions processed by a thread pool. Each partition receives its index and total count, allowing it to select its slice of data independently.

This example reads database rows using range-based partitioning (index-friendly `WHERE id >= ? AND id < ?`) and writes each row to Kafka:

```java
record DBAndKafka(DataSource db, KafkaProducer<String, Event> producer) {}

SimplifiedMapReduce<DBAndKafka, Void, Void> mapReduce = new SimplifiedMapReduce<>(
    12,  // partitions
    6,   // worker threads
    3    // max retries per partition
);

mapReduce.process(
    new DBAndKafka(dataSource, kafkaProducer),
    task -> {
        // Range-based partitioning: divide the ID space into equal ranges
        long rangeSize = Long.MAX_VALUE / task.totalPartitions();
        long rangeStart = rangeSize * task.partitionIndex();
        long rangeEnd = (task.partitionIndex() == task.totalPartitions() - 1)
            ? Long.MAX_VALUE
            : rangeSize * (task.partitionIndex() + 1);

        String sql = "SELECT * FROM events WHERE id >= ? AND id < ?";
        try (PreparedStatement stmt = task.data().db().getConnection().prepareStatement(sql)) {
            stmt.setLong(1, rangeStart);
            stmt.setLong(2, rangeEnd);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Event event = mapRow(rs);
                task.data().producer().send(
                    new ProducerRecord<>("events-topic", event.getId(), event));
            }
        }
        return null;
    }
).join();
```

### 2. Batch Processing with Accumulation

Use `WorkDistributor.process()` to process a list of items in parallel and accumulate results into a single value. Returns a `CompletableFuture` — call `.join()` to block, or compose with `.thenApply()`, `.thenAccept()`, etc.

```java
WorkDistributor<Integer, Integer, Integer> distributor = new WorkDistributor<>(
    10,            // worker threads
    100,           // queue capacity
    3,             // max retries
    x -> x * 2,   // processing function
    Integer::sum,  // accumulator
    0              // initial accumulator value
);

Integer sum = distributor.process(List.of(1, 2, 3, 4, 5)).join();
// sum = 30
```

### 3. Non-blocking Interactive Workflow

Use `start()` / `submitTask()` / `getStats()` for fine-grained control. Submit tasks on your own schedule, poll progress, and shut down early if needed.

```java
WorkDistributor<Integer, Integer, Integer> distributor = new WorkDistributor<>(
    10, 100, 3, x -> processData(x), Integer::sum, 0);

distributor.start();

// Submit tasks
for (int i = 0; i < 10_000; i++) {
    distributor.submitTask(i, i);
}

// Poll progress
CompletableFuture<Integer> future = distributor.awaitCompletion();
while (!future.isDone()) {
    Stats stats = distributor.getStats();
    System.out.printf("completed=%d, failed=%d, pending=%d%n",
        stats.completed(), stats.failed(), stats.pendingWork());

    if (shouldCancel()) {
        distributor.shutdown();  // graceful: finish in-progress, discard queued
        break;
    }
    Thread.sleep(1000);
}

Integer result = distributor.getResult();
```

### 4. Streaming Event Processing

Use the same `start()` / `submitTask()` API for continuous event ingestion. The bounded work queue provides natural backpressure — `submitTask()` blocks when the queue is full.

```java
WorkDistributor<Event, Void, Void> distributor = new WorkDistributor<>(
    20,     // worker threads
    1000,   // queue capacity (backpressure threshold)
    3,      // max retries
    event -> {
        processEvent(event);  // write to DB, send to Kafka, etc.
        return null;
    }
);

distributor.start();

// Main thread ingests events continuously
int eventId = 0;
while (running) {
    Event event = eventSource.poll();
    if (event != null) {
        distributor.submitTask(event, eventId++);
    }

    // Monitor throughput
    if (eventId % 10_000 == 0) {
        Stats stats = distributor.getStats();
        log.info("Processed: {}, Pending: {}", stats.completed(), stats.pendingWork());
    }
}

distributor.shutdown();
```

## Development

Requires Java 17+.

```bash
./mvnw clean install   # build
./mvnw test            # run tests
```

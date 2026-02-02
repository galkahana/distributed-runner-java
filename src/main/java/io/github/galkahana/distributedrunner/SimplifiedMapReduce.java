package io.github.galkahana.distributedrunner;

import io.github.galkahana.distributedrunner.WorkDistributor.ThrowingFunction;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

/**
 * A local multithreaded map-reduce implementation for parallel data processing with automatic partition distribution.
 *
 * <p>
 * Provides a simple, reliable way to parallelize large data processing tasks across multiple threads using a
 * partitioning strategy. Each partition operates independently, optionally aggregating results at the end.
 * Features:
 * <ul>
 *   <li><b>Partition-based parallelism:</b> Splits work into N partitions processed by M worker threads</li>
 *   <li><b>Flexible data access:</b> Each partition receives its index (0 to N-1) and total count to filter relevant data</li>
 *   <li><b>Optional result aggregation:</b> Combine partition results using a custom accumulator function</li>
 *   <li><b>Automatic retry logic:</b> Failed partitions are retried up to maxRetries times before failing</li>
 *   <li><b>Type-safe generics:</b> Strongly typed shared data (D), partition output (O), and accumulator (A)</li>
 * </ul>
 * </p>
 *
 * <h2>Usage Patterns</h2>
 *
 * <h3>Pattern 1: Side-Effect-Only Processing (No Accumulation)</h3>
 * <p>
 * When each partition performs side effects (e.g., writing to external systems) without needing to aggregate results.
 * </p>
 * <pre>{@code
 * record DBAndKafka(DataSource db, KafkaProducer<String, Event> producer) {}
 *
 * SimplifiedMapReduce<DBAndKafka, Void, Void> mapReduce = new SimplifiedMapReduce<>(12, 6, 3);
 *
 * mapReduce.process(
 *     dbAndKafka,
 *     task -> {
 *         String sql = """
 *             SELECT * FROM events
 *             WHERE MOD(ABS(event_hash), ?) = ?
 *             """;
 *         try (PreparedStatement stmt = task.data().db().getConnection().prepareStatement(sql)) {
 *             stmt.setInt(1, task.totalPartitions());
 *             stmt.setInt(2, task.partitionIndex());
 *             ResultSet rs = stmt.executeQuery();
 *             while (rs.next()) {
 *                 Event event = mapRow(rs);
 *                 task.data().producer().send(new ProducerRecord<>("events-topic", event.getId(), event));
 *             }
 *         }
 *         return null;  // No result needed for side-effect-only processing
 *     }
 * );
 * }</pre>
 *
 * <h3>Pattern 2: Hash-Based Entity Partitioning</h3>
 * <p>
 * When processing entities with stable identifiers (IDs, hash columns), use modulo hashing to distribute
 * entities across partitions. Each partition processes entities where {@code Math.abs(hash) % totalPartitions == partitionIndex}.
 * </p>
 *
 * <p><b>SQL Example:</b> Processing database rows with a hash column</p>
 * <pre>{@code
 * SimplifiedMapReduce<DBConfig, Long, Long> mapReduce = new SimplifiedMapReduce<>(8, 4, 3);
 *
 * Long totalCount = mapReduce.process(
 *     dbConfig,
 *     task -> {
 *         String sql = """
 *             SELECT COUNT(*) FROM entities
 *             WHERE MOD(ABS(entity_hash), ?) = ?
 *             """;
 *         try (PreparedStatement stmt = task.data().getConnection().prepareStatement(sql)) {
 *             stmt.setInt(1, task.totalPartitions());
 *             stmt.setInt(2, task.partitionIndex());
 *             ResultSet rs = stmt.executeQuery();
 *             return rs.next() ? rs.getLong(1) : 0L;
 *         }
 *     },
 *     Long::sum,  // Sum counts from all partitions
 *     0L
 * );
 * }</pre>
 *
 * <p><b>Java Example:</b> Processing in-memory entities</p>
 * <pre>{@code
 * List<Entity> entities = loadAllEntities();
 *
 * SimplifiedMapReduce<List<Entity>, Integer, Integer> mapReduce = new SimplifiedMapReduce<>(8, 4, 3);
 *
 * Integer totalProcessed = mapReduce.process(
 *     entities,
 *     task -> {
 *         return task.data().stream()
 *             .filter(e -> Math.abs(e.getId().hashCode()) % task.totalPartitions() == task.partitionIndex())
 *             .mapToInt(e -> processEntity(e))
 *             .sum();
 *     },
 *     Integer::sum,
 *     0
 * );
 * }</pre>
 *
 * <h3>Pattern 3: Range-Based Historical Data Partitioning</h3>
 * <p>
 * For time-series or sequential data, divide the full time/ID range into equal intervals. Each partition
 * processes one interval based on its index.
 * </p>
 * <pre>{@code
 * record TimeRange(Instant start, Instant end) {}
 *
 * SimplifiedMapReduce<TimeRange, Long, Long> mapReduce = new SimplifiedMapReduce<>(10, 5, 3);
 *
 * Long totalRecords = mapReduce.process(
 *     new TimeRange(startTime, endTime),
 *     task -> {
 *         Duration fullRange = Duration.between(task.data().start(), task.data().end());
 *         Duration interval = fullRange.dividedBy(task.totalPartitions());
 *
 *         Instant partitionStart = task.data().start().plus(interval.multipliedBy(task.partitionIndex()));
 *         Instant partitionEnd = task.data().start().plus(interval.multipliedBy(task.partitionIndex() + 1));
 *
 *         String sql = """
 *             SELECT COUNT(*) FROM events
 *             WHERE timestamp >= ? AND timestamp < ?
 *             """;
 *         // Execute query with partitionStart and partitionEnd...
 *         return executeCount(sql, partitionStart, partitionEnd);
 *     },
 *     Long::sum,
 *     0L
 * );
 * }</pre>
 *
 * @param <D> Shared data type passed to all partitions (e.g., DB connection info, config, data list)
 * @param <O> Output type from processing a single partition
 * @param <A> Accumulator type for aggregating results across all partitions
 * @param numPartitions Number of partitions to split work into (should be >= numWorkers to avoid idle workers)
 * @param numWorkers Number of concurrent worker threads
 * @param maxRetries Maximum retry attempts per partition before failing
 */
public record SimplifiedMapReduce<D, O, A>(int numPartitions, int numWorkers, int maxRetries) {

    /**
     * Information passed to each partition worker.
     *
     * @param partitionIndex Zero-based partition index (0 to totalPartitions-1)
     * @param totalPartitions Total number of partitions
     * @param data Generic data for this partition (e.g., connection info, configuration)
     */
    public record PartitionTask<D>(int partitionIndex, int totalPartitions, D data) {}

    /**
     * Process all partitions with result accumulation.
     *
     * @param sharedData Data passed to all partitions (e.g., DB connection string, config)
     * @param processor Function to process one partition and return a result
     * @param accumulator Function to combine partition results
     * @param initialValue Initial accumulator value
     * @return Accumulated result from all partitions
     */
    public A process(
            D sharedData,
            ThrowingFunction<PartitionTask<D>, O> processor,
            BiFunction<A, O, A> accumulator,
            A initialValue) throws InterruptedException {

        // Create work distributor
        WorkDistributor<PartitionTask<D>, O, A> distributor = new WorkDistributor<>(
                numWorkers,
                numPartitions,  // queue capacity = number of partitions
                maxRetries,
                processor,
                accumulator,
                initialValue
        );

        // Create partition tasks and process
        List<PartitionTask<D>> tasks = IntStream.range(0, numPartitions)
                .mapToObj(i -> new PartitionTask<>(i, numPartitions, sharedData))
                .toList();

        return distributor.process(tasks);
    }

    /**
     * Process all partitions without accumulation.
     * Use this when partition processing has side effects only (e.g., writing to DB, sending to kafka).
     *
     * @param sharedData Data passed to all partitions
     * @param processor Function to process one partition
     */
    public void process(
            D sharedData,
            ThrowingFunction<PartitionTask<D>, O> processor) throws InterruptedException {

        process(sharedData, processor, (a, o) -> null, null);
    }
}

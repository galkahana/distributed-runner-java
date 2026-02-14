package io.github.galkahana.distributedrunner;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

/**
 * Partition-based parallel processing built on {@link WorkDistributor}.
 * <p>
 * Splits work into N partitions processed by M worker threads. Each partition receives its index and
 * total count via {@link PartitionTask}, allowing it to select its data slice independently.
 * Supports optional result accumulation. See README for examples.
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
     * @return CompletableFuture that completes with the accumulated result from all partitions
     */
    public CompletableFuture<A> process(
            D sharedData,
            ThrowingFunction<PartitionTask<D>, O> processor,
            BiFunction<A, O, A> accumulator,
            A initialValue) throws InterruptedException {

        WorkDistributor<PartitionTask<D>, O, A> distributor = new WorkDistributor<>(
                numWorkers,
                numPartitions,  // queue capacity = number of partitions
                maxRetries,
                processor,
                accumulator,
                initialValue
        );

        // Create partition tasks per partition count, indicating partition index and total partitions
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
     * @return CompletableFuture that completes when all partitions finish
     */
    public CompletableFuture<Void> process(
            D sharedData,
            ThrowingFunction<PartitionTask<D>, O> processor) throws InterruptedException {

        return process(sharedData, processor, (a, o) -> null, null)
                .thenApply(ignored -> null);
    }
}

package io.github.galkahana.distributedrunner;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimplifiedMapReduceTest {

    private static final int NUM_PARTITIONS = 6;
    private static final int NUM_WORKERS = 4;
    private static final int MAX_RETRIES = 3;

    @Test
    public void testWithAccumulation_ReturnsAggregatedResult() throws Exception {
        // Arrange
        List<String> testData = List.of(
            "The quick brown fox",
            "jumps over the lazy dog",
            "Lorem ipsum dolor sit amet",
            "consectetur adipiscing elit",
            "sed do eiusmod tempor",
            "incididunt ut labore et dolore",
            "magna aliqua enim ad minim",
            "veniam quis nostrud exercitation",
            "ullamco laboris nisi ut aliquip",
            "ex ea commodo consequat"
        );
        // count the expected total letters
        int expectedLetters = testData.stream()
                .mapToInt(line -> (int) line.chars().filter(Character::isLetter).count())
                .sum();

        SimplifiedMapReduce<List<String>, Integer, Integer> mapReduce =
            new SimplifiedMapReduce<>(NUM_PARTITIONS, NUM_WORKERS, MAX_RETRIES);

        // Act (counting the letters using distributed tasks)
        Integer totalLetters = mapReduce.process(
            testData,
            task -> {
                // Filter lines for this partition and count letters
                return task.data().stream()
                    .filter(line -> Math.abs(line.hashCode()) % task.totalPartitions() == task.partitionIndex())
                    .mapToInt(line -> (int) line.chars().filter(Character::isLetter).count())
                    .sum();
            },
            Integer::sum,
            0
        );

        // Assert
        assertEquals(totalLetters.intValue(), expectedLetters);
    }

    @Test
    public void testWithoutAccumulation_ProcessesEachItemOnce() throws Exception {
        // Arrange
        List<Integer> numbers = IntStream.range(0, 20)
            .boxed()
            .collect(Collectors.toList());

        // Track number processing counts
        ConcurrentHashMap<Integer, AtomicInteger> processedCounts = new ConcurrentHashMap<>();

        SimplifiedMapReduce<List<Integer>, Void, Void> mapReduce =
            new SimplifiedMapReduce<>(NUM_PARTITIONS, NUM_WORKERS, MAX_RETRIES);

        // Act
        mapReduce.process(
            numbers,
            task -> {
                // Filter numbers for this partition and count them
                task.data().stream()
                    .filter(num -> Math.abs(num.hashCode()) % task.totalPartitions() == task.partitionIndex())
                    .forEach(num -> processedCounts.computeIfAbsent(num, k -> new AtomicInteger(0)).incrementAndGet());
                return null;
            }
        );

        // Assert - Verify each number was processed exactly once
        for (Integer num : numbers) {
            int count = processedCounts.getOrDefault(num, new AtomicInteger(0)).get();
            assertEquals(count, 1, "Number " + num + " should be processed exactly once");
        }
    }

    @Test
    public void testTransientFailures_RecoverWithRetries() throws Exception {
        // Arrange
        List<Integer> numbers = IntStream.range(0, 10)
            .boxed()
            .collect(Collectors.toList());

        // Track attempt counts per partition
        ConcurrentHashMap<Integer, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            attemptCounts.put(i, new AtomicInteger(0));
        }

        SimplifiedMapReduce<List<Integer>, Integer, Integer> mapReduce =
            new SimplifiedMapReduce<>(NUM_PARTITIONS, NUM_WORKERS, MAX_RETRIES);

        // Act
        Integer sum = mapReduce.process(
            numbers,
            task -> {
                int attemptNumber = attemptCounts.get(task.partitionIndex()).incrementAndGet();

                // Fail on first two attempts for each partition
                if (attemptNumber <= 2) {
                    throw new RuntimeException("Simulated failure on attempt " + attemptNumber);
                }

                // Succeed on third attempt
                return task.data().stream()
                    .filter(num -> Math.abs(num.hashCode()) % task.totalPartitions() == task.partitionIndex())
                    .mapToInt(num -> num)
                    .sum();
            },
            Integer::sum,
            0
        );

        // Assert
        int expectedSum = numbers.stream().mapToInt(Integer::intValue).sum();
        assertEquals(sum.intValue(), expectedSum, "Sum should be correct after retries");

        // Verify all partitions were retried
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            assertEquals(attemptCounts.get(i).get(), 3, "Partition " + i + " should have 3 attempts");
        }
    }

    @Test
    public void testPersistentFailures_ThrowsException() {
        // Arrange
        List<Integer> numbers = List.of(1, 2, 3);

        SimplifiedMapReduce<List<Integer>, Integer, Integer> mapReduce =
            new SimplifiedMapReduce<>(NUM_PARTITIONS, NUM_WORKERS, 2); // Only 2 retries

        // Act & Assert - This should throw after max retries exceeded
        assertThrows(RuntimeException.class, () -> {
            mapReduce.process(
                numbers,
                task -> {
                    // Always fail
                    throw new RuntimeException("Persistent failure");
                },
                Integer::sum,
                0
            );
        });
    }

    @Test
    public void testManyItems_DistributeAcrossAllPartitions() throws Exception {
        // Arrange
        List<String> testData = IntStream.range(0, 100)
            .mapToObj(i -> "Item-" + i)
            .collect(Collectors.toList());

        ConcurrentHashMap<Integer, AtomicInteger> itemsPerPartition = new ConcurrentHashMap<>();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            itemsPerPartition.put(i, new AtomicInteger(0));
        }

        SimplifiedMapReduce<List<String>, Integer, Integer> mapReduce =
            new SimplifiedMapReduce<>(NUM_PARTITIONS, NUM_WORKERS, MAX_RETRIES);

        // Act
        mapReduce.process(
            testData,
            task -> {
                int count = (int) task.data().stream()
                    .filter(item -> Math.abs(item.hashCode()) % task.totalPartitions() == task.partitionIndex())
                    .count();

                itemsPerPartition.get(task.partitionIndex()).set(count);
                return count;
            },
            Integer::sum,
            0
        );

        // Assert
        int totalItems = itemsPerPartition.values().stream()
            .mapToInt(AtomicInteger::get)
            .sum();

        assertEquals(totalItems, testData.size(), "All items should be distributed");

        // Each partition should have at least some items (not necessarily equal distribution)
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            assertTrue(itemsPerPartition.get(i).get() > 0,
                "Partition " + i + " should have at least one item");
        }
    }

    @Test
    public void testUsingDifferentPartitionCounts_YieldsSameResults() throws Exception {
        // Arrange
        List<String> testData = List.of(
            "alpha", "beta", "gamma", "delta", "epsilon",
            "zeta", "eta", "theta", "iota", "kappa"
        );

        // Act - Process with different partition counts
        Integer result4Partitions = new SimplifiedMapReduce<List<String>, Integer, Integer>(4, 4, MAX_RETRIES)
            .process(testData, this::countLettersInPartition, Integer::sum, 0);

        Integer result8Partitions = new SimplifiedMapReduce<List<String>, Integer, Integer>(8, 4, MAX_RETRIES)
            .process(testData, this::countLettersInPartition, Integer::sum, 0);

        Integer result2Partitions = new SimplifiedMapReduce<List<String>, Integer, Integer>(2, 2, MAX_RETRIES)
            .process(testData, this::countLettersInPartition, Integer::sum, 0);

        // Assert - All should produce the same result
        assertEquals(result4Partitions, result8Partitions,
            "Results should be consistent regardless of partition count");
        assertEquals(result4Partitions, result2Partitions,
            "Results should be consistent regardless of partition count");
    }

    // Helper method for counting letters in a partition
    private Integer countLettersInPartition(SimplifiedMapReduce.PartitionTask<List<String>> task) {
        return task.data().stream()
            .filter(line -> Math.abs(line.hashCode()) % task.totalPartitions() == task.partitionIndex())
            .mapToInt(line -> (int) line.chars().filter(Character::isLetter).count())
            .sum();
    }
}

package io.github.galkahana.distributedrunner;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class WorkDistributorTest {

    private static final int NUM_WORKERS = 4;
    private static final int QUEUE_CAPACITY = 100;
    private static final int MAX_RETRIES = 3;

    @Test
    public void testWithAccumulation_ReturnsAggregatedResult() throws Exception {
        // Arrange
        List<Integer> numbers = IntStream.range(0, 50)
            .boxed()
            .collect(Collectors.toList());

        int expectedSum = numbers.stream().mapToInt(Integer::intValue).sum();

        WorkDistributor<Integer, Integer, Integer> distributor = new WorkDistributor<>(
            NUM_WORKERS,
            QUEUE_CAPACITY,
            MAX_RETRIES,
            num -> num * 2,  // Double each number
            Integer::sum,
            0
        );

        // Act
        Integer result = distributor.process(numbers).join();

        // Assert
        assertEquals(result.intValue(), expectedSum * 2, "Sum of doubled numbers should match");
    }

    @Test
    public void testWithoutAccumulation_ProcessesEachItemOnce() throws Exception {
        // Arrange
        List<Integer> numbers = IntStream.range(0, 50)
            .boxed()
            .collect(Collectors.toList());

        ConcurrentHashMap<Integer, AtomicInteger> processedCounts = new ConcurrentHashMap<>();

        WorkDistributor<Integer, Void, Void> distributor = new WorkDistributor<>(
            NUM_WORKERS,
            QUEUE_CAPACITY,
            MAX_RETRIES,
            num -> {
                processedCounts.computeIfAbsent(num, k -> new AtomicInteger(0)).incrementAndGet();
                return null;
            }
        );

        // Act
        distributor.process(numbers).join();

        // Assert - Verify each number was processed exactly once
        assertEquals(processedCounts.size(), numbers.size(), "All numbers should be processed");
        for (Integer num : numbers) {
            int count = processedCounts.getOrDefault(num, new AtomicInteger(0)).get();
            assertEquals(count, 1, "Number " + num + " should be processed exactly once");
        }
    }

    @Test
    public void testTransientFailures_RecoverWithRetries() throws Exception {
        // Arrange
        List<Integer> numbers = IntStream.range(0, 20)
            .boxed()
            .collect(Collectors.toList());

        // Track attempts per task ID
        ConcurrentHashMap<Integer, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();

        WorkDistributor<Integer, Integer, Integer> distributor = new WorkDistributor<>(
            NUM_WORKERS,
            QUEUE_CAPACITY,
            MAX_RETRIES,
            num -> {
                int attemptNumber = attemptCounts.computeIfAbsent(num, k -> new AtomicInteger(0)).incrementAndGet();

                // Fail on first two attempts
                if (attemptNumber <= 2) {
                    throw new RuntimeException("Simulated failure on attempt " + attemptNumber);
                }

                // Succeed on third attempt
                return num;
            },
            Integer::sum,
            0
        );

        // Act
        Integer sum = distributor.process(numbers).join();

        // Assert
        int expectedSum = numbers.stream().mapToInt(Integer::intValue).sum();
        assertEquals(sum.intValue(), expectedSum, "Sum should be correct after retries");

        // Verify all tasks were retried at least 3 times
        for (Integer num : numbers) {
            int attempts = attemptCounts.get(num).get();
            assertEquals(attempts, 3, "Task " + num + " should have 3 attempts");
        }
    }

    @Test
    public void testPersistentFailures_ThrowsException() {
        // Arrange
        List<Integer> numbers = List.of(1, 2, 3);

        WorkDistributor<Integer, Integer, Integer> distributor = new WorkDistributor<>(
            NUM_WORKERS,
            QUEUE_CAPACITY,
            2,  // Only 2 retries
            num -> {
                // Always fail
                throw new RuntimeException("Persistent failure");
            },
            Integer::sum,
            0
        );

        // Act & Assert - This should throw after max retries exceeded
        CompletionException ex = assertThrows(CompletionException.class,
            () -> distributor.process(numbers).join());
        assertInstanceOf(RuntimeException.class, ex.getCause());
    }

    @Test
    public void testShutdown_DiscardsQueuedTasks() throws Exception {
        // Arrange - Large number of slow tasks
        int totalTasks = 200;

        WorkDistributor<Integer, Void, Void> distributor = new WorkDistributor<>(
            NUM_WORKERS,
            totalTasks,  // Large queue to hold all tasks
            MAX_RETRIES,
            num -> {
                Thread.sleep(100);  // Slow task to allow time for shutdown
                return null;
            }
        );

        // Act
        distributor.start();

        // Submit all tasks
        for (int i = 0; i < totalTasks; i++) {
            distributor.submitTask(i, i);
        }

        // Wait a bit for some tasks to start processing
        Thread.sleep(200);

        // Graceful shutdown - should discard queued tasks
        distributor.shutdown();

        // Assert
        Stats stats = distributor.getStats();
        int submitted = stats.submitted();
        int completed = stats.completed();
        int discarded = stats.discarded();

        assertEquals(submitted, totalTasks, "All tasks should have been submitted");
        assertTrue(completed > 0, "Some tasks should have completed");
        assertTrue(discarded > 0, "Some tasks should have been discarded");
    }

    @Test
    public void testGetStats_ReturnsAccurateProgress() throws Exception {
        // Arrange
        List<Integer> numbers = IntStream.range(0, 30)
            .boxed()
            .collect(Collectors.toList());

        WorkDistributor<Integer, Integer, Integer> distributor = new WorkDistributor<>(
            NUM_WORKERS,
            QUEUE_CAPACITY,
            MAX_RETRIES,
            num -> num * 2,
            Integer::sum,
            0
        );

        // Act
        distributor.process(numbers).join();
        Stats stats = distributor.getStats();

        // Assert
        assertEquals(stats.submitted(), numbers.size(), "All tasks should be submitted");
        assertEquals(stats.completed(), numbers.size(), "All tasks should be completed");
        assertEquals(stats.failed(), 0, "No tasks should have failed");
        assertEquals(stats.discarded(), 0, "No tasks should be discarded");
    }
}

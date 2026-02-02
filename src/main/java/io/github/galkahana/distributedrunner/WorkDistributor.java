package io.github.galkahana.distributedrunner;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;

/**
 * Multithreaded work distribution engine for local parallel processing with automatic retry and optional result accumulation.
 *
 * <h2>Mission</h2>
 * <p>
 * Provides a flexible, thread-pool-based execution engine that supports three distinct processing patterns:
 * blocking batch processing (MapReduce-style), async batch with monitoring (Spark-style), and continuous
 * streaming (Flink-style). Used internally by {@link SimplifiedMapReduce} and can be used directly for
 * more advanced scenarios.
 * </p>
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li><b>Automatic retry:</b> Failed tasks retry up to maxRetries before permanent failure</li>
 *   <li><b>Result accumulation:</b> Optional aggregation of results across all tasks</li>
 *   <li><b>Thread-safe submission:</b> Tasks can be submitted from multiple threads concurrently</li>
 *   <li><b>Backpressure control:</b> Queue capacity limits memory usage - {@code submitTask()} blocks when full,
 *       naturally slowing producers when workers can't keep up</li>
 *   <li><b>Real-time monitoring:</b> Live stats on progress, failures, and queue sizes via {@code getStats()}</li>
 *   <li><b>Flexible shutdown:</b> Graceful or immediate termination with {@code shutdown()} / {@code shutdownNow()}</li>
 *   <li><b>Error handling:</b> Automatic error propagation with {@code getResult()}</li>
 * </ul>
 *
 * <h2>Usage Patterns</h2>
 *
 * <h3>Pattern 1: Blocking Batch Processing (MapReduce/Spark-style)</h3>
 * <p>
 * Process a fixed dataset and block until all tasks complete. Simplest pattern for batch jobs.
 * Used internally by {@link SimplifiedMapReduce}.
 * </p>
 * <pre>{@code
 * WorkDistributor<Integer, Integer, Integer> distributor =
 *     new WorkDistributor<>(
 *         10,            // workers
 *         100,           // queue capacity
 *         3,             // max retries
 *         x -> x * 2,    // processing function
 *         Integer::sum,  // accumulator
 *         0              // initial value
 *     );
 *
 * List<Integer> numbers = List.of(1, 2, 3, 4, 5);
 * Integer sum = distributor.process(numbers);  // Blocks until done, returns: 30
 * }</pre>
 *
 * <h3>Pattern 2: Async Batch with Monitoring (Spark-style)</h3>
 * <p>
 * Process a batch asynchronously while main thread remains responsive for UI updates, progress monitoring,
 * or early shutdown. Tasks are submitted upfront, then monitored until completion.
 * </p>
 * <pre>{@code
 * WorkDistributor<Integer, Integer, Integer> distributor =
 *     new WorkDistributor<>(10, 100, 3, x -> processData(x), Integer::sum, 0);
 *
 * distributor.start();  // Start worker threads
 *
 * // Submit all tasks
 * for (int i = 0; i < 10000; i++) {
 *     distributor.submitTask(i, i);
 * }
 *
 * // Main thread remains responsive - poll for progress
 * while (true) {
 *     Stats stats = distributor.getStats();
 *     updateUI(stats);  // Show progress to user
 *
 *     if (userRequestedCancel()) {
 *         distributor.shutdown();  // Early termination
 *         break;
 *     }
 *
 *     if (stats.submitted() == stats.completed() + stats.failed()) {
 *         break;  // All done
 *     }
 *
 *     Thread.sleep(1000);
 * }
 *
 * Integer result = distributor.getResult();  // Get final result (or throws if error)
 * }</pre>
 *
 * <h3>Pattern 3: Continuous Streaming (Flink-style)</h3>
 * <p>
 * Process incoming events continuously using multiple threads. Main thread handles event ingestion,
 * workers process events in parallel. Runs indefinitely until explicitly shut down.
 * </p>
 * <pre>{@code
 * // No accumulator needed for side-effect-only streaming
 * WorkDistributor<Event, Void, Void> distributor =
 *     new WorkDistributor<>(
 *         20,                      // workers for parallel processing
 *         1000,                    // queue capacity
 *         3,                       // max retries
 *         event -> {
 *             processEvent(event);  // Write to DB, send to Kafka, etc.
 *             return null;
 *         }
 *     );
 *
 * distributor.start();
 *
 * // Main thread continuously ingests events
 * AtomicInteger eventId = new AtomicInteger(0);
 * while (running) {
 *     Event event = eventSource.poll();  // Kafka, queue, HTTP endpoint, etc.
 *     if (event != null) {
 *         distributor.submitTask(event, eventId.getAndIncrement());
 *     }
 *
 *     // Optional: Monitor health and throughput
 *     if (eventId.get() % 1000 == 0) {
 *         Stats stats = distributor.getStats();
 *         log.info("Processed: {}, Pending: {}", stats.completed(), stats.pendingWork());
 *     }
 * }
 *
 * distributor.shutdown();  // Clean shutdown when done
 * }</pre>
 *
 * <h2>Control Functions (Async Modes)</h2>
 * <ul>
 *   <li><b>{@code start()}</b> - Start worker threads (required before {@code submitTask()})</li>
 *   <li><b>{@code submitTask(data, id)}</b> - Submit task for processing (thread-safe, can be called from multiple threads)</li>
 *   <li><b>{@code getStats()}</b> - Get real-time processing statistics (non-blocking)</li>
 *   <li><b>{@code getResult()}</b> - Get current accumulated result (throws if fatal error occurred)</li>
 *   <li><b>{@code awaitCompletion()}</b> - Block until all submitted tasks complete (used by blocking mode)</li>
 *   <li><b>{@code shutdown()}</b> - Graceful shutdown: discard pending tasks, wait for current work to finish</li>
 *   <li><b>{@code shutdownNow()}</b> - Immediate shutdown: interrupt all workers, abort current work</li>
 * </ul>
 *
 * @param <I> Input task data type
 * @param <O> Output result data type from processing a single task
 * @param <A> Accumulator type for aggregating results across all tasks
 */
@Slf4j
public class WorkDistributor<I, O, A> {

    // Polling interval for monitoring loops (milliseconds)
    private static final int POLL_INTERVAL_MS = 100;

    // Configuration
    private final int numWorkers;
    private final int maxRetries;
    private final ThrowingFunction<I, O> processingFunction;
    private final BiFunction<A, O, A> accumulatorFunction;
    private final A initialAccumulatorValue;

    // Queues and threads
    private final BlockingQueue<Task<I>> workQueue;
    private final BlockingQueue<Result<O>> resultQueue;

    private final List<Thread> workerThreads;
    private Thread collectorThread;
    private volatile boolean running = false;
    private volatile boolean shutdownRequested = false;
    private volatile boolean threadsStopped = false;

    // State tracking
    private final AtomicInteger tasksSubmitted = new AtomicInteger(0);  // AtomicInteger to allow multiple writer threads
    private volatile int tasksCompleted = 0;
    private volatile int tasksFailed = 0;
    private volatile int tasksDiscarded = 0;

    private volatile A accumulatedResult;
    private volatile Throwable fatalError;

    // Poison pill for thread termination
    private final Task<I> poisonPill = new Task<>(null, -1, true, 0, 0);

    /**
     * Create a work distributor with accumulation.
     */
    public WorkDistributor(
            int numWorkers,
            int workQueueCapacity,
            int maxRetries,
            ThrowingFunction<I, O> processingFunction,
            BiFunction<A, O, A> accumulatorFunction,
            A initialAccumulatorValue) {

        this.numWorkers = numWorkers;
        this.maxRetries = maxRetries;
        this.processingFunction = processingFunction;
        this.accumulatorFunction = accumulatorFunction;
        this.initialAccumulatorValue = initialAccumulatorValue;

        this.workQueue = new LinkedBlockingQueue<>(workQueueCapacity);
        this.resultQueue = new LinkedBlockingQueue<>();
        this.workerThreads = new ArrayList<>();
    }

    /**
     * Create a work distributor without accumulation (results are ignored).
     */
    public WorkDistributor(
            int numWorkers,
            int workQueueCapacity,
            int maxRetries,
            ThrowingFunction<I, O> processingFunction) {

        this(numWorkers, workQueueCapacity, maxRetries,
             processingFunction,
             (acc, result) -> acc,  // No-op accumulator
             null);                 // Null initial value
    }

    // ============= FUNCTIONAL INTERFACES =============

    /**
     * Processing function that can throw checked exceptions.
     *
     * @param <I> Input type
     * @param <O> Output type
     */
    @FunctionalInterface
    public interface ThrowingFunction<I, O> {
        /**
         * Apply the function to the input.
         *
         * @param input Input value
         * @return Output value
         * @throws Exception if processing fails
         */
        O apply(I input) throws Exception;
    }

    // ============= INTERNAL HELPERS =============

    /**
     * Throws if a fatal error has occurred during processing.
     * Rethrows RuntimeExceptions directly, wraps other Throwables.
     */
    private void throwIfFatalError() {
        if (fatalError != null) {
            if (fatalError instanceof RuntimeException) {
                throw (RuntimeException) fatalError;
            }
            throw new RuntimeException(fatalError);
        }
    }

    // ============= INTERNAL CLASSES =============

    private record Task<I>(I data, int id, boolean poison, int attemptNumber, int maxRetries) {
        // Convenience constructor for regular tasks
        Task(I data, int id, int maxRetries) {
            this(data, id, false, 0, maxRetries);
        }

        // retry version of the task
        Task<I> retry() {
            return new Task<>(data, id, false, attemptNumber + 1, maxRetries);
        }
    }

    private record Result<O>(int taskId, boolean success, O data, Throwable exception) {
        // Simplified success and failure constructors
        static <O> Result<O> success(int taskId, O data) {
            return new Result<>(taskId, true, data, null);
        }

        static <O> Result<O> failure(int taskId, Throwable exception) {
            return new Result<>(taskId, false, null, exception);
        }
    }

    // ============= BLOCKING MODE API =============

    /**
     * Process all tasks in blocking mode.
     * Submits all tasks, waits for completion, returns accumulated result.
     *
     * @param taskDataList List of task data to process
     * @return Accumulated result
     * @throws InterruptedException If interrupted while processing
     * @throws RuntimeException If a task fails after max retries or other fatal errors occur
     */
    public A process(List<I> taskDataList) throws InterruptedException {
        start();

        // Submit all tasks
        for (int i = 0; i < taskDataList.size(); i++) {
            submitTask(taskDataList.get(i), i);
        }

        // Wait for completion
        awaitCompletion();

        return getResult();
    }

    // ============= ASYNC MODE API =============

    /**
     * Start the distributor in async mode.
     * Call this before submitting tasks asynchronously.
     * Starts underlying system threads - workers and result collector.
     */
    public synchronized void start() {
        if (running) {
            throw new IllegalStateException("Already running");
        }

        running = true;
        accumulatedResult = initialAccumulatorValue;

        // Start worker threads
        for (int i = 0; i < numWorkers; i++) {
            final int workerId = i;
            Thread worker = new Thread(() -> workerLoop(workerId), "Worker-" + i);
            worker.start();
            workerThreads.add(worker);
        }

        // Start result collector thread
        collectorThread = new Thread(this::collectorLoop, "ResultCollector");
        collectorThread.start();

        log.info("Started {} workers and result collector", numWorkers);
    }

    /**
     * Submit a task asynchronously.
     * Must call start() before using this method.
     * Thread-safe - can be called from multiple threads concurrently.
     *
     * @param taskData The task data to process
     * @param taskId Unique task identifier
     * @throws IllegalStateException If not started or shutdown has been requested
     */
    public void submitTask(I taskData, int taskId) throws InterruptedException {
        if (!running) {
            throw new IllegalStateException("Not started. Call start() first.");
        }
        if (shutdownRequested) {
            throw new IllegalStateException("Shutdown has been requested. Cannot submit new tasks.");
        }

        workQueue.put(new Task<>(taskData, taskId, maxRetries));
        tasksSubmitted.incrementAndGet();
    }

    /**
     * Wait for all submitted tasks to complete naturally.
     * Blocks until all tasks are done or a fatal error occurs.
     * Used primarily by blocking mode (process() method).
     */
    public void awaitCompletion() throws InterruptedException {
        // Wait for all tasks to be processed
        while (running) {
            int submitted = tasksSubmitted.get();
            int completed = tasksCompleted + tasksFailed;

            if (submitted > 0 && completed >= submitted) {
                // All tasks processed
                break;
            }

            // Check if fatal error occurred
            if (fatalError != null) {
                log.error("Fatal error occurred, aborting");
                break;
            }

            // Check if collector thread died
            if (collectorThread != null && !collectorThread.isAlive()) {
                log.error("Result collector thread died unexpectedly");
                fatalError = new IllegalStateException("Result collector thread died unexpectedly");
                break;
            }

            // Check if any worker thread died
            for (int i = 0; i < workerThreads.size(); i++) {
                Thread worker = workerThreads.get(i);
                if (!worker.isAlive()) {
                    log.error("Worker thread {} died unexpectedly", i);
                    fatalError = new IllegalStateException("Worker thread " + i + " died unexpectedly");
                    break;
                }
            }

            // If worker thread died, exit immediately
            if (fatalError != null) {
                break;
            }

            // Sleep briefly before checking again
            // Note: Busy-wait pattern is appropriate here for monitoring thread health and task completion.
            //noinspection BusyWait
            Thread.sleep(POLL_INTERVAL_MS);
        }

        // Stop all threads
        stopThreads();
    }

    /**
     * Get the current accumulated result.
     * Can be called at any time to get intermediate results.
     *
     * @return Current accumulated result
     * @throws RuntimeException If a fatal error has occurred during processing
     */
    public A getResult() {
        throwIfFatalError();
        return accumulatedResult;
    }

    /**
     * Graceful shutdown - clear pending tasks and wait for current work to complete.
     * Discards all queued tasks and waits for workers to finish their current task.
     * New task submissions will be rejected after calling this method.
     */
    public synchronized void shutdown() throws InterruptedException {
        log.info("Shutdown requested");
        shutdownRequested = true;

        // Drain and discard all pending tasks
        List<Task<I>> discardedTasks = new ArrayList<>();
        workQueue.drainTo(discardedTasks);
        tasksDiscarded += discardedTasks.size();

        if (!discardedTasks.isEmpty()) {
            log.info("Discarded {} pending tasks", discardedTasks.size());
        }

        // Wait for workers to finish current tasks and cleanup
        stopThreads();
    }

    /**
     * Immediate shutdown - interrupt all workers and abort current work.
     * New task submissions will be rejected after calling this method.
     */
    public synchronized void shutdownNow() throws InterruptedException {
        log.info("Immediate shutdown requested");
        shutdownRequested = true;
        running = false;

        // Drain and discard all pending tasks
        List<Task<I>> discardedTasks = new ArrayList<>();
        workQueue.drainTo(discardedTasks);
        tasksDiscarded += discardedTasks.size();

        // Interrupt all threads
        for (Thread worker : workerThreads) {
            worker.interrupt();
        }

        if (collectorThread != null) {
            collectorThread.interrupt();
        }

        // Wait for cleanup
        stopThreads();
    }

    // ============= INTERNAL LOOPS =============

    /**
     * Worker thread loop - processes tasks with retry.
     */
    private void workerLoop(int workerId) {
        log.debug("Worker {} started", workerId);

        while (true) {
            try {
                Task<I> task = workQueue.take();

                if (task.poison) {
                    log.debug("Worker {} received poison pill", workerId);
                    break;
                }

                // Process task
                try {
                    log.debug("Worker {} processing task {} (attempt {})",
                        workerId, task.id, task.attemptNumber + 1);

                    O resultData = processingFunction.apply(task.data);

                    // Success - send result
                    resultQueue.put(Result.success(task.id, resultData));

                } catch (InterruptedException e) {
                    // Thread interrupted - don't retry, fail permanently
                    log.warn("Worker {} interrupted while processing task {}", workerId, task.id);
                    resultQueue.put(Result.failure(task.id, e));
                    Thread.currentThread().interrupt();
                    break;

                } catch (Exception e) {
                    // All other exceptions are transient - retry if under limit
                    log.warn("Worker {} task {} failed (attempt {}): {}",
                        workerId, task.id, task.attemptNumber + 1, e.getMessage());

                    if (task.attemptNumber < maxRetries) {
                        // Retry
                        workQueue.put(task.retry());
                    } else {
                        // Max retries reached - permanent failure
                        log.error("Worker {} task {} failed after {} attempts",
                            workerId, task.id, task.attemptNumber + 1);
                        // Send fatal error via result queue for collector to store
                        resultQueue.put(Result.failure(task.id, e));
                    }
                }

            } catch (InterruptedException e) {
                log.debug("Worker {} interrupted", workerId);
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.debug("Worker {} stopped", workerId);
    }

    /**
     * Result collector loop - accumulates results automatically.
     */
    private void collectorLoop() {
        log.debug("Result collector started");

        while (running) {
            try {
                Result<O> result = resultQueue.poll(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);

                if (result == null) {
                    // Check if we're done
                    int submitted = tasksSubmitted.get();
                    int completed = tasksCompleted + tasksFailed;

                    if (submitted > 0 && completed >= submitted) {
                        // All results collected
                        log.debug("All results collected");
                        break;
                    }

                    continue;
                }

                if (result.success) {
                    // Accumulate successful result
                    try {
                        //noinspection NonAtomicOperationOnVolatileField
                        accumulatedResult = accumulatorFunction.apply(accumulatedResult, result.data);
                        //noinspection NonAtomicOperationOnVolatileField
                        tasksCompleted++;

                        log.debug("Accumulated result from task {}", result.taskId);
                    } catch (Exception e) {
                        log.error("Error accumulating result from task {}", result.taskId, e);
                        fatalError = e;
                    }
                } else {
                    // Failed result

                    //noinspection NonAtomicOperationOnVolatileField
                    tasksFailed++;
                    log.error("Task {} failed permanently: {}", result.taskId, result.exception.getMessage());

                    // Store fatal error
                    fatalError = result.exception;
                }

            } catch (InterruptedException e) {
                log.debug("Result collector interrupted");
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.debug("Result collector stopped");
    }

    /**
     * Stop all worker threads gracefully.
     * Safe to call multiple times - only executes once.
     */
    private synchronized void stopThreads() throws InterruptedException {
        // Check if already stopped
        if (threadsStopped) {
            log.debug("Threads already stopped, skipping");
            return;
        }

        running = false;

        // Send poison pills to workers
        for (int i = 0; i < numWorkers; i++) {
            workQueue.put(poisonPill);
        }

        // Wait for workers
        for (Thread worker : workerThreads) {
            worker.join();
        }

        // Wait for collector
        if (collectorThread != null) {
            collectorThread.join();
        }

        threadsStopped = true;

        log.info("All threads stopped. Stats: submitted={}, completed={}, failed={}, discarded={}",
            tasksSubmitted.get(), tasksCompleted, tasksFailed, tasksDiscarded);
    }

    // ============= MONITORING =============

    /**
     * Get current processing statistics.
     */
    public Stats getStats() {
        return new Stats(
            tasksSubmitted.get(),
            tasksCompleted,
            tasksFailed,
            tasksDiscarded,
            workQueue.size(),
            resultQueue.size()
        );
    }

    /**
     * Processing statistics snapshot.
     *
     * @param submitted Total tasks submitted
     * @param completed Tasks completed successfully
     * @param failed Tasks failed after max retries
     * @param discarded Tasks discarded during shutdown
     * @param pendingWork Tasks waiting in work queue
     * @param pendingResults Results waiting in result queue
     */
    public record Stats(int submitted, int completed, int failed, int discarded,
                        int pendingWork, int pendingResults) {
    }
}

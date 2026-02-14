package io.github.galkahana.distributedrunner;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;


import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Multithreaded work distribution engine with automatic retry, backpressure, and optional result accumulation.
 * <p>
 * Supports three usage patterns: batch processing ({@link #process}), interactive workflows
 * ({@link #start}/{@link #submitTask}/{@link #awaitCompletion}), and continuous streaming.
 * Used internally by {@link SimplifiedMapReduce}. See README for examples.
 *
 * @param <I> Input task data type
 * @param <O> Output result data type from processing a single task
 * @param <A> Accumulator type for aggregating results across all tasks
 */
@Slf4j
public class WorkDistributor<I, O, A> {

    private final int numWorkers;
    private final int workQueueCapacity;
    private final int maxRetries;
    private final ThrowingFunction<I, O> processingFunction;
    private final BiFunction<A, O, A> accumulatorFunction;
    private final A initialAccumulatorValue;
    private final Retry retry;

    private ThreadPoolExecutor executor;
    private CompletionService<Result<O>> completionService;
    private Thread collectorThread;

    private volatile boolean running = false;
    private volatile boolean submissionComplete = false; // Prevents premature completion detection mid-submission

    private final CompletableFuture<A> completionFuture = new CompletableFuture<>();
    private final AtomicInteger tasksSubmitted = new AtomicInteger(0);
    private volatile int tasksCompleted = 0, tasksFailed = 0, tasksDiscarded = 0;
    private volatile A accumulatedResult;
    private volatile Throwable fatalError;


    /**
     * Create a work distributor with accumulation.
     * @param numWorkers Number of concurrent worker threads
     * @param workQueueCapacity Capacity of the work queue
     *                          (should be >= numWorkers to avoid blocking. Use it to control backpressure)
     * @param maxRetries Maximum retry attempts per task before failing
     * @param processingFunction Function to process a single task
     * @param accumulatorFunction Function to accumulate results
     * @param initialAccumulatorValue Initial value for the accumulator
     */
    public WorkDistributor(
            int numWorkers,
            int workQueueCapacity,
            int maxRetries,
            ThrowingFunction<I, O> processingFunction,
            BiFunction<A, O, A> accumulatorFunction,
            A initialAccumulatorValue) {

        this.numWorkers = numWorkers;
        this.workQueueCapacity = workQueueCapacity;
        this.maxRetries = maxRetries;
        this.processingFunction = processingFunction;
        this.accumulatorFunction = accumulatorFunction;
        this.initialAccumulatorValue = initialAccumulatorValue;

        RetryConfig config = RetryConfig.custom()
                .maxAttempts(maxRetries + 1)
                .retryOnException(e -> !(e instanceof InterruptedException))
                .build();
        this.retry = Retry.of("task-retry", config);
    }

    /**
     * Create a work distributor without accumulation (results are ignored).
     * @param workQueueCapacity Capacity of the work queue
     *                          (should be >= numWorkers to avoid blocking. Use it to control backpressure)
     * @param maxRetries Maximum retry attempts per task before failing
     * @param processingFunction Function to process a single task
     * */
    public WorkDistributor(
            int numWorkers,
            int workQueueCapacity,
            int maxRetries,
            ThrowingFunction<I, O> processingFunction) {

        this(numWorkers, workQueueCapacity, maxRetries,
             processingFunction,
             (acc, result) -> acc,
             null);
    }

    private record Result<O>(int taskId, boolean success, O data, Throwable exception) {
        static <O> Result<O> success(int taskId, O data) {
            return new Result<>(taskId, true, data, null);
        }

        static <O> Result<O> failure(int taskId, Throwable exception) {
            return new Result<>(taskId, false, null, exception);
        }
    }

    /**
     * Process a precomputed list of tasks and return a future that completes with the accumulated result.
     * Task submission is synchronous (may block due to backpressure), but completion is async.
     *
     * @param taskDataList List of task data to process
     * @return CompletableFuture that completes with the accumulated result, or completes exceptionally on failure
     * @throws InterruptedException If interrupted during task submission
     */
    public CompletableFuture<A> process(List<I> taskDataList) throws InterruptedException {
        start();
        for (int i = 0; i < taskDataList.size(); i++) {
            submitTask(taskDataList.get(i), i);
        }
        return awaitCompletion();
    }

    /**
     * Start the distributor. Call this before submitting any jobs.
     * Starts up the underlying executor and result collector thread.
     */
    public synchronized void start() {
        if (running) throw new IllegalStateException("Already running");
        running = true;
        accumulatedResult = initialAccumulatorValue;

        executor = new ThreadPoolExecutor(numWorkers, numWorkers, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(workQueueCapacity));
        executor.setRejectedExecutionHandler((runnable, exec) -> {
            if (exec.isShutdown()) throw new RejectedExecutionException("Executor is shut down");
            try {
                exec.getQueue().put(runnable);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException("Interrupted while waiting for queue space", e);
            }
        });

        completionService = new ExecutorCompletionService<>(executor);
        collectorThread = new Thread(this::collectorLoop, "ResultCollector");
        collectorThread.start();

        log.info("Started executor with {} workers and result collector", numWorkers);
    }

    /**
     * Submit a task.
     * Thread-safe - can be called from multiple threads concurrently.
     *
     * @param taskData The task data to process
     * @param taskId Unique task identifier
     * @throws IllegalStateException If not started or shutdown has been requested
     */
    public void submitTask(I taskData, int taskId) throws InterruptedException {
        if (!running) throw new IllegalStateException("Not running. Call start() first, and ensure shutdown has not been called.");

        try {
            completionService.submit(() -> executeWithRetry(taskData, taskId));
            tasksSubmitted.incrementAndGet();
        } catch (RejectedExecutionException e) {
            if (e.getCause() instanceof InterruptedException) throw (InterruptedException) e.getCause();
            throw new IllegalStateException("Task submission rejected", e);
        }
    }

    /**
     * Get current processing statistics.
     */
    public Stats getStats() {
        int submitted = tasksSubmitted.get();
        int pendingWork = executor != null ? executor.getQueue().size() : 0;
        int activeProcessing = executor != null ? executor.getActiveCount() : 0;
        return new Stats(submitted, tasksCompleted, tasksFailed, tasksDiscarded, pendingWork,
                Math.max(0, submitted - tasksCompleted - tasksFailed - tasksDiscarded - pendingWork - activeProcessing));
    }

    /**
     * Signal that no more tasks will be submitted and return a future that completes
     * with the accumulated result when all tasks finish (or completes exceptionally on failure).
     *
     * @return CompletableFuture that completes with the accumulated result
     */
    public CompletableFuture<A> awaitCompletion() {
        submissionComplete = true;
        return completionFuture;
    }

    /**
     * Get the current accumulated result. Will return final result if all tasks are processed.
     * Throws if a fatal error has occurred during processing.
     *
     * @return Current accumulated result
     * @throws RuntimeException If a fatal error has occurred during processing
     */
    public A getResult() {
        if (fatalError != null) {
            throw fatalError instanceof RuntimeException
                ? (RuntimeException) fatalError
                : new RuntimeException(fatalError);
        }

        return accumulatedResult;
    }

    /**
     * Graceful shutdown - clear pending tasks and wait for threads to finish their work on tasks in progress.
     * Blocks until all threads have stopped.
     */
    public synchronized void shutdown() throws InterruptedException {
        if (!running) return;
        log.info("Shutdown requested");
        running = false;

        int discarded = executor.getQueue().drainTo(new ArrayList<>());
        tasksDiscarded += discarded;
        if (discarded > 0) log.info("Discarded {} pending tasks", discarded);
        executor.shutdown();

        stopCollector();
    }

    private void stopCollector() throws InterruptedException{
        collectorThread.interrupt();
        collectorThread.join();
    }

    /**
     * Immediate shutdown - interrupt all workers and abort current work.
     * Blocks until all threads have stopped.
     */
    public synchronized void shutdownNow() throws InterruptedException {
        if (!running) return;
        log.info("Immediate shutdown requested");
        running = false;

        List<Runnable> pending = executor.shutdownNow();
        tasksDiscarded += pending.size();

        stopCollector();
    }

    private Result<O> executeWithRetry(I taskData, int taskId) {
        try {
            O result = retry.executeCallable(() -> {
                log.debug("Processing task {}", taskId);
                return processingFunction.apply(taskData);
            });
            return Result.success(taskId, result);
        } catch (InterruptedException e) {
            log.warn("Task {} interrupted", taskId);
            Thread.currentThread().interrupt();
            return Result.failure(taskId, e);
        } catch (Exception e) {
            log.error("Task {} failed after {} attempts", taskId, maxRetries + 1);
            return Result.failure(taskId, e);
        }
    }

    private void stopExecutor() {
        try {
            executor.shutdown(); // no-op if already shut down
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                log.warn("Executor did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        running = false;

        log.info("All threads stopped. Stats: submitted={}, completed={}, failed={}, discarded={}",
            tasksSubmitted.get(), tasksCompleted, tasksFailed, tasksDiscarded);
    }

    private void resolveCompletion() {
        if (fatalError != null) {
            completionFuture.completeExceptionally(
                fatalError instanceof RuntimeException
                    ? fatalError
                    : new RuntimeException(fatalError));
        } else {
            completionFuture.complete(accumulatedResult);
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField") // Only collector thread mutates these fields
    private void accumulateResult(O data) {
        accumulatedResult = accumulatorFunction.apply(accumulatedResult, data);
        tasksCompleted++;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField") // Only collector thread mutates these fields
    private void recordTaskFailure(Throwable error) {
        tasksFailed++;
        fatalError = error;
    }

    private boolean hasMoreResults() {
        return !submissionComplete
                || tasksCompleted + tasksFailed + tasksDiscarded < tasksSubmitted.get();
    }

    private void collectorLoop() {
        log.debug("Result collector started");
        try {
            while (hasMoreResults()) {
                Result<O> result = completionService.take().get();

                if (result.success) {
                    try {
                        accumulateResult(result.data);
                        log.debug("Accumulated result from task {}", result.taskId);
                    } catch (Exception e) {
                        log.error("Error accumulating result from task {}", result.taskId, e);
                        recordTaskFailure(e);
                        break;
                    }
                } else {
                    log.error("Task {} failed permanently: {}", result.taskId, result.exception.getMessage());
                    recordTaskFailure(result.exception);
                    break;
                }
            }
        } catch (ExecutionException e) {
            log.error("Unexpected execution exception in collector", e);
            recordTaskFailure(e.getCause());
        } catch (InterruptedException e) {
            log.debug("Result collector interrupted");
        } catch (CancellationException e) {
            log.debug("A task was cancelled");
        } finally {
            stopExecutor();
            resolveCompletion();
        }
    }


}

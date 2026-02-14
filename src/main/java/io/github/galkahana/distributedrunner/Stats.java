package io.github.galkahana.distributedrunner;

/**
 * Type definition for processing statistics.
 *
 * @param submitted Total tasks submitted
 * @param completed Tasks completed successfully
 * @param failed Tasks failed after max retries
 * @param discarded Tasks discarded during shutdown
 * @param pendingWork Tasks waiting in work queue
 * @param pendingResults Results waiting to be collected
 */
public record Stats(int submitted, int completed, int failed, int discarded,
                    int pendingWork, int pendingResults) {
}

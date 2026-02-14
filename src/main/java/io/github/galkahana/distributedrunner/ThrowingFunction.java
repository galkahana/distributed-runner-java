package io.github.galkahana.distributedrunner;

/**
 * Type definition for per task processing function.
 *
 * @param <I> Input type
 * @param <O> Output type
 */
@FunctionalInterface
public interface ThrowingFunction<I, O> {
    O apply(I input) throws Exception;
}

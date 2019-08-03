package org.tarantool.util;

/**
 * Represents a function that accepts two arguments and
 * produces a result or throws an exception.
 *
 * @param <T> the type of the first argument to the function
 * @param <U> the type of the second argument to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
public interface ThrowingBiFunction<T, U, R, E extends Exception> {

    /**
     * Applies this function to the given arguments.
     *
     * @param argument1 first argument
     * @param argument2 second argument
     *
     * @return function result
     *
     * @throws E if any error occurs
     */
    R apply(T argument1, U argument2) throws E;

}

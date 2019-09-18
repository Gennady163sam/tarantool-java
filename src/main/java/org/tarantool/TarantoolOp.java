package org.tarantool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TarantoolOp<V> extends CompletableFuture<V>  {

    /**
     * Missed in jdk8 CompletableFuture operator to limit execution
     * by time.
     */
    public TarantoolOp<V> orTimeout(long timeout, TimeUnit unit) {
        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout cannot be negative");
        }
        if (unit == null) {
            throw new IllegalArgumentException("Time unit cannot be null");
        }
        if (timeout == 0 || isDone()) {
            return this;
        }
        ScheduledFuture<?> abandonByTimeoutAction = TimeoutScheduler.EXECUTOR.schedule(
            () -> {
                if (!this.isDone()) {
                    this.completeExceptionally(new TimeoutException());
                }
            },
            timeout, unit
        );
        whenComplete(
            (ignored, error) -> {
                if (error == null && !abandonByTimeoutAction.isDone()) {
                    abandonByTimeoutAction.cancel(false);
                }
            }
        );
        return this;
    }

    /**
     * Runs timeout operation as a delayed task.
     */
    static class TimeoutScheduler {

        static final ScheduledThreadPoolExecutor EXECUTOR;

        static {
            EXECUTOR =
                new ScheduledThreadPoolExecutor(1, new TarantoolThreadDaemonFactory("tarantoolTimeout"));
            EXECUTOR.setRemoveOnCancelPolicy(true);
        }
    }

}

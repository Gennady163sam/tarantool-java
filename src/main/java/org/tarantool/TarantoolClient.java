package org.tarantool;

import org.tarantool.schema.TarantoolSchemaMeta;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface TarantoolClient {
    TarantoolClientOps<List<?>, Object, List<?>> syncOps();

    TarantoolClientOps<List<?>, Object, Future<List<?>>> asyncOps();

    TarantoolClientOps<List<?>, Object, CompletionStage<List<?>>> composableAsyncOps();

    TarantoolClientOps<List<?>, Object, Long> fireAndForgetOps();

    TarantoolSQLOps<Object, Long, List<Map<String, Object>>> sqlSyncOps();

    TarantoolSQLOps<Object, Future<Long>, Future<List<Map<String, Object>>>> sqlAsyncOps();

    void close();

    boolean isAlive();

    boolean isClosed();

    void waitAlive() throws InterruptedException;

    boolean waitAlive(long timeout, TimeUnit unit) throws InterruptedException;

    TarantoolSchemaMeta getSchemaMeta();

}

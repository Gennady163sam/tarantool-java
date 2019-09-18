package org.tarantool;

import org.tarantool.dsl.TarantoolRequestConvertible;

public interface TarantoolClientOps<O, P, R> {
    R select(Integer space, Integer index, O key, int offset, int limit, int iterator);

    R select(String space, String index, O key, int offset, int limit, int iterator);

    R select(Integer space, Integer index, O key, int offset, int limit, Iterator iterator);

    R select(String space, String index, O key, int offset, int limit, Iterator iterator);

    R insert(Integer space, O tuple);

    R insert(String space, O tuple);

    R replace(Integer space, O tuple);

    R replace(String space, O tuple);

    R update(Integer space, O key, P... tuple);

    R update(String space, O key, P... tuple);

    R upsert(Integer space, O key, O defTuple, P... ops);

    R upsert(String space, O key, O defTuple, P... ops);

    R delete(Integer space, O key);

    R delete(String space, O key);

    R call(String function, Object... args);

    R eval(String expression, Object... args);

    R execute(TarantoolRequestConvertible requestSpec);

    void ping();

    void close();

}

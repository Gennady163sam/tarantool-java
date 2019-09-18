package org.tarantool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TarantoolRequest implements Comparable<TarantoolRequest> {

    /**
     * A task identifier used in {@link TarantoolClientImpl#futures}.
     */
    private long id;

    /**
     * Tarantool binary protocol operation code.
     */
    private Code code;

    /**
     * Schema ID when this future was registered
     * successfully.
     */
    private long startedSchemaId;

    /**
     * Schema ID when this future was completed
     * successfully.
     */
    private long completedSchemaId;

    /**
     * Arguments of operation.
     */
    private List<TarantoolRequestArgument> arguments;

    /**
     * Request timeout start just after initialization.
     */
    private Duration timeout;

    /**
     * Asynchronous request result.
     */
    private TarantoolOp<?> result = new TarantoolOp<>();

    /**
     * Internal flag indicating that this request
     * is used to sync up the schema and if nothing
     * changes then it will fail the dependent request.
     */
    private boolean sync;
    private TarantoolRequest syncDependent;

    public TarantoolRequest(Code code) {
        this.code = code;
        this.arguments = new ArrayList<>();
    }

    public TarantoolRequest(Code code, TarantoolRequestArgument... arguments) {
        this.code = code;
        this.arguments = Arrays.asList(arguments);
    }

    /**
     * Initializes a request and starts its timer.
     *
     * @param sid            internal request id
     * @param defaultTimeout default timeout
     */
    void begin(long sid, Duration defaultTimeout) {
        this.id = sid;
        Objects.requireNonNull(defaultTimeout);
        long time = timeout == null ? defaultTimeout.toMillis() : timeout.toMillis();
        result.orTimeout(time, TimeUnit.MILLISECONDS);
    }

    void beginSync(long sid, Duration defaultTimeout, TarantoolRequest syncDependent) {
        this.id = sid;
        Objects.requireNonNull(defaultTimeout);
        Objects.requireNonNull(syncDependent);
        this.syncDependent = syncDependent;
        this.sync = true;
        long time = timeout == null ? defaultTimeout.toMillis() : timeout.toMillis();
        result.orTimeout(time, TimeUnit.MILLISECONDS);
    }

    public long getId() {
        return id;
    }

    public long getStartedSchemaId() {
        return startedSchemaId;
    }

    public void setStartedSchemaId(long startedSchemaId) {
        this.startedSchemaId = startedSchemaId;
    }

    public long getCompletedSchemaId() {
        return completedSchemaId;
    }

    public void setCompletedSchemaId(long completedSchemaId) {
        this.completedSchemaId = completedSchemaId;
    }

    public TarantoolOp<?> getResult() {
        return result;
    }

    public Code getCode() {
        return code;
    }

    public void setCode(Code code) {
        this.code = code;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    boolean isSync() {
        return sync;
    }

    TarantoolRequest getSyncDependent() {
        return syncDependent;
    }

    /**
     * Serializability means this requests is capable being
     * translated in a binary packet according to {@code iproto}
     * protocol.
     *
     * @return {@literal true} if this request is serializable
     */
    public boolean isSerializable() {
        return arguments.stream().allMatch(TarantoolRequestArgument::isSerializable);
    }

    public List<Object> getArguments() {
        return arguments.stream().map(TarantoolRequestArgument::getValue).collect(Collectors.toList());
    }

    public void addArguments(TarantoolRequestArgument... arguments) {
        this.arguments.addAll(Arrays.asList(arguments));
    }

    @Override
    public int compareTo(TarantoolRequest other) {
        return Long.compareUnsigned(this.id, other.id);
    }

}

package org.tarantool;

import org.tarantool.dsl.TarantoolRequestConvertible;
import org.tarantool.protocol.ProtoConstants;
import org.tarantool.protocol.ProtoUtils;
import org.tarantool.protocol.ReadableViaSelectorChannel;
import org.tarantool.protocol.TarantoolGreeting;
import org.tarantool.protocol.TarantoolPacket;
import org.tarantool.schema.TarantoolMetaSpacesCache;
import org.tarantool.schema.TarantoolSchemaMeta;
import org.tarantool.util.TupleTwo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

public class TarantoolClientImpl extends TarantoolBase<Future<?>> implements TarantoolClient {

    public static final CommunicationException NOT_INIT_EXCEPTION
        = new CommunicationException("Not connected, initializing connection");

    protected TarantoolClientConfig config;
    protected Duration operationTimeout;

    /**
     * External.
     */
    protected SocketChannelProvider socketProvider;
    protected SocketChannel channel;
    protected ReadableViaSelectorChannel readChannel;

    protected volatile Exception thumbstone;

    protected ScheduledExecutorService workExecutor;

    protected StampedLock schemaLock = new StampedLock();
    protected BlockingQueue<TarantoolRequest> delayedOpsQueue;

    protected Map<Long, TarantoolRequest> futures;
    protected AtomicInteger pendingResponsesCount = new AtomicInteger();

    /**
     * Write properties.
     */
    protected ByteBuffer sharedBuffer;
    protected ReentrantLock bufferLock = new ReentrantLock(false);
    protected Condition bufferNotEmpty = bufferLock.newCondition();
    protected Condition bufferEmpty = bufferLock.newCondition();

    protected ByteBuffer writerBuffer;
    protected ReentrantLock writeLock = new ReentrantLock(true);

    /**
     * Interfaces.
     */
    protected SyncOps syncOps;
    protected FireAndForgetOps fireAndForgetOps;
    protected ComposableAsyncOps composableAsyncOps;
    protected UnsafeSchemaOps unsafeSchemaOps;

    /**
     * Inner.
     */
    protected TarantoolClientStats stats;
    protected StateHelper state = new StateHelper(StateHelper.RECONNECT);
    protected Thread reader;
    protected Thread writer;

    protected TarantoolSchemaMeta schemaMeta = new TarantoolMetaSpacesCache(this);

    protected Thread connector = new Thread(new Runnable() {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                reconnect(thumbstone);
                try {
                    state.awaitReconnection();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    });

    public TarantoolClientImpl(String address, TarantoolClientConfig config) {
        this(new SingleSocketChannelProviderImpl(address), config);
    }

    public TarantoolClientImpl(SocketChannelProvider socketProvider, TarantoolClientConfig config) {
        initClient(socketProvider, config);
        if (socketProvider instanceof ConfigurableSocketChannelProvider) {
            ConfigurableSocketChannelProvider configurableProvider = (ConfigurableSocketChannelProvider) socketProvider;
            configurableProvider.setConnectionTimeout(config.connectionTimeout);
            configurableProvider.setRetriesLimit(config.retryCount);
        }
        startConnector(config.initTimeoutMillis);
    }

    private void initClient(SocketChannelProvider socketProvider, TarantoolClientConfig config) {
        this.thumbstone = NOT_INIT_EXCEPTION;
        this.config = config;
        this.initialRequestSize = config.defaultRequestSize;
        this.operationTimeout = Duration.ofMillis(config.operationExpiryTimeMillis);
        this.socketProvider = socketProvider;
        this.stats = new TarantoolClientStats();
        this.futures = new ConcurrentHashMap<>(config.predictedFutures);
        this.delayedOpsQueue = new PriorityBlockingQueue<>(128);
        this.workExecutor =
            Executors.newSingleThreadScheduledExecutor(new TarantoolThreadDaemonFactory("tarantool-worker"));
        this.sharedBuffer = ByteBuffer.allocateDirect(config.sharedBufferSize);
        this.writerBuffer = ByteBuffer.allocateDirect(sharedBuffer.capacity());
        this.connector.setDaemon(true);
        this.connector.setName("Tarantool connector");
        this.syncOps = new SyncOps();
        this.composableAsyncOps = new ComposableAsyncOps();
        this.fireAndForgetOps = new FireAndForgetOps();
        this.unsafeSchemaOps = new UnsafeSchemaOps();
        if (!config.useNewCall) {
            setCallCode(Code.OLD_CALL);
            this.syncOps.setCallCode(Code.OLD_CALL);
            this.fireAndForgetOps.setCallCode(Code.OLD_CALL);
            this.composableAsyncOps.setCallCode(Code.OLD_CALL);
        }
    }

    private void startConnector(long initTimeoutMillis) {
        connector.start();
        try {
            if (!waitAlive(initTimeoutMillis, TimeUnit.MILLISECONDS)) {
                CommunicationException e = new CommunicationException(
                    initTimeoutMillis +
                        "ms is exceeded when waiting for client initialization. " +
                        "You could configure init timeout in TarantoolConfig"
                );

                close(e);
                throw e;
            }
        } catch (InterruptedException e) {
            close(e);
            throw new IllegalStateException(e);
        }
    }

    protected void reconnect(Throwable lastError) {
        SocketChannel channel = null;
        int retryNumber = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                channel = socketProvider.get(retryNumber++, lastError == NOT_INIT_EXCEPTION ? null : lastError);
            } catch (Exception e) {
                closeChannel(channel);
                lastError = e;
                if (!(e instanceof SocketProviderTransientException)) {
                    close(e);
                    return;
                }
            }
            try {
                if (channel != null) {
                    connect(channel);
                    return;
                }
            } catch (Exception e) {
                closeChannel(channel);
                lastError = e;
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    protected void connect(final SocketChannel channel) throws Exception {
        try {
            TarantoolGreeting greeting = ProtoUtils.connect(channel, config.username, config.password, msgPackLite);
            this.serverVersion = greeting.getServerVersion();
        } catch (IOException e) {
            closeChannel(channel);
            throw new CommunicationException("Couldn't connect to tarantool", e);
        }

        channel.configureBlocking(false);
        this.channel = channel;
        this.readChannel = new ReadableViaSelectorChannel(channel);

        bufferLock.lock();
        try {
            sharedBuffer.clear();
        } finally {
            bufferLock.unlock();
        }
        this.thumbstone = null;
        startThreads(channel.socket().getRemoteSocketAddress().toString());
        updateSchema();
    }

    protected void startThreads(String threadName) throws InterruptedException {
        final CountDownLatch ioThreadStarted = new CountDownLatch(2);
        final AtomicInteger leftIoThreads = new AtomicInteger(2);
        reader = new Thread(() -> {
            ioThreadStarted.countDown();
            if (state.acquire(StateHelper.READING)) {
                try {
                    readThread();
                } finally {
                    state.release(StateHelper.READING | StateHelper.SCHEMA_UPDATING);
                    // only last of two IO-threads can signal for reconnection
                    if (leftIoThreads.decrementAndGet() == 0) {
                        state.trySignalForReconnection();
                    }
                }
            }
        });
        writer = new Thread(() -> {
            ioThreadStarted.countDown();
            if (state.acquire(StateHelper.WRITING)) {
                try {
                    writeThread();
                } finally {
                    state.release(StateHelper.WRITING | StateHelper.SCHEMA_UPDATING);
                    // only last of two IO-threads can signal for reconnection
                    if (leftIoThreads.decrementAndGet() == 0) {
                        state.trySignalForReconnection();
                    }
                }
            }
        });
        state.release(StateHelper.RECONNECT);

        configureThreads(threadName);
        reader.start();
        writer.start();
        ioThreadStarted.await();
    }

    protected void configureThreads(String threadName) {
        reader.setName("Tarantool " + threadName + " reader");
        writer.setName("Tarantool " + threadName + " writer");
        writer.setPriority(config.writerThreadPriority);
        reader.setPriority(config.readerThreadPriority);
    }

    @Override
    public TarantoolSchemaMeta getSchemaMeta() {
        return schemaMeta;
    }

    @Override
    public TarantoolResultSet executeRequest(TarantoolRequestConvertible requestSpec) {
        TarantoolRequest request = requestSpec.toTarantoolRequest(getSchemaMeta());
        List<?> result = (List<?>) syncGet(exec(request));
        return new InMemoryResultSet(result, isSingleResultRow(request.getCode()));
    }

    private boolean isSingleResultRow(Code code) {
        return code == Code.EVAL || code == Code.CALL || code == Code.OLD_CALL;
    }

    /**
     * Executes an operation with default timeout.
     *
     * @param request operation data
     *
     * @return deferred result
     *
     * @see #setOperationTimeout(long)
     */
    @Override
    protected Future<?> exec(TarantoolRequest request) {
        return doExec(request).getResult();
    }

    protected TarantoolRequest doExec(TarantoolRequest request) {
        long stamp = schemaLock.readLock();
        try {
            // space or index names could not be found in cache
            // it's possible the client keeps the outdated schema
            request.begin(syncId.incrementAndGet(), operationTimeout);
            if (!request.isSerializable()) {
                delayedOpsQueue.add(request);
                // the schema is not ready, skip sync-op
                if (isSchemaLoaded()) {
                    optimisticSchemaUpdate(request);
                }
                return request;
            }
            // postpone operation if the schema is not ready
            if (!isSchemaLoaded()) {
                delayedOpsQueue.add(request);
                return request;
            }
            return registerOperation(request, schemaMeta.getSchemaVersion());
        } finally {
            schemaLock.unlockRead(stamp);
        }
    }

    /**
     * Checks whether the schema is fully cached.
     *
     * @return {@literal true} if the schema is loaded
     */
    private boolean isSchemaLoaded() {
        return schemaMeta.isInitialized() && !state.isStateSet(StateHelper.SCHEMA_UPDATING);
    }

    protected TarantoolRequest registerOperation(TarantoolRequest request, long schemaId) {
        if (isDead(request)) {
            return request;
        }
        futures.put(request.getId(), request);
        if (isDead(request)) {
            futures.remove(request.getId());
            return request;
        }
        try {
            request.setStartedSchemaId(schemaId);
            write(request.getCode(), request.getId(), schemaId, request.getArguments().toArray());
        } catch (Exception e) {
            futures.remove(request.getId());
            fail(request, e);
        }
        return request;
    }

    protected synchronized void die(String message, Exception cause) {
        if (thumbstone != null) {
            return;
        }
        final CommunicationException error = new CommunicationException(message, cause);
        this.thumbstone = error;
        while (!futures.isEmpty()) {
            Iterator<Map.Entry<Long, TarantoolRequest>> iterator = futures.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, TarantoolRequest> elem = iterator.next();
                if (elem != null) {
                    TarantoolRequest request = elem.getValue();
                    fail(request, error);
                }
                iterator.remove();
            }
        }

        TarantoolRequest request;
        while ((request = delayedOpsQueue.poll()) != null) {
            fail(request, error);
        }

        pendingResponsesCount.set(0);

        bufferLock.lock();
        try {
            sharedBuffer.clear();
            bufferEmpty.signalAll();
        } finally {
            bufferLock.unlock();
        }
        stopIO();
    }

    @Override
    public void ping() {
        syncGet(exec(new TarantoolRequest(Code.PING)));
    }

    protected void write(Code code, Long syncId, Long schemaId, Object... args)
        throws Exception {
        ByteBuffer buffer = ProtoUtils.createPacket(msgPackLite, code, syncId, schemaId, args);

        if (directWrite(buffer)) {
            return;
        }
        sharedWrite(buffer);

    }

    protected void sharedWrite(ByteBuffer buffer) throws InterruptedException, TimeoutException {
        long start = System.currentTimeMillis();
        if (bufferLock.tryLock(config.writeTimeoutMillis, TimeUnit.MILLISECONDS)) {
            try {
                int rem = buffer.remaining();
                stats.sharedMaxPacketSize = Math.max(stats.sharedMaxPacketSize, rem);
                if (rem > initialRequestSize) {
                    stats.sharedPacketSizeGrowth++;
                }
                while (sharedBuffer.remaining() < buffer.limit()) {
                    stats.sharedEmptyAwait++;
                    long remaining = config.writeTimeoutMillis - (System.currentTimeMillis() - start);
                    try {
                        if (remaining < 1 || !bufferEmpty.await(remaining, TimeUnit.MILLISECONDS)) {
                            stats.sharedEmptyAwaitTimeouts++;
                            throw new TimeoutException(
                                config.writeTimeoutMillis +
                                    "ms is exceeded while waiting for empty buffer. " +
                                    "You could configure write timeout it in TarantoolConfig"
                            );
                        }
                    } catch (InterruptedException e) {
                        throw new CommunicationException("Interrupted", e);
                    }
                }
                sharedBuffer.put(buffer);
                pendingResponsesCount.incrementAndGet();
                bufferNotEmpty.signalAll();
                stats.buffered++;
            } finally {
                bufferLock.unlock();
            }
        } else {
            stats.sharedWriteLockTimeouts++;
            throw new TimeoutException(
                config.writeTimeoutMillis +
                    "ms is exceeded while waiting for shared buffer lock. " +
                    "You could configure write timeout in TarantoolConfig"
            );
        }
    }

    private boolean directWrite(ByteBuffer buffer) throws InterruptedException, IOException, TimeoutException {
        if (sharedBuffer.capacity() * config.directWriteFactor <= buffer.limit()) {
            if (writeLock.tryLock(config.writeTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    int rem = buffer.remaining();
                    stats.directMaxPacketSize = Math.max(stats.directMaxPacketSize, rem);
                    if (rem > initialRequestSize) {
                        stats.directPacketSizeGrowth++;
                    }
                    writeFully(channel, buffer);
                    stats.directWrite++;
                    pendingResponsesCount.incrementAndGet();
                } finally {
                    writeLock.unlock();
                }
                return true;
            } else {
                stats.directWriteLockTimeouts++;
                throw new TimeoutException(
                    config.writeTimeoutMillis +
                        "ms is exceeded while waiting for channel lock. " +
                        "You could configure write timeout in TarantoolConfig"
                );
            }
        }
        return false;
    }

    protected void readThread() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                TarantoolPacket packet = ProtoUtils.readPacket(readChannel, msgPackLite);

                Map<Integer, Object> headers = packet.getHeaders();

                Long syncId = (Long) headers.get(Key.SYNC.getId());
                TarantoolRequest request = futures.remove(syncId);
                stats.received++;
                pendingResponsesCount.decrementAndGet();
                complete(packet, request);
            } catch (Exception e) {
                die("Cant read answer", e);
                return;
            }
        }
    }

    protected void writeThread() {
        writerBuffer.clear();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                bufferLock.lock();
                try {
                    while (sharedBuffer.position() == 0) {
                        bufferNotEmpty.await();
                    }
                    sharedBuffer.flip();
                    writerBuffer.put(sharedBuffer);
                    sharedBuffer.clear();
                    bufferEmpty.signalAll();
                } finally {
                    bufferLock.unlock();
                }
                writerBuffer.flip();
                writeLock.lock();
                try {
                    writeFully(channel, writerBuffer);
                } finally {
                    writeLock.unlock();
                }
                writerBuffer.clear();
                stats.sharedWrites++;
            } catch (Exception e) {
                die("Cant write bytes", e);
                return;
            }
        }
    }

    protected void fail(TarantoolRequest request, Exception e) {
        request.getResult().completeExceptionally(e);
    }

    protected void complete(TarantoolPacket packet, TarantoolRequest request) {
        TarantoolOp<?> result = request.getResult();
        if (result.isDone()) {
            return;
        }

        long code = packet.getCode();
        long schemaId = packet.getSchemaId();
        if (code == ProtoConstants.SUCCESS) {
            request.setCompletedSchemaId(schemaId);
            if (request.isSync()) {
                completeSync(request);
            } else if (request.getCode() == Code.EXECUTE) {
                completeSql(request, packet);
            } else {
                ((TarantoolOp) result).complete(packet.getData());
            }
        } else if (code == ProtoConstants.ERR_WRONG_SCHEMA_VERSION) {
            if (schemaId > schemaMeta.getSchemaVersion()) {
                delayedOpsQueue.add(request);
            } else {
                registerOperation(request, schemaMeta.getSchemaVersion());
            }
        } else {
            Object error = packet.getError();
            fail(request, serverError(code, error));
        }

        if (request.getStartedSchemaId() == 0) {
            return;
        }
        // it's possible to receive bigger version than current
        // i.e. after DDL operation or wrong schema version response
        if (schemaId > schemaMeta.getSchemaVersion()) {
            updateSchema();
        }
    }

    /**
     * When a sync is successful it's required
     * to re-evaluate dependent request and complete
     * exceptionally with a proper error.
     *
     * @param request sync request
     */
    private void completeSync(TarantoolRequest request) {
        TarantoolRequest source = request.getSyncDependent();
        if (delayedOpsQueue.remove(source)) {
            try {
                source.getArguments();
            } catch (Exception e) {
                fail(source, e);
            }
        }
    }

    /**
     * Optimistic means the client issues ping request
     * to check whether it keeps an actual schema version or not.
     * If the schema is outdated, client uses refreshes its schema
     * cache.
     * <p>
     * It's suitable when we don't refresh all the cache directly because
     * of the schema can be large enough to be loaded fast.
     *
     * @see #updateSchema()
     */
    private void optimisticSchemaUpdate(TarantoolRequest source) {
        TarantoolRequest pingRequest = new TarantoolRequest(Code.PING);
        pingRequest.beginSync(syncId.incrementAndGet(), operationTimeout, source);
        registerOperation(pingRequest, schemaMeta.getSchemaVersion());
    }

    private void updateSchema() {
        performSchemaAction(() -> {
            if (state.acquire(StateHelper.SCHEMA_UPDATING)) {
                workExecutor.execute(createUpdateSchemaTask());
            }
        });
    }

    private Runnable createUpdateSchemaTask() {
        return () -> {
            try {
                schemaMeta.refresh();
            } catch (Exception cause) {
                workExecutor.schedule(createUpdateSchemaTask(), 300L, TimeUnit.MILLISECONDS);
                return;
            }
            performSchemaAction(() -> {
                try {
                    rescheduleDelayedOperations();
                } finally {
                    state.release(StateHelper.SCHEMA_UPDATING);
                }
            });
        };
    }

    private void rescheduleDelayedOperations() {
        TarantoolRequest request;
        while ((request = delayedOpsQueue.poll()) != null) {
            TarantoolOp<?> op = request.getResult();
            if (!op.isDone()) {
                registerOperation(request, schemaMeta.getSchemaVersion());
            }
        }
    }

    protected void completeSql(TarantoolRequest request, TarantoolPacket pack) {
        Long rowCount = SqlProtoUtils.getSQLRowCount(pack);
        TarantoolOp<?> result = request.getResult();
        if (rowCount != null) {
            ((TarantoolOp) result).complete(rowCount);
        } else {
            List<Map<String, Object>> values = SqlProtoUtils.readSqlResult(pack);
            ((TarantoolOp) result).complete(values);
        }
    }

    /**
     * Convenient guard scope that executes given runnable
     * inside schema write lock.
     *
     * @param action to be executed
     */
    protected void performSchemaAction(Runnable action) {
        long stamp = schemaLock.writeLock();
        try {
            action.run();
        } finally {
            schemaLock.unlockWrite(stamp);
        }
    }

    protected <T> T syncGet(Future<T> result) {
        try {
            return result.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CommunicationException) {
                throw (CommunicationException) e.getCause();
            } else if (e.getCause() instanceof TarantoolException) {
                throw (TarantoolException) e.getCause();
            } else {
                throw new IllegalStateException(e.getCause());
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    protected void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        ProtoUtils.writeFully(channel, buffer);
    }

    @Override
    public void close() {
        close(new Exception("Connection is closed."));
        try {
            state.awaitState(StateHelper.CLOSED);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    protected void close(Exception e) {
        if (state.close()) {
            if (workExecutor != null) {
                workExecutor.shutdownNow();
            }
            connector.interrupt();
            die(e.getMessage(), e);
        }
    }

    protected void stopIO() {
        if (reader != null) {
            reader.interrupt();
        }
        if (writer != null) {
            writer.interrupt();
        }
        if (readChannel != null) {
            try {
                readChannel.close(); // also closes this.channel
            } catch (IOException ignored) {
                // no-op
            }
        }
        closeChannel(channel);
    }

    /**
     * Gets the default timeout for client operations.
     *
     * @return timeout in millis
     */
    public long getOperationTimeout() {
        return operationTimeout.toMillis();
    }

    /**
     * Sets the default operation timeout.
     *
     * @param operationTimeout timeout in millis
     */
    public void setOperationTimeout(long operationTimeout) {
        this.operationTimeout = Duration.ofMillis(operationTimeout);
    }

    @Override
    public boolean isAlive() {
        return state.isStateSet(StateHelper.ALIVE) && thumbstone == null;
    }

    @Override
    public boolean isClosed() {
        return state.isStateSet(StateHelper.CLOSED);
    }

    @Override
    public void waitAlive() throws InterruptedException {
        state.awaitState(StateHelper.ALIVE);
    }

    @Override
    public boolean waitAlive(long timeout, TimeUnit unit) throws InterruptedException {
        return state.awaitState(StateHelper.ALIVE, timeout, unit);
    }

    @Override
    public TarantoolClientOps<List<?>, Object, List<?>> syncOps() {
        return syncOps;
    }

    @Override
    public TarantoolClientOps<List<?>, Object, Future<List<?>>> asyncOps() {
        return (TarantoolClientOps) this;
    }

    @Override
    public TarantoolClientOps<List<?>, Object, CompletionStage<List<?>>> composableAsyncOps() {
        return composableAsyncOps;
    }

    @Override
    public TarantoolClientOps<List<?>, Object, Long> fireAndForgetOps() {
        return fireAndForgetOps;
    }

    public TarantoolClientOps<List<?>, Object, TupleTwo<List<?>, Long>> unsafeSchemaOps() {
        return unsafeSchemaOps;
    }

    protected TarantoolRequest makeSqlRequest(String sql, List<Object> bind) {
        TarantoolRequest request = new TarantoolRequest(Code.EXECUTE);
        request.addArguments(
            RequestArguments.value(Key.SQL_TEXT),
            RequestArguments.value(sql),
            RequestArguments.value(Key.SQL_BIND),
            RequestArguments.value(bind)
        );
        return request;
    }

    @Override
    public TarantoolSQLOps<Object, Long, List<Map<String, Object>>> sqlSyncOps() {
        return new TarantoolSQLOps<Object, Long, List<Map<String, Object>>>() {
            @Override
            public Long update(String sql, Object... bind) {
                return (Long) syncGet(exec(makeSqlRequest(sql, Arrays.asList(bind))));
            }

            @Override
            public List<Map<String, Object>> query(String sql, Object... bind) {
                return (List<Map<String, Object>>) syncGet(exec(makeSqlRequest(sql, Arrays.asList(bind))));
            }
        };
    }

    @Override
    public TarantoolSQLOps<Object, Future<Long>, Future<List<Map<String, Object>>>> sqlAsyncOps() {
        return new TarantoolSQLOps<Object, Future<Long>, Future<List<Map<String, Object>>>>() {
            @Override
            public Future<Long> update(String sql, Object... bind) {
                return (Future<Long>) exec(makeSqlRequest(sql, Arrays.asList(bind)));
            }

            @Override
            public Future<List<Map<String, Object>>> query(String sql, Object... bind) {
                return (Future<List<Map<String, Object>>>) exec(makeSqlRequest(sql, Arrays.asList(bind)));
            }
        };
    }

    protected class SyncOps extends BaseClientOps<List<?>> {

        @Override
        protected List<?> exec(TarantoolRequest request) {
            return (List) syncGet(TarantoolClientImpl.this.exec(request));
        }

    }

    protected class FireAndForgetOps extends BaseClientOps<Long> {

        @Override
        protected Long exec(TarantoolRequest request) {
            if (thumbstone == null) {
                try {
                    return doExec(request).getId();
                } catch (Exception e) {
                    throw new CommunicationException("Execute failed", e);
                }
            } else {
                throw new CommunicationException("Connection is not alive", thumbstone);
            }
        }

    }

    protected boolean isDead(TarantoolRequest request) {
        if (this.thumbstone != null) {
            fail(request, new CommunicationException("Connection is dead", thumbstone));
            return true;
        }
        return false;
    }

    /**
     * A subclass may use this as a trigger to start retries.
     * This method is called when state becomes ALIVE.
     */
    protected void onReconnect() {
        // No-op, override.
    }

    public Exception getThumbstone() {
        return thumbstone;
    }

    public TarantoolClientStats getStats() {
        return stats;
    }

    /**
     * Manages state changes.
     */
    protected final class StateHelper {

        static final int UNINITIALIZED = 0;
        static final int READING = 1;
        static final int WRITING = 2;

        static final int ALIVE = READING | WRITING;
        static final int SCHEMA_UPDATING = 1 << 2;

        static final int RECONNECT = 1 << 3;
        static final int CLOSED = 1 << 4;

        private final AtomicInteger state;

        private final AtomicReference<CountDownLatch> nextAliveLatch =
            new AtomicReference<>(new CountDownLatch(1));

        private final CountDownLatch closedLatch = new CountDownLatch(1);

        /**
         * The condition variable to signal a reconnection is needed from reader /
         * writer threads and waiting for that signal from the reconnection thread.
         * <p>
         * The lock variable to access this condition.
         *
         * @see #awaitReconnection()
         * @see #trySignalForReconnection()
         */
        protected final ReentrantLock connectorLock = new ReentrantLock();
        protected final Condition reconnectRequired = connectorLock.newCondition();

        protected StateHelper(int state) {
            this.state = new AtomicInteger(state);
        }

        protected int getState() {
            return state.get();
        }

        boolean isStateSet(int mask) {
            return (getState() & mask) == mask;
        }

        /**
         * Set CLOSED state, drop RECONNECT state.
         */
        protected boolean close() {
            for (; ; ) {
                int currentState = getState();

                /* CLOSED is the terminal state. */
                if (isStateSet(CLOSED)) {
                    return false;
                }

                /* Clear all states and set CLOSED. */
                if (compareAndSet(currentState, CLOSED)) {
                    return true;
                }
            }
        }

        /**
         * Move from a current state to a give one.
         * <p>
         * Some moves are forbidden.
         */
        protected boolean acquire(int mask) {
            for (; ; ) {
                int currentState = getState();

                /* CLOSED is the terminal state. */
                if ((isStateSet(CLOSED))) {
                    return false;
                }

                /* Don't move to READING, WRITING or ALIVE from RECONNECT. */
                if ((currentState & RECONNECT) > mask) {
                    return false;
                }

                /* Cannot move from a state to the same state. */
                if (isStateSet(mask)) {
                    return false;
                }

                /* Set acquired state. */
                if (compareAndSet(currentState, currentState | mask)) {
                    return true;
                }
            }
        }

        protected void release(int mask) {
            for (; ; ) {
                int currentState = getState();
                if (compareAndSet(currentState, currentState & ~mask)) {
                    return;
                }
            }
        }

        protected boolean compareAndSet(int expect, int update) {
            if (!state.compareAndSet(expect, update)) {
                return false;
            }

            boolean wasAlreadyAlive = (expect & ALIVE) == ALIVE;
            if (!wasAlreadyAlive && (update & ALIVE) == ALIVE) {
                CountDownLatch latch = nextAliveLatch.getAndSet(new CountDownLatch(1));
                latch.countDown();
                onReconnect();
            } else if (update == CLOSED) {
                closedLatch.countDown();
            }
            return true;
        }

        /**
         * Reconnection uses another way to await state via receiving a signal
         * instead of latches.
         */
        protected void awaitState(int state) throws InterruptedException {
            if (state == RECONNECT) {
                awaitReconnection();
            } else {
                CountDownLatch latch = getStateLatch(state);
                if (latch != null) {
                    latch.await();
                }
            }
        }

        protected boolean awaitState(int state, long timeout, TimeUnit timeUnit) throws InterruptedException {
            CountDownLatch latch = getStateLatch(state);
            return (latch == null) || latch.await(timeout, timeUnit);
        }

        private CountDownLatch getStateLatch(int state) {
            if (state == CLOSED) {
                return closedLatch;
            }
            if (state == ALIVE) {
                if (isStateSet(CLOSED)) {
                    throw new IllegalStateException("State is CLOSED.");
                }
                CountDownLatch latch = nextAliveLatch.get();
                /* It may happen so that an error is detected but the state is still alive.
                 Wait for the 'next' alive state in such cases. */
                return (isStateSet(ALIVE) && thumbstone == null) ? null : latch;
            }
            return null;
        }

        /**
         * Blocks until a reconnection signal will be received.
         *
         * @see #trySignalForReconnection()
         */
        private void awaitReconnection() throws InterruptedException {
            connectorLock.lock();
            try {
                while (!isStateSet(RECONNECT)) {
                    reconnectRequired.await();
                }
            } finally {
                connectorLock.unlock();
            }
        }

        /**
         * Signals to the connector that reconnection process can be performed.
         *
         * @see #awaitReconnection()
         */
        private void trySignalForReconnection() {
            if (compareAndSet(StateHelper.UNINITIALIZED, StateHelper.RECONNECT)) {
                connectorLock.lock();
                try {
                    reconnectRequired.signal();
                } finally {
                    connectorLock.unlock();
                }
            }
        }

    }

    protected class ComposableAsyncOps extends BaseClientOps<CompletionStage<List<?>>> {

        @Override
        protected CompletionStage<List<?>> exec(TarantoolRequest request) {
            return (CompletionStage<List<?>>) TarantoolClientImpl.this.exec(request);
        }

        @Override
        public void close() {
            TarantoolClientImpl.this.close();
        }

    }

    /**
     * Used by internal services to ignore schema ID issues.
     */
    protected class UnsafeSchemaOps extends BaseClientOps<TupleTwo<List<?>, Long>> {

        protected TupleTwo<List<?>, Long> exec(TarantoolRequest request) {
            long syncId = TarantoolClientImpl.this.syncId.incrementAndGet();
            request.begin(syncId, operationTimeout);
            List<?> result = (List<?>) syncGet(registerOperation(request, 0L).getResult());
            return TupleTwo.of(result, request.getCompletedSchemaId());
        }

    }

    protected abstract class BaseClientOps<R> extends AbstractTarantoolOps<R> {

        @Override
        protected TarantoolSchemaMeta getSchemaMeta() {
            return TarantoolClientImpl.this.getSchemaMeta();
        }

        @Override
        public void close() {
            throw new IllegalStateException("You should close TarantoolClient instead.");
        }

    }

}

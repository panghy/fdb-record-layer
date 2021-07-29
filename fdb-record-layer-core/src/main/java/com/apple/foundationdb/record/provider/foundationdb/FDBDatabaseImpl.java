/*
 * FDBDatabase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.AsyncLoadingCache;
import com.apple.foundationdb.record.LoggableTimeoutException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.ResolverStateProto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ScopedValue;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache;
import com.apple.foundationdb.record.provider.foundationdb.storestate.PassThroughRecordStoreStateCache;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A known FDB {@link Database}, associated with a cluster file location.
 *
 * @see FDBDatabase
 */
@API(API.Status.STABLE)
public class FDBDatabaseImpl implements FDBDatabase {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseImpl.class);

    @Nonnull
    private final FDBDatabaseFactoryImpl factory;
    @Nullable
    private final String clusterFile;
    /* Null until openFDB is called. */
    @Nullable
    private Database database;
    @Nullable
    private Function<FDBStoreTimer.Wait, Pair<Long, TimeUnit>> asyncToSyncTimeout;
    @Nonnull
    private ExceptionMapper asyncToSyncExceptionMapper;
    @Nonnull
    private AsyncLoadingCache<LocatableResolver, ResolverStateProto.State> resolverStateCache;
    @Nonnull
    private Cache<ScopedValue<String>, ResolverResult> directoryCache;
    // Version that the current directory cache was initialized with. A version counter is kept in the directory layer
    // state. Major changes to the directory layer will increment the version stored in the database, when that version
    // moves past directoryCacheVersion we invalidate the current directoryCache and update directoryCacheVersion.
    @Nonnull
    private AtomicInteger directoryCacheVersion = new AtomicInteger();
    @Nonnull
    private Cache<ScopedValue<Long>, String> reverseDirectoryInMemoryCache;
    private boolean opened;
    private final Object reverseDirectoryCacheLock = new Object();
    private volatile FDBReverseDirectoryCache reverseDirectoryCache;
    private final int reverseDirectoryMaxRowsPerTransaction;
    private final long reverseDirectoryMaxMillisPerTransaction;
    @Nonnull
    private FDBRecordStoreStateCache storeStateCache = PassThroughRecordStoreStateCache.instance();
    private final Supplier<Boolean> transactionIsTracedSupplier;
    private final long warnAndCloseOpenContextsAfterSeconds;

    private boolean trackLastSeenVersionOnRead = false;
    private boolean trackLastSeenVersionOnCommit = false;

    @Nonnull
    private final Supplier<BlockingInAsyncDetection> blockingInAsyncDetectionSupplier;

    @Nonnull
    private final Function<FDBLatencySource, Long> latencyInjector;

    private String datacenterId;

    @Nonnull
    private FDBLocalityProvider localityProvider;

    @Nonnull
    private static ImmutablePair<Long, Long> initialVersionPair = new ImmutablePair<>(null, null);
    @Nonnull
    private AtomicReference<ImmutablePair<Long, Long>> lastSeenFDBVersion = new AtomicReference<>(initialVersionPair);

    private final NavigableMap<Long, FDBRecordContext> trackedOpenContexts = new ConcurrentSkipListMap<>();

    @VisibleForTesting
    public FDBDatabaseImpl(@Nonnull FDBDatabaseFactoryImpl factory, @Nullable String clusterFile) {
        this.factory = factory;
        this.clusterFile = clusterFile;
        this.asyncToSyncExceptionMapper = (ex, ev) -> FDBExceptions.wrapException(ex);
        this.reverseDirectoryMaxRowsPerTransaction = factory.getReverseDirectoryRowsPerTransaction();
        this.reverseDirectoryMaxMillisPerTransaction = factory.getReverseDirectoryMaxMillisPerTransaction();
        this.transactionIsTracedSupplier = factory.getTransactionIsTracedSupplier();
        this.warnAndCloseOpenContextsAfterSeconds = factory.getWarnAndCloseOpenContextsAfterSeconds();
        this.blockingInAsyncDetectionSupplier = factory.getBlockingInAsyncDetectionSupplier();
        this.reverseDirectoryInMemoryCache = CacheBuilder.newBuilder()
                .maximumSize(DEFAULT_MAX_REVERSE_CACHE_ENTRIES)
                .recordStats()
                .build();
        this.directoryCache = CacheBuilder.newBuilder()
                .maximumSize(factory.getDirectoryCacheSize())
                .recordStats()
                .build();
        this.resolverStateCache = new AsyncLoadingCache<>(factory.getStateRefreshTimeMillis());
        this.latencyInjector = factory.getLatencyInjector();
        this.datacenterId = factory.getDatacenterId();
        this.localityProvider = factory.getLocalityProvider();
    }

    protected synchronized void openFDB() {
        if (!opened) {
            final FDB fdb = factory.initFDB();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of("Opening FDB", LogMessageKeys.CLUSTER, clusterFile));
            }
            database = fdb.open(clusterFile);
            setDirectoryCacheSize(factory.getDirectoryCacheSize());
            opened = true;
        }
    }

    @Override
    public synchronized void setDirectoryCacheSize(int size) {
        int maxSize = (size > 0) ? size : 0;
        directoryCache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumSize(maxSize)
                .build();
    }

    @Override
    public synchronized void setDatacenterId(String datacenterId) {
        this.datacenterId = datacenterId;
        database().options().setDatacenterId(datacenterId);
    }

    @Override
    public synchronized String getDatacenterId() {
        return datacenterId;
    }

    @Override
    @Nonnull
    public synchronized FDBLocalityProvider getLocalityProvider() {
        return localityProvider;
    }

    @Override
    public synchronized void setTrackLastSeenVersionOnRead(boolean trackLastSeenVersion) {
        this.trackLastSeenVersionOnRead = trackLastSeenVersion;
    }

    @Override
    public synchronized boolean isTrackLastSeenVersionOnRead() {
        return trackLastSeenVersionOnRead;
    }

    @Override
    public synchronized void setTrackLastSeenVersionOnCommit(boolean trackLastSeenVersion) {
        this.trackLastSeenVersionOnCommit = trackLastSeenVersion;
    }

    @Override
    public synchronized boolean isTrackLastSeenVersionOnCommit() {
        return trackLastSeenVersionOnCommit;
    }

    @Override
    public synchronized void setTrackLastSeenVersion(boolean trackLastSeenVersion) {
        this.trackLastSeenVersionOnRead = trackLastSeenVersion;
        this.trackLastSeenVersionOnCommit = trackLastSeenVersion;
    }

    @Override
    public synchronized boolean isTrackLastSeenVersion() {
        return trackLastSeenVersionOnRead || trackLastSeenVersionOnCommit;
    }

    @Override
    @Nullable
    public String getClusterFile() {
        return clusterFile;
    }

    @Override
    @Nonnull
    public FDBDatabaseFactory getFactory() {
        return factory;
    }

    @Override
    @Nonnull
    public Database database() {
        openFDB();
        return database;
    }

    @Override
    @Nonnull
    public FDBRecordContext openContext() {
        return openContext(null, null);
    }

    @Override
    @Nonnull
    public FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                        @Nullable FDBStoreTimer timer) {
        return openContext(mdcContext, timer, null);
    }

    @Override
    @Nonnull
    public FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                        @Nullable FDBStoreTimer timer,
                                        @Nullable WeakReadSemantics weakReadSemantics) {
        return openContext(mdcContext, timer, weakReadSemantics, FDBTransactionPriority.DEFAULT);
    }

    @Override
    @Nonnull
    public FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                        @Nullable FDBStoreTimer timer,
                                        @Nullable WeakReadSemantics weakReadSemantics,
                                        @Nonnull FDBTransactionPriority priority) {
        return openContext(mdcContext, timer, weakReadSemantics, priority, null);
    }


    @Override
    @Nonnull
    public FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                        @Nullable FDBStoreTimer timer,
                                        @Nullable WeakReadSemantics weakReadSemantics,
                                        @Nonnull FDBTransactionPriority priority,
                                        @Nullable String transactionId) {
        FDBRecordContextConfig contextConfig = FDBRecordContextConfig.newBuilder()
                .setMdcContext(mdcContext)
                .setTimer(timer)
                .setWeakReadSemantics(weakReadSemantics)
                .setPriority(priority)
                .setTransactionId(transactionId)
                .build();
        return openContext(contextConfig);
    }

    @Override
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public FDBRecordContext openContext(@Nonnull FDBRecordContextConfig contextConfig) {
        openFDB();
        final Executor executor = newContextExecutor(contextConfig.getMdcContext());
        final Transaction transaction = createTransaction(contextConfig, executor);

        // TODO: Compatibility with STABLE API.
        if (transactionIsTracedSupplier.get()) {
            contextConfig = contextConfig.toBuilder()
                    .setTrackOpen(true)
                    .setLogTransaction(true)
                    .setSaveOpenStackTrace(true)
                    .build();
        }

        FDBRecordContext context = new FDBRecordContext(this, transaction, contextConfig);
        final WeakReadSemantics weakReadSemantics = context.getWeakReadSemantics();
        if (isTrackLastSeenVersion() && (weakReadSemantics != null)) {
            Pair<Long, Long> pair = lastSeenFDBVersion.get();
            if (pair != initialVersionPair) {
                long version = pair.getLeft();
                long versionTimeMillis = pair.getRight();
                // If the following condition holds, a subsequent getReadVersion (on this transaction) returns version,
                // otherwise getReadVersion does not use the cached value and results in a GRV call to FDB
                if (version >= weakReadSemantics.getMinVersion() &&
                        (System.currentTimeMillis() - versionTimeMillis) <= weakReadSemantics.getStalenessBoundMillis()) {
                    context.setReadVersion(version);
                    context.increment(FDBStoreTimer.Counts.SET_READ_VERSION_TO_LAST_SEEN);
                }
            }
        }

        if (warnAndCloseOpenContextsAfterSeconds > 0) {
            warnAndCloseOldTrackedOpenContexts(warnAndCloseOpenContextsAfterSeconds);
        }
        if (contextConfig.isTrackOpen()) {
            trackOpenContext(context);
        }

        return context;
    }

    private void logNoOpFailure(@Nonnull Throwable err) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error(KeyValueLogMessage.of("unable to perform no-op operation against fdb",
                    LogMessageKeys.CLUSTER, getClusterFile()), err);
        }
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> performNoOpAsync() {
        return performNoOpAsync(null);
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> performNoOpAsync(@Nullable FDBStoreTimer timer) {
        return performNoOpAsync(null, timer);
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> performNoOpAsync(@Nullable Map<String, String> mdcContext,
                                                    @Nullable FDBStoreTimer timer) {
        final FDBRecordContext context = openContext(mdcContext, timer);
        boolean futureStarted = false;

        try {
            // Set the read version of the transaction, then read it back. This requires no I/O, but it does
            // require the network thread be running. The exact value used for the read version is unimportant.
            // Note that this calls setReadVersion and getReadVersion on the Transaction object, *not* on the
            // FDBRecordContext. This is because the FDBRecordContext will cache the value of setReadVersion to
            // avoid having to go back to the FDB network thread, but we do not want that for instrumentation.
            final Transaction tr = context.ensureActive();
            final long startTime = System.nanoTime();
            tr.setReadVersion(1066L);
            CompletableFuture<Long> future = tr.getReadVersion();
            if (timer != null) {
                future = context.instrument(FDBStoreTimer.Events.PERFORM_NO_OP, future, startTime);
            }
            futureStarted = true;
            return future.thenAccept(ignore -> { }).whenComplete((vignore, err) -> {
                context.close();
                if (err != null) {
                    logNoOpFailure(err);
                }
            });
        } catch (RuntimeException e) {
            logNoOpFailure(e);
            CompletableFuture<Void> errFuture = new CompletableFuture<>();
            errFuture.completeExceptionally(e);
            return errFuture;
        } finally {
            if (!futureStarted) {
                context.close();
            }
        }
    }

    @Override
    public void performNoOp() {
        performNoOp(null);
    }

    @Override
    public void performNoOp(@Nullable FDBStoreTimer timer) {
        performNoOp(null, timer);
    }

    @Override
    public void performNoOp(@Nullable Map<String, String> mdcContext, @Nullable FDBStoreTimer timer) {
        asyncToSync(timer, FDBStoreTimer.Waits.WAIT_PERFORM_NO_OP, performNoOpAsync(mdcContext, timer));
    }

    private long versionTimeEstimate(long startMillis) {
        return startMillis + (System.currentTimeMillis() - startMillis) / 2;
    }

    @Override
    public long getResolverStateCacheRefreshTime() {
        return resolverStateCache.getRefreshTimeSeconds();
    }

    @Override
    @VisibleForTesting
    public void setResolverStateRefreshTimeMillis(long resolverStateRefreshTimeMillis) {
        resolverStateCache.clear();
        resolverStateCache = new AsyncLoadingCache<>(resolverStateRefreshTimeMillis);
    }

    @Override
    @Nonnull
    @API(API.Status.INTERNAL)
    public CompletableFuture<ResolverStateProto.State> getStateForResolver(@Nonnull LocatableResolver resolver,
                                                                           @Nonnull Supplier<CompletableFuture<ResolverStateProto.State>> loader) {
        return resolverStateCache.orElseGet(resolver, loader);
    }

    // Update lastSeenFDBVersion if readVersion is newer
    @Override
    @API(API.Status.INTERNAL)
    public void updateLastSeenFDBVersion(long startTime, long readVersion) {
        lastSeenFDBVersion.updateAndGet(pair ->
                (pair.getLeft() == null || readVersion > pair.getLeft()) ?
                        new ImmutablePair<>(readVersion, versionTimeEstimate(startTime)) : pair);
    }

    @Override
    @Nonnull
    @API(API.Status.INTERNAL)
    public FDBReverseDirectoryCache getReverseDirectoryCache() {
        if (reverseDirectoryCache == null) {
            synchronized (reverseDirectoryCacheLock) {
                if (reverseDirectoryCache == null) {
                    reverseDirectoryCache = new FDBReverseDirectoryCache(
                            this,
                            reverseDirectoryMaxRowsPerTransaction,
                            reverseDirectoryMaxMillisPerTransaction);
                }
            }
        }
        return reverseDirectoryCache;
    }

    private void setDirectoryCacheVersion(int version) {
        directoryCacheVersion.set(version);
    }

    @Override
    @API(API.Status.INTERNAL)
    public int getDirectoryCacheVersion() {
        return directoryCacheVersion.get();
    }

    @Override
    public CacheStats getDirectoryCacheStats() {
        return directoryCache.stats();
    }

    @Override
    @Nonnull
    @API(API.Status.INTERNAL)
    public Cache<ScopedValue<String>, ResolverResult> getDirectoryCache(int atVersion) {
        if (atVersion > getDirectoryCacheVersion()) {
            synchronized (this) {
                if (atVersion > getDirectoryCacheVersion()) {
                    directoryCache = CacheBuilder.newBuilder()
                            .recordStats()
                            .maximumSize(factory.getDirectoryCacheSize())
                            .build();
                    setDirectoryCacheVersion(atVersion);
                }
            }
        }
        return directoryCache;
    }

    @Override
    @Nonnull
    @API(API.Status.INTERNAL)
    public Cache<ScopedValue<Long>, String> getReverseDirectoryInMemoryCache() {
        return reverseDirectoryInMemoryCache;
    }

    @Override
    @API(API.Status.INTERNAL)
    public void clearForwardDirectoryCache() {
        directoryCache.invalidateAll();
    }

    @Override
    @VisibleForTesting
    @API(API.Status.INTERNAL)
    public void clearReverseDirectoryCache() {
        synchronized (reverseDirectoryCacheLock) {
            reverseDirectoryCache = null;
            reverseDirectoryInMemoryCache.invalidateAll();
        }
    }

    @Override
    @Nonnull
    public FDBRecordStoreStateCache getStoreStateCache() {
        return storeStateCache;
    }

    @Override
    public void setStoreStateCache(@Nonnull FDBRecordStoreStateCache storeStateCache) {
        storeStateCache.validateDatabase(this);
        this.storeStateCache = storeStateCache;
    }

    @Override
    @VisibleForTesting
    @API(API.Status.INTERNAL)
    public void clearCaches() {
        resolverStateCache.clear();
        clearForwardDirectoryCache();
        clearReverseDirectoryCache();
        storeStateCache.clear();
    }

    @Override
    public synchronized void close() {
        if (opened) {
            database.close();
            database = null;
            opened = false;
            directoryCacheVersion.set(0);
            clearCaches();
            reverseDirectoryInMemoryCache.invalidateAll();
        }
    }

    @Override
    @Nonnull
    public Executor getExecutor() {
        return factory.getExecutor();
    }

    @Override
    public Executor newContextExecutor(@Nullable Map<String, String> mdcContext) {
        return factory.newContextExecutor(mdcContext);
    }

    /**
     * Creates a new transaction against the database.
     *
     * @param executor the executor to be used for asynchronous operations
     * @return newly created transaction
     */
    private Transaction createTransaction(@Nonnull FDBRecordContextConfig config, @Nonnull Executor executor) {
        final FDBStoreTimer timer = config.getTimer();
        boolean enableAssertions = config.areAssertionsEnabled();
        //noinspection ConstantConditions
        Transaction transaction = database.createTransaction(executor, timer);
        if (timer != null || enableAssertions) {
            transaction = new InstrumentedTransaction(timer, transaction, enableAssertions);
        }

        return transaction;
    }

    @Override
    @Nonnull
    public FDBDatabaseRunner newRunner(@Nonnull FDBRecordContextConfig.Builder contextConfigBuilder) {
        return new FDBDatabaseRunnerImpl(this, contextConfigBuilder);
    }

    @Override
    @Nonnull
    public FDBDatabaseRunner newRunner() {
        return newRunner(FDBRecordContextConfig.newBuilder());
    }

    @Override
    @Nonnull
    public FDBDatabaseRunner newRunner(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext) {
        return newRunner(FDBRecordContextConfig.newBuilder().setTimer(timer).setMdcContext(mdcContext));
    }

    @Override
    @Nonnull
    public FDBDatabaseRunner newRunner(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                                       @Nullable WeakReadSemantics weakReadSemantics) {
        return newRunner(FDBRecordContextConfig.newBuilder().setTimer(timer).setMdcContext(mdcContext).setWeakReadSemantics(weakReadSemantics));
    }

    @Override
    public <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
        try (FDBDatabaseRunner runner = newRunner()) {
            return runner.run(retriable);
        }
    }

    @Override
    public <T> T run(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                     @Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
        try (FDBDatabaseRunner runner = newRunner(timer, mdcContext)) {
            return runner.run(retriable);
        }
    }

    @Override
    public <T> T run(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext, @Nullable WeakReadSemantics weakReadSemantics,
                     @Nonnull Function<? super FDBRecordContext, ? extends T> retriable) {
        try (FDBDatabaseRunner runner = newRunner(timer, mdcContext, weakReadSemantics)) {
            return runner.run(retriable);
        }
    }

    @Override
    @Nonnull
    @API(API.Status.MAINTAINED)
    public <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable) {
        return runAsync(retriable, null);
    }

    @Override
    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    public <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                             @Nullable List<Object> additionalLogMessageKeyValues) {
        final FDBDatabaseRunner runner = newRunner();
        return runner.runAsync(retriable, additionalLogMessageKeyValues).whenComplete((t, e) -> runner.close());
    }

    @Override
    @Nonnull
    @API(API.Status.MAINTAINED)
    public <T> CompletableFuture<T> runAsync(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                                             @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable) {
        return runAsync(timer, mdcContext, retriable, null);
    }

    @Override
    @Nonnull
    @API(API.Status.MAINTAINED)
    public <T> CompletableFuture<T> runAsync(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                                             @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                             @Nullable List<Object> additionalLogMessageKeyValues) {
        final FDBDatabaseRunner runner = newRunner(timer, mdcContext);
        return runner.runAsync(retriable, additionalLogMessageKeyValues).whenComplete((t, e) -> runner.close());
    }

    @Override
    @Nonnull
    @API(API.Status.MAINTAINED)
    public <T> CompletableFuture<T> runAsync(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext, @Nullable WeakReadSemantics weakReadSemantics,
                                             @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable) {
        final FDBDatabaseRunner runner = newRunner(timer, mdcContext, weakReadSemantics);
        return runner.runAsync(retriable).whenComplete((t, e) -> runner.close());
    }

    @Override
    public boolean hasAsyncToSyncTimeout() {
        return asyncToSyncTimeout != null;
    }

    @Override
    @Nullable
    public Pair<Long, TimeUnit> getAsyncToSyncTimeout(FDBStoreTimer.Wait event) {
        if (asyncToSyncTimeout == null) {
            return null;
        } else {
            return asyncToSyncTimeout.apply(event);
        }
    }

    @Override
    @Nullable
    public Function<FDBStoreTimer.Wait, Pair<Long, TimeUnit>> getAsyncToSyncTimeout() {
        return asyncToSyncTimeout;
    }

    @Override
    public void setAsyncToSyncTimeout(@Nullable Function<FDBStoreTimer.Wait, Pair<Long, TimeUnit>> asyncToSyncTimeout) {
        this.asyncToSyncTimeout = asyncToSyncTimeout;
    }

    @Override
    public void setAsyncToSyncTimeout(long asyncToSyncTimeout, @Nonnull TimeUnit asyncToSyncTimeoutUnit) {
        setAsyncToSyncTimeout(event -> new ImmutablePair<>(asyncToSyncTimeout, asyncToSyncTimeoutUnit));
    }

    @Override
    public void clearAsyncToSyncTimeout() {
        asyncToSyncTimeout = null;
    }

    @Override
    public void setAsyncToSyncExceptionMapper(@Nonnull ExceptionMapper asyncToSyncExceptionMapper) {
        this.asyncToSyncExceptionMapper = asyncToSyncExceptionMapper;
    }

    @Override
    public RuntimeException mapAsyncToSyncException(@Nonnull Throwable ex) {
        return asyncToSyncExceptionMapper.apply(ex, null);
    }

    @Override
    @Nullable
    public <T> T asyncToSync(@Nullable FDBStoreTimer timer, FDBStoreTimer.Wait event, @Nonnull CompletableFuture<T> async) {
        checkIfBlockingInFuture(async);
        if (async.isDone()) {
            try {
                return async.get();
            } catch (ExecutionException ex) {
                throw asyncToSyncExceptionMapper.apply(ex, event);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw asyncToSyncExceptionMapper.apply(ex, event);
            }
        } else {

            final Pair<Long, TimeUnit> timeout = getAsyncToSyncTimeout(event);
            final long startTime = System.nanoTime();
            try {
                if (timeout != null) {
                    return async.get(timeout.getLeft(), timeout.getRight());
                } else {
                    return async.get();
                }
            } catch (TimeoutException ex) {
                if (timer != null) {
                    timer.recordTimeout(event, startTime);
                    throw asyncToSyncExceptionMapper.apply(new LoggableTimeoutException(ex, LogMessageKeys.TIME_LIMIT.toString(), timeout.getLeft(), LogMessageKeys.TIME_UNIT.toString(), timeout.getRight()), event);
                }
                throw asyncToSyncExceptionMapper.apply(ex, event);
            } catch (ExecutionException ex) {
                throw asyncToSyncExceptionMapper.apply(ex, event);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw asyncToSyncExceptionMapper.apply(ex, event);
            } finally {
                if (timer != null) {
                    timer.recordSinceNanoTime(event, startTime);
                }
            }
        }
    }

    @Override
    public <T> T join(CompletableFuture<T> future) {
        checkIfBlockingInFuture(future);
        return future.join();
    }

    @Override
    public <T> T joinNow(CompletableFuture<T> future) {
        if (future.isDone()) {
            return future.join();
        }
        final BlockingInAsyncDetection behavior = getBlockingInAsyncDetection();
        final StackTraceElement caller = Thread.currentThread().getStackTrace()[1];
        logOrThrowBlockingInAsync(behavior, false, caller, FDBDatabase.BLOCKING_FOR_FUTURE_MESSAGE);

        // If the behavior only logs, we still need to block to return a value.
        // If the behavior throws an exception, we won't reach here.
        return future.join();
    }

    @Override
    public <T> T get(CompletableFuture<T> future) throws InterruptedException, ExecutionException {
        checkIfBlockingInFuture(future);
        return future.get();
    }

    @Override
    @VisibleForTesting
    public int warnAndCloseOldTrackedOpenContexts(long minAgeSeconds) {
        long nanoTime = System.nanoTime() - TimeUnit.SECONDS.toNanos(minAgeSeconds);
        if (trackedOpenContexts.isEmpty()) {
            return 0;
        }
        try {
            if (trackedOpenContexts.firstKey() > nanoTime) {
                return 0;
            }
        } catch (NoSuchElementException ex) {
            return 0;
        }
        int count = 0;
        for (FDBRecordContext context : trackedOpenContexts.headMap(nanoTime, true).values()) {
            KeyValueLogMessage msg = KeyValueLogMessage.build("context not closed",
                    LogMessageKeys.AGE_SECONDS, TimeUnit.NANOSECONDS.toSeconds(nanoTime - context.getTrackOpenTimeNanos()),
                    LogMessageKeys.TRANSACTION_ID, context.getTransactionId());
            if (context.getOpenStackTrace() != null) {
                LOGGER.warn(msg.toString(), context.getOpenStackTrace());
            } else {
                LOGGER.warn(msg.toString());
            }
            context.closeTransaction(true);
            count++;
        }
        return count;
    }

    protected void trackOpenContext(FDBRecordContext context) {
        long key = System.nanoTime();
        while (key == 0 || trackedOpenContexts.putIfAbsent(key, context) != null) {
            key++;  // Might not have nanosecond resolution and need something non-zero and unique.
        }
        context.setTrackOpenTimeNanos(key);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public void untrackOpenContext(FDBRecordContext context) {
        FDBRecordContext found = trackedOpenContexts.remove(context.getTrackOpenTimeNanos());
        if (found != context) {
            throw new RecordCoreException("tracked context does not match");
        }
    }

    @Override
    @API(API.Status.INTERNAL)
    public BlockingInAsyncDetection getBlockingInAsyncDetection() {
        return blockingInAsyncDetectionSupplier.get();
    }

    @Override
    public long getLatencyToInject(FDBLatencySource fdbLatencySource) {
        return latencyInjector.apply(fdbLatencySource);
    }

    private void checkIfBlockingInFuture(CompletableFuture<?> future) {
        BlockingInAsyncDetection behavior = getBlockingInAsyncDetection();
        if (behavior == BlockingInAsyncDetection.DISABLED) {
            return;
        }

        final boolean isComplete = future.isDone();
        if (isComplete && behavior.ignoreComplete()) {
            return;
        }

        final StackTraceElement[] stack = Thread.currentThread().getStackTrace();

        // If, during our traversal of the stack looking for blocking calls, we discover that one of our
        // callers may have been a method that was producing a CompletableFuture (as indicated by a method name
        // ending in "Async"), we will keep track of where this happened. What this may indicate is that some
        // poor, otherwise well-intentioned individual, may be doing something like:
        //
        // @Nonnull
        // public CompletableFuture<Void> doSomethingAsync(@Nonnull FDBRecordStore store) {
        //    Message record = store.loadRecord(Tuple.from(1066L));
        //    return AsyncUtil.DONE;
        // }
        //
        // There are possibly legitimate situations in which this might occur, but those are probably rare, so
        // we will simply log the fact that this has taken place and where it has taken place here.
        StackTraceElement possiblyAsyncReturningLocation = null;

        for (StackTraceElement stackElement : stack) {
            if (stackElement.getClassName().startsWith(CompletableFuture.class.getName())) {
                logOrThrowBlockingInAsync(behavior, isComplete, stackElement, FDBDatabase.BLOCKING_IN_ASYNC_CONTEXT_MESSAGE);
            } else if (stackElement.getMethodName().endsWith("Async")) {
                possiblyAsyncReturningLocation = stackElement;
            }
        }

        if (possiblyAsyncReturningLocation != null && !isComplete) {
            // Maybe one day this will be configurable, but for now we will only allow this situation to log
            logOrThrowBlockingInAsync(BlockingInAsyncDetection.IGNORE_COMPLETE_WARN_BLOCKING, isComplete,
                    possiblyAsyncReturningLocation, FDBDatabase.BLOCKING_RETURNING_ASYNC_MESSAGE);
        }
    }

    private void logOrThrowBlockingInAsync(@Nonnull BlockingInAsyncDetection behavior,
                                           boolean isComplete,
                                           @Nonnull StackTraceElement stackElement,
                                           @Nonnull String title) {
        final RuntimeException exception = new BlockingInAsyncException(title)
                .addLogInfo(
                        LogMessageKeys.FUTURE_COMPLETED, isComplete,
                        LogMessageKeys.CALLING_CLASS, stackElement.getClassName(),
                        LogMessageKeys.CALLING_METHOD, stackElement.getMethodName(),
                        LogMessageKeys.CALLING_LINE, stackElement.getLineNumber());

        if (!isComplete && behavior.throwExceptionOnBlocking()) {
            throw exception;
        } else {
            LOGGER.warn(KeyValueLogMessage.of(title,
                    LogMessageKeys.FUTURE_COMPLETED, isComplete,
                    LogMessageKeys.CALLING_CLASS, stackElement.getClassName(),
                    LogMessageKeys.CALLING_METHOD, stackElement.getMethodName(),
                    LogMessageKeys.CALLING_LINE, stackElement.getLineNumber()),
                    exception);
        }
    }

    @Override
    public CompletableFuture<Tuple> loadBoundaryKeys(@Nonnull FDBTransactionContext context, Tuple key) {
        CompletableFuture<Tuple> result = context.ensureActive().get(key.pack())
                .thenApply(bytes -> bytes == null ? null : Tuple.fromBytes(bytes));
        return context.instrument(FDBStoreTimer.Events.LOAD_BOUNDARY_KEYS, result);
    }

}

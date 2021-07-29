/*
 * FDBDatabase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.ResolverStateProto;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ScopedValue;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCache;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Interface for a FoundationDB Database.
 *
 * <p>
 * All reads and writes to the database are transactional: an open {@link FDBRecordContext} is needed.
 * An {@code FDBDatabase} is needed to open an {@code FDBRecordContext}.
 * </p>
 *
 * <pre><code>
 * final FDBDatabase fdb = FDBDatabaseFactoryImpl.instance().getDatabase();
 * try (FDBRecordContext ctx = fdb.openContext()) {
 *     ...
 * }
 * </code></pre>
 *
 * @see FDBDatabaseFactory
 */
@API(API.Status.STABLE)
public interface FDBDatabase {

    // The number of cache entries to maintain in memory
    int DEFAULT_MAX_REVERSE_CACHE_ENTRIES = 5000;
    // public for javadoc purposes
    int DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS = 30;
    /**
     * Text of message that is logged or exception that is thrown when a blocking API call
     * (<code>asyncToSync()</code>, {@link #join(CompletableFuture)}, or {@link #get(CompletableFuture)}) is
     * called from within a <code>CompletableFuture</code> completion state.
     */
    String BLOCKING_IN_ASYNC_CONTEXT_MESSAGE = "Blocking in an asynchronous context";
    /**
     * Message that is logged when it is detected that a blocking call is being made in a method that may be producing
     * a future (specifically a method that ends in "<code>Async</code>").
     */
    String BLOCKING_RETURNING_ASYNC_MESSAGE = "Blocking in future producing call";
    /**
     * Message that is logged when one joins on a future and expects it to be completed, but it is not yet done.
     */
    String BLOCKING_FOR_FUTURE_MESSAGE = "Blocking on a future that should be completed";

    void setDirectoryCacheSize(int size);

    void setDatacenterId(String datacenterId);

    String getDatacenterId();

    /**
     * Get the locality provider that is used to discover the server location of the keys.
     *
     * @return the locality provider
     */
    @Nonnull
    FDBLocalityProvider getLocalityProvider();

    void setTrackLastSeenVersionOnRead(boolean trackLastSeenVersion);

    boolean isTrackLastSeenVersionOnRead();

    void setTrackLastSeenVersionOnCommit(boolean trackLastSeenVersion);

    boolean isTrackLastSeenVersionOnCommit();

    void setTrackLastSeenVersion(boolean trackLastSeenVersion);

    boolean isTrackLastSeenVersion();

    /**
     * Get the path to the cluster file that this database was created with. Will return <code>null</code> if using the
     * default cluster file. To get the resolved cluster file path for databases created with the default path, use
     * {@link FDBSystemOperations#getClusterFilePath(FDBDatabaseRunner)} instead.
     *
     * @return The path to the cluster file.
     *
     * @see FDBSystemOperations#getClusterFilePath(FDBDatabaseRunner)
     */
    @Nullable
    String getClusterFile();

    /**
     * Get the factory that produced this database.
     * @return the database factory
     */
    @Nonnull
    FDBDatabaseFactory getFactory();

    /**
     * Get the underlying FDB database.
     *
     * @return the FDB database
     */
    @Nonnull
    Database database();

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database.
     *
     * @return a new record context
     *
     * @see Database#createTransaction
     */
    @Nonnull
    FDBRecordContext openContext();

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database.
     *
     * <p>
     * If the logger context includes "uuid" as a key, the value associated with that key will be associated
     * with the returned transaction as its debug identifier. See
     * {@link #openContext(Map, FDBStoreTimer, WeakReadSemantics, FDBTransactionPriority, String)} for more details.
     * </p>
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     *
     * @return a new record context
     *
     * @see Database#createTransaction
     */
    @Nonnull
    FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                 @Nullable FDBStoreTimer timer);

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database.
     *
     * <p>
     * If the logger context includes "uuid" as a key, the value associated with that key will be associated
     * with the returned transaction as its debug identifier. See
     * {@link #openContext(Map, FDBStoreTimer, WeakReadSemantics, FDBTransactionPriority, String)} for more details.
     * </p>
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     * @param weakReadSemantics allowable staleness information if caching read versions
     *
     * @return a new record context
     *
     * @see Database#createTransaction()
     */
    @Nonnull
    FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                 @Nullable FDBStoreTimer timer,
                                 @Nullable WeakReadSemantics weakReadSemantics);

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database.
     *
     * <p>
     * If the logger context includes "uuid" as a key, the value associated with that key will be associated
     * with the returned transaction as its debug identifier. See
     * {@link #openContext(Map, FDBStoreTimer, WeakReadSemantics, FDBTransactionPriority, String)} for more details.
     * </p>
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     * @param weakReadSemantics allowable staleness information if caching read versions
     * @param priority the priority of the transaction being created
     *
     * @return a new record context
     *
     * @see Database#createTransaction
     */
    @Nonnull
    FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                 @Nullable FDBStoreTimer timer,
                                 @Nullable WeakReadSemantics weakReadSemantics,
                                 @Nonnull FDBTransactionPriority priority);

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database.
     *
     * <p>
     * If the passed {@code transactionId} is {@code null}, then this method will set the transaction ID
     * of the given transaction to the value of the "uuid" key in the MDC context if present. The transaction ID
     * should typically consist solely of printable ASCII characters and should not exceed 100 bytes. The ID may
     * be truncated or dropped if the ID will not fit in 100 bytes. See {@link FDBRecordContext#getTransactionId()}
     * for more details.
     * </p>
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     * @param weakReadSemantics allowable staleness information if caching read versions
     * @param priority the priority of the transaction being created
     * @param transactionId the transaction ID to associate with this transaction
     *
     * @return a new record context
     *
     * @see Database#createTransaction
     */
    @Nonnull
    FDBRecordContext openContext(@Nullable Map<String, String> mdcContext,
                                 @Nullable FDBStoreTimer timer,
                                 @Nullable WeakReadSemantics weakReadSemantics,
                                 @Nonnull FDBTransactionPriority priority,
                                 @Nullable String transactionId);

    /**
     * Open a new record context with a new transaction begun on the underlying FDB database. Various
     * options on the transaction will be informed based on the passed context.
     *
     * @param contextConfig a configuration object specifying various options on the returned context
     *
     * @return a new record context
     *
     * @see Database#createTransaction
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    FDBRecordContext openContext(@Nonnull FDBRecordContextConfig contextConfig);

    /**
     * Perform a no-op against FDB to check network thread liveness. See {@link #performNoOp(Map, FDBStoreTimer)}
     * for more information. This will use the default MDC for running threads and will not instrument the
     * operation.
     *
     * @return a future that will complete after being run by the FDB network thread
     *
     * @see #performNoOp(Map, FDBStoreTimer)
     */
    @Nonnull
    CompletableFuture<Void> performNoOpAsync();

    /**
     * Perform a no-op against FDB to check network thread liveness. See {@link #performNoOp(Map, FDBStoreTimer)}
     * for more information. This will use the default MDC for running threads.
     *
     * @param timer the timer to use for instrumentation
     *
     * @return a future that will complete after being run by the FDB network thread
     *
     * @see #performNoOp(Map, FDBStoreTimer)
     */
    @Nonnull
    CompletableFuture<Void> performNoOpAsync(@Nullable FDBStoreTimer timer);

    /**
     * Perform a no-op against FDB to check network thread liveness. This operation will not change the underlying data
     * in any way, nor will it perform any I/O against the FDB cluster. However, it will schedule some amount of work
     * onto the FDB client and wait for it to complete. The FoundationDB client operates by scheduling onto an event
     * queue that is then processed by a single thread (the "network thread"). This method can be used to determine if
     * the network thread has entered a state where it is no longer processing requests or if its time to process
     * requests has increased. If the network thread is busy, this operation may take some amount of time to complete,
     * which is why this operation returns a future.
     *
     * <p>
     * If the provided {@link FDBStoreTimer} is not {@code null}, then this will update the {@link
     * FDBStoreTimer.Events#PERFORM_NO_OP}
     * operation with related timing information. This can then be monitored to detect instances of client saturation
     * where the performance bottleneck lies in scheduling work to run on the FDB event queue.
     * </p>
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     *
     * @return a future that will complete after being run by the FDB network thread
     */
    @Nonnull
    CompletableFuture<Void> performNoOpAsync(@Nullable Map<String, String> mdcContext,
                                             @Nullable FDBStoreTimer timer);

    /**
     * Perform a no-op against FDB to check network thread liveness. This is a blocking version of {@link
     * #performNoOpAsync()}.
     * Note that the future is expected to complete relatively quickly in most circumstances, but it may take a long
     * time if the network thread has been saturated, e.g., if there is a lot of work being scheduled to run in FDB.
     *
     * @see #performNoOpAsync()
     */
    void performNoOp();

    /**
     * Perform a no-op against FDB to check network thread liveness. This is a blocking version of {@link
     * #performNoOpAsync(FDBStoreTimer)}.
     * Note that the future is expected to complete relatively quickly in most circumstances, but it may take a long
     * time if the network thread has been saturated, e.g., if there is a lot of work being scheduled to run in FDB.
     *
     * @param timer the timer to use for instrumentation
     *
     * @see #performNoOpAsync(FDBStoreTimer)
     */
    void performNoOp(@Nullable FDBStoreTimer timer);

    /**
     * Perform a no-op against FDB to check network thread liveness. This is a blocking version of {@link
     * #performNoOpAsync(Map, FDBStoreTimer)}.
     * Note that the future is expected to complete relatively quickly in most circumstances, but it may take a long
     * time if the network thread has been saturated, e.g., if there is a lot of work being scheduled to run in FDB.
     *
     * @param mdcContext logger context to set in running threads
     * @param timer the timer to use for instrumentation
     *
     * @see #performNoOpAsync(Map, FDBStoreTimer)
     */
    void performNoOp(@Nullable Map<String, String> mdcContext, @Nullable FDBStoreTimer timer);

    long getResolverStateCacheRefreshTime();

    @VisibleForTesting
    void setResolverStateRefreshTimeMillis(long resolverStateRefreshTimeMillis);

    @Nonnull
    @API(API.Status.INTERNAL)
    CompletableFuture<ResolverStateProto.State> getStateForResolver(@Nonnull LocatableResolver resolver,
                                                                    @Nonnull Supplier<CompletableFuture<ResolverStateProto.State>> loader);

    // Update lastSeenFDBVersion if readVersion is newer
    @API(API.Status.INTERNAL)
    void updateLastSeenFDBVersion(long startTime, long readVersion);

    @Nonnull
    @API(API.Status.INTERNAL)
    FDBReverseDirectoryCache getReverseDirectoryCache();

    @API(API.Status.INTERNAL)
    int getDirectoryCacheVersion();

    CacheStats getDirectoryCacheStats();

    @Nonnull
    @API(API.Status.INTERNAL)
    Cache<ScopedValue<String>, ResolverResult> getDirectoryCache(int atVersion);

    @Nonnull
    @API(API.Status.INTERNAL)
    Cache<ScopedValue<Long>, String> getReverseDirectoryInMemoryCache();

    @API(API.Status.INTERNAL)
    void clearForwardDirectoryCache();

    @VisibleForTesting
    @API(API.Status.INTERNAL)
    void clearReverseDirectoryCache();

    /**
     * Get the store state cache for this database. This cache will be used when initializing record stores associated
     * with this database.
     *
     * @return the store state cache for this database
     *
     * @see FDBRecordStoreStateCache
     */
    @Nonnull
    FDBRecordStoreStateCache getStoreStateCache();

    /**
     * Set the store state cache for this database. The provided cache will be used when initializing record stores
     * with this database. Note that the store state cache should <em>not</em> be set with a store state cache
     * that is used by a different database.
     *
     * @param storeStateCache the store state cache
     */
    void setStoreStateCache(@Nonnull FDBRecordStoreStateCache storeStateCache);

    @VisibleForTesting
    @API(API.Status.INTERNAL)
    void clearCaches();

    void close();

    @Nonnull
    Executor getExecutor();

    Executor newContextExecutor(@Nullable Map<String, String> mdcContext);

    /**
     * Create an {@link FDBDatabaseRunner} for use against this database.
     * Changes made to {@code contextConfigBuilder} subsequently will continue to be reflected in contexts opened
     * by the runner.
     *
     * @param contextConfigBuilder options for contexts opened by the new runner
     *
     * @return a new runner
     */
    @Nonnull
    FDBDatabaseRunner newRunner(@Nonnull FDBRecordContextConfig.Builder contextConfigBuilder);

    /**
     * Create an {@link FDBDatabaseRunner} for use against this database.
     *
     * @return a new runner
     */
    @Nonnull
    FDBDatabaseRunner newRunner();

    /**
     * Create an {@link FDBDatabaseRunner} for use against this database.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     *
     * @return a new runner
     */
    @Nonnull
    FDBDatabaseRunner newRunner(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext);

    /**
     * Create an {@link FDBDatabaseRunner} for use against this database.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param weakReadSemantics allowable staleness information if caching read versions
     *
     * @return a new runner
     */
    @Nonnull
    FDBDatabaseRunner newRunner(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                                @Nullable WeakReadSemantics weakReadSemantics);

    /**
     * Runs a transactional function against this <code>FDBDatabase</code> with retry logic.
     * <p>
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     *
     * @return result of function after successful run and commit
     *
     * @see #newRunner()
     * @see FDBDatabaseRunner#run
     */
    <T> T run(@Nonnull Function<? super FDBRecordContext, ? extends T> retriable);

    /**
     * Runs a transactional function against this <code>FDBDatabase</code> with retry logic.
     * <p>
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     *
     * @return result of function after successful run and commit
     *
     * @see #newRunner(FDBStoreTimer, Map)
     * @see FDBDatabaseRunner#run
     */
    <T> T run(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
              @Nonnull Function<? super FDBRecordContext, ? extends T> retriable);

    /**
     * Runs a transactional function against this <code>FDBDatabase</code> with retry logic.
     * This will run the function and commit the transaction associated with it until the
     * function either completes successfully or encounters a non-retriable error. An error
     * is considered retriable if it is a {@link RecordCoreRetriableTransactionException},
     * a retriable {@link FDBException}, or is an exception caused by a retriable error.
     * The function will not be run more than the number of times specified by
     * {@link FDBDatabaseFactory#getMaxAttempts() FDBDatabaseFactory.getMaxAttempts()}.
     * It also important that the function provided is idempotent as the function
     * may be applied multiple times successfully if the transaction commit returns
     * a <code>commit_unknown_result</code> error.
     *
     * <p>
     * If this <code>FDBDatabase</code> is configured to cache read versions, one
     * can specify that this function should use the cached version by supplying
     * a non-<code>null</code> {@link WeakReadSemantics} object to the
     * <code>weakReadSemantics</code> parameter. Each time that the function is
     * retried, the cached read version is checked again, so each retry
     * might get different read versions.
     * </p>
     *
     * <p>
     * This is a blocking call, and this function will not return until the database
     * has synchronously returned a response as to the success or failure of this
     * operation. If one wishes to achieve the same functionality in a non-blocking
     * manner, see {@link #runAsync(FDBStoreTimer, Map, WeakReadSemantics, Function) runAsync()}.
     * </p>
     * <p>
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param weakReadSemantics allowable staleness parameters if caching read versions
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     *
     * @return result of function after successful run and commit
     *
     * @see #newRunner(FDBStoreTimer, Map, WeakReadSemantics)
     * @see FDBDatabaseRunner#run
     */
    <T> T run(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext, @Nullable WeakReadSemantics weakReadSemantics,
              @Nonnull Function<? super FDBRecordContext, ? extends T> retriable);

    /**
     * Runs a transactional function asynchronously against this <code>FDBDatabase</code> with retry logic.
     * <p>
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     *
     * @return future that will contain the result of function after successful run and commit
     *
     * @see #newRunner()
     * @see FDBDatabaseRunner#runAsync
     */
    @Nonnull
    @API(API.Status.MAINTAINED)
    <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable);

    /**
     * Runs a transactional function asynchronously against this <code>FDBDatabase</code> with retry logic.
     * <p>
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param retriable the database operation to run transactionally
     * @param additionalLogMessageKeyValues key/value pairs that will be included in retry exceptions that are logged
     * @param <T> return type of function to run
     *
     * @return future that will contain the result of function after successful run and commit
     *
     * @see #newRunner()
     * @see FDBDatabaseRunner#runAsync
     */
    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    <T> CompletableFuture<T> runAsync(@Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                      @Nullable List<Object> additionalLogMessageKeyValues);

    /**
     * Runs a transactional function asynchronously against this <code>FDBDatabase</code> with retry logic.
     * <p>
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     *
     * @return future that will contain the result of function after successful run and commit
     *
     * @see #newRunner(FDBStoreTimer, Map)
     * @see FDBDatabaseRunner#runAsync
     */
    @Nonnull
    @API(API.Status.MAINTAINED)
    <T> CompletableFuture<T> runAsync(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                                      @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable);

    /**
     * Runs a transactional function asynchronously against this <code>FDBDatabase</code> with retry logic.
     * <p>
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param retriable the database operation to run transactionally
     * @param additionalLogMessageKeyValues key/value pairs that will be included in retry exceptions that are logged
     * @param <T> return type of function to run
     *
     * @return future that will contain the result of function after successful run and commit
     *
     * @see #newRunner(FDBStoreTimer, Map)
     * @see FDBDatabaseRunner#runAsync
     */
    @Nonnull
    @API(API.Status.MAINTAINED)
    <T> CompletableFuture<T> runAsync(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext,
                                      @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable,
                                      @Nullable List<Object> additionalLogMessageKeyValues);

    /**
     * Runs a transactional function asynchronously against this <code>FDBDatabase</code> with retry logic.
     * This will run the function and commit the transaction associated with it until the
     * function either completes successfully or encounters a non-retriable error. An error
     * is considered retriable if it is a {@link RecordCoreRetriableTransactionException},
     * a retriable {@link FDBException}, or is an exception caused by a retriable error.
     * The function will not be run more than the number of times specified by
     * {@link FDBDatabaseFactory#getMaxAttempts() FDBDatabaseFactory.getMaxAttempts()}.
     * It also important that the function provided is idempotent as the function
     * may be applied multiple times successfully if the transaction commit returns
     * a <code>commit_uknown_result</code> error.
     *
     * <p>
     * If this <code>FDBDatabase</code> is configured to cache read versions, one
     * can specify that this function should use the cached version by supplying
     * a non-<code>null</code> {@link WeakReadSemantics} object to the
     * <code>weakReadSemantics</code> parameter. Each time that the function is
     * retried, the cached read version is checked again, so each retry
     * might get different read versions.
     * </p>
     *
     * <p>
     * This is a non-blocking call, and this function will return immediately with
     * a future that will not be ready until the database has returned a response as
     * to the success or failure of this operation. If one wishes to achieve the same
     * functionality in a blocking manner, see {@link #run(FDBStoreTimer, Map, WeakReadSemantics, Function) run()}.
     * </p>
     * <p>
     * This creates a new {@link FDBDatabaseRunner runner} and closes it when complete.
     * To better control the lifetime / sharing of the runner, create it separately.
     *
     * @param timer the timer to use for instrumentation
     * @param mdcContext logger context to set in running threads
     * @param weakReadSemantics allowable staleness information if caching read versions
     * @param retriable the database operation to run transactionally
     * @param <T> return type of function to run
     *
     * @return future that will contain the result of function after successful run and commit
     *
     * @see #newRunner(FDBStoreTimer, Map, WeakReadSemantics)
     * @see FDBDatabaseRunner#runAsync
     */
    @Nonnull
    @API(API.Status.MAINTAINED)
    <T> CompletableFuture<T> runAsync(@Nullable FDBStoreTimer timer, @Nullable Map<String, String> mdcContext, @Nullable WeakReadSemantics weakReadSemantics,
                                      @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> retriable);

    boolean hasAsyncToSyncTimeout();

    @Nullable
    Pair<Long, TimeUnit> getAsyncToSyncTimeout(FDBStoreTimer.Wait event);

    @Nullable
    Function<FDBStoreTimer.Wait, Pair<Long, TimeUnit>> getAsyncToSyncTimeout();

    void setAsyncToSyncTimeout(@Nullable Function<FDBStoreTimer.Wait, Pair<Long, TimeUnit>> asyncToSyncTimeout);

    void setAsyncToSyncTimeout(long asyncToSyncTimeout, @Nonnull TimeUnit asyncToSyncTimeoutUnit);

    void clearAsyncToSyncTimeout();

    void setAsyncToSyncExceptionMapper(@Nonnull ExceptionMapper asyncToSyncExceptionMapper);

    RuntimeException mapAsyncToSyncException(@Nonnull Throwable ex);

    @Nullable
    <T> T asyncToSync(@Nullable FDBStoreTimer timer, FDBStoreTimer.Wait event, @Nonnull CompletableFuture<T> async);

    /**
     * Join a future, following the same logic that <code>asyncToSync()</code> uses to validate that the operation
     * isn't blocking in an asynchronous context.
     *
     * @param future the future to be completed
     * @param <T> the type of the value produced by the future
     *
     * @return the result value
     */
    <T> T join(CompletableFuture<T> future);

    /**
     * Join a future but validate that the future is already completed. This can be used to unwrap a completed
     * future while allowing for bugs caused by inadvertently waiting on incomplete futures to be caught.
     * In particular, this will throw an exception if the {@link BlockingInAsyncDetection} behavior is set
     * to throw an exception on incomplete futures and otherwise just log that future was waited on.
     *
     * @param future the future that should already be completed
     * @param <T> the type of the value produced by the future
     *
     * @return the result value
     */
    <T> T joinNow(CompletableFuture<T> future);

    /**
     * Get a future, following the same logic that <code>asyncToSync()</code> uses to validate that the operation
     * isn't blocking in an asynchronous context.
     *
     * @param future the future to be completed
     * @param <T> the type of the value produced by the future
     *
     * @return the result value
     *
     * @throws java.util.concurrent.CancellationException if the future was cancelled
     * @throws ExecutionException if the future completed exceptionally
     * @throws InterruptedException if the current thread was interrupted
     */
    <T> T get(CompletableFuture<T> future) throws InterruptedException, ExecutionException;

    /**
     * Log warning and close tracked contexts that have been open for too long.
     *
     * @param minAgeSeconds number of seconds above which to warn
     *
     * @return number of such contexts found
     */
    @VisibleForTesting
    int warnAndCloseOldTrackedOpenContexts(long minAgeSeconds);

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    void untrackOpenContext(FDBRecordContext context);

    @API(API.Status.INTERNAL)
    BlockingInAsyncDetection getBlockingInAsyncDetection();

    /**
     * Given a specific FDB API call, computes an amount of latency (in milliseconds) that should be injected
     * prior to performing the actual operation.  This latency is computed via an installed latency injector
     * via {@link FDBDatabaseFactory#setLatencyInjector(Function)}.
     *
     * @param fdbLatencySource the call for which the latency is to be computed
     * @return the amount of delay, in milliseconds, to inject for the provided operation
     */
    long getLatencyToInject(FDBLatencySource fdbLatencySource);

    CompletableFuture<Tuple> loadBoundaryKeys(@Nonnull FDBTransactionContext context, Tuple key);

    /**
     * Function for mapping an underlying exception to a synchronous failure.
     * <p>
     * It is possible for this function to be called with the result of calling it previously.
     * Therefore, if wrapping exceptions with some application-specific exception class, it is best
     * to check for being passed an {@code ex} that is already of that class and in that case just return it.
     *
     * @see #setAsyncToSyncExceptionMapper
     * @see #asyncToSync
     * @see FDBExceptions#wrapException(Throwable)
     */
    @FunctionalInterface
    interface ExceptionMapper {
        RuntimeException apply(@Nonnull Throwable ex, @Nullable StoreTimer.Event event);
    }

    /**
     * 1. Bounds for stale reads; and 2. indication whether FDB's strict causal consistency guarantee isn't required.
     *
     * <p>
     * Stale reads never cause inconsistency <em>within</em> the database. If something had changed beforehand but
     * was not seen, the commit will conflict, while it would have succeeded if the read has been current.
     * </p>
     *
     * <p>
     * It is still possible to have inconsistency <em>external</em> to the database. For example, if something writes
     * a record and then sends you the id of that record via some means other than in the same database, you might
     * receive it but then access the database from before it committed and miss what they wanted you to see.
     * </p>
     *
     * <p>
     * Setting the causal read risky flag leads to a lower latency at the cost of weaker semantics in case of failures.
     * The read version of the transaction will be a committed version, and usually will be the latest committed,
     * but it might be an older version in the event of a fault on network partition.
     * </p>
     */
    class WeakReadSemantics {
        // Minimum version at which the read should be performed (usually the last version seen by the client)
        private long minVersion;

        // How stale a cached read version can be
        private long stalenessBoundMillis;

        // Whether the transaction should be set with a causal read risky flag.
        private boolean isCausalReadRisky;

        public WeakReadSemantics(long minVersion, long stalenessBoundMillis, boolean isCausalReadRisky) {
            this.minVersion = minVersion;
            this.stalenessBoundMillis = stalenessBoundMillis;
            this.isCausalReadRisky = isCausalReadRisky;
        }

        public long getMinVersion() {
            return minVersion;
        }

        public long getStalenessBoundMillis() {
            return stalenessBoundMillis;
        }

        public boolean isCausalReadRisky() {
            return isCausalReadRisky;
        }
    }
}

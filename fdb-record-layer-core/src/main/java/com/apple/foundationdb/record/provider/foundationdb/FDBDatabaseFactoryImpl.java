/*
 * FDBDatabaseFactory.java
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

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCacheFactory;
import com.apple.foundationdb.record.provider.foundationdb.storestate.PassThroughRecordStoreStateCacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A singleton maintaining a list of {@link FDBDatabaseImpl} instances, indexed by their cluster file location.
 */
@API(API.Status.STABLE)
public class FDBDatabaseFactoryImpl implements FDBDatabaseFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseFactoryImpl.class);

    protected static final Function<FDBLatencySource, Long> DEFAULT_LATENCY_INJECTOR = api -> 0L;

    private static final int API_VERSION = 630;

    @Nonnull
    private static final FDBDatabaseFactoryImpl INSTANCE = new FDBDatabaseFactoryImpl();
    // when set to true, static options have been set on the FDB instance.
    //made volatile because multiple FDBDatabaseFactory instances can be created technically, and thus can be racy during init
    private static volatile boolean staticOptionsSet = false;

    @Nonnull
    private FDBLocalityProvider localityProvider = FDBLocalityUtil.instance();

    /* Next few null until initFDB is called */

    @Nullable
    private Executor networkExecutor = null;

    @Nonnull
    private Executor executor = ForkJoinPool.commonPool();

    @Nonnull
    private Function<Executor, Executor> contextExecutor = Function.identity();

    @Nullable
    private FDB fdb;
    private boolean inited;

    private boolean unclosedWarning = true;
    @Nullable
    private String traceDirectory = null;
    @Nullable
    private String traceLogGroup = null;
    @Nonnull
    private FDBTraceFormat traceFormat = FDBTraceFormat.DEFAULT;
    private int directoryCacheSize = DEFAULT_DIRECTORY_CACHE_SIZE;
    private boolean trackLastSeenVersion;
    private String datacenterId;

    private int maxAttempts = 10;
    private long maxDelayMillis = 1000;
    private long initialDelayMillis = 10;
    private int reverseDirectoryRowsPerTransaction = FDBReverseDirectoryCache.MAX_ROWS_PER_TRANSACTION;
    private long reverseDirectoryMaxMillisPerTransaction = FDBReverseDirectoryCache.MAX_MILLIS_PER_TRANSACTION;
    private long stateRefreshTimeMillis = TimeUnit.SECONDS.toMillis(FDBDatabase.DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS);
    private long transactionTimeoutMillis = DEFAULT_TR_TIMEOUT_MILLIS;
    private boolean runLoopProfilingEnabled;

    //made volatile because multiple FDBDatabaseFactory instances can be created technically, and thus can be racy during init
    private static volatile int threadsPerClientVersion = 1; //default is 1, which is basically disabled

    /**
     * The default is a log-based predicate, which can also be used to enable tracing on a more granular level
     * (such as by request) using {@link #setTransactionIsTracedSupplier(Supplier)}.
     */
    @Nonnull
    private Supplier<Boolean> transactionIsTracedSupplier = LOGGER::isTraceEnabled;
    private long warnAndCloseOpenContextsAfterSeconds;
    @Nonnull
    private Supplier<BlockingInAsyncDetection> blockingInAsyncDetectionSupplier = () -> BlockingInAsyncDetection.DISABLED;
    @Nonnull
    private FDBRecordStoreStateCacheFactory storeStateCacheFactory = PassThroughRecordStoreStateCacheFactory.instance();

    @Nonnull
    private Function<FDBLatencySource, Long> latencyInjector = DEFAULT_LATENCY_INJECTOR;

    private final Map<String, FDBDatabaseImpl> databases = new HashMap<>();

    @Nonnull
    public static FDBDatabaseFactoryImpl instance() {
        return FDBDatabaseFactoryImpl.INSTANCE;
    }

    protected synchronized FDB initFDB() {
        if (!inited) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of("Starting FDB"));
            }
            fdb = FDB.selectAPIVersion(API_VERSION);
            fdb.setUnclosedWarning(unclosedWarning);
            setStaticOptions(fdb);
            NetworkOptions options = fdb.options();
            if (!traceFormat.isDefaultValue()) {
                options.setTraceFormat(traceFormat.getOptionValue());
            }
            if (traceDirectory != null) {
                options.setTraceEnable(traceDirectory);
            }
            if (traceLogGroup != null) {
                options.setTraceLogGroup(traceLogGroup);
            }
            if (runLoopProfilingEnabled) {
                options.setEnableRunLoopProfiling();
            }
            if (networkExecutor == null) {
                fdb.startNetwork();
            } else {
                fdb.startNetwork(networkExecutor);
            }
            inited = true;
        }
        return fdb;
    }

    /**
     * Set the number of threads per FDB client version. The default value is 1.
     *
     * @param threadsPerClientV the number of threads per client version. Cannot be less than 1
     */
    static void setThreadsPerClientVersion(int threadsPerClientV) {
        if (FDBDatabaseFactoryImpl.staticOptionsSet) {
            throw new RecordCoreException("threads per client version cannot be changed as the version has already been initiated");
        }
        if (threadsPerClientV < 1) {
            //if the thread count is too low, disable the setting
            threadsPerClientV = 1;
        }
        FDBDatabaseFactoryImpl.threadsPerClientVersion = threadsPerClientV;
    }

    static int getThreadsPerClientVersion() {
        return FDBDatabaseFactoryImpl.threadsPerClientVersion;
    }

    private static synchronized void setStaticOptions(final FDB fdb) {
        /*
         * There are a few FDB settings that have to be set statically, but also need to have room
         * for configuration. For the most part, FDBDatabaseFactory is a singleton and so in _theory_ this shouldn't
         * matter. However, in practice it is possible to create multiple factories(i.e. in test code and such),
         * and doing so may cause problems with these settings (errors thrown, that kind of thing). To avoid that,
         * we have to follow a somewhat goofy pattern of making those settings static, and checking to ensure that
         * we only set those options once. This block of code does that.
         *
         * Note that this method is synchronized on the class; this is so that multiple concurrent attempts to
         * init an FDBDatabaseFactory won't cause this function to fail halfway through.
         */
        if (!staticOptionsSet) {
            fdb.options().setClientThreadsPerVersion(threadsPerClientVersion);

            staticOptionsSet = true;
        }
    }

    @Override
    @Nonnull
    public Executor getNetworkExecutor() {
        return networkExecutor;
    }

    @Override
    public void setNetworkExecutor(@Nonnull Executor networkExecutor) {
        this.networkExecutor = networkExecutor;
    }

    @Override
    @Nonnull
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public void setExecutor(@Nonnull Executor executor) {
        this.executor = executor;
    }

    @Override
    public void setContextExecutor(@Nonnull Function<Executor, Executor> contextExecutor) {
        this.contextExecutor = contextExecutor;
    }

    /**
     * Creates a new {@code Executor} for use by a specific {@code FDBRecordContext}. If {@code mdcContext}
     * is not {@code null}, the executor will ensure that the provided MDC present within the context of the
     * executor thread.
     *
     * @param mdcContext if present, the MDC context to be made available within the executors threads
     * @return a new executor to be used by a {@code FDBRecordContext}
     */
    @Nonnull
    protected Executor newContextExecutor(@Nullable Map<String, String> mdcContext) {
        Executor newExecutor = contextExecutor.apply(getExecutor());
        if (mdcContext != null) {
            newExecutor = new ContextRestoringExecutor(newExecutor, mdcContext);
        }
        return newExecutor;
    }

    @Override
    public synchronized void shutdown() {
        if (inited) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of("Shutting down FDB"));
            }
            for (FDBDatabase database : databases.values()) {
                database.close();
            }
            // TODO: Does this do the right thing yet?
            fdb.stopNetwork();
            inited = false;
        }
    }

    @Override
    public synchronized void clear() {
        for (FDBDatabase database : databases.values()) {
            database.close();
        }
        databases.clear();
    }

    @Override
    public boolean isUnclosedWarning() {
        return unclosedWarning;
    }

    @Override
    public void setUnclosedWarning(boolean unclosedWarning) {
        this.unclosedWarning = unclosedWarning;
    }

    @Override
    @SpotBugsSuppressWarnings("IS2_INCONSISTENT_SYNC")
    public void setTrace(@Nullable String traceDirectory, @Nullable String traceLogGroup) {
        this.traceDirectory = traceDirectory;
        this.traceLogGroup = traceLogGroup;
    }

    @Override
    public void setTraceFormat(@Nonnull FDBTraceFormat traceFormat) {
        this.traceFormat = traceFormat;
    }

    @Override
    public synchronized int getDirectoryCacheSize() {
        return directoryCacheSize;
    }

    @Override
    @SuppressWarnings("PMD.BooleanGetMethodName")
    public synchronized boolean getTrackLastSeenVersion() {
        return trackLastSeenVersion;
    }

    @Override
    public synchronized String getDatacenterId() {
        return datacenterId;
    }

    @Override
    public synchronized void setDirectoryCacheSize(int directoryCacheSize) {
        this.directoryCacheSize = directoryCacheSize;
        for (FDBDatabase database : databases.values()) {
            database.setDirectoryCacheSize(directoryCacheSize);
        }
    }

    @Override
    public synchronized void setTrackLastSeenVersion(boolean trackLastSeenVersion) {
        this.trackLastSeenVersion = trackLastSeenVersion;
        for (FDBDatabase database : databases.values()) {
            database.setTrackLastSeenVersion(trackLastSeenVersion);
        }
    }

    @Override
    public synchronized void setDatacenterId(String datacenterId) {
        this.datacenterId = datacenterId;
        for (FDBDatabase database : databases.values()) {
            database.setDatacenterId(datacenterId);
        }
    }

    @Override
    public synchronized void setRunLoopProfilingEnabled(boolean runLoopProfilingEnabled) {
        if (inited) {
            throw new RecordCoreException("run loop profiling can not be enabled as the client has already started");
        }
        this.runLoopProfilingEnabled = runLoopProfilingEnabled;
    }

    @Override
    public boolean isRunLoopProfilingEnabled() {
        return runLoopProfilingEnabled;
    }

    @Override
    public int getMaxAttempts() {
        return maxAttempts;
    }

    @Override
    public void setMaxAttempts(int maxAttempts) {
        if (maxAttempts <= 0) {
            throw new RecordCoreException("Cannot set maximum number of attempts to less than or equal to zero");
        }
        this.maxAttempts = maxAttempts;
    }

    @Override
    public long getMaxDelayMillis() {
        return maxDelayMillis;
    }

    @Override
    public void setMaxDelayMillis(long maxDelayMillis) {
        if (maxDelayMillis < 0) {
            throw new RecordCoreException("Cannot set maximum delay milliseconds to less than or equal to zero");
        } else if (maxDelayMillis < initialDelayMillis) {
            throw new RecordCoreException("Cannot set maximum delay to less than minimum delay");
        }
        this.maxDelayMillis = maxDelayMillis;
    }

    @Override
    public long getInitialDelayMillis() {
        return initialDelayMillis;
    }

    @Override
    public void setInitialDelayMillis(long initialDelayMillis) {
        if (initialDelayMillis < 0) {
            throw new RecordCoreException("Cannot set initial delay milleseconds to less than zero");
        } else if (initialDelayMillis > maxDelayMillis) {
            throw new RecordCoreException("Cannot set initial delay to greater than maximum delay");
        }
        this.initialDelayMillis = initialDelayMillis;
    }

    @Override
    public void setReverseDirectoryRowsPerTransaction(int rowsPerTransaction) {
        this.reverseDirectoryRowsPerTransaction = rowsPerTransaction;
    }

    @Override
    public int getReverseDirectoryRowsPerTransaction() {
        return reverseDirectoryRowsPerTransaction;
    }

    @Override
    public void setReverseDirectoryMaxMillisPerTransaction(long millisPerTransaction) {
        this.reverseDirectoryMaxMillisPerTransaction = millisPerTransaction;
    }

    @Override
    public long getReverseDirectoryMaxMillisPerTransaction() {
        return reverseDirectoryMaxMillisPerTransaction;
    }

    // TODO: Demote these to UNSTABLE and deprecate at some point.

    @Override
    public void setTransactionIsTracedSupplier(Supplier<Boolean> transactionIsTracedSupplier) {
        this.transactionIsTracedSupplier = transactionIsTracedSupplier;
    }

    @Override
    public Supplier<Boolean> getTransactionIsTracedSupplier() {
        return transactionIsTracedSupplier;
    }

    @Override
    public long getWarnAndCloseOpenContextsAfterSeconds() {
        return warnAndCloseOpenContextsAfterSeconds;
    }

    @Override
    public void setWarnAndCloseOpenContextsAfterSeconds(long warnAndCloseOpenContextsAfterSeconds) {
        this.warnAndCloseOpenContextsAfterSeconds = warnAndCloseOpenContextsAfterSeconds;
    }

    @Override
    public void setBlockingInAsyncDetection(@Nonnull BlockingInAsyncDetection behavior) {
        setBlockingInAsyncDetection(() -> behavior);
    }

    @Override
    public void setBlockingInAsyncDetection(@Nonnull Supplier<BlockingInAsyncDetection> supplier) {
        this.blockingInAsyncDetectionSupplier = supplier;
    }

    @Nonnull
    protected Supplier<BlockingInAsyncDetection> getBlockingInAsyncDetectionSupplier() {
        return this.blockingInAsyncDetectionSupplier;
    }

    @Override
    public void setLatencyInjector(@Nonnull Function<FDBLatencySource, Long> latencyInjector) {
        this.latencyInjector = latencyInjector;
    }

    @Override
    public Function<FDBLatencySource, Long> getLatencyInjector() {
        return latencyInjector;
    }

    @Override
    public void clearLatencyInjector() {
        this.latencyInjector = DEFAULT_LATENCY_INJECTOR;
    }

    @Override
    public long getStateRefreshTimeMillis() {
        return stateRefreshTimeMillis;
    }

    @Override
    public void setStateRefreshTimeMillis(long stateRefreshTimeMillis) {
        this.stateRefreshTimeMillis = stateRefreshTimeMillis;
    }

    @Override
    public void setTransactionTimeoutMillis(long transactionTimeoutMillis) {
        if (transactionTimeoutMillis < DEFAULT_TR_TIMEOUT_MILLIS) {
            throw new RecordCoreArgumentException("cannot set transaction timeout millis to " + transactionTimeoutMillis);
        }
        this.transactionTimeoutMillis = transactionTimeoutMillis;
    }

    @Override
    public long getTransactionTimeoutMillis() {
        return transactionTimeoutMillis;
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    public FDBRecordStoreStateCacheFactory getStoreStateCacheFactory() {
        return storeStateCacheFactory;
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public void setStoreStateCacheFactory(@Nonnull FDBRecordStoreStateCacheFactory storeStateCacheFactory) {
        this.storeStateCacheFactory = storeStateCacheFactory;
    }

    @Override
    @Nonnull
    public synchronized FDBDatabaseImpl getDatabase(@Nullable String clusterFile) {
        FDBDatabaseImpl database = databases.get(clusterFile);
        if (database == null) {
            database = new FDBDatabaseImpl(this, clusterFile);
            database.setDirectoryCacheSize(getDirectoryCacheSize());
            database.setTrackLastSeenVersion(getTrackLastSeenVersion());
            database.setResolverStateRefreshTimeMillis(getStateRefreshTimeMillis());
            database.setDatacenterId(getDatacenterId());
            database.setStoreStateCache(storeStateCacheFactory.getCache(database));
            databases.put(clusterFile, database);
        }
        return database;
    }

    @Override
    @Nonnull
    public synchronized FDBDatabaseImpl getDatabase() {
        return getDatabase(null);
    }

    @Override
    @Nonnull
    public FDBLocalityProvider getLocalityProvider() {
        return localityProvider;
    }

    @Override
    public void setLocalityProvider(@Nonnull FDBLocalityProvider localityProvider) {
        this.localityProvider = localityProvider;
    }
}

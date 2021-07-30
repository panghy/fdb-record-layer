/*
 * FDBDatabaseFactory.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.storestate.FDBRecordStoreStateCacheFactory;
import com.apple.foundationdb.record.provider.foundationdb.storestate.PassThroughRecordStoreStateCacheFactory;

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
 * Abstract class for {@code FDBDatabaseFactory} with most boiler-plate code extracted.
 */
public abstract class FDBDatabaseFactoryBase implements FDBDatabaseFactory {

    protected static final Function<FDBLatencySource, Long> DEFAULT_LATENCY_INJECTOR = api -> 0L;

    protected final Map<String, FDBDatabaseImpl> databases = new HashMap<>();

    @Nullable
    protected volatile Executor networkExecutor = null;
    @Nonnull
    protected Function<Executor, Executor> contextExecutor = Function.identity();
    protected boolean unclosedWarning = true;
    @Nonnull
    protected Supplier<BlockingInAsyncDetection> blockingInAsyncDetectionSupplier = () -> BlockingInAsyncDetection.DISABLED;
    @Nonnull
    protected FDBRecordStoreStateCacheFactory storeStateCacheFactory = PassThroughRecordStoreStateCacheFactory.instance();
    @Nonnull
    private Executor executor = ForkJoinPool.commonPool();
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
    private long warnAndCloseOpenContextsAfterSeconds;

    @Nonnull
    private Function<FDBLatencySource, Long> latencyInjector = DEFAULT_LATENCY_INJECTOR;

    @Override
    @Nullable
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

    @Override
    public void setLatencyInjector(@Nonnull Function<FDBLatencySource, Long> latencyInjector) {
        this.latencyInjector = latencyInjector;
    }

    @Nonnull
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
    public Executor newContextExecutor(@Nullable Map<String, String> mdcContext) {
        Executor newExecutor = contextExecutor.apply(getExecutor());
        if (mdcContext != null) {
            newExecutor = new ContextRestoringExecutor(newExecutor, mdcContext);
        }
        return newExecutor;
    }

    @Override
    @Nonnull
    public Supplier<BlockingInAsyncDetection> getBlockingInAsyncDetectionSupplier() {
        return this.blockingInAsyncDetectionSupplier;
    }
}

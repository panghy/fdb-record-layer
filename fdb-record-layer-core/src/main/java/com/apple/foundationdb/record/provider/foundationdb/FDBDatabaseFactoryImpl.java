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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.annotation.API;
import io.github.panghy.lionrock.client.foundationdb.RemoteFoundationDBDatabaseFactory;
import io.github.panghy.lionrock.foundationdb.record.RemoteFDBLocalityProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * A singleton maintaining a list of {@link FDBDatabaseImpl} instances, indexed by their cluster file location.
 */
@API(API.Status.STABLE)
public class FDBDatabaseFactoryImpl extends FDBDatabaseFactoryBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseFactoryImpl.class);
    @Nonnull
    private static final FDBDatabaseFactoryImpl INSTANCE = new FDBDatabaseFactoryImpl(
            ManagedChannelBuilder.forAddress("localhost", 6565)
                    .maxInboundMessageSize(32_000_000)
                    .usePlaintext().build(), "fdb-record-layer");

    private final ManagedChannel channel;
    private final String clientIdentifier;
    /**
     * The default is a log-based predicate, which can also be used to enable tracing on a more granular level
     * (such as by request) using {@link #setTransactionIsTracedSupplier(Supplier)}.
     */
    @Nonnull
    private Supplier<Boolean> transactionIsTracedSupplier = LOGGER::isTraceEnabled;

    @Nonnull
    private FDBLocalityProvider localityProvider = RemoteFDBLocalityProvider.instance();

    public static FDBDatabaseFactoryImpl instance() {
        return FDBDatabaseFactoryImpl.INSTANCE;
    }

    /**
     * Create a new {@link FDBDatabaseFactoryImpl}.
     *
     * @param channel The gRPC {@link ManagedChannel} to talk to the backend.
     * @param clientIdentifier The client identifier when communicating with the server.
     */
    public FDBDatabaseFactoryImpl(ManagedChannel channel, String clientIdentifier) {
        this.channel = channel;
        this.clientIdentifier = clientIdentifier;
    }

    @Override
    public void shutdown() {
        channel.shutdown();
    }

    @Override
    public void setTrace(@Nullable String s, @Nullable String s1) {
    }

    @Override
    public void setTraceFormat(@Nonnull FDBTraceFormat fdbTraceFormat) {
    }

    @Override
    public void setRunLoopProfilingEnabled(boolean b) {
    }

    @Override
    public boolean isRunLoopProfilingEnabled() {
        return false;
    }

    @Override
    public void setTransactionIsTracedSupplier(Supplier<Boolean> transactionIsTracedSupplier) {
        this.transactionIsTracedSupplier = transactionIsTracedSupplier;
    }

    @Override
    public Supplier<Boolean> getTransactionIsTracedSupplier() {
        return transactionIsTracedSupplier;
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
    public synchronized FDBDatabase getDatabase() {
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

    @Nonnull
    @Override
    public Database open(String databaseName) {
        if (databaseName == null) {
            databaseName = "fdb";
        }
        return RemoteFoundationDBDatabaseFactory.open(databaseName, clientIdentifier, channel);
    }
}

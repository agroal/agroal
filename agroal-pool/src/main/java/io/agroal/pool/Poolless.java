// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.transaction.TransactionIntegration;
import io.agroal.pool.MetricsRepository.EmptyMetricsRepository;
import io.agroal.pool.util.AgroalSynchronizer;
import io.agroal.pool.util.StampedCopyOnWriteArrayList;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;

import static io.agroal.api.AgroalDataSource.FlushMode.ALL;
import static io.agroal.pool.ConnectionHandler.State.CHECKED_OUT;
import static io.agroal.pool.ConnectionHandler.State.DESTROYED;
import static io.agroal.pool.ConnectionHandler.State.FLUSH;
import static io.agroal.pool.util.ListenerHelper.*;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;

/**
 * Alternative implementation of ConnectionPool for the special case of flush-on-close (and min-size == 0)
 * In particular, this removes the need for the executor. Also there is no thread-local connection cache as connections are not reused
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class Poolless implements Pool {

    private final AgroalConnectionPoolConfiguration configuration;
    private final AgroalDataSourceListener[] listeners;

    private final StampedCopyOnWriteArrayList<ConnectionHandler> allConnections;

    private final AgroalSynchronizer synchronizer;
    private final ConnectionFactory connectionFactory;
    private final TransactionIntegration transactionIntegration;

    private final LongAccumulator maxUsed = new LongAccumulator( Math::max, Long.MIN_VALUE );
    private final AtomicInteger activeCount = new AtomicInteger();

    private MetricsRepository metricsRepository;
    private volatile boolean shutdown;

    public Poolless(AgroalConnectionPoolConfiguration configuration, AgroalDataSourceListener... listeners) {
        this.configuration = configuration;
        this.listeners = listeners;

        allConnections = new StampedCopyOnWriteArrayList<>( ConnectionHandler.class );

        synchronizer = new AgroalSynchronizer();
        connectionFactory = new ConnectionFactory( configuration.connectionFactoryConfiguration(), listeners );
        transactionIntegration = configuration.transactionIntegration();
    }

    public void init() {
        if ( !configuration.maxLifetime().isZero() ) {
            fireOnWarning( listeners, "Max lifetime not supported in pool-less mode" );
        }
        if ( !configuration.idleValidationTimeout().isZero() ) {
            fireOnWarning( listeners, "Idle validation not supported in pool-less mode" );
        }
        if ( !configuration.leakTimeout().isZero() ) {
            fireOnWarning( listeners, "Leak detection not supported in pool-less mode" );
        }
        if ( !configuration.reapTimeout().isZero() ) {
            fireOnWarning( listeners, "Connection reap not supported in pool-less mode" );
        }
        if ( configuration.initialSize() != 0 ) {
            fireOnWarning( listeners, "Initial size is zero in pool-less mode" );
        }
        if ( configuration.minSize() != 0 ) {
            fireOnWarning( listeners, "Min size always zero in pool-less mode" );
        }

        transactionIntegration.addResourceRecoveryFactory( connectionFactory );
    }

    public AgroalConnectionPoolConfiguration getConfiguration() {
        return configuration;
    }

    public AgroalDataSourceListener[] getListeners() {
        return listeners;
    }

    // --- //

    @Override
    public void close() {
        transactionIntegration.removeResourceRecoveryFactory( connectionFactory );
        shutdown = true;

        for ( ConnectionHandler handler : allConnections ) {
            handler.setState( FLUSH );
            destroyConnection( handler );
        }
        allConnections.clear();

        synchronizer.release( synchronizer.getQueueLength() );
    }

    // --- //

    public Connection getConnection() throws SQLException {
        fireBeforeConnectionAcquire( listeners );
        long metricsStamp = metricsRepository.beforeConnectionAcquire();

        if ( shutdown ) {
            throw new SQLException( "This pool is closed and does not handle any more connections!" );
        }

        ConnectionHandler checkedOutHandler = handlerFromTransaction();
        if ( checkedOutHandler == null ) {
            checkedOutHandler = handlerFromSharedCache();
        }

        metricsRepository.afterConnectionAcquire( metricsStamp );
        fireOnConnectionAcquired( listeners, checkedOutHandler );

        transactionIntegration.associate( checkedOutHandler, checkedOutHandler.getXaResource() );
        return checkedOutHandler.newConnectionWrapper();
    }

    private ConnectionHandler handlerFromTransaction() throws SQLException {
        return (ConnectionHandler) transactionIntegration.getTransactionAware();
    }

    private ConnectionHandler handlerFromSharedCache() throws SQLException {
        long remaining = configuration.acquisitionTimeout().toNanos();
        remaining = remaining > 0 ? remaining : Long.MAX_VALUE;
        try {
            for ( ; ; ) {
                // Try to get a "token" to create a new connection
                if ( activeCount.incrementAndGet() <= configuration.maxSize() ) {
                    return createConnection();
                } else {
                    activeCount.decrementAndGet();
                }

                // Pool full, will have to wait for a connection to be returned
                long synchronizationStamp = synchronizer.getStamp();
                long start = nanoTime();
                if ( remaining < 0 || !synchronizer.tryAcquireNanos( synchronizationStamp, remaining ) ) {
                    throw new SQLException( "Sorry, acquisition timeout!" );
                }
                if ( shutdown ) {
                    throw new SQLException( "Can't create new connection as the pool is shutting down" );
                }
                remaining -= nanoTime() - start;
            }
        } catch ( InterruptedException e ) {
            currentThread().interrupt();
            throw new SQLException( "Interrupted while acquiring" );
        }
    }

    // --- //

    public void returnConnectionHandler(ConnectionHandler handler) throws SQLException {
        fireBeforeConnectionReturn( listeners, handler );
        if ( transactionIntegration.disassociate( handler ) ) {
            activeCount.decrementAndGet();
            synchronizer.releaseConditional();

            flushHandler( handler );
        }
    }

    // --- Exposed statistics //

    @Override
    public void onMetricsEnabled(boolean metricsEnabled) {
        setMetricsRepository( metricsEnabled ? new DefaultMetricsRepository( this ) : new EmptyMetricsRepository() );
    }

    public MetricsRepository getMetrics() {
        return metricsRepository;
    }

    public void setMetricsRepository(MetricsRepository metricsRepository) {
        this.metricsRepository = metricsRepository;
    }

    public long activeCount() {
        return allConnections.size();
    }

    public long availableCount() {
        return 0;
    }

    public long maxUsedCount() {
        return maxUsed.get();
    }

    public void resetMaxUsedCount() {
        maxUsed.reset();
    }

    public long awaitingCount() {
        return synchronizer.getQueueLength();
    }

    // --- create //

    public ConnectionHandler createConnection() throws SQLException {
        fireBeforeConnectionCreation( listeners );
        long metricsStamp = metricsRepository.beforeConnectionCreation();

        try {
            ConnectionHandler handler = new ConnectionHandler( connectionFactory.createConnection(), this );

            fireOnConnectionCreation( listeners, handler );
            metricsRepository.afterConnectionCreation( metricsStamp );

            handler.setState( CHECKED_OUT );
            allConnections.add( handler );

            maxUsed.accumulate( allConnections.size() );
            fireOnConnectionPooled( listeners, handler );

            return handler;
        } catch ( SQLException e ) {
            fireOnWarning( listeners, e );
            throw e;
        }
    }

    // --- flush //

    public void flushPool(AgroalDataSource.FlushMode mode) {
        if ( mode == ALL ) {
            for ( ConnectionHandler handler : allConnections ) {
                fireBeforeConnectionFlush( listeners, handler );
                flushHandler( handler );
            }
        }
    }

    public void flushHandler(ConnectionHandler handler) {
        handler.setState( FLUSH );
        allConnections.remove( handler );
        metricsRepository.afterConnectionFlush();
        fireOnConnectionFlush( listeners, handler );
        destroyConnection( handler );
    }

    // --- destroy //

    public void destroyConnection(ConnectionHandler handler) {
        fireBeforeConnectionDestroy( listeners, handler );
        try {
            handler.closeConnection();
        } catch ( SQLException e ) {
            fireOnWarning( listeners, e );
        }
        handler.setState( DESTROYED );
        metricsRepository.afterConnectionDestroy();
        fireOnConnectionDestroy( listeners, handler );
    }
}

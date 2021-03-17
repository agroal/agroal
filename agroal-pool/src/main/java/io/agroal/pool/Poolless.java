// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalPoolInterceptor;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.transaction.TransactionIntegration;
import io.agroal.pool.MetricsRepository.EmptyMetricsRepository;
import io.agroal.pool.util.AgroalSynchronizer;
import io.agroal.pool.util.StampedCopyOnWriteArrayList;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Function;

import static io.agroal.api.AgroalDataSource.FlushMode.ALL;
import static io.agroal.api.AgroalDataSource.FlushMode.LEAK;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.MultipleAcquisitionAction.OFF;
import static io.agroal.pool.ConnectionHandler.State.CHECKED_OUT;
import static io.agroal.pool.ConnectionHandler.State.DESTROYED;
import static io.agroal.pool.ConnectionHandler.State.FLUSH;
import static io.agroal.pool.util.InterceptorHelper.fireOnConnectionAcquiredInterceptor;
import static io.agroal.pool.util.InterceptorHelper.fireOnConnectionReturnInterceptor;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionAcquire;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionCreation;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionDestroy;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionFlush;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionReturn;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionAcquired;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionCreation;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionDestroy;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionFlush;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionPooled;
import static io.agroal.pool.util.ListenerHelper.fireOnInfo;
import static io.agroal.pool.util.ListenerHelper.fireOnWarning;
import static java.lang.Integer.toHexString;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.copyOfRange;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

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

    private List<AgroalPoolInterceptor> interceptors;
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
            fireOnInfo( listeners, "Max lifetime not supported in pool-less mode" );
        }
        if ( !configuration.idleValidationTimeout().isZero() ) {
            fireOnInfo( listeners, "Idle validation not supported in pool-less mode" );
        }
        if ( !configuration.leakTimeout().isZero() ) {
            fireOnInfo( listeners, "Leak detection not pro-active in pool-less mode" );
        }
        if ( !configuration.reapTimeout().isZero() ) {
            fireOnInfo( listeners, "Connection reap not supported in pool-less mode" );
        }
        if ( configuration.initialSize() != 0 ) {
            fireOnInfo( listeners, "Initial size is zero in pool-less mode" );
        }
        if ( configuration.minSize() != 0 ) {
            fireOnInfo( listeners, "Min size always zero in pool-less mode" );
        }

        transactionIntegration.addResourceRecoveryFactory( connectionFactory );
    }

    public AgroalConnectionPoolConfiguration getConfiguration() {
        return configuration;
    }

    public AgroalDataSourceListener[] getListeners() {
        return listeners;
    }

    public List<AgroalPoolInterceptor> getPoolInterceptors() {
        return unmodifiableList( interceptors );
    }

    public void setPoolInterceptors(Collection<? extends AgroalPoolInterceptor> list) {
        if ( list.stream().anyMatch( i -> i.getPriority() < 0 ) ) {
            throw new IllegalArgumentException( "Negative priority values on AgroalPoolInterceptor are reserved." );
        }
        if ( list.isEmpty() && ( interceptors == null || interceptors.isEmpty() ) ) {
            return;
        }
        interceptors = list.stream().sorted( AgroalPoolInterceptor.DEFAULT_COMPARATOR ).collect( toList() );

        Function<AgroalPoolInterceptor, String> interceptorName = i -> i.getClass().getName() + "@" + toHexString( identityHashCode( i ) ) + " (priority " + i.getPriority() + ")";
        fireOnInfo( listeners, "Pool interceptors: " + interceptors.stream().map( interceptorName ).collect( joining( " >>> ", "[", "]" ) ) );
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

    private long beforeAcquire() throws SQLException {
        fireBeforeConnectionAcquire( listeners );
        if ( shutdown ) {
            throw new SQLException( "This pool is closed and does not handle any more connections!" );
        }
        return metricsRepository.beforeConnectionAcquire();
    }

    private void checkMultipleAcquisition() throws SQLException {
        if ( configuration.multipleAcquisition() != OFF ) {
            for ( ConnectionHandler handler : allConnections ) {
                if ( handler.getHoldingThread() == currentThread() ) {
                    switch ( configuration.multipleAcquisition() ) {
                        case STRICT:
                            throw new SQLException( "Acquisition of multiple connections by the same Thread." );
                        case WARN:
                            fireOnWarning( listeners, "Acquisition of multiple connections by the same Thread. This can lead to pool exhaustion and eventually a deadlock!" );
                        case OFF:
                        default:
                            // no action
                    }
                    break;
                }
            }
        }
    }

    public Connection getConnection() throws SQLException {
        long stamp = beforeAcquire();

        ConnectionHandler checkedOutHandler = handlerFromTransaction();
        if ( checkedOutHandler != null ) {
            // AG-140 - If associate throws here is fine, it's assumed the synchronization that returns the connection has been registered
            transactionIntegration.associate( checkedOutHandler, checkedOutHandler.getXaResource() );
            return afterAcquire( stamp, checkedOutHandler );
        }
        checkMultipleAcquisition();

        try {
            checkedOutHandler = handlerFromSharedCache();
            transactionIntegration.associate( checkedOutHandler, checkedOutHandler.getXaResource() );
            fireOnConnectionAcquiredInterceptor( interceptors, checkedOutHandler );
            return afterAcquire( stamp, checkedOutHandler );
        } catch ( Throwable t ) {
            if ( checkedOutHandler != null ) {
                // AG-140 - Flush handler to prevent leak
                flushHandler( checkedOutHandler );
            }
            throw t;
        }
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

    private Connection afterAcquire(long metricsStamp, ConnectionHandler checkedOutHandler) throws SQLException {
        metricsRepository.afterConnectionAcquire( metricsStamp );
        fireOnConnectionAcquired( listeners, checkedOutHandler );

        if ( !checkedOutHandler.isEnlisted() ) {
            switch ( configuration.transactionRequirement() ) {
                case STRICT:
                    returnConnectionHandler( checkedOutHandler );
                    throw new SQLException( "Connection acquired without transaction." );
                case WARN:
                    fireOnWarning( listeners, new SQLException( "Connection acquired without transaction." ) );
                case OFF: // do nothing
                default:
            }
        }
        if ( !configuration.leakTimeout().isZero() || configuration.multipleAcquisition() != OFF ) {
            if ( checkedOutHandler.getHoldingThread() != null && checkedOutHandler.getHoldingThread() != currentThread() ) {
                Throwable warn = new Throwable( "Shared connection between threads '" + checkedOutHandler.getHoldingThread().getName() + "' and '" + currentThread().getName() + "'" );
                warn.setStackTrace( checkedOutHandler.getHoldingThread().getStackTrace() );
                fireOnWarning( listeners, warn );
            }
            checkedOutHandler.setHoldingThread( currentThread() );
            checkedOutHandler.touch();
            if ( configuration.enhancedLeakReport() ) {
                StackTraceElement[] stackTrace = currentThread().getStackTrace();
                checkedOutHandler.setAcquisitionStackTrace( copyOfRange( stackTrace, 4, stackTrace.length) );
            }
        }
        return checkedOutHandler.newConnectionWrapper();
    }

    // --- //

    public void returnConnectionHandler(ConnectionHandler handler) throws SQLException {
        fireBeforeConnectionReturn( listeners, handler );
        try {
            if ( !transactionIntegration.disassociate( handler ) ) {
                return;
            }
        } catch ( Throwable ignored ) {
        }

        fireOnConnectionReturnInterceptor( interceptors, handler );
        flushHandler( handler );
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
        return activeCount.get();
    }

    public long availableCount() {
        return configuration.maxSize() - activeCount.get();
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
        } else if ( mode == LEAK ) {
            for ( ConnectionHandler handler : allConnections ) {
                if ( handler.isLeak( configuration.leakTimeout() ) ) {
                    fireBeforeConnectionFlush( listeners, handler );
                    flushHandler( handler );
                }
            }
        }
    }

    public void flushHandler(ConnectionHandler handler) {
        handler.setState( FLUSH );
        allConnections.remove( handler );
        activeCount.decrementAndGet();
        synchronizer.releaseConditional();
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

// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalPoolInterceptor;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.transaction.TransactionIntegration;
import io.agroal.pool.MetricsRepository.EmptyMetricsRepository;
import io.agroal.pool.util.StampedCopyOnWriteArrayList;
import io.agroal.pool.util.XAConnectionAdaptor;

import javax.sql.XAConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;

import static io.agroal.api.AgroalDataSource.FlushMode.ALL;
import static io.agroal.api.AgroalDataSource.FlushMode.LEAK;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.MultipleAcquisitionAction.OFF;
import static io.agroal.pool.ConnectionHandler.State.CHECKED_OUT;
import static io.agroal.pool.ConnectionHandler.State.FLUSH;
import static io.agroal.pool.ConnectionHandler.State.VALIDATION;
import static io.agroal.pool.util.InterceptorHelper.fireOnConnectionAcquiredInterceptor;
import static io.agroal.pool.util.InterceptorHelper.fireOnConnectionCreateInterceptor;
import static io.agroal.pool.util.InterceptorHelper.fireOnConnectionDestroyInterceptor;
import static io.agroal.pool.util.InterceptorHelper.fireOnConnectionReturnInterceptor;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionAcquire;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionCreation;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionDestroy;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionFlush;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionReturn;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionValidation;
import static io.agroal.pool.util.ListenerHelper.fireBeforePoolBlock;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionAcquired;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionCreation;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionCreationFailure;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionDestroy;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionFlush;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionInvalid;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionPooled;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionValid;
import static io.agroal.pool.util.ListenerHelper.fireOnInfo;
import static io.agroal.pool.util.ListenerHelper.fireOnPoolInterceptor;
import static io.agroal.pool.util.ListenerHelper.fireOnWarning;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.copyOfRange;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Alternative implementation of ConnectionPool for the special case of flush-on-close (and min-size == 0)
 * In particular, this removes the need for the executor. Also, there is no thread-local connection cache as connections are not reused
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class Poolless implements Pool {

    static {
        try {
            TRANSFER_POISON = new ConnectionHandler( new XAConnectionAdaptor( null ), null );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

    private static final ConnectionHandler TRANSFER_POISON; // Dummy object to unblock waiting threads, for example on close()
    private static final long ONE_SECOND = TimeUnit.SECONDS.toNanos( 1 );

    private final AgroalConnectionPoolConfiguration configuration;
    private final AgroalDataSourceListener[] listeners;

    private final StampedCopyOnWriteArrayList<ConnectionHandler> allConnections;
    private final TransferQueue<ConnectionHandler> handlerTransferQueue = new LinkedTransferQueue<>();

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
        connectionFactory = new ConnectionFactory( configuration.connectionFactoryConfiguration(), listeners );
        transactionIntegration = configuration.transactionIntegration();
    }

    private TransactionIntegration.ResourceRecoveryFactory getResourceRecoveryFactory() {
        return connectionFactory.hasRecoveryCredentials() || !configuration.connectionFactoryConfiguration().poolRecovery() ? connectionFactory : this;
    }

    public void init() {
        if ( !configuration.maxLifetime().isZero() ) {
            fireOnInfo( listeners, "Max lifetime not supported in pool-less mode" );
        }
        if ( configuration.validateOnBorrow() ) {
            fireOnInfo( listeners, "Validation on borrow is not supported in pool-less mode" );
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
        if ( configuration.recoveryEnable() ) {
            transactionIntegration.addResourceRecoveryFactory( getResourceRecoveryFactory() );
        }
    }

    public AgroalConnectionPoolConfiguration getConfiguration() {
        return configuration;
    }

    public int defaultJdbcIsolationLevel() {
        return connectionFactory.defaultJdbcIsolationLevel();
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
        interceptors.forEach( interceptor -> fireOnPoolInterceptor( listeners, interceptor ) );
    }

    // --- //

    @Override
    public boolean isRecoverable() {
        return connectionFactory.isRecoverable();
    }

    @Override
    public XAConnection getRecoveryConnection() throws SQLException {
        long stamp = beforeAcquire();
        checkMultipleAcquisition();

        ConnectionHandler checkedOutHandler = handlerFromSharedCache();
        fireOnConnectionAcquiredInterceptor( interceptors, checkedOutHandler );
        afterAcquire( stamp, checkedOutHandler, false );
        return checkedOutHandler.xaConnectionWrapper();
    }

    // --- //

    @Override
    public void close() {
        if ( configuration.recoveryEnable() ) {
            transactionIntegration.removeResourceRecoveryFactory( getResourceRecoveryFactory() );
        }
        shutdown = true;

        for ( ConnectionHandler handler : allConnections ) {
            handler.setState( FLUSH );
            destroyConnection( handler );
        }
        allConnections.clear();
        while ( handlerTransferQueue.tryTransfer( TRANSFER_POISON ) ) ; // Unblock waiting threads with CancellationException
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

    @Override
    public Connection getConnection() throws SQLException {
        return internalGetConnection( false );
    }

    @Override
    public Connection getReadOnlyConnection() throws SQLException {
        return internalGetConnection( true );
    }

    private Connection internalGetConnection(boolean readOnly) throws SQLException {
        long stamp = beforeAcquire();

        ConnectionHandler checkedOutHandler = handlerFromTransaction();
        if ( checkedOutHandler != null ) {
            // AG-140 - If associate throws here is fine, it's assumed the synchronization that returns the connection has been registered
            transactionIntegration.associate( checkedOutHandler, checkedOutHandler.getXaResource() );
            afterAcquire( stamp, checkedOutHandler, false );
            if ( readOnly && !checkedOutHandler.rawConnection().isReadOnly() ) {
                throw new SQLException( "Attempted to modify read-only state while enlisted in transaction" );
            }
            return checkedOutHandler.connectionWrapper();
        }
        checkMultipleAcquisition();

        try {
            checkedOutHandler = handlerFromSharedCache();
            transactionIntegration.associate( checkedOutHandler, checkedOutHandler.getXaResource() );
            fireOnConnectionAcquiredInterceptor( interceptors, checkedOutHandler );
            afterAcquire( stamp, checkedOutHandler, true );
            if ( readOnly ) {
                checkedOutHandler.setDirtyAttribute( ConnectionHandler.DirtyAttribute.READ_ONLY );
                checkedOutHandler.rawConnection().setReadOnly( true );
            }
            return checkedOutHandler.connectionWrapper();
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
        long deadline = remaining > 0 ? nanoTime() + remaining : Long.MAX_VALUE;
        int retries = configuration.establishmentRetryAttempts();
        try {
            for ( ; ; ) {
                // Try to get a "token" to create a new connection
                if ( activeCount.incrementAndGet() <= configuration.maxSize() ) {
                    try {
                        return createConnection();
                    } catch ( SQLException e ) {
                        activeCount.decrementAndGet();
                        long timeout = deadline - nanoTime();
                        if ( --retries < 0 || timeout <= 0 ) {
                            throw e;
                        } else {
                            fireOnInfo( listeners, "Retrying establishment of connection after " + e.getClass().getName() );
                            waitAvailableHandler( Long.min( configuration.establishmentRetryInterval().toNanos(), timeout - ONE_SECOND / 10 ), false );
                            continue;
                        }
                    }
                } else {
                    activeCount.decrementAndGet();
                }
                waitAvailableHandler( deadline - nanoTime(), true ); // Wait for a connection to be returned
            }
        } catch ( InterruptedException e ) {
            currentThread().interrupt();
            throw new SQLException( "Interrupted while acquiring" );
        }
    }

    private void waitAvailableHandler(long timeout, boolean strict) throws InterruptedException, SQLException {
        fireBeforePoolBlock( listeners, timeout );
        ConnectionHandler handler = handlerTransferQueue.poll( timeout, NANOSECONDS );
        if ( strict && handler == null ) {
            throw new SQLException( "Sorry, acquisition timeout!" );
        } else if ( handler == TRANSFER_POISON ) {
            throw new CancellationException();
        }
    }

    @SuppressWarnings( "SingleCharacterStringConcatenation" )
    private void afterAcquire(long metricsStamp, ConnectionHandler checkedOutHandler, boolean verifyEnlistment) throws SQLException {
        metricsRepository.afterConnectionAcquire( metricsStamp );
        fireOnConnectionAcquired( listeners, checkedOutHandler );

        if ( verifyEnlistment && !checkedOutHandler.isEnlisted() ) {
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
                checkedOutHandler.setAcquisitionStackTrace( copyOfRange( stackTrace, 4, stackTrace.length ) );
            }
        }
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
        metricsRepository = metricsEnabled ? new DefaultMetricsRepository( this ) : new EmptyMetricsRepository();
    }

    public MetricsRepository getMetrics() {
        return metricsRepository;
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
        return handlerTransferQueue.getWaitingConsumerCount();
    }

    // --- health check //

    @Override
    public boolean isHealthy(boolean newConnection) throws SQLException {
        // the main difference here is that connect will not block (and potentially timeout) on full pool
        if ( newConnection ) {
            activeCount.incrementAndGet();
        }
        ConnectionHandler healthHandler = newConnection ? createConnection() : handlerFromSharedCache();

        try {
            fireBeforeConnectionValidation( listeners, healthHandler );
            if ( healthHandler.setState( CHECKED_OUT, VALIDATION ) && healthHandler.isValid() && healthHandler.setState( VALIDATION, CHECKED_OUT ) ) {
                fireOnConnectionValid( listeners, healthHandler );
                return true;
            } else {
                metricsRepository.afterConnectionInvalid();
                fireOnConnectionInvalid( listeners, healthHandler );
                return false;
            }
        } finally {
            returnConnectionHandler( healthHandler );
        }
    }

    // --- create //

    private ConnectionHandler createConnection() throws SQLException {
        fireBeforeConnectionCreation( listeners );
        long metricsStamp = metricsRepository.beforeConnectionCreation();

        try {
            ConnectionHandler handler = new ConnectionHandler( connectionFactory.createConnection(), this );
            metricsRepository.afterConnectionCreation( metricsStamp );

            fireOnConnectionCreation( listeners, handler );
            fireOnConnectionCreateInterceptor( interceptors, handler );

            handler.setState( CHECKED_OUT );
            allConnections.add( handler );

            maxUsed.accumulate( allConnections.size() );
            fireOnConnectionPooled( listeners, handler );

            return handler;
        } catch ( SQLException e ) {
            fireOnConnectionCreationFailure( listeners, e );
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

    private void flushHandler(ConnectionHandler handler) {
        handler.setState( FLUSH );
        allConnections.remove( handler );
        activeCount.decrementAndGet();
        handlerTransferQueue.tryTransfer( handler );
        metricsRepository.afterConnectionFlush();
        fireOnConnectionFlush( listeners, handler );
        destroyConnection( handler );
    }

    // --- destroy //

    private void destroyConnection(ConnectionHandler handler) {
        fireBeforeConnectionDestroy( listeners, handler );
        try {
            fireOnConnectionDestroyInterceptor( interceptors, handler );
            handler.closeConnection();
        } catch ( SQLException e ) {
            fireOnWarning( listeners, e );
        }
        metricsRepository.afterConnectionDestroy();
        fireOnConnectionDestroy( listeners, handler );
    }
}

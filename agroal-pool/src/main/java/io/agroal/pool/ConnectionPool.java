// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalPoolInterceptor;
import io.agroal.api.cache.ConnectionCache;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.transaction.TransactionIntegration;
import io.agroal.pool.MetricsRepository.EmptyMetricsRepository;
import io.agroal.pool.util.PriorityScheduledExecutor;
import io.agroal.pool.util.StampedCopyOnWriteArrayList;
import io.agroal.pool.util.XAConnectionAdaptor;

import javax.sql.XAConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

import static io.agroal.api.AgroalDataSource.FlushMode.GRACEFUL;
import static io.agroal.api.AgroalDataSource.FlushMode.LEAK;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.MultipleAcquisitionAction.OFF;
import static io.agroal.pool.ConnectionHandler.State.CHECKED_IN;
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
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionLeak;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionReap;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionReturn;
import static io.agroal.pool.util.ListenerHelper.fireBeforeConnectionValidation;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionAcquired;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionCreation;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionDestroy;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionFlush;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionInvalid;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionLeak;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionPooled;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionReap;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionReturn;
import static io.agroal.pool.util.ListenerHelper.fireOnConnectionValid;
import static io.agroal.pool.util.ListenerHelper.fireOnInfo;
import static io.agroal.pool.util.ListenerHelper.fireOnPoolInterceptor;
import static io.agroal.pool.util.ListenerHelper.fireOnWarning;
import static java.lang.Long.MAX_VALUE;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class ConnectionPool implements Pool {

    static {
        try {
            TRANSFER_POISON = new ConnectionHandler( new XAConnectionAdaptor( null ), null );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

    private static final AtomicInteger HOUSEKEEP_COUNT = new AtomicInteger();
    private static final ConnectionHandler TRANSFER_POISON; // Dummy object to unblock waiting threads, for example on close()
    private static final long ONE_SECOND = SECONDS.toNanos( 1 );

    private final AgroalConnectionPoolConfiguration configuration;
    private final AgroalDataSourceListener[] listeners;

    private final StampedCopyOnWriteArrayList<ConnectionHandler> allConnections;
    private final TransferQueue<ConnectionHandler> handlerTransferQueue = new LinkedTransferQueue<>();

    private final ConnectionFactory connectionFactory;
    private final PriorityScheduledExecutor housekeepingExecutor;
    private final TransactionIntegration transactionIntegration;

    private final boolean borrowValidationEnabled;
    private final boolean idleValidationEnabled;
    private final boolean leakEnabled;
    private final boolean validationEnabled;
    private final boolean reapEnabled;
    private final boolean recoveryEnabled;

    private final LongAccumulator maxUsed = new LongAccumulator( Math::max, Long.MIN_VALUE );
    private final LongAdder activeCount = new LongAdder();

    private MetricsRepository metricsRepository;
    private ConnectionCache localCache;
    private List<AgroalPoolInterceptor> interceptors;

    public ConnectionPool(AgroalConnectionPoolConfiguration configuration, AgroalDataSourceListener... listeners) {
        this.configuration = configuration;
        this.listeners = listeners;

        allConnections = new StampedCopyOnWriteArrayList<>( ConnectionHandler.class );
        localCache = configuration.connectionCache();

        connectionFactory = new ConnectionFactory( configuration.connectionFactoryConfiguration(), listeners );
        housekeepingExecutor = new PriorityScheduledExecutor( 1, "agroal-" + HOUSEKEEP_COUNT.incrementAndGet(), listeners );
        transactionIntegration = configuration.transactionIntegration();

        borrowValidationEnabled = configuration.validateOnBorrow();
        idleValidationEnabled = !configuration.validateOnBorrow() && !configuration.idleValidationTimeout().isZero();
        leakEnabled = !configuration.leakTimeout().isZero();
        validationEnabled = !configuration.validationTimeout().isZero();
        reapEnabled = !configuration.reapTimeout().isZero();
        recoveryEnabled = configuration.recoveryEnable();
    }

    private TransactionIntegration.ResourceRecoveryFactory getResourceRecoveryFactory() {
        return connectionFactory.hasRecoveryCredentials() || !configuration.connectionFactoryConfiguration().poolRecovery() ? connectionFactory : this;
    }

    public void init() {
        if ( configuration.acquisitionTimeout().compareTo( configuration.connectionFactoryConfiguration().loginTimeout() ) < 0 ) {
            fireOnWarning( listeners, "Login timeout should be smaller than acquisition timeout" );
        }

        if ( leakEnabled ) {
            housekeepingExecutor.schedule( new LeakTask(), configuration.leakTimeout().toNanos(), NANOSECONDS );
        }
        if ( validationEnabled ) {
            housekeepingExecutor.schedule( new ValidationTask(), configuration.validationTimeout().toNanos(), NANOSECONDS );
        }
        if ( reapEnabled ) {
            housekeepingExecutor.schedule( new ReapTask(), configuration.reapTimeout().toNanos(), NANOSECONDS );
        }
        if ( recoveryEnabled ) {
            transactionIntegration.addResourceRecoveryFactory( getResourceRecoveryFactory() );
        }

        // fill to the initial size
        if ( configuration.initialSize() < configuration.minSize() ) {
            fireOnInfo( listeners, "Initial size smaller than min. Connections will be created when necessary" );
        } else if ( configuration.initialSize() > configuration.maxSize() ) {
            fireOnInfo( listeners, "Initial size bigger than max. Connections will be destroyed as soon as they return to the pool" );
        }
        for ( int n = configuration.initialSize(); n > 0; n-- ) {
            housekeepingExecutor.executeNow( new CreateConnectionTask().initial() );
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

    public void flushPool(AgroalDataSource.FlushMode mode) {
        if ( mode == LEAK && !leakEnabled ) {
            fireOnWarning( listeners, "Flushing leak connections with no specified leak timeout." );
            return;
        }
        housekeepingExecutor.execute( new FlushTask( mode ) );
    }

    @Override
    public void close() {
        if ( recoveryEnabled ) {
            transactionIntegration.removeResourceRecoveryFactory( getResourceRecoveryFactory() );
        }

        for ( Runnable task : housekeepingExecutor.shutdownNow() ) {
            if ( task instanceof DestroyConnectionTask ) {
                task.run();
            }
        }

        for ( ConnectionHandler handler : allConnections ) {
            handler.setState( FLUSH );
            new DestroyConnectionTask( handler ).run();
        }
        allConnections.clear();
        activeCount.reset();

        while ( handlerTransferQueue.tryTransfer( TRANSFER_POISON ) ) ; // Unblock waiting threads with CancellationException
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
        ConnectionHandler checkedOutHandler = null;

        try {
            do {
                checkedOutHandler = (ConnectionHandler) localCache.get();
                if ( checkedOutHandler == null ) {
                    checkedOutHandler = handlerFromSharedCache();
                }
            } while ( ( borrowValidationEnabled && !borrowValidation( checkedOutHandler ) )
                    || ( idleValidationEnabled && !idleValidation( checkedOutHandler ) ) );

            if ( metricsRepository.collectPoolMetrics() ) {
                activeCount.increment();
            }
            fireOnConnectionAcquiredInterceptor( interceptors, checkedOutHandler );
            afterAcquire( stamp, checkedOutHandler, false );
            return checkedOutHandler.xaConnectionWrapper();
        } catch ( Throwable t ) {
            if ( checkedOutHandler != null ) {
                checkedOutHandler.setState( CHECKED_OUT, CHECKED_IN );
            }
            throw t;
        }
    }

    // --- //

    private long beforeAcquire() throws SQLException {
        fireBeforeConnectionAcquire( listeners );
        if ( housekeepingExecutor.isShutdown() ) {
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
        long stamp = beforeAcquire();

        ConnectionHandler checkedOutHandler = handlerFromTransaction();
        if ( checkedOutHandler != null ) {
            // AG-140 - If associate throws here is fine, it's assumed the synchronization that returns the connection has been registered
            transactionIntegration.associate( checkedOutHandler, checkedOutHandler.getXaResource() );
            afterAcquire( stamp, checkedOutHandler, true );
            return checkedOutHandler.connectionWrapper();
        }
        checkMultipleAcquisition();

        try {
            do {
                checkedOutHandler = (ConnectionHandler) localCache.get();
                if ( checkedOutHandler == null ) {
                    checkedOutHandler = handlerFromSharedCache();
                }
            } while ( ( borrowValidationEnabled && !borrowValidation( checkedOutHandler ) )
                    || ( idleValidationEnabled && !idleValidation( checkedOutHandler ) ) );
            transactionIntegration.associate( checkedOutHandler, checkedOutHandler.getXaResource() );

            if ( metricsRepository.collectPoolMetrics() ) {
                activeCount.increment();
            }
            fireOnConnectionAcquiredInterceptor( interceptors, checkedOutHandler );
            afterAcquire( stamp, checkedOutHandler, true );
            return checkedOutHandler.connectionWrapper();
        } catch ( Throwable t ) {
            if ( checkedOutHandler != null ) {
                // AG-140 - Return the connection to the pool to prevent leak
                checkedOutHandler.setState( CHECKED_OUT, CHECKED_IN );
            }
            throw t;
        }
    }

    private ConnectionHandler handlerFromTransaction() throws SQLException {
        return (ConnectionHandler) transactionIntegration.getTransactionAware();
    }

    private ConnectionHandler handlerFromSharedCache() throws SQLException {
        long remaining = configuration.acquisitionTimeout().toNanos();
        long deadline = remaining > 0 ? nanoTime() + remaining : MAX_VALUE;
        Future<ConnectionHandler> task = null;
        try {
            for ( int i = 0; ; i++ ) {
                // If min-size increases, create a connection right away
                if ( allConnections.size() < configuration.minSize() ) {
                    task = housekeepingExecutor.executeNow( new CreateConnectionTask() );
                }
                if ( i == 0 && handlerTransferQueue.hasWaitingConsumer() ) { // On the first iteration, block right away if there are other threads already blocked
                    // There is a race condition here (if a thread was blocked but by the time poll is executed a transfer allready took place)
                    // Because of that do not block for the whole remaining duration. Do it for at most a second and then move on to perform a scan
                    ConnectionHandler handler = handlerTransferQueue.poll( Long.min( ONE_SECOND, remaining > 0 ? remaining * 9 / 10 : MAX_VALUE ), NANOSECONDS );
                    if ( handler != null && handler.acquire() ) {
                        return handler;
                    }
                }
                // Try to find an available connection in the pool
                for ( ConnectionHandler handler : allConnections ) {
                    if ( handler.acquire() ) {
                        return handler;
                    }
                }
                // If no connections are available and there is room, create one
                if ( task == null && allConnections.size() < configuration.maxSize() ) {
                    task = housekeepingExecutor.executeNow( new CreateConnectionTask() );
                }
                if ( task == null ) {
                    ConnectionHandler handler = waitAvailableHandler( deadline );
                    if ( handler.acquire() ) {
                        return handler;
                    }
                } else {
                    // Wait for the new connection instead of the transferQueue to propagate any exception on connection establishment
                    ConnectionHandler handler = task.get( deadline - nanoTime(), NANOSECONDS );
                    if ( handler != null && handler.acquire() ) {
                        return handler;
                    }
                    task = null;
                }
            }
        } catch ( InterruptedException e ) {
            currentThread().interrupt();
            throw new SQLException( "Interrupted while acquiring" );
        } catch ( ExecutionException e ) {
            throw unwrapExecutionException( e );
        } catch ( RejectedExecutionException | CancellationException e ) {
            throw new SQLException( "Can't create new connection as the pool is shutting down", e );
        } catch ( TimeoutException e ) {
            task.cancel( true );
            // AG-201: Last effort. Connections may have returned to the pool while waiting.
            for ( ConnectionHandler handler : allConnections ) {
                if ( handler.acquire() ) {
                    return handler;
                }
            }
            throw new SQLException( "Acquisition timeout while waiting for new connection", e );
        }
    }

    private ConnectionHandler waitAvailableHandler(long deadline) throws InterruptedException, SQLException {
        ConnectionHandler handler = handlerTransferQueue.poll( deadline - nanoTime(), NANOSECONDS );
        if ( handler == null ) {
            throw new SQLException( "Sorry, acquisition timeout!" );
        } else if ( handler == TRANSFER_POISON ) {
            throw new CancellationException();
        }
        return handler;
    }

    private SQLException unwrapExecutionException(ExecutionException ee) {
        try {
            throw ee.getCause();
        } catch ( RuntimeException | Error re ) {
            throw re;
        } catch ( SQLException se ) {
            return se;
        } catch ( Throwable t ) {
            return new SQLException( "Exception while creating new connection", t );
        }
    }

    private boolean idleValidation(ConnectionHandler handler) {
        if ( !handler.isIdle( configuration.idleValidationTimeout() ) ) {
            return true;
        }
        return borrowValidation( handler );
    }

    private boolean borrowValidation(ConnectionHandler handler) {
        if ( handler.setState( CHECKED_OUT, VALIDATION ) ) {
            return performValidation( handler, CHECKED_OUT );
        }
        return false;
    }

    // handler must be in VALIDATION state
    private boolean performValidation(ConnectionHandler handler, ConnectionHandler.State targetState) {
        fireBeforeConnectionValidation( listeners, handler );
        if ( handler.isValid() && handler.setState( VALIDATION, targetState ) ) {
            fireOnConnectionValid( listeners, handler );
            handlerTransferQueue.tryTransfer( handler );
            return true;
        } else {
            removeFromPool( handler );
            metricsRepository.afterConnectionInvalid();
            fireOnConnectionInvalid( listeners, handler );
            return false;
        }
    }

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
        if ( leakEnabled || reapEnabled ) {
            checkedOutHandler.touch();
        }
        if ( leakEnabled || configuration.multipleAcquisition() != OFF ) {
            if ( checkedOutHandler.getHoldingThread() != null && checkedOutHandler.getHoldingThread() != currentThread() ) {
                Throwable warn = new Throwable( "Shared connection between threads '" + checkedOutHandler.getHoldingThread().getName() + "' and '" + currentThread().getName() + "'" );
                warn.setStackTrace( checkedOutHandler.getHoldingThread().getStackTrace() );
                fireOnWarning( listeners, warn );
            }
            checkedOutHandler.setHoldingThread( currentThread() );
            if ( configuration.enhancedLeakReport() ) {
                checkedOutHandler.setAcquisitionStackTrace( currentThread().getStackTrace() );
            }
        }
    }

    // --- //

    public void returnConnectionHandler(ConnectionHandler handler) throws SQLException {
        fireBeforeConnectionReturn( listeners, handler );
        if ( leakEnabled ) {
            handler.setHoldingThread( null );
            if ( configuration.enhancedLeakReport() ) {
                handler.setAcquisitionStackTrace( null );
            }
        }
        if ( idleValidationEnabled || reapEnabled ) {
            handler.touch();
        }
        try {
            if ( !transactionIntegration.disassociate( handler ) ) {
                return;
            }
        } catch ( Throwable ignored ) {
        }

        if ( metricsRepository.collectPoolMetrics() ) {
            activeCount.decrement();
        }

        // resize on change of max-size, or flush on close
        int currentSize = allConnections.size();
        if ( ( currentSize > configuration.maxSize() && currentSize > configuration.minSize() ) || configuration.flushOnClose() ) {
            handler.setState( FLUSH );
            removeFromPool( handler );
            metricsRepository.afterConnectionReap();
            fireOnConnectionReap( listeners, handler );
            return;
        }

        try {
            handler.resetConnection();
        } catch ( SQLException sqlException ) {
            fireOnWarning( listeners, sqlException );
        }
        localCache.put( handler );
        fireOnConnectionReturnInterceptor( interceptors, handler );

        if ( handler.setState( CHECKED_OUT, CHECKED_IN ) ) {
            // here the handler is already up for grabs
            handlerTransferQueue.tryTransfer( handler );
            metricsRepository.afterConnectionReturn();
            fireOnConnectionReturn( listeners, handler );
        } else {
            // handler not in CHECKED_OUT implies FLUSH
            removeFromPool( handler );
            metricsRepository.afterConnectionFlush();
            fireOnConnectionFlush( listeners, handler );
        }
    }

    private void removeFromPool(ConnectionHandler handler) {
        allConnections.remove( handler );
        housekeepingExecutor.execute( new FillTask() );
        housekeepingExecutor.execute( new DestroyConnectionTask( handler ) );
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
        return activeCount.sum();
    }

    public long availableCount() {
        return allConnections.size() - activeCount.sum();
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
        ConnectionHandler healthHandler;
        Future<ConnectionHandler> task = null;
        if ( newConnection ) {
            try {
                do {
                    task = housekeepingExecutor.executeNow( new CreateConnectionTask().initial() );
                    healthHandler = task.get( configuration.acquisitionTimeout().isZero() ? MAX_VALUE : configuration.acquisitionTimeout().toNanos(), NANOSECONDS );
                } while ( !healthHandler.setState( CHECKED_IN, VALIDATION ) );
            } catch ( InterruptedException e ) {
                currentThread().interrupt();
                throw new SQLException( "Interrupted while acquiring" );
            } catch ( ExecutionException ee ) {
                throw unwrapExecutionException( ee );
            } catch ( RejectedExecutionException | CancellationException e ) {
                throw new SQLException( "Can't create new connection as the pool is shutting down", e );
            } catch ( TimeoutException e ) {
                task.cancel( true );
                throw new SQLException( "Acquisition timeout on health check" );
            }
        } else {
            healthHandler = handlerFromSharedCache();
            healthHandler.setState( CHECKED_OUT, VALIDATION );
        }
        return performValidation( healthHandler, CHECKED_IN );
    }

    // --- create //

    private final class CreateConnectionTask implements Callable<ConnectionHandler> {

        private boolean initial;

        // initial connections do not take configuration.maxSize into account
        private CreateConnectionTask initial() {
            initial = true;
            return this;
        }

        @Override
        public ConnectionHandler call() throws SQLException {
            if ( !initial && allConnections.size() >= configuration.maxSize() ) {
                return null;
            }
            fireBeforeConnectionCreation( listeners );
            long metricsStamp = metricsRepository.beforeConnectionCreation();

            try {
                ConnectionHandler handler = new ConnectionHandler( connectionFactory.createConnection(), ConnectionPool.this );
                metricsRepository.afterConnectionCreation( metricsStamp );

                if ( !configuration.maxLifetime().isZero() ) {
                    handler.setMaxLifetimeTask( housekeepingExecutor.schedule( new FlushTask( GRACEFUL, handler ), configuration.maxLifetime().toNanos(), NANOSECONDS ) );
                }

                fireOnConnectionCreation( listeners, handler );
                fireOnConnectionCreateInterceptor( interceptors, handler );

                handler.setState( CHECKED_IN );
                allConnections.add( handler );

                if ( metricsRepository.collectPoolMetrics() ) {
                    maxUsed.accumulate( allConnections.size() );
                }
                fireOnConnectionPooled( listeners, handler );

                handlerTransferQueue.tryTransfer( handler );
                return handler;
            } catch ( SQLException e ) {
                fireOnWarning( listeners, e );
                throw e;
            } catch ( Throwable t ) {
                fireOnWarning( listeners, "Failed to create connection due to " + t.getClass().getSimpleName() );
                throw t;
            }
        }
    }

    // --- flush //

    private final class FlushTask implements Runnable {

        private final AgroalDataSource.FlushMode mode;
        private final ConnectionHandler handler;

        @SuppressWarnings( "WeakerAccess" )
        FlushTask(AgroalDataSource.FlushMode mode) {
            this.mode = mode;
            this.handler = null;
        }

        @SuppressWarnings( {"WeakerAccess", "SameParameterValue"} )
        FlushTask(AgroalDataSource.FlushMode mode, ConnectionHandler handler) {
            this.mode = mode;
            this.handler = handler;
        }

        @Override
        public void run() {
            for ( ConnectionHandler ch : handler != null ? Collections.singleton( handler ) : allConnections ) {
                fireBeforeConnectionFlush( listeners, ch );
                flush( mode, ch );
            }
            afterFlush( mode );
        }

        private void flush(AgroalDataSource.FlushMode mode, ConnectionHandler handler) {
            switch ( mode ) {
                case ALL:
                    handler.setState( FLUSH );
                    flushHandler( handler );
                    break;
                case GRACEFUL:
                    if ( handler.setState( CHECKED_IN, FLUSH ) ) {
                        flushHandler( handler );
                    } else if ( !handler.setState( CHECKED_OUT, FLUSH ) && handler.isAcquirable() ) {
                        // concurrency caused both transitions fail but handler is still acquirable. re-schedule this task.
                        housekeepingExecutor.execute( this );
                    }
                    break;
                case LEAK:
                    if ( handler.isLeak( configuration.leakTimeout() ) && handler.setState( CHECKED_OUT, FLUSH ) ) {
                        flushHandler( handler );
                    }
                    break;
                case IDLE:
                    if ( allConnections.size() > configuration.minSize() && handler.setState( CHECKED_IN, FLUSH ) ) {
                        flushHandler( handler );
                    }
                    break;
                case INVALID:
                    fireBeforeConnectionValidation( listeners, handler );
                    if ( handler.setState( CHECKED_IN, VALIDATION ) ) {
                        if ( handler.isValid() && handler.setState( VALIDATION, CHECKED_IN ) ) {
                            fireOnConnectionValid( listeners, handler );
                        } else {
                            handler.setState( VALIDATION, FLUSH );
                            fireOnConnectionInvalid( listeners, handler );
                            flushHandler( handler );
                        }
                    }
                    break;
                default:
            }
        }

        private void flushHandler(ConnectionHandler handler) {
            allConnections.remove( handler );
            metricsRepository.afterConnectionFlush();
            fireOnConnectionFlush( listeners, handler );
            housekeepingExecutor.execute( new DestroyConnectionTask( handler ) );
        }

        private void afterFlush(AgroalDataSource.FlushMode mode) {
            switch ( mode ) {
                case ALL:
                case GRACEFUL:
                case INVALID:
                case LEAK:
                case FILL:
                    // refill to minSize
                    housekeepingExecutor.execute( new FillTask() );
                    break;
                case IDLE:
                    break;
                default:
                    fireOnWarning( listeners, "Unsupported Flush mode " + mode );
            }
        }
    }

    // --- fill task //

    private final class FillTask implements Runnable {

        @Override
        public void run() {
            for ( int n = configuration.minSize() - allConnections.size(); n > 0; n-- ) {
                housekeepingExecutor.executeNow( new CreateConnectionTask() );
            }
        }
    }

    // --- leak detection //

    private final class LeakTask implements Runnable {

        @Override
        public void run() {
            housekeepingExecutor.schedule( this, configuration.leakTimeout().toNanos(), NANOSECONDS );

            for ( ConnectionHandler handler : allConnections ) {
                housekeepingExecutor.execute( new LeakConnectionTask( handler ) );
            }
        }

        private class LeakConnectionTask implements Runnable {

            private final ConnectionHandler handler;

            LeakConnectionTask(ConnectionHandler handler) {
                this.handler = handler;
            }

            @Override
            public void run() {
                fireBeforeConnectionLeak( listeners, handler );
                if ( handler.isLeak( configuration.leakTimeout() ) ) {
                    metricsRepository.afterLeakDetection();
                    fireOnConnectionLeak( listeners, handler );
                }
            }
        }
    }

    // --- validation //

    private final class ValidationTask implements Runnable {

        @Override
        public void run() {
            housekeepingExecutor.schedule( this, configuration.validationTimeout().toNanos(), NANOSECONDS );

            for ( ConnectionHandler handler : allConnections ) {
                housekeepingExecutor.execute( new ValidateConnectionTask( handler ) );
            }
        }

        private class ValidateConnectionTask implements Runnable {

            private final ConnectionHandler handler;

            ValidateConnectionTask(ConnectionHandler handler) {
                this.handler = handler;
            }

            @Override
            public void run() {
                if ( handler.setState( CHECKED_IN, VALIDATION ) ) {
                    performValidation( handler, CHECKED_IN );
                }
            }
        }
    }

    // --- reap //

    private final class ReapTask implements Runnable {

        @Override
        public void run() {
            housekeepingExecutor.schedule( this, configuration.reapTimeout().toNanos(), NANOSECONDS );

            // reset the thead local cache
            localCache.reset();

            for ( ConnectionHandler handler : allConnections ) {
                housekeepingExecutor.execute( new ReapConnectionTask( handler ) );
            }
        }

        private class ReapConnectionTask implements Runnable {

            private final ConnectionHandler handler;

            ReapConnectionTask(ConnectionHandler handler) {
                this.handler = handler;
            }

            @Override
            public void run() {
                fireBeforeConnectionReap( listeners, handler );
                if ( allConnections.size() > configuration.minSize() && handler.setState( CHECKED_IN, FLUSH ) ) {
                    if ( handler.isIdle( configuration.reapTimeout() ) ) {
                        removeFromPool( handler );
                        metricsRepository.afterConnectionReap();
                        fireOnConnectionReap( listeners, handler );
                    } else {
                        handler.setState( CHECKED_IN );
                        // for debug, something like: fireOnWarning( listeners,  "Connection " + handler.getConnection() + " used recently. Do not reap!" );
                    }
                }
            }
        }
    }

    // --- destroy //

    private final class DestroyConnectionTask implements Runnable {

        private final ConnectionHandler handler;

        DestroyConnectionTask(ConnectionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void run() {
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
}

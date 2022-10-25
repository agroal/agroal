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
import io.agroal.pool.util.AgroalSynchronizer;
import io.agroal.pool.util.PriorityScheduledExecutor;
import io.agroal.pool.util.StampedCopyOnWriteArrayList;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static io.agroal.api.AgroalDataSource.FlushMode.GRACEFUL;
import static io.agroal.api.AgroalDataSource.FlushMode.LEAK;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.MultipleAcquisitionAction.OFF;
import static io.agroal.pool.ConnectionHandler.State.CHECKED_IN;
import static io.agroal.pool.ConnectionHandler.State.CHECKED_OUT;
import static io.agroal.pool.ConnectionHandler.State.FLUSH;
import static io.agroal.pool.ConnectionHandler.State.VALIDATION;
import static io.agroal.pool.util.InterceptorHelper.fireOnConnectionAcquiredInterceptor;
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
import static io.agroal.pool.util.ListenerHelper.fireOnDebug;
import static io.agroal.pool.util.ListenerHelper.fireOnInfo;
import static io.agroal.pool.util.ListenerHelper.fireOnWarning;
import static java.lang.Integer.toHexString;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class ConnectionPool implements Pool {

    private static final AtomicInteger HOUSEKEEP_COUNT = new AtomicInteger();

    private final AgroalConnectionPoolConfiguration configuration;
    private final AgroalDataSourceListener[] listeners;

    private final StampedCopyOnWriteArrayList<ConnectionHandler> allConnections;

    private final AgroalSynchronizer synchronizer;
    private final ConnectionFactory connectionFactory;
    private final PriorityScheduledExecutor housekeepingExecutor;
    private final TransactionIntegration transactionIntegration;

    private final boolean idleValidationEnabled;
    private final boolean leakEnabled;
    private final boolean validationEnabled;
    private final boolean reapEnabled;

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

        synchronizer = new AgroalSynchronizer();
        connectionFactory = new ConnectionFactory( configuration.connectionFactoryConfiguration(), listeners );
        housekeepingExecutor = new PriorityScheduledExecutor( 1, "agroal-" + HOUSEKEEP_COUNT.incrementAndGet(), listeners );
        transactionIntegration = configuration.transactionIntegration();

        idleValidationEnabled = !configuration.idleValidationTimeout().isZero();
        leakEnabled = !configuration.leakTimeout().isZero();
        validationEnabled = !configuration.validationTimeout().isZero();
        reapEnabled = !configuration.reapTimeout().isZero();
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

        transactionIntegration.addResourceRecoveryFactory( connectionFactory );

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

        Function<AgroalPoolInterceptor, String> interceptorName = i -> i.getClass().getName() + "@" + toHexString( identityHashCode( i ) ) + " (priority " + i.getPriority() + ")";
        fireOnDebug( listeners, "Pool interceptors: " + interceptors.stream().map( interceptorName ).collect( joining( " >>> ", "[", "]" ) ) );
    }

    public void flushPool(AgroalDataSource.FlushMode mode) {
        if ( mode == LEAK && !leakEnabled ) {
            fireOnWarning( listeners, "Flushing leak connections with no specified leak timout." );
            return;
        }
        housekeepingExecutor.execute( new FlushTask( mode ) );
    }

    @Override
    public void close() {
        transactionIntegration.removeResourceRecoveryFactory( connectionFactory );

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

        synchronizer.release( synchronizer.getQueueLength() );
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
            return afterAcquire( stamp, checkedOutHandler );
        }
        checkMultipleAcquisition();

        try {
            do {
                checkedOutHandler = (ConnectionHandler) localCache.get();
                if ( checkedOutHandler == null ) {
                    checkedOutHandler = handlerFromSharedCache();
                }
            } while ( idleValidationEnabled && !idleValidation( checkedOutHandler ) );
            transactionIntegration.associate( checkedOutHandler, checkedOutHandler.getXaResource() );

            activeCount.increment();
            fireOnConnectionAcquiredInterceptor( interceptors, checkedOutHandler );
            return afterAcquire( stamp, checkedOutHandler );
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
        remaining = remaining > 0 ? remaining : Long.MAX_VALUE;
        Future<ConnectionHandler> task = null;
        try {
            for ( ; ; ) {
                // If min-size increases, create a connection right away
                if ( allConnections.size() < configuration.minSize() ) {
                    long start = nanoTime();
                    task = housekeepingExecutor.executeNow( new CreateConnectionTask() );
                    ConnectionHandler handler = task.get( remaining, NANOSECONDS );
                    if ( handler != null && handler.setState( CHECKED_IN, CHECKED_OUT ) ) {
                        return handler;
                    }
                    remaining -= nanoTime() - start;
                    continue;
                }
                // Try to find an available connection in the pool
                for ( ConnectionHandler handler : allConnections ) {
                    if ( handler.setState( CHECKED_IN, CHECKED_OUT ) ) {
                        return handler;
                    }
                }
                // If no connections are available and there is room, create one
                if ( allConnections.size() < configuration.maxSize() ) {
                    long start = nanoTime();
                    task = housekeepingExecutor.executeNow( new CreateConnectionTask() );
                    ConnectionHandler handler = task.get( remaining, NANOSECONDS );
                    if ( handler != null && handler.setState( CHECKED_IN, CHECKED_OUT ) ) {
                        return handler;
                    }
                    remaining -= nanoTime() - start;
                    continue;
                }
                // Pool full, will have to wait for a connection to be returned
                long synchronizationStamp = synchronizer.getStamp();
                long start = nanoTime();
                if ( remaining < 0 || !synchronizer.tryAcquireNanos( synchronizationStamp, remaining ) ) {
                    throw new SQLException( "Sorry, acquisition timeout!" );
                }
                remaining -= nanoTime() - start;
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
            throw new SQLException( "Acquisition timeout while waiting for new connection", e );
        }
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
            synchronizer.releaseConditional();
            return true;
        } else {
            removeFromPool( handler );
            metricsRepository.afterConnectionInvalid();
            fireOnConnectionInvalid( listeners, handler );
            return false;
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
        return checkedOutHandler.connectionWrapper();
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

        activeCount.decrement();

        // resize on change of max-size, or flush on close
        int currentSize = allConnections.size();
        if ( currentSize > configuration.maxSize() || configuration.flushOnClose() && currentSize > configuration.minSize() ) {
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
            synchronizer.releaseConditional();
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
        synchronizer.releaseConditional();
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
        return synchronizer.getQueueLength();
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
                    healthHandler = task.get( configuration.acquisitionTimeout().isZero() ? Long.MAX_VALUE : configuration.acquisitionTimeout().toNanos(), NANOSECONDS );
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
                if ( !configuration.maxLifetime().isZero() ) {
                    handler.setMaxLifetimeTask( housekeepingExecutor.schedule( new FlushTask( GRACEFUL, handler ), configuration.maxLifetime().toNanos(), NANOSECONDS ) );
                }

                metricsRepository.afterConnectionCreation( metricsStamp );
                fireOnConnectionCreation( listeners, handler );

                handler.setState( CHECKED_IN );
                allConnections.add( handler );

                maxUsed.accumulate( allConnections.size() );
                fireOnConnectionPooled( listeners, handler );

                return handler;
            } catch ( SQLException e ) {
                fireOnWarning( listeners, e );
                throw e;
            } catch ( Throwable t ) {
                fireOnWarning( listeners, "Failed to create connection due to " + t.getClass().getSimpleName() );
                throw t;
            } finally {
                // not strictly needed, but not harmful either
                synchronizer.releaseConditional();
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
            if ( handler != null ) {
                fireBeforeConnectionFlush( listeners, handler );
                flush( mode, handler );
            } else {
                for ( ConnectionHandler handler : allConnections ) {
                    fireBeforeConnectionFlush( listeners, handler );
                    flush( mode, handler );
                }
                afterFlush( mode );
            }
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
            synchronizer.releaseConditional();
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
                handler.closeConnection();
            } catch ( SQLException e ) {
                fireOnWarning( listeners, e );
            }
            metricsRepository.afterConnectionDestroy();
            fireOnConnectionDestroy( listeners, handler );
        }
    }
}

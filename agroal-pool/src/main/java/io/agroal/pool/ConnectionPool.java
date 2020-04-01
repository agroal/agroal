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
import io.agroal.pool.util.PriorityScheduledExecutor;
import io.agroal.pool.util.StampedCopyOnWriteArrayList;
import io.agroal.pool.util.UncheckedArrayList;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.agroal.api.AgroalDataSource.FlushMode.ALL;
import static io.agroal.api.AgroalDataSource.FlushMode.GRACEFUL;
import static io.agroal.pool.ConnectionHandler.State.CHECKED_IN;
import static io.agroal.pool.ConnectionHandler.State.CHECKED_OUT;
import static io.agroal.pool.ConnectionHandler.State.DESTROYED;
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
    private ThreadLocal<List<ConnectionHandler>> localCache;
    private List<AgroalPoolInterceptor> interceptors;

    public ConnectionPool(AgroalConnectionPoolConfiguration configuration, AgroalDataSourceListener... listeners) {
        this.configuration = configuration;
        this.listeners = listeners;

        allConnections = new StampedCopyOnWriteArrayList<>( ConnectionHandler.class );
        localCache = new ConnectionHandlerThreadLocal();

        synchronizer = new AgroalSynchronizer();
        connectionFactory = new ConnectionFactory( configuration.connectionFactoryConfiguration(), listeners );
        housekeepingExecutor = new PriorityScheduledExecutor( 1, "Agroal_" + identityHashCode( this ) );
        transactionIntegration = configuration.transactionIntegration();

        idleValidationEnabled = !configuration.idleValidationTimeout().isZero();
        leakEnabled = !configuration.leakTimeout().isZero();
        validationEnabled = !configuration.validationTimeout().isZero();
        reapEnabled = !configuration.reapTimeout().isZero();
    }

    public void init() {
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

    public AgroalDataSourceListener[] getListeners() {
        return listeners;
    }

    public List<AgroalPoolInterceptor> getPoolInterceptors() {
        return unmodifiableList( interceptors );
    }

    public void setPoolInterceptors(Collection<AgroalPoolInterceptor> list) {
        if ( list.stream().anyMatch( i -> i.getPriority() < 0 ) ) {
            throw new IllegalArgumentException( "Negative priority values on AgroalPoolInterceptor are reserved." );
        }
        interceptors = list.stream().sorted( AgroalPoolInterceptor.DEFAULT_COMPARATOR ).collect( toList() );

        Function<AgroalPoolInterceptor, String> interceptorName = i -> i.getClass().getName() + "@" + toHexString( identityHashCode( i ) ) + " (priority " + i.getPriority() + ")";
        fireOnInfo( listeners, "Pool interceptors: " + interceptors.stream().map( interceptorName ).collect( joining( " >>> ", "[", "]" ) ) );
    }

    public void flushPool(AgroalDataSource.FlushMode mode) {
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

    public Connection getConnection() throws SQLException {
        fireBeforeConnectionAcquire( listeners );
        long metricsStamp = metricsRepository.beforeConnectionAcquire();

        if ( housekeepingExecutor.isShutdown() ) {
            throw new SQLException( "This pool is closed and does not handle any more connections!" );
        }

        ConnectionHandler checkedOutHandler = handlerFromTransaction();
        if ( checkedOutHandler == null ) {
            do {
                checkedOutHandler = handlerFromLocalCache();
                if ( checkedOutHandler == null ) {
                    checkedOutHandler = handlerFromSharedCache();
                }
            } while ( idleValidationEnabled && !idleValidation( checkedOutHandler ) );
            activeCount.increment();
            fireOnConnectionAcquiredInterceptor( interceptors, checkedOutHandler );
        }

        metricsRepository.afterConnectionAcquire( metricsStamp );
        fireOnConnectionAcquired( listeners, checkedOutHandler );

        if ( leakEnabled || reapEnabled ) {
            checkedOutHandler.setLastAccess( nanoTime() );
        }
        if ( leakEnabled ) {
            if ( checkedOutHandler.getHoldingThread() != null && checkedOutHandler.getHoldingThread() != currentThread() ) {
                Throwable warn = new Throwable( "Shared connection between threads '" + checkedOutHandler.getHoldingThread().getName() + "' and '" + currentThread().getName() + "'" );
                warn.setStackTrace( checkedOutHandler.getHoldingThread().getStackTrace() );
                fireOnWarning( listeners, warn );
            }
            checkedOutHandler.setHoldingThread( currentThread() );
        }

        transactionIntegration.associate( checkedOutHandler, checkedOutHandler.getXaResource() );
        return checkedOutHandler.newConnectionWrapper();
    }

    private ConnectionHandler handlerFromTransaction() throws SQLException {
        return (ConnectionHandler) transactionIntegration.getTransactionAware();
    }

    private ConnectionHandler handlerFromLocalCache() {
        List<ConnectionHandler> cachedConnections = localCache.get();
        while ( !cachedConnections.isEmpty() ) {
            ConnectionHandler handler = cachedConnections.remove( cachedConnections.size() - 1 );
            if ( handler.setState( CHECKED_IN, CHECKED_OUT ) ) {
                return handler;
            }
        }
        return null;
    }

    private ConnectionHandler handlerFromSharedCache() throws SQLException {
        long remaining = configuration.acquisitionTimeout().toNanos();
        remaining = remaining > 0 ? remaining : Long.MAX_VALUE;
        try {
            for ( ; ; ) {
                // If min-size increases, create a connection right away
                if ( allConnections.size() < configuration.minSize() ) {
                    ConnectionHandler handler = housekeepingExecutor.executeNow( new CreateConnectionTask() ).get();
                    if ( handler != null && handler.setState( CHECKED_IN, CHECKED_OUT ) ) {
                        return handler;
                    }
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
                    ConnectionHandler handler = housekeepingExecutor.executeNow( new CreateConnectionTask() ).get();
                    if ( handler != null && handler.setState( CHECKED_IN, CHECKED_OUT ) ) {
                        return handler;
                    }
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
            try {
                throw e.getCause();
            } catch ( RuntimeException | Error | SQLException e2 ) {
                throw e2;
            } catch ( Throwable t ) {
                throw new SQLException( "Exception while creating new connection", t );
            }
        } catch ( RejectedExecutionException | CancellationException e ) {
            throw new SQLException( "Can't create new connection as the pool is shutting down", e );
        }
    }

    private boolean idleValidation(ConnectionHandler handler) {
        if ( nanoTime() - handler.getLastAccess() < configuration.idleValidationTimeout().toNanos() ) {
            return true;
        }
        fireBeforeConnectionValidation( listeners, handler );
        if ( handler.setState( CHECKED_OUT, VALIDATION ) ) {
            if ( configuration.connectionValidator().isValid( handler.getConnection() ) ) {
                handler.setState( CHECKED_OUT );
                fireOnConnectionValid( listeners, handler );
                return true;
            } else {
                handler.setState( FLUSH );
                allConnections.remove( handler );
                metricsRepository.afterConnectionInvalid();
                fireOnConnectionInvalid( listeners, handler );
                housekeepingExecutor.execute( new DestroyConnectionTask( handler ) );
            }
        }
        return false;
    }

    // --- //

    public void returnConnectionHandler(ConnectionHandler handler) throws SQLException {
        fireBeforeConnectionReturn( listeners, handler );
        if ( leakEnabled ) {
            handler.setHoldingThread( null );
        }
        if ( idleValidationEnabled || reapEnabled ) {
            handler.setLastAccess( nanoTime() );
        }
        if ( transactionIntegration.disassociate( handler ) ) {
            activeCount.decrement();

            // resize on change of max-size, or flush on close
            int currentSize = allConnections.size();
            if ( currentSize > configuration.maxSize() || configuration.flushOnClose() && currentSize > configuration.minSize() ) {
                handler.setState( FLUSH );
                allConnections.remove( handler );
                synchronizer.releaseConditional();
                housekeepingExecutor.execute( new FlushTask( ALL, handler ) );
                return;
            }

            handler.resetConnection();
            localCache.get().add( handler );
            fireOnConnectionReturnInterceptor( interceptors, handler );

            if ( handler.setState( CHECKED_OUT, CHECKED_IN ) ) {
                // here the handler is already up for grabs
                synchronizer.releaseConditional();
                metricsRepository.afterConnectionReturn();
                fireOnConnectionReturn( listeners, handler );
            } else {
                // handler not in CHECKED_OUT implies FLUSH
                allConnections.remove( handler );
                housekeepingExecutor.execute( new FlushTask( ALL, handler ) );
                housekeepingExecutor.execute( new FillTask() );
            }
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

    // --- //

    private final class ConnectionHandlerThreadLocal extends ThreadLocal<List<ConnectionHandler>> {

        @Override
        protected List<ConnectionHandler> initialValue() {
            return new UncheckedArrayList<ConnectionHandler>( ConnectionHandler.class );
        }
    }

    // --- create //

    private final class CreateConnectionTask implements Callable<ConnectionHandler> {

        private boolean initial;

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

                fireOnConnectionCreation( listeners, handler );
                metricsRepository.afterConnectionCreation( metricsStamp );

                handler.setState( CHECKED_IN );
                allConnections.add( handler );

                maxUsed.accumulate( allConnections.size() );
                fireOnConnectionPooled( listeners, handler );

                return handler;
            } catch ( SQLException e ) {
                fireOnWarning( listeners, e );
                throw e;
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

        public FlushTask(AgroalDataSource.FlushMode mode) {
            this.mode = mode;
            this.handler = null;
        }

        public FlushTask(AgroalDataSource.FlushMode mode, ConnectionHandler handler) {
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
                    } else {
                        handler.setState( FLUSH );
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
                        if ( configuration.connectionValidator().isValid( handler.getConnection() ) ) {
                            fireOnConnectionValid( listeners, handler );
                            handler.setState( CHECKED_IN );
                        } else {
                            fireOnConnectionInvalid( listeners, handler );
                            flushHandler( handler );
                        }
                    }
                    break;
                default:
            }
        }

        public void flushHandler(ConnectionHandler handler) {
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

            public LeakConnectionTask(ConnectionHandler handler) {
                this.handler = handler;
            }

            @Override
            public void run() {
                fireBeforeConnectionLeak( listeners, handler );
                Thread thread = handler.getHoldingThread();
                if ( thread != null && nanoTime() - handler.getLastAccess() > configuration.leakTimeout().toNanos() ) {
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

            public ValidateConnectionTask(ConnectionHandler handler) {
                this.handler = handler;
            }

            @Override
            public void run() {
                fireBeforeConnectionValidation( listeners, handler );
                if ( handler.setState( CHECKED_IN, VALIDATION ) ) {
                    if ( configuration.connectionValidator().isValid( handler.getConnection() ) ) {
                        handler.setState( CHECKED_IN );
                        fireOnConnectionValid( listeners, handler );
                    } else {
                        handler.setState( FLUSH );
                        allConnections.remove( handler );
                        metricsRepository.afterConnectionInvalid();
                        fireOnConnectionInvalid( listeners, handler );
                        housekeepingExecutor.execute( new DestroyConnectionTask( handler ) );
                    }
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
            localCache = new ConnectionHandlerThreadLocal();

            for ( ConnectionHandler handler : allConnections ) {
                housekeepingExecutor.execute( new ReapConnectionTask( handler ) );
            }
        }

        private class ReapConnectionTask implements Runnable {

            private final ConnectionHandler handler;

            public ReapConnectionTask(ConnectionHandler handler) {
                this.handler = handler;
            }

            @Override
            public void run() {
                fireBeforeConnectionReap( listeners, handler );
                if ( allConnections.size() > configuration.minSize() && handler.setState( CHECKED_IN, FLUSH ) ) {
                    if ( nanoTime() - handler.getLastAccess() > configuration.reapTimeout().toNanos() ) {
                        allConnections.remove( handler );
                        metricsRepository.afterConnectionReap();
                        fireOnConnectionReap( listeners, handler );
                        housekeepingExecutor.execute( new DestroyConnectionTask( handler ) );
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

        public DestroyConnectionTask(ConnectionHandler handler) {
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
            handler.setState( DESTROYED );
            metricsRepository.afterConnectionDestroy();
            fireOnConnectionDestroy( listeners, handler );
        }
    }
}

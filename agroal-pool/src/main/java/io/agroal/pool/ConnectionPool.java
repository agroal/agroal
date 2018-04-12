// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.MetricsEnabledListener;
import io.agroal.api.transaction.TransactionIntegration;
import io.agroal.pool.MetricsRepository.EmptyMetricsRepository;
import io.agroal.pool.util.AgroalSynchronizer;
import io.agroal.pool.util.PriorityScheduledExecutor;
import io.agroal.pool.util.StampedCopyOnWriteArrayList;
import io.agroal.pool.util.UncheckedArrayList;
import io.agroal.pool.wrapper.ConnectionWrapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.agroal.pool.ConnectionHandler.State.CHECKED_IN;
import static io.agroal.pool.ConnectionHandler.State.CHECKED_OUT;
import static io.agroal.pool.ConnectionHandler.State.DESTROYED;
import static io.agroal.pool.ConnectionHandler.State.FLUSH;
import static io.agroal.pool.ConnectionHandler.State.VALIDATION;
import static io.agroal.pool.util.ListenerHelper.*;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class ConnectionPool implements MetricsEnabledListener, AutoCloseable {

    private final AgroalConnectionPoolConfiguration configuration;

    private final AgroalDataSourceListener[] listeners;
    private final ThreadLocal<UncheckedArrayList<ConnectionHandler>> localCache;

    private final StampedCopyOnWriteArrayList<ConnectionHandler> allConnections;

    private final AgroalSynchronizer synchronizer = new AgroalSynchronizer();
    private final ConnectionFactory connectionFactory;
    private final PriorityScheduledExecutor housekeepingExecutor;
    private final TransactionIntegration transactionIntegration;

    private final boolean leakEnabled;
    private final boolean validationEnabled;
    private final boolean reapEnabled;

    private volatile long maxUsed = 0;
    private MetricsRepository metricsRepository;

    public ConnectionPool(AgroalConnectionPoolConfiguration configuration, AgroalDataSourceListener... listeners) {
        this.configuration = configuration;
        this.listeners = listeners;

        allConnections = new StampedCopyOnWriteArrayList<>( ConnectionHandler.class );

        localCache = ThreadLocal.withInitial( () -> new UncheckedArrayList<ConnectionHandler>( ConnectionHandler.class ) );
        connectionFactory = new ConnectionFactory( configuration.connectionFactoryConfiguration(), listeners );
        housekeepingExecutor = new PriorityScheduledExecutor( 1, "Agroal_" + identityHashCode( this ) );
        transactionIntegration = configuration.transactionIntegration();

        leakEnabled = !configuration.leakTimeout().isZero();
        validationEnabled = !configuration.validationTimeout().isZero();
        reapEnabled = !configuration.reapTimeout().isZero();
    }

    public void init() {
        fill( configuration.initialSize() );

        if ( leakEnabled ) {
            housekeepingExecutor.schedule( new LeakTask(), configuration.leakTimeout().toNanos(), NANOSECONDS );
        }
        if ( validationEnabled ) {
            housekeepingExecutor.schedule( new ValidationTask(), configuration.validationTimeout().toNanos(), NANOSECONDS );
        }
        if ( reapEnabled ) {
            housekeepingExecutor.schedule( new ReapTask(), configuration.reapTimeout().toNanos(), NANOSECONDS );
        }
    }

    private void fill(int newSize) {
        int connectionCount = newSize - allConnections.size();
        while ( connectionCount-- > 0 ) {
            newConnectionHandler();
        }
    }

    public void flush(AgroalDataSource.FlushMode mode) {
        housekeepingExecutor.execute( new FlushTask( mode ) );
    }

    @Override
    public void close() {
        housekeepingExecutor.shutdownNow();
    }

    // --- //

    private Future<?> newConnectionHandler() {
        return housekeepingExecutor.executeNow( () -> {
            if ( allConnections.size() >= configuration.maxSize() ) {
                return;
            }
            fireBeforeConnectionCreation( listeners );
            long metricsStamp = metricsRepository.beforeConnectionCreation();

            try {
                ConnectionHandler handler = new ConnectionHandler( connectionFactory.createConnection(), this );
                handler.setState( CHECKED_IN );
                allConnections.add( handler );
                maxUsed = Math.max( maxUsed, allConnections.size() );
                metricsRepository.afterConnectionCreation( metricsStamp );
                fireOnConnectionCreation( listeners, handler );
            } catch ( SQLException e ) {
                throw new RuntimeException( "Exception while creating new connection", e );
            } finally {
                // not strictly needed, but not harmful either
                synchronizer.releaseConditional();
            }
        } );
    }

    // --- //

    public Connection getConnection() throws SQLException {
        fireBeforeConnectionAcquire( listeners );
        long metricsStamp = metricsRepository.beforeConnectionAcquire();

        if ( housekeepingExecutor.isShutdown() ) {
            throw new SQLException( "This pool is closed and does not handle any more connections!" );
        }

        ConnectionHandler checkedOutHandler = null;
        ConnectionWrapper connectionWrapper = wrapperFromTransaction();
        if ( connectionWrapper != null ) {
            checkedOutHandler = connectionWrapper.getHandler();
        }
        if ( checkedOutHandler == null ) {
            checkedOutHandler = handlerFromLocalCache();
        }
        if ( checkedOutHandler == null ) {
            checkedOutHandler = handlerFromSharedCache();
        }

        metricsRepository.afterConnectionAcquire( metricsStamp );
        fireOnConnectionAcquired( listeners, checkedOutHandler );

        if ( leakEnabled || reapEnabled ) {
            checkedOutHandler.setLastAccess( nanoTime() );
        }
        if ( leakEnabled ) {
            if ( checkedOutHandler.getHoldingThread() != null ) {
                Throwable warn = new Throwable( "Shared connection between threads '" + checkedOutHandler.getHoldingThread().getName() + "' and '" + currentThread().getName() + "'" );
                warn.setStackTrace( checkedOutHandler.getHoldingThread().getStackTrace() );
                fireOnWarning( listeners, warn );
            }
            checkedOutHandler.setHoldingThread( currentThread() );
        }

        connectionWrapper = new ConnectionWrapper( checkedOutHandler );
        transactionIntegration.associate( connectionWrapper, checkedOutHandler.getXaResource() );
        return connectionWrapper;
    }

    private ConnectionWrapper wrapperFromTransaction() throws SQLException {
        return (ConnectionWrapper) transactionIntegration.getConnection();
    }

    private ConnectionHandler handlerFromLocalCache() {
        UncheckedArrayList<ConnectionHandler> cachedConnections = localCache.get();
        while ( !cachedConnections.isEmpty() ) {
            ConnectionHandler handler = cachedConnections.removeLast();
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
                for ( ConnectionHandler handler : allConnections.getUnderlyingArray() ) {
                    if ( handler.setState( CHECKED_IN, CHECKED_OUT ) ) {
                        return handler;
                    }
                }
                if ( allConnections.size() < configuration.maxSize() ) {
                    newConnectionHandler().get();
                    continue;
                }
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
            throw new SQLException( "Exception while creating new connection", e );
        }
    }

    // --- //

    public void returnConnection(ConnectionHandler handler) throws SQLException {
        fireBeforeConnectionReturn( listeners, handler );
        if ( leakEnabled ) {
            handler.setHoldingThread( null );
        }
        if ( reapEnabled ) {
            handler.setLastAccess( nanoTime() );
        }
        if ( transactionIntegration.disassociate( handler.getConnection() ) ) {
            handler.resetConnection( configuration.connectionFactoryConfiguration() );
            localCache.get().add( handler );
            handler.setState( CHECKED_IN );
            synchronizer.releaseConditional();
            metricsRepository.afterConnectionReturn();
            fireOnConnectionReturn( listeners, handler );
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

    private long activeCount(ConnectionHandler[] handlers) {
        int l = 0;
        for ( ConnectionHandler handler : handlers ) {
            if ( handler.isActive() ) {
                l++;
            }
        }
        return l;
    }

    public long activeCount() {
        return activeCount( allConnections.getUnderlyingArray() );
    }

    public long availableCount() {
        ConnectionHandler[] handlers = allConnections.getUnderlyingArray();
        return handlers.length - activeCount( handlers );
    }

    public long maxUsedCount() {
        return maxUsed;
    }

    public void resetMaxUsedCount() {
        maxUsed = 0;
    }

    public long awaitingCount() {
        return synchronizer.getQueueLength();
    }

    // --- flush //

    private final class FlushTask implements Runnable {

        private final AgroalDataSource.FlushMode mode;

        public FlushTask(AgroalDataSource.FlushMode mode) {
            this.mode = mode;
        }

        @Override
        public void run() {
            // TODO:
        }
    }

    // --- leak detection //

    private final class LeakTask implements Runnable {

        @Override
        public void run() {
            for ( ConnectionHandler handler : allConnections.getUnderlyingArray() ) {
                housekeepingExecutor.execute( new LeakConnectionTask( handler ) );
            }
            housekeepingExecutor.schedule( this, configuration.leakTimeout().toNanos(), NANOSECONDS );
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
            allConnections.forEach( hadler -> housekeepingExecutor.submit( new ValidateConnectionTask( hadler ) ) );
            housekeepingExecutor.schedule( this, configuration.validationTimeout().toNanos(), NANOSECONDS );
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
            // TODO Clear thread-local connection cache
            // localCache = ThreadLocal.withInitial( () -> new UncheckedArrayList<ConnectionHandler>( ConnectionHandler.class ) ) );

            allConnections.forEach( hadler -> housekeepingExecutor.submit( new ReapConnectionTask( hadler ) ) );
            housekeepingExecutor.schedule( this, configuration.reapTimeout().toNanos(), NANOSECONDS );
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
                        // System.out.println( "Connection " + handler.getConnection() + " used recently. Do not reap!" );
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
            fireOnConnectionDestroy( listeners, handler );
        }
    }
}

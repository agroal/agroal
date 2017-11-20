// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.transaction.TransactionIntegration;
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
import static io.agroal.pool.ListenerHelper.*;
import static java.lang.System.identityHashCode;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class ConnectionPool implements AutoCloseable {

    private final AgroalConnectionPoolConfiguration configuration;

    private final DataSource dataSource;
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

    public ConnectionPool(AgroalConnectionPoolConfiguration configuration, DataSource dataSource) {
        this.configuration = configuration;
        this.dataSource = dataSource;

        allConnections = new StampedCopyOnWriteArrayList<>( ConnectionHandler.class );

        localCache = ThreadLocal.withInitial( () -> new UncheckedArrayList<ConnectionHandler>( ConnectionHandler.class ) );
        connectionFactory = new ConnectionFactory( configuration.connectionFactoryConfiguration() );
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
            fireBeforeConnectionCreation( dataSource );
            long metricsStamp = dataSource.metricsRepository().beforeConnectionCreation();

            try {
                ConnectionHandler handler = new ConnectionHandler( connectionFactory.createConnection(), this );
                handler.setState( CHECKED_IN );
                allConnections.add( handler );
                maxUsed = Math.max( maxUsed, allConnections.size() );
                dataSource.metricsRepository().afterConnectionCreation( metricsStamp );
                fireOnConnectionCreation( dataSource, handler );
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
        fireBeforeConnectionAcquire( dataSource );
        long metricsStamp = dataSource.metricsRepository().beforeConnectionAcquire();

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

        dataSource.metricsRepository().afterConnectionAcquire( metricsStamp );
        fireOnConnectionAcquired( dataSource, checkedOutHandler );

        if ( leakEnabled || reapEnabled ) {
            checkedOutHandler.setLastAccess( nanoTime() );
        }
        if ( leakEnabled ) {
            if ( checkedOutHandler.getHoldingThread() != null ) {
                Throwable warn = new Throwable( "Shared connection between threads '" + checkedOutHandler.getHoldingThread().getName() + "' and '" + currentThread().getName() + "'" );
                warn.setStackTrace( checkedOutHandler.getHoldingThread().getStackTrace() );
                fireOnWarning( dataSource, warn );
            }
            checkedOutHandler.setHoldingThread( currentThread() );
        }

        connectionWrapper = new ConnectionWrapper( checkedOutHandler );
        transactionIntegration.associate( connectionWrapper );
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
        fireBeforeConnectionReturn( dataSource, handler );
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
            dataSource.metricsRepository().afterConnectionReturn();
            fireOnConnectionReturn( dataSource, handler );
        }
    }

    // --- Exposed statistics //

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
                fireBeforeConnectionLeak( dataSource, handler );
                Thread thread = handler.getHoldingThread();
                if ( thread != null && nanoTime() - handler.getLastAccess() > configuration.leakTimeout().toNanos() ) {
                    dataSource.metricsRepository().afterLeakDetection();
                    fireOnConnectionLeak( dataSource, handler );
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
                fireBeforeConnectionValidation( dataSource, handler );
                if ( handler.setState( CHECKED_IN, VALIDATION ) ) {
                    if ( configuration.connectionValidator().isValid( handler.getConnection() ) ) {
                        handler.setState( CHECKED_IN );
                        fireOnConnectionValid( dataSource, handler );
                    } else {
                        handler.setState( FLUSH );
                        allConnections.remove( handler );
                        dataSource.metricsRepository().afterConnectionInvalid();
                        fireOnConnectionInvalid( dataSource, handler );
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
                fireBeforeConnectionReap( dataSource, handler );
                if ( allConnections.size() > configuration.minSize() && handler.setState( CHECKED_IN, FLUSH ) ) {
                    if ( nanoTime() - handler.getLastAccess() > configuration.reapTimeout().toNanos() ) {
                        allConnections.remove( handler );
                        dataSource.metricsRepository().afterConnectionReap();
                        fireOnConnectionReap( dataSource, handler );
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
            fireBeforeConnectionDestroy( dataSource, handler );
            try {
                handler.closeConnection();
            } catch ( SQLException e ) {
                fireOnWarning( dataSource, e );
            }
            handler.setState( DESTROYED );
            fireOnConnectionDestroy( dataSource, handler );
        }
    }
}

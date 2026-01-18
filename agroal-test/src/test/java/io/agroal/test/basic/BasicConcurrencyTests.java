// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.CONCURRENCY;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( CONCURRENCY )
public class BasicConcurrencyTests {

    private static final Logger logger = getLogger( BasicConcurrencyTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver();
        if ( Utils.isWindowsOS() ) {
            Utils.windowsTimerHack();
        }
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Multiple threads" )
    @SuppressWarnings( "ObjectAllocationInLoop" )
    void basicConnectionAcquireTest() throws SQLException {
        int MAX_POOL_SIZE = 10, THREAD_POOL_SIZE = 32, CALLS = 50000, SLEEP_TIME = 1, OVERHEAD = 1;

        ExecutorService executor = newFixedThreadPool( THREAD_POOL_SIZE );
        CountDownLatch latch = new CountDownLatch( CALLS );
        BasicConcurrencyTestsListener listener = new BasicConcurrencyTestsListener();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {

            for ( int i = 0; i < CALLS; i++ ) {
                executor.submit( () -> {
                    try {
                        Connection connection = dataSource.getConnection();
                        // logger.info( format( "{0} got {1}", Thread.currentThread().getName(), connection ) );
                        LockSupport.parkNanos( ofMillis( SLEEP_TIME ).toNanos() );
                        connection.close();
                    } catch ( SQLException e ) {
                        fail( "Unexpected SQLException " + e.getMessage() );
                    } finally {
                        latch.countDown();
                    }
                } );
            }

            try {
                long waitTime = ( SLEEP_TIME + OVERHEAD ) * CALLS / MAX_POOL_SIZE;
                logger.info( format( "Main thread waiting for {0}ms", waitTime ) );
                if ( !latch.await( waitTime, MILLISECONDS ) ) {
                    fail( "Did not execute within the required amount of time" );
                }
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
            logger.info( format( "Closing DataSource" ) );
        }
        logger.info( format( "Main thread proceeding with assertions" ) );

        assertAll( () -> {
            assertEquals( MAX_POOL_SIZE, listener.getCreationCount().longValue() );
            assertEquals( CALLS, listener.getAcquireCount().longValue() );
            assertEquals( CALLS, listener.getReturnCount().longValue() );
        } );
    }

    @Test
    @DisplayName( "Concurrent DataSource in closed state" )
    @SuppressWarnings( {"BusyWait", "JDBCResourceOpenedButNotSafelyClosed", "MethodCallInLoopCondition"} )
    void concurrentDataSourceCloseTest() throws SQLException, InterruptedException {
        int MAX_POOL_SIZE = 10, THREAD_POOL_SIZE = 2, ACQUISITION_TIMEOUT_MS = 2000;

        BasicConcurrencyTestsListener listener = new BasicConcurrencyTestsListener();
        ExecutorService executor = newFixedThreadPool( THREAD_POOL_SIZE );
        CountDownLatch latch = new CountDownLatch( 1 );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .acquisitionTimeout( ofMillis( ACQUISITION_TIMEOUT_MS ) )
                );

        AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener );

        executor.submit( () -> {
            for ( int i = 0; i < MAX_POOL_SIZE; i++ ) {
                try {
                    Connection connection = dataSource.getConnection();
                    assertNotNull( connection, "Expected non null connection" );
                    //connection.close();
                } catch ( SQLException e ) {
                    fail( "SQLException", e );
                }
            }

            try {
                assertEquals( 0, dataSource.getMetrics().availableCount(), "Should not be any available connections" );

                logger.info( "Blocked waiting for a connection" );
                dataSource.getConnection();

                fail( "Expected SQLException" );
            } catch ( SQLException e ) {
                // SQLException should not be because of acquisition timeout
                assertTrue( e.getCause() instanceof RejectedExecutionException || e.getCause() instanceof CancellationException, "Cause for SQLException should be either RejectedExecutionException or CancellationException" );
                latch.countDown();

                logger.info( "Unblocked after datasource close" );
            } catch ( Throwable t ) {
                fail( "Unexpected throwable", t );
            }
        } );

        do {
            Thread.sleep( ACQUISITION_TIMEOUT_MS / 10 );
        } while ( dataSource.getMetrics().awaitingCount() == 0 );

        logger.info( "Closing the datasource" );
        dataSource.close();

        if ( !latch.await( ACQUISITION_TIMEOUT_MS, MILLISECONDS ) ) {
            fail( "Did not execute within the required amount of time" );
        }

        assertAll( () -> {
            assertThrows( SQLException.class, dataSource::getConnection );
            assertFalse( listener.getWarning().get(), "Unexpected warning" );
            assertEquals( MAX_POOL_SIZE, listener.getCreationCount().longValue() );
            assertEquals( MAX_POOL_SIZE, listener.getAcquireCount().longValue() );
            assertEquals( MAX_POOL_SIZE, listener.getDestroyCount().longValue() );
            assertEquals( MAX_POOL_SIZE, dataSource.getMetrics().destroyCount(), "Destroy count" );
            assertEquals( 0, listener.getReturnCount().longValue() );
            assertEquals( 0, dataSource.getMetrics().activeCount(), "Active connections" );
            assertEquals( 0, dataSource.getMetrics().availableCount(), "Should not be any available connections" );
        } );

        // Subsequent calls to dataSource.close() should not throw any exception
        dataSource.close();
    }

    @Test
    @DisplayName( "DataSource close" )
    void dataSourceCloseTest() throws SQLException, InterruptedException {
        int MAX_POOL_SIZE = 10, TIMEOUT_MS = 1000;

        ShutdownListener listener = new ShutdownListener( MAX_POOL_SIZE );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        // Add periodic tasks that should be cancelled on close
                        .reapTimeout( ofSeconds( 10 ) )
                        .validationTimeout( ofSeconds( 2 ) )
                );

        AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener );

        if ( !listener.getStartupLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
            fail( "Did not execute within the required amount of time" );
        }

        assertEquals( MAX_POOL_SIZE, dataSource.getMetrics().availableCount() );

        Connection c = dataSource.getConnection();
        assertEquals( 1, dataSource.getMetrics().activeCount() );
        assertFalse( c.isClosed() );

        dataSource.close();

        // Connections take a while to be destroyed because the executor has to wait on the listener.
        // We check right after close() to make sure all were destroyed when the method returns.
        assertAll( () -> {
            assertFalse( listener.getWarning(), "Datasource closed but there are tasks to run" );
            assertEquals( 0, dataSource.getMetrics().availableCount() );
            assertEquals( MAX_POOL_SIZE, dataSource.getMetrics().destroyCount() );
        } );
    }

    @Test
    @DisplayName( "FlushOnClose DataSource under pressure" )
    void flushOnClosePressureTest() throws SQLException {
        int MAX_POOL_SIZE = 10, THREAD_POOL_SIZE = 5, CALLS = 5000, SLEEP_TIME = 1, OVERHEAD = 5;

        ExecutorService executor = newFixedThreadPool( THREAD_POOL_SIZE );
        CountDownLatch latch = new CountDownLatch( CALLS );
        BasicConcurrencyTestsListener listener1 = new BasicConcurrencyTestsListener();
        ResourceLimitListener listener2 = new ResourceLimitListener( MAX_POOL_SIZE );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .flushOnClose()
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener1, listener2 ) ) {

            for (int i = 0; i < CALLS; i++) {
                executor.submit(() -> {
                    try {
                        Connection connection = dataSource.getConnection();
                        // logger.info( format( "{0} got {1}", Thread.currentThread().getName(), connection ) );
                        LockSupport.parkNanos(ofMillis(SLEEP_TIME).toNanos());
                        connection.close();
                    } catch (SQLException e) {
                        fail("Unexpected SQLException " + e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }

            try {
                long waitTime = ( SLEEP_TIME + OVERHEAD ) * CALLS / MAX_POOL_SIZE;
                logger.info( format( "Main thread waiting for {0}ms", waitTime ) );
                if ( !latch.await( waitTime, MILLISECONDS ) ) {
                    fail( "Did not execute within the required amount of time" );
                }
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
            logger.info( format( "Closing DataSource" ) );
        }
        logger.info( format( "Main thread proceeding with assertions" ) );

        assertAll( () -> {
            assertFalse(listener2.getLimitExceeded().get(), "Unexpected resource limit exceeded");
            assertEquals(CALLS, listener1.getCreationCount().longValue());
            assertEquals(CALLS, listener1.getDestroyCount().longValue());
        } );
    }

    // --- //

    @SuppressWarnings( "WeakerAccess" )
    private static class BasicConcurrencyTestsListener implements AgroalDataSourceListener {

        private final LongAdder creationCount = new LongAdder(), acquireCount = new LongAdder(), returnCount = new LongAdder(), destroyCount = new LongAdder();

        private final AtomicBoolean warning = new AtomicBoolean( false );

        BasicConcurrencyTestsListener() {
        }

        @Override
        public void onConnectionPooled(Connection connection) {
            creationCount.increment();
        }

        @Override
        public void onConnectionAcquire(Connection connection) {
            acquireCount.increment();
        }

        @Override
        public void onConnectionReturn(Connection connection) {
            returnCount.increment();
        }

        @Override
        public void beforeConnectionDestroy(Connection connection) {
            destroyCount.increment();
        }

        @Override
        public void onWarning(String message) {
            warning.set( true );
        }

        @Override
        public void onWarning(Throwable throwable) {
            warning.set( true );
        }

        // --- //

        LongAdder getCreationCount() {
            return creationCount;
        }

        LongAdder getAcquireCount() {
            return acquireCount;
        }

        LongAdder getReturnCount() {
            return returnCount;
        }

        LongAdder getDestroyCount() {
            return destroyCount;
        }

        AtomicBoolean getWarning() {
            return warning;
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    private static class ShutdownListener implements AgroalDataSourceListener {
        private boolean warning;
        private final CountDownLatch startupLatch;

        ShutdownListener(int poolSize) {
            startupLatch = new CountDownLatch( poolSize );
        }

        @Override
        public void onConnectionPooled(Connection connection) {
            startupLatch.countDown();
        }

        @Override
        public void onConnectionDestroy(Connection connection) {
            try {
                // sleep for 1 ms
                Thread.sleep( 1 );
            } catch ( InterruptedException e ) {
                fail( "Interrupted" );
            }
        }

        @Override
        public void onWarning(String message) {
            warning = true;
        }

        CountDownLatch getStartupLatch() {
            return startupLatch;
        }

        boolean getWarning() {
            return warning;
        }
    }

    private static class ResourceLimitListener implements AgroalDataSourceListener {
        private final int maxPoolSize;
        private final LongAdder poolSize = new LongAdder();
        private final AtomicBoolean limitExceeded = new AtomicBoolean( false );

        ResourceLimitListener(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
        }

        @Override
        public void onConnectionCreation(Connection connection) {
            poolSize.increment();
            if ( poolSize.sum() > maxPoolSize ) {
                limitExceeded.set( true );
            }
        }

        @Override
        public void onConnectionDestroy(Connection connection) {
           poolSize.decrement();
        }

        public AtomicBoolean getLimitExceeded() {
            return limitExceeded;
        }
    }
}

// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalPoolInterceptor;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

    private double overheadFactor = 1.0;

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

    @BeforeEach
    void setupDynamicOverhead(){
        overheadFactor = Utils.timerAccuracy( 100, 95 ) * 1.1; // 10% safety margin
        logger.info( format( "Dynamic overhead factor of {0} (P95)", overheadFactor ) );
    }

    // --- //

    @Test
    @DisplayName( "Multiple threads" )
    @SuppressWarnings( "ObjectAllocationInLoop" )
    void basicConnectionAcquireTest() throws SQLException {
        int MAX_POOL_SIZE = 10, THREAD_POOL_SIZE = 32, CALLS = 50000, SLEEP_TIME = 1, OVERHEAD = 0;

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
                long start = System.nanoTime();
                long waitTime = (long) ( (double) ( ( SLEEP_TIME + OVERHEAD ) * CALLS / MAX_POOL_SIZE ) * overheadFactor );
                logger.info( format( "Main thread waiting for {0}ms", waitTime ) );
                if ( !latch.await( waitTime, MILLISECONDS ) ) {
                    fail( format( "Did not execute within the required amount of time {0}ms --- {1} calls made", waitTime, dataSource.getMetrics().acquireCount() ) );
                }
                logger.info( format( "Executed in {0}ms", ( System.nanoTime() - start ) / 1_000_000 ) );
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

        AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener, new SandOnTheCogs() );

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

        AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener, new SandOnTheCogs() );

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
        int MAX_POOL_SIZE = 10, THREAD_POOL_SIZE = 5, CALLS = 5000, SLEEP_TIME = 1, OVERHEAD =  0;

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
                long start = System.nanoTime();
                long waitTime = (long) ( (double) ( ( SLEEP_TIME + OVERHEAD ) * CALLS / THREAD_POOL_SIZE ) * overheadFactor );
                logger.info( format( "Main thread waiting for {0}ms", waitTime ) );
                if ( !latch.await( waitTime, MILLISECONDS ) ) {
                    fail( format( "Did not execute within the required amount of time {0}ms --- {1} calls made", waitTime, dataSource.getMetrics().acquireCount() ) );
                }
                logger.info( format( "Executed in {0}ms", ( System.nanoTime() - start ) / 1_000_000 ) );
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
            logger.info( format( "Closing DataSource" ) );
        }
        logger.info( format( "Main thread proceeding with assertions" ) );

        assertAll( () -> {
            assertFalse( listener2.getLimitExceeded().get(), "Unexpected resource limit exceeded" );
            assertEquals( CALLS, listener1.getCreationCount().longValue() );
            assertEquals( CALLS, listener1.getDestroyCount().longValue() );
        } );
    }

    // --- //


    /* This listener can be used to stress out some of the concurrency points in the pool.
     * It aims to amplify any timing issue that can be undetected otherwise.
     */
    public static class SandOnTheCogs implements AgroalDataSourceListener {

        // there a 4 calls throughout the acquisition cycle (beforeConnectionAcquire, onConnectionAcquire, beforeConnectionReturn, onConnectionReturn)
        private static final long NANOS = 250_000L;

        public static void precisionSleep(long nanos) {
            long start = System.nanoTime();
            if ( nanos > 10_000 ) {
                LockSupport.parkNanos( nanos - 10_000 );
            }
            while ( System.nanoTime() - start < nanos ) {
                Thread.onSpinWait();
            }
        }

        @Override
        public void beforeConnectionCreation() {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionCreation(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionCreationFailure(SQLException sqlException) {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionPooled(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void beforeConnectionAcquire() {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionAcquire(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void beforeConnectionReturn(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionReturn(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void beforeConnectionLeak(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionLeak(Connection connection, Thread thread) {
            precisionSleep( NANOS );
        }

        @Override
        public void beforeConnectionValidation(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionValid(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionInvalid(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void beforeConnectionFlush(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionFlush(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void beforeConnectionReap(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionReap(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void beforeConnectionDestroy(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void onConnectionDestroy(Connection connection) {
            precisionSleep( NANOS );
        }

        @Override
        public void onPoolInterceptor(AgroalPoolInterceptor interceptor) {
            precisionSleep( NANOS );
        }

        @Override
        public void beforePoolBlock(long timeout) {
            precisionSleep( NANOS );
        }

        @Override
        public void onWarning(String message) {
            precisionSleep( NANOS );
        }

        @Override
        public void onWarning(Throwable throwable) {
            precisionSleep( NANOS );
        }

        @Override
        public void onInfo(String message) {
            precisionSleep( NANOS );
        }
    }

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

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
    public static void setup() {
        registerMockDriver();
    }

    @AfterAll
    public static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Multiple threads" )
    public void basicConnectionAcquireTest() throws SQLException {
        int MAX_POOL_SIZE = 10, THREAD_POOL_SIZE = 32, CALLS = 50000, SLEEP_TIME = 1;

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
                long waitTime = (long) ( SLEEP_TIME * CALLS * 1.5 / MAX_POOL_SIZE );
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
            assertEquals( MAX_POOL_SIZE, listener.creationCount.longValue() );
            assertEquals( CALLS, listener.acquireCount.longValue() );
            assertEquals( CALLS, listener.returnCount.longValue() );
        } );
    }

    @Test
    @DisplayName( "Concurrent DataSource in closed state" )
    public void concurrentDataSourceCloseTest() throws SQLException, InterruptedException {
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
            assertFalse( listener.warning.get(), "Unexpected warning" );
            assertEquals( MAX_POOL_SIZE, listener.creationCount.longValue() );
            assertEquals( MAX_POOL_SIZE, listener.acquireCount.longValue() );
            assertEquals( 0, listener.returnCount.longValue() );
            assertEquals( 0, dataSource.getMetrics().activeCount(), "Active connections" );
            assertEquals( 0, dataSource.getMetrics().availableCount(), "Should not be any available connections" );
        } );

        // Subsequent calls to dataSource.close() should not throw any exception
        dataSource.close();
    }

    // --- //

    private static class BasicConcurrencyTestsListener implements AgroalDataSourceListener {

        private final LongAdder creationCount = new LongAdder(), acquireCount = new LongAdder(), returnCount = new LongAdder();

        private final AtomicBoolean warning = new AtomicBoolean( false );

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
        public void onWarning(String message) {
            warning.set( true );
        }

        @Override
        public void onWarning(Throwable throwable) {
            warning.set( true );
        }
    }
}

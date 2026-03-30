// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class ValidationTests {

    static final Logger logger = getLogger( ValidationTests.class.getName() );

    @BeforeAll
    static void setupMockDriver() {
        registerMockDriver( ValidationThrowsConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "validation throws fatal exception" )
    void validationThrowsTest() throws Exception {
        int MAX_POOL_SIZE = 3, VALIDATION_MS = 1000, IDLE_VALIDATION_MS = 100;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .validationTimeout( ofMillis( VALIDATION_MS ) )
                        .idleValidationTimeout( ofMillis( IDLE_VALIDATION_MS ) )
                        .acquisitionTimeout( ofMillis( 2 * VALIDATION_MS ) )
                        .connectionValidator( connection -> {
                            try {
                                return connection.isValid( 0 );
                            } catch ( SQLException e ) {
                                throw new RuntimeException( e ); // unsafe validator
                            }
                        } )
                        .exceptionSorter( AgroalConnectionPoolConfiguration.ExceptionSorter.fatalExceptionSorter() )
                );

        InvalidationListener listener = new InvalidationListener( MAX_POOL_SIZE );
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {

            logger.info( format( "Awaiting for validation of all the {0} connections on the pool", MAX_POOL_SIZE ) );
            listener.awaitValidation( 3 * VALIDATION_MS );

            // With flush-on-first-failure, only the first connection is validated (and found invalid).
            // The remaining connections are flushed directly without validation.
            long invalidCount = dataSource.getMetrics().invalidCount();
            long flushCount = dataSource.getMetrics().flushCount();
            assertEquals( MAX_POOL_SIZE, invalidCount + flushCount, "Expected all connections removed (invalid + flush)" );
            assertEquals( 0, dataSource.getMetrics().availableCount(), "Expected no available connections" );

            long creationsBefore = dataSource.getMetrics().creationCount();
            try ( Connection connection = dataSource.getConnection() ) {
                assertNotNull( connection.getSchema(), "Expected non null value" );
                assertEquals( creationsBefore + 1, dataSource.getMetrics().creationCount(), "Expected connection creation" );
            }

            logger.info( format( "Short sleep to trigger idle validation" ) );
            Thread.sleep( 2 * IDLE_VALIDATION_MS );

            long invalidsBefore = dataSource.getMetrics().invalidCount();
            long creationsBefore2 = dataSource.getMetrics().creationCount();
            try ( Connection connection = dataSource.getConnection() ) {
                assertNotNull( connection.getSchema(), "Expected non null value" );
                assertEquals( invalidsBefore + 1, dataSource.getMetrics().invalidCount(), "Expected connection invalid count" );
                assertEquals( creationsBefore2 + 1, dataSource.getMetrics().creationCount(), "Expected connection creation" );
            }
        }
    }

    @Test
    @DisplayName( "idle validation test" )
    void idleValidationTest() throws Exception {
        int POOL_SIZE = 1, IDLE_VALIDATION_MS = 100, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( POOL_SIZE )
                        .maxSize( POOL_SIZE )
                        .idleValidationTimeout( ofMillis( IDLE_VALIDATION_MS ) )
                        .acquisitionTimeout( ofMillis( TIMEOUT_MS ) )
                        .connectionValidator( AgroalConnectionPoolConfiguration.ConnectionValidator.emptyValidator() )
                );

        BeforeValidationListener listener = new BeforeValidationListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {

            logger.info( format( "Short sleep to trigger idle validation" ) );
            Thread.sleep( 2 * IDLE_VALIDATION_MS );

            assertEquals( POOL_SIZE, dataSource.getMetrics().availableCount(), "Expected connection available count" );
            assertEquals( 0, dataSource.getMetrics().invalidCount(), "Expected connection invalid count" );
            assertEquals( 0, listener.getValidationAttempts(), "Expected validation count" );

            try ( Connection c = dataSource.getConnection() ) {
                assertEquals( 1, listener.getValidationAttempts(), "Expected validation count" );
                logger.info( "Got valid idle connection " + c);
            }

            assertEquals( POOL_SIZE, dataSource.getMetrics().availableCount(), "Expected connection available count" );
            assertEquals( 1, dataSource.getMetrics().acquireCount(), "Expected connection acquire count" );
            assertEquals( 0, dataSource.getMetrics().invalidCount(), "Expected connection invalid count" );
        }
    }

    // --- //

    private static class InvalidationListener implements AgroalDataSourceListener {
        private final CountDownLatch latch;

        @SuppressWarnings( "WeakerAccess" )
        InvalidationListener(int validationCount) {
            latch = new CountDownLatch( validationCount );
        }

        @Override
        public void onConnectionInvalid(Connection connection) {
            latch.countDown();
        }

        @Override
        public void onConnectionFlush(Connection connection) {
            latch.countDown();
        }

        void awaitValidation(int timeoutMS) {
            try {
                if ( !latch.await( timeoutMS, MILLISECONDS ) ) {
                    fail( format( "Validation of {0} connections", latch.getCount() ) );
                }
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
        }
    }

    private static class BeforeValidationListener implements AgroalDataSourceListener {
        private final AtomicInteger counter = new AtomicInteger( 0 );

        @SuppressWarnings( "WeakerAccess" )
        BeforeValidationListener() {
        }

        @Override
        public void beforeConnectionValidation(Connection connection) {
            counter.incrementAndGet();
        }

        int getValidationAttempts() {
            return counter.get();
        }

    }

    @Test
    @DisplayName( "flush all idle connections on first validation failure" )
    void flushOnFirstValidationFailureTest() throws Exception {
        int POOL_SIZE = 10, VALIDATION_MS = 500, BLOCK_MS = 200;

        // Reset stale flag before test
        SlowStaleConnection.stale.set( false );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( POOL_SIZE )
                        .maxSize( POOL_SIZE )
                        .minSize( 0 )
                        .validationTimeout( ofMillis( VALIDATION_MS ) )
                        .connectionValidator( AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidator() )
                        .connectionFactoryConfiguration( cf -> cf.connectionProviderClass( SlowStaleDataSource.class ) )
                );

        // Count both flush (for connections flushed without validation) and invalid (for the first connection that fails validation)
        CountDownLatch removedLatch = new CountDownLatch( POOL_SIZE );
        AgroalDataSourceListener listener = new AgroalDataSourceListener() {
            @Override
            public void onConnectionFlush(Connection connection) {
                removedLatch.countDown();
            }

            @Override
            public void onConnectionInvalid(Connection connection) {
                removedLatch.countDown();
            }
        };

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            // Wait for initial fill to complete on background thread
            Thread.sleep( 100 );
            assertEquals( POOL_SIZE, dataSource.getMetrics().creationCount(), "All connections should be created" );

            // Make all connections stale — isValid() will now block for BLOCK_MS then return false
            SlowStaleConnection.stale.set( true );

            logger.info( format( "Stale flag set. Waiting for background validation to trigger (interval: {0}ms, block: {1}ms)", VALIDATION_MS, BLOCK_MS ) );
            long start = System.nanoTime();

            // Wait for all connections to be removed (flushed or invalidated).
            // Background validation runs every VALIDATION_MS. After detecting the first stale connection,
            // it should flush all remaining idle connections without calling isValid() on each.
            boolean flushed = removedLatch.await( 3 * VALIDATION_MS, MILLISECONDS );
            long elapsed = ( System.nanoTime() - start ) / 1_000_000;

            assertTrue( flushed, "All connections should have been flushed" );
            logger.info( format( "All {0} connections flushed in {1}ms", POOL_SIZE, elapsed ) );

            // The key assertion: elapsed time should be much less than POOL_SIZE * BLOCK_MS
            // Without the fix, it would take at least POOL_SIZE * BLOCK_MS = 2000ms
            // With the fix, only 1 connection is validated (BLOCK_MS), the rest are flushed immediately
            long worstCaseWithoutFix = (long) POOL_SIZE * BLOCK_MS;
            assertTrue( elapsed < worstCaseWithoutFix,
                    format( "Flush should complete faster than sequential validation of all connections. Elapsed: {0}ms, worst case without fix: {1}ms", elapsed, worstCaseWithoutFix ) );
        }
    }

    // --- //

    public static class SlowStaleDataSource implements MockDataSource {

        @Override
        public Connection getConnection() throws SQLException {
            return new SlowStaleConnection();
        }
    }

    public static class SlowStaleConnection implements MockConnection {

        static final AtomicBoolean stale = new AtomicBoolean( false );

        @Override
        public String getSchema() throws SQLException {
            return "slow_stale";
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            if ( stale.get() ) {
                try {
                    // Simulate socket read timeout on a broken connection
                    Thread.sleep( 200 );
                } catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                }
                return false;
            }
            return true;
        }
    }

    // --- //

    public static class ValidationThrowsConnection implements MockConnection {

        private boolean closed;

        @Override
        public void close() throws SQLException {
            closed = true;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed;
        }

        @Override
        public String getSchema() throws SQLException {
            return "validation_only";
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            logger.info( "Throwing exception on validation" );
            throw new SQLException( "Throwing on validation" );
        }
    }
}

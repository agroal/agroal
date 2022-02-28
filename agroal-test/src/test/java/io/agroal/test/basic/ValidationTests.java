// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
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
    void validationThrowsTest() throws SQLException, InterruptedException {
        int MAX_POOL_SIZE = 3, VALIDATION_MS = 1000, IDLE_VALIDATION_MS = 100;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .validationTimeout( ofMillis( VALIDATION_MS ) )
                        .idleValidationTimeout( ofMillis( IDLE_VALIDATION_MS ) )
                        .acquisitionTimeout( ofMillis( 2 * VALIDATION_MS ) )
                        .connectionValidator( AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidator() )
                        .exceptionSorter( AgroalConnectionPoolConfiguration.ExceptionSorter.fatalExceptionSorter() )
                );

        InvalidationListener listener = new InvalidationListener( MAX_POOL_SIZE );
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {

            logger.info( format( "Awaiting for validation of all the {0} connections on the pool", MAX_POOL_SIZE ) );
            listener.awaitValidation( 3 * VALIDATION_MS );

            assertEquals( MAX_POOL_SIZE, dataSource.getMetrics().invalidCount(), "Expected connection invalid count" );
            assertEquals( 0, dataSource.getMetrics().availableCount(), "Expected no available connections" );

            try ( Connection connection = dataSource.getConnection() ) {
                assertNotNull( connection.getSchema(), "Expected non null value" );
                assertEquals( MAX_POOL_SIZE + 1, dataSource.getMetrics().creationCount(), "Expected connection creation" );
            }

            logger.info( format( "Short sleep to trigger idle validation" ) );
            Thread.sleep( 2 * IDLE_VALIDATION_MS );

            try ( Connection connection = dataSource.getConnection() ) {
                assertNotNull( connection.getSchema(), "Expected non null value" );
                assertEquals( MAX_POOL_SIZE + 1, dataSource.getMetrics().invalidCount(), "Expected connection invalid count" );
                assertEquals( MAX_POOL_SIZE + 2, dataSource.getMetrics().creationCount(), "Expected connection creation" );
            }
        }
    }

    @Test
    @DisplayName( "idle validation test" )
    void idleValidationTest() throws SQLException, InterruptedException {
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

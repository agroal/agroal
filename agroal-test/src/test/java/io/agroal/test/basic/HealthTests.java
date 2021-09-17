// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.time.Duration.ofMillis;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class HealthTests {

    private static final Logger logger = getLogger( HealthTests.class.getName() );

    @BeforeAll
    static void setupMockDriver() {
        registerMockDriver();
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Perform health checks" )
    void healthChecksTest() throws SQLException, InterruptedException {
        ValidationCountListener listener = new ValidationCountListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .acquisitionTimeout( ofMillis( 100 ) )
                ), listener ) ) {

            assertEquals( 0, dataSource.getMetrics().creationCount(), "Expected empty pool" );

            logger.info( "Performing health check on empty pool" );
            assertTrue( dataSource.isHealthy( false ) );

            assertEquals( 1, dataSource.getMetrics().creationCount(), "Expected connection in pool" );
            assertEquals( 1, dataSource.getMetrics().availableCount(), "Expected connection in pool" );
            assertEquals( 1, listener.beforeCount(), "Expected validation to have been performed" );
            assertEquals( 1, listener.validCount(), "Expected validation to have been successful" );
            assertEquals( 0, listener.invalidCount(), "Expected validation to have been successful" );

            logger.info( "Performing health check on non-empty pool" );
            assertTrue( dataSource.isHealthy( false ) );

            assertEquals( 1, dataSource.getMetrics().creationCount(), "Expected connection to be re-used" );
            assertEquals( 2, listener.beforeCount(), "Expected validation to have been performed" );

            logger.info( "Performing health check on non-empty pool, on a new connection" );
            assertTrue( dataSource.isHealthy( true ) );

            assertEquals( 2, dataSource.getMetrics().creationCount(), "Expected connection to be created" );
            assertEquals( 2, dataSource.getMetrics().availableCount(), "Expected extra connection in pool" );
            assertEquals( 3, listener.beforeCount(), "Expected validation to have been performed" );

            dataSource.flush( AgroalDataSource.FlushMode.ALL );
            Thread.sleep( 100 );
            assertEquals( 0, dataSource.getMetrics().availableCount(), "Expected empty pool" );

            try ( Connection c = dataSource.getConnection() ) {
                assertEquals( 3, dataSource.getMetrics().creationCount(), "Expected connection to be created" );

                logger.info( "Performing health check on exhausted pool" );
                assertThrows( SQLException.class, () -> dataSource.isHealthy( false ), "Expected acquisition timeout" );

                logger.info( "Performing health check on exhausted pool, on a new connection" );
                assertTrue( dataSource.isHealthy( true ) );

                assertEquals( 4, dataSource.getMetrics().creationCount(), "Expected connection to be re-used" );
                assertEquals( 1, dataSource.getMetrics().availableCount(), "Expected one connection in pool" );
                assertEquals( 1, dataSource.getMetrics().activeCount(), "Expect single connection in use" );
                assertEquals( 1, dataSource.getMetrics().acquireCount(), "Expect acquisition metric to report a single acquisition" );
                assertEquals( 4, listener.beforeCount(), "Expected validation to have been performed" );

                // use the connection
                c.getSchema();
            }

            assertEquals( 1, dataSource.getMetrics().availableCount(), "Expected connection flush on close" );
        }
    }

    @Test
    @DisplayName( "Perform health checks on poolless data source" )
    void polllessTest() throws SQLException, InterruptedException {
        ValidationCountListener listener = new ValidationCountListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS )
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionValidator( c -> false ) // connections are always invalid
                        .acquisitionTimeout( ofMillis( 100 ) )
                ), listener ) ) {

            assertEquals( 0, dataSource.getMetrics().creationCount(), "Expected empty pool" );

            logger.info( "Performing health check on pool(less)" );
            assertFalse( dataSource.isHealthy( false ) );

            assertEquals( 1, dataSource.getMetrics().creationCount(), "Expected connection in pool" );
            assertEquals( 0, dataSource.getMetrics().activeCount(), "Expected no active connection" );
            assertEquals( 1, dataSource.getMetrics().availableCount(), "Expected no connection in pool" );
            assertEquals( 1, listener.beforeCount(), "Expected validation to have been performed" );
            assertEquals( 0, listener.validCount(), "Expected validation to have been successful" );
            assertEquals( 1, listener.invalidCount(), "Expected validation to have been successful" );

            logger.info( "Performing health check on pool(less), on a new connection" );
            assertFalse( dataSource.isHealthy( true ) );

            assertEquals( 2, dataSource.getMetrics().creationCount(), "Expected connection to be created" );
            assertEquals( 0, dataSource.getMetrics().activeCount(), "Expected no active connection" );
            assertEquals( 1, dataSource.getMetrics().availableCount(), "Expected no connection in pool" );
            assertEquals( 2, listener.beforeCount(), "Expected validation to have been performed" );

            try ( Connection c = dataSource.getConnection() ) {
                assertEquals( 3, dataSource.getMetrics().creationCount(), "Expected connection to be created" );

                logger.info( "Performing health check on exhausted pool(less)" );
                assertThrows( SQLException.class, () -> dataSource.isHealthy( false ), "Expected acquisition timeout" );

                logger.info( "Performing health check on exhausted pool(less), on a new connection" );
                assertFalse( dataSource.isHealthy( true ) );

                assertEquals( 4, dataSource.getMetrics().creationCount(), "Expected connection to be re-used" );
                assertEquals( 0, dataSource.getMetrics().availableCount(), "Expected no connection in pool" );
                assertEquals( 1, dataSource.getMetrics().activeCount(), "Expect single connection in use" );
                assertEquals( 1, dataSource.getMetrics().acquireCount(), "Expect acquisition metric to report a single acquisition" );
                assertEquals( 3, listener.beforeCount(), "Expected validation to have been performed" );

                // use the connection
                c.getSchema();
            }
        }
    }

    @Test
    @DisplayName( "Perform health checks when connection provider throws" )
    void bogusFactoryTest() throws SQLException, InterruptedException {
        ValidationCountListener listener = new ValidationCountListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionFactoryConfiguration( cf -> cf.connectionProviderClass( UnhealthyDataSource.class ) )
                ), listener ) ) {

            assertThrows( SQLException.class, () -> dataSource.isHealthy( true ), "Expected exception from connection provider" );

            assertEquals( 0, dataSource.getMetrics().creationCount(), "Expected no connection to be created" );
            assertEquals( 0, listener.beforeCount(), "Expected no validation to have been performed" );
        }
    }

    // --- //

    private static class ValidationCountListener implements AgroalDataSourceListener {

        private final AtomicInteger before = new AtomicInteger(), valid = new AtomicInteger(), invalid = new AtomicInteger();

        ValidationCountListener() {
        }

        @Override
        public void beforeConnectionValidation(Connection connection) {
            before.incrementAndGet();
        }

        @Override
        public void onConnectionValid(Connection connection) {
            valid.incrementAndGet();
        }

        @Override
        public void onConnectionInvalid(Connection connection) {
            invalid.incrementAndGet();
        }

        public int beforeCount() {
            return before.get();
        }

        public int validCount() {
            return valid.get();
        }

        public int invalidCount() {
            return invalid.get();
        }
    }

    // --- //

    public static class UnhealthyDataSource implements MockDataSource {

        @Override
        public Connection getConnection() throws SQLException {
            throw new SQLException( "Unobtainable" );
        }
    }
}

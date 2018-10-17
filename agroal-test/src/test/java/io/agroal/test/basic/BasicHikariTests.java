// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import static io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation.HIKARI;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static java.lang.System.nanoTime;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class BasicHikariTests {

    private static final Logger logger = getLogger( BasicHikariTests.class.getName() );

    private static final Driver fakeDriver = new FakeDriver();

    @BeforeAll
    public static void setupMockDriver() throws SQLException {
        DriverManager.registerDriver( fakeDriver );
    }

    @AfterAll
    public static void teardown() throws SQLException {
        DriverManager.deregisterDriver( fakeDriver );
    }

    // --- //

    @Test
    @DisplayName( "Mock driver providing fake connections" )
    public void basicConnectionAcquireTest() throws SQLException {
        int VALIDATION_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( HIKARI )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .validationTimeout( ofMillis( VALIDATION_MS ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClassName( fakeDriver.getClass().getName() )
                                .jdbcUrl( "jdbc://" )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            Connection connection = dataSource.getConnection();
            assertEquals( connection.getSchema(), FakeDriver.FakeConnection.FAKE_SCHEMA );
            logger.info( format( "Got schema \"{0}\" from {1}", connection.getSchema(), connection ) );
            connection.close();
        }
    }

    @Test
    @DisplayName( "Connection wrapper in closed state" )
    public void basicConnectionCloseTest() throws SQLException {
        int VALIDATION_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( HIKARI )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .validationTimeout( ofMillis( VALIDATION_MS ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClassName( fakeDriver.getClass().getName() )
                                .jdbcUrl( "jdbc://" )
                        )
                );
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            Connection connection = dataSource.getConnection();

            assertAll( () -> {
                assertFalse( connection.isClosed(), "Expected open connection, but it's closed" );
                assertNotNull( connection.getSchema(), "Expected non null value" );
            } );

            connection.close();

            assertAll( () -> {
                assertThrows( SQLException.class, connection::getSchema );
                assertTrue( connection.isClosed(), "Expected closed connection, but it's open" );
            } );
        }
    }

    @Test
    @DisplayName( "Acquisition timeout" )
    public void basicAcquisitionTimeoutTest() throws SQLException {
        int MAX_POOL_SIZE = 100, ACQUISITION_TIMEOUT_MS = 1000, VALIDATION_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( HIKARI )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( MAX_POOL_SIZE )
                        .acquisitionTimeout( ofMillis( ACQUISITION_TIMEOUT_MS ) )
                        .validationTimeout( ofMillis( VALIDATION_MS ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClassName( fakeDriver.getClass().getName() )
                                .jdbcUrl( "jdbc://" )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {

            for ( int i = 0; i < MAX_POOL_SIZE; i++ ) {
                Connection connection = dataSource.getConnection();
                assertNotNull( connection.getSchema(), "Expected non null value" );
                //connection.close();
            }
            logger.info( format( "Holding all {0} connections from the pool and requesting a new one", MAX_POOL_SIZE ) );

            long start = nanoTime(), timeoutBound = (long) ( ACQUISITION_TIMEOUT_MS * 1.1 );
            assertTimeoutPreemptively( ofMillis( timeoutBound ), () -> assertThrows( SQLException.class, dataSource::getConnection ), "Expecting acquisition timeout" );

            long elapsed = NANOSECONDS.toMillis( nanoTime() - start );
            logger.info( format( "Acquisition timeout after {0}ms - Configuration is {1}ms", elapsed, ACQUISITION_TIMEOUT_MS ) );
            assertTrue( elapsed > ACQUISITION_TIMEOUT_MS, "Acquisition timeout before time" );
        }
    }

    // --- //

    public static class FakeDriver implements MockDriver {
        @Override
        public Connection connect(String url, Properties info) {
            return new FakeConnection();
        }

        private static class FakeConnection implements MockConnection {

            private static final String FAKE_SCHEMA = "skeema";

            @Override
            public String getSchema() {
                return FAKE_SCHEMA;
            }
        }
    }
}

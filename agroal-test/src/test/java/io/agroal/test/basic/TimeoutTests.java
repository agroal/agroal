// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
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
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.lang.System.nanoTime;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class TimeoutTests {

    static final Logger logger = getLogger( TimeoutTests.class.getName() );

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
    @DisplayName( "Acquisition timeout" )
    void basicAcquisitionTimeoutTest() throws SQLException {
        int MAX_POOL_SIZE = 100, ACQUISITION_TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( MAX_POOL_SIZE )
                        .acquisitionTimeout( ofMillis( ACQUISITION_TIMEOUT_MS ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {

            for ( int i = 0; i < MAX_POOL_SIZE; i++ ) {
                Connection connection = dataSource.getConnection();
                assertFalse( connection.isClosed(), "Expected open connection" );
                // connection.close();
            }
            logger.info( format( "Holding all {0} connections from the pool and requesting a new one", MAX_POOL_SIZE ) );

            long start = nanoTime(), timeoutBound = (long) ( ACQUISITION_TIMEOUT_MS * 1.1 );
            assertTimeoutPreemptively( ofMillis( timeoutBound ), () -> assertThrows( SQLException.class, dataSource::getConnection ), "Expecting acquisition timeout" );

            long elapsed = NANOSECONDS.toMillis( nanoTime() - start );
            logger.info( format( "Acquisition timeout after {0}ms - Configuration is {1}ms", elapsed, ACQUISITION_TIMEOUT_MS ) );
            assertTrue( elapsed >= ACQUISITION_TIMEOUT_MS, "Acquisition timeout before time" );
        }
    }

    @Test
    @DisplayName( "Acquisition timeout of new connection" )
    void acquisitionTimeoutOfNewConnectionTest() throws SQLException {
        int ACQUISITION_TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .acquisitionTimeout( ofMillis( ACQUISITION_TIMEOUT_MS ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( SleepyDatasource.class )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {

            SleepyDatasource.setSleep();

            long start = nanoTime(), timeoutBound = (long) ( ACQUISITION_TIMEOUT_MS * 1.1 );
            assertTimeoutPreemptively( ofMillis( timeoutBound ), () -> assertThrows( SQLException.class, dataSource::getConnection ), "Expecting acquisition timeout" );

            long elapsed = NANOSECONDS.toMillis( nanoTime() - start );
            logger.info( format( "Acquisition timeout after {0}ms - Configuration is {1}ms", elapsed, ACQUISITION_TIMEOUT_MS ) );
            assertTrue( elapsed >= ACQUISITION_TIMEOUT_MS, "Acquisition timeout before time" );

            SleepyDatasource.unsetSleep();

            // Try again, to ensure that the Agroal thread has not become stuck after that first getConnection call
            logger.info( "Attempting another getConnection() call" );
            try ( Connection c = dataSource.getConnection() ) {
                assertFalse( c.isClosed(), "Expected a good, healthy connection" );
            }
        }
    }

    // --- //

    public static class SleepyDatasource implements MockDataSource {

        private static boolean doSleep;

        public static void setSleep() {
            doSleep = true;
        }

        public static void unsetSleep() {
            doSleep = false;
        }

        @Override
        public Connection getConnection() throws SQLException {
            if ( !doSleep ) {
                return new SleepyMockConnection();
            }

            try {
                logger.info( "This connection will take a while to get established ..." );
                Thread.sleep( Integer.MAX_VALUE );
            } catch ( InterruptedException e ) {
                logger.info( "Datasource disturbed in it's sleep" );
            }
            throw new SQLException( "I have a bad awakening!" );
        }

        private static class SleepyMockConnection implements MockConnection {
            SleepyMockConnection() {
            }
        }
    }
}

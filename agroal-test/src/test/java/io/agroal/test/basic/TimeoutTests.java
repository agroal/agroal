// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
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
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.lang.System.nanoTime;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

    @Test
    @DisplayName( "Login timeout" )
    void loginTimeoutTest() throws SQLException {
        int LOGIN_TIMEOUT_S = 2;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( LoginTimeoutDatasource.class )
                                .loginTimeout( ofSeconds( LOGIN_TIMEOUT_S ) )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {

            LoginTimeoutDatasource.setTimeout();

            long start = nanoTime(), timeoutBound = LOGIN_TIMEOUT_S * 1500;
            assertTimeoutPreemptively( ofMillis( timeoutBound ), () -> assertThrows( SQLException.class, dataSource::getConnection ), "Expecting login timeout" );

            long elapsed = NANOSECONDS.toMillis( nanoTime() - start );
            logger.info( format( "Login timeout after {0}ms - Configuration is {1}s", elapsed, LOGIN_TIMEOUT_S ) );
            assertTrue( elapsed >= LOGIN_TIMEOUT_S * 1000, "Login timeout before time" );

            LoginTimeoutDatasource.unsetTimeout();

            // Try again, to ensure that the Agroal thread has not become stuck after that first getConnection call
            logger.info( "Attempting another getConnection() call" );
            try ( Connection c = dataSource.getConnection() ) {
                assertFalse( c.isClosed(), "Expected a good, healthy connection" );
            }
        }

        AgroalDataSourceConfigurationSupplier bogusConfiguration = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .acquisitionTimeout( ofSeconds( LOGIN_TIMEOUT_S ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( LoginTimeoutDatasource.class )
                                .loginTimeout( ofSeconds( 2 * LOGIN_TIMEOUT_S ) )
                        )
                );

        OnWarningListener warningListener = new OnWarningListener();
        LoginTimeoutDatasource.setTimeout();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( bogusConfiguration, warningListener ) ) {
            assertTrue( warningListener.getWarning().get(), "Expected a warning on the size of acquisition timeout" );

            logger.info( "Checking datasource health" );
            assertTimeoutPreemptively( ofMillis( LOGIN_TIMEOUT_S * 1500 ), () -> assertThrows( SQLException.class, () -> dataSource.isHealthy( true ) ), "Expecting SQLException on heath check" );
        }
    }

    @Test
    @DisplayName( "Login timeout on initial connections" )
    void loginTimeoutInitialTest() throws SQLException {
        int LOGIN_TIMEOUT_S = 1, INITIAL_SIZE = 5;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( INITIAL_SIZE )
                        .maxSize( INITIAL_SIZE )
                        .acquisitionTimeout( ofSeconds( 2 * LOGIN_TIMEOUT_S ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( LoginTimeoutDatasource.class )
                                .loginTimeout( ofSeconds( LOGIN_TIMEOUT_S ) )
                        )
                );

        LoginTimeoutDatasource.setTimeout();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {

            long start = nanoTime(), timeoutBound = LOGIN_TIMEOUT_S * 2500;
            assertTimeoutPreemptively( ofMillis( timeoutBound ), () -> assertThrows( SQLException.class, dataSource::getConnection ), "Expecting login timeout" );

            long elapsed = NANOSECONDS.toMillis( nanoTime() - start );
            logger.info( format( "Acquisition timeout after {0}ms - Configuration is {1}ms", elapsed, LOGIN_TIMEOUT_S * 2000 ) );
            assertTrue( elapsed >= LOGIN_TIMEOUT_S * 2000, "Acquisition timeout before time" );
            
            assertEquals( 0, dataSource.getMetrics().creationCount(), "Expected no created connection" );
        }
    }

    @Test
    @DisplayName( "Pool-less Login timeout" )
    void poollessLoginTimeoutTest() throws SQLException, InterruptedException {
        int ACQUISITION_TIMEOUT_MS = 1500, LOGIN_TIMEOUT_S = 1; // acquisition timeout > login timeout
        CountDownLatch latch = new CountDownLatch( 1 );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .acquisitionTimeout( ofMillis( ACQUISITION_TIMEOUT_MS ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( LoginTimeoutDatasource.class )
                                .loginTimeout( ofSeconds( LOGIN_TIMEOUT_S ) )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            LoginTimeoutDatasource.unsetTimeout();

            new Thread(() -> {
                try (Connection c = dataSource.getConnection() ) {
                    latch.countDown();
                    assertFalse( c.isClosed(), "Expected good connection" );
                    logger.info( "Holding connection and sleeping for a duration slightly smaller then acquisition timeout" );
                    Thread.sleep( (long) (ACQUISITION_TIMEOUT_MS * 0.8) );
                } catch ( SQLException e ) {
                    fail( "Unexpected exception", e );
                } catch ( InterruptedException e ) {
                    fail( e );
                }
            } ).start();

            // await good connection to poison data source
            assertTrue( latch.await( ACQUISITION_TIMEOUT_MS, MILLISECONDS ) );
            LoginTimeoutDatasource.setTimeout();

            long start = nanoTime(), timeoutBound = ACQUISITION_TIMEOUT_MS + LOGIN_TIMEOUT_S * 1500;
            assertTimeoutPreemptively( ofMillis( timeoutBound ), () -> assertThrows( SQLException.class, dataSource::getConnection ), "Expecting login timeout" );

            long elapsed = NANOSECONDS.toMillis( nanoTime() - start );
            logger.info( format( "Login timeout after {0}ms - Configuration is {1}ms + {2}s", elapsed, ACQUISITION_TIMEOUT_MS, LOGIN_TIMEOUT_S ) );
            assertTrue( elapsed >= ACQUISITION_TIMEOUT_MS * 0.8 + LOGIN_TIMEOUT_S * 1000, "Login timeout before time" );
        }
    }

    // --- //

    private static class OnWarningListener implements AgroalDataSourceListener {

        private final AtomicBoolean warning = new AtomicBoolean( false );

        OnWarningListener() {
        }

        @Override
        public void onWarning(String message) {
            warning.set( true );
        }

        @Override
        public void onWarning(Throwable throwable) {
            warning.set( true );
        }

        AtomicBoolean getWarning() {
            return warning;
        }
    }

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

    public static class LoginTimeoutDatasource implements MockDataSource {

        private static boolean doTimeout;
        private int loginTimeout;

        public static void setTimeout() {
            doTimeout = true;
        }

        public static void unsetTimeout() {
            doTimeout = false;
        }

        @Override
        public Connection getConnection() throws SQLException {
            assertNotEquals( 0, loginTimeout, "Expected login timeout to be set to something" );
            if ( !doTimeout ) {
                return new LoginTimeoutConnection();
            }

            try {
                logger.info( "Pretending to wait for connection to be established ..." );
                Thread.sleep( loginTimeout * 1000L );
                throw new SQLException( "Login timeout after " + loginTimeout + " seconds." );
            } catch ( InterruptedException e ) {
                throw new SQLException( e );
            }
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            loginTimeout = seconds;
        }

        private static class LoginTimeoutConnection implements MockConnection {
            LoginTimeoutConnection() {
            }
        }
    }
}

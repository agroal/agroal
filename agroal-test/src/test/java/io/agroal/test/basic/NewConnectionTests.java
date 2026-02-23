// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.AgroalSecurityProvider;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDataSource;
import io.agroal.test.NoWarningsAgroalListener;
import io.agroal.test.WarningsAgroalListener;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;

import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.NONE;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.READ_COMMITTED;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.READ_UNCOMMITTED;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.REPEATABLE_READ;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.SERIALIZABLE;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.lang.System.nanoTime;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class NewConnectionTests {

    static final Logger logger = getLogger( NewConnectionTests.class.getName() );

    @BeforeAll
    static void setupMockDriver() {
        registerMockDriver( FakeConnection.class );
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
    @DisplayName( "Test connection isolation" )
    void isolationTest() throws SQLException {
        isolation( NONE, Connection.TRANSACTION_NONE );
        isolation( READ_UNCOMMITTED, Connection.TRANSACTION_READ_UNCOMMITTED );
        isolation( READ_COMMITTED, Connection.TRANSACTION_READ_COMMITTED );
        isolation( REPEATABLE_READ, Connection.TRANSACTION_REPEATABLE_READ );
        isolation( SERIALIZABLE, Connection.TRANSACTION_SERIALIZABLE );
    }

    private static void isolation(AgroalConnectionFactoryConfiguration.TransactionIsolation isolation, int level) throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ).connectionFactoryConfiguration( cf -> cf.jdbcTransactionIsolation( isolation ) ) ) ) ) {
            Connection connection = dataSource.getConnection();
            assertEquals( connection.getTransactionIsolation(), level );
            logger.info( format( "Got isolation \"{0}\" from {1}", connection.getTransactionIsolation(), connection ) );
            connection.close();
        }
    }

    @Test
    @DisplayName( "Test connection readOnly status" )
    void readOnlyConnectionTest() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ) ) ) {
            Connection connection = dataSource.getReadOnlyConnection();
            assertTrue( connection.isReadOnly() );
            logger.info( format( "Got readOnly \"{0}\" from {1}", connection.isReadOnly(), connection ) );
            connection.close();
        }
    }

    @Test
    @DisplayName( "Test connection autoCommit status" )
    void autoCommitTest() throws SQLException {
        autocommit( false );
        autocommit( true );
    }

    private static void autocommit(boolean autoCommit) throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ).connectionFactoryConfiguration( cf -> cf.autoCommit( autoCommit ) ) ) ) ) {
            Connection connection = dataSource.getConnection();
            assertEquals( connection.getAutoCommit(), autoCommit );
            logger.info( format( "Got autoCommit \"{0}\" from {1}", connection.getAutoCommit(), connection ) );
            connection.close();
        }
    }

    @Test
    @DisplayName( "Test faulty URL setter" )
    void faultyUrlTest() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( FaultyUrlDataSource.class )
                                .jdbcUrl( "the_url" )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new NoWarningsAgroalListener() ) ) {
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some URL" );
            }
        }
    }

    @Test
    @DisplayName( "Properties injection test" )
    void propertiesInjectionTest() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( PropertiesDataSource.class )
                                .jdbcProperty( "connectionProperties", "url=some_url;custom=some_custom_prop" )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new NoWarningsAgroalListener() ) ) {
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some URL" );
            }
        }
    }

    @Test
    @DisplayName( "Multiple methods injection test" )
    void multipleMethodsInjectionTest() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( MultipleSettersDataSource.class )
                                .jdbcProperty( "someString", "some_value" )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new NoWarningsAgroalListener() ) ) {
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c + " with some someString set" );
            }
        }
    }

    @Test
    @DisplayName( "Exception on new connection" )
    void newConnectionExceptionTest() throws SQLException {
        int INITIAL_SIZE = 3, INITIAL_TIMEOUT_MS = 100 * INITIAL_SIZE;
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .initialSize( INITIAL_SIZE )
                        .connectionFactoryConfiguration( cf -> cf
                                .credential( new Object() )
                                .addSecurityProvider( new ExceptionSecurityProvider() )
                        )
                );

        WarningsAgroalListener warningsListener = new WarningsAgroalListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningsListener ) ) {
            Thread.sleep( INITIAL_TIMEOUT_MS );

            assertEquals( 0, dataSource.getMetrics().creationCount() );
            assertEquals( 0, warningsListener.warningCount(), "Unexpected warning(s)" );
            assertEquals( INITIAL_SIZE, warningsListener.failuresCount(), "Expected warning(s)" );
        } catch ( InterruptedException e ) {
            fail( "Interrupt " + e );
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @DisplayName( "New connection retries" )
    void newConnectionRetries(boolean poolless) throws SQLException {
        int RETRIES = 4, INTERVAL_MS = 500;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( poolless ? AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS : AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )  .establishmentRetryAttempts( RETRIES )
                        .establishmentRetryInterval( Duration.ofMillis( INTERVAL_MS ) )
                        .connectionFactoryConfiguration( cf -> cf.connectionProviderClass( BrokenDataSource.class ) ) );

        CreationAttemptsListener attemptsListener = new CreationAttemptsListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, attemptsListener ) ) {
            long start = nanoTime();
            try ( Connection c = dataSource.getConnection() ) {
                fail( "Not expecting to get a connection but got: " + c );
            } catch ( SQLException e ) {
                long elapsed = ( nanoTime() - start ) / 1_000_000;
                assertEquals( 1 + RETRIES, attemptsListener.creationCount.get(), "Did not attempt to establish a connection the expected number of times" );
                assertTrue( elapsed >= RETRIES * INTERVAL_MS, "Elapsed time of " + elapsed + " is too short" );
                assertTrue( elapsed <= ( RETRIES + 1 ) * INTERVAL_MS, "Elapsed time of " + elapsed + " is too long" );
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @DisplayName( "AcquisitionTimeout with connection retries" )
    void acquisitionTimeoutWithConnectionRetries(boolean poolless) throws SQLException {
        int RETRIES = 10, INTERVAL_MS = 500, TIMEOUT = 2000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( poolless ? AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS : AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .establishmentRetryAttempts( RETRIES )
                        .establishmentRetryInterval( Duration.ofMillis( INTERVAL_MS ) )
                        .acquisitionTimeout( Duration.ofSeconds( 2 ) )
                        .connectionFactoryConfiguration( cf -> cf.connectionProviderClass( BrokenDataSource.class ).jdbcProperty( "waitTime", "200" ) ) );

        CreationAttemptsListener attemptsListener = new CreationAttemptsListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, attemptsListener ) ) {
            long start = nanoTime();
            try ( Connection c = dataSource.getConnection() ) {
                fail( "Not expecting to get a connection but got: " + c );
            } catch ( SQLException e ) {
                long elapsed = ( nanoTime() - start ) / 1_000_000;
                assertEquals( TIMEOUT / INTERVAL_MS, attemptsListener.creationCount.get(), "Did not attempt to establish a connection the expected number of times" );
                assertTrue( elapsed >= TIMEOUT, "Elapsed time of " + elapsed + " is too short" );
                assertTrue( elapsed <= TIMEOUT + INTERVAL_MS, "Elapsed time of " + elapsed + " is too long" );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @DisplayName( "New connection retries default values" )
    void defaultConnectionRetries(boolean poolless) throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( poolless ? AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS : AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionFactoryConfiguration( cf -> cf.connectionProviderClass( BrokenDataSource.class ) ) );

        CreationAttemptsListener defaultAttemptsListener = new CreationAttemptsListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, defaultAttemptsListener ) ) {
            long start = nanoTime();
            try ( Connection c = dataSource.getConnection() ) {
                fail( "Not expecting to get a connection but got: " + c );
            } catch ( SQLException e ) {
                long elapsed = nanoTime() - start;
                assertEquals( 2, defaultAttemptsListener.creationCount.get(), "Did not attempt to establish a connection the expected number of times" );
                assertTrue( elapsed >= Duration.ofSeconds( 1 ).toNanos(), "Elapsed time of " + elapsed + " is too short" );
                assertTrue( elapsed <= Duration.ofSeconds( 2 ).toNanos(), "Elapsed time of " + elapsed + " is too long" );
            }
        }
    }

    // --- //

        public static class BrokenDataSource implements MockDataSource {

        private long waitTime = 0;

        public void setWaitTime(long time) {
            waitTime = time;
        }

        @Override
        public Connection getConnection() throws SQLException {
            LockSupport.parkNanos( waitTime * 1_000_000 );
            throw new SQLException( "This datasource is completely borken !!" );
        }
    }

    public static class FaultyUrlDataSource implements MockDataSource {

        private String url;

        public void setURL(String url) {
            this.url = url;
        }

        @Override
        public Connection getConnection() throws SQLException {
            assertNotNull( url, "Expected URL to be set before getConnection()" );
            return new MockConnection.Empty();
        }
    }

    public static class PropertiesDataSource implements MockDataSource {

        private Properties connectionProperties;

        public void setConnectionProperties(Properties properties) {
            connectionProperties = properties;
        }

        @Override
        public Connection getConnection() throws SQLException {
            assertEquals( "some_url", connectionProperties.getProperty( "url" ), "Expected URL property to be set before getConnection()" );
            assertEquals( "some_custom_prop", connectionProperties.getProperty( "custom" ), "Expected Custom property to be set before getConnection()" );
            assertNull( connectionProperties.getProperty( "connectionProperties" ), "Not expecting property to be set before getConnection()" );
            return new MockConnection.Empty();
        }
    }

    public static class MultipleSettersDataSource implements MockDataSource {

        private String some = "default";

        @Deprecated
        public void setSomeString(String ignore) {
            some = "string_method";
        }
        
        public void setSomeString(char[] chars) {
            some = new String( chars );
        }

        @Override
        public Connection getConnection() throws SQLException {
            assertEquals( "some_value", some, "Expected property to be set before getConnection()" );
            return new MockConnection.Empty();
        }
    }

    public static class ExceptionSecurityProvider implements AgroalSecurityProvider {

        @Override
        public Properties getSecurityProperties(Object securityObject) {
            throw new RuntimeException( "SecurityProvider throws!" );
        }
    }

    public static class FakeConnection implements MockConnection {

        private int isolation;
        private boolean readOnly;
        private boolean autoCommit;

        @Override
        public int getTransactionIsolation() {
            return isolation;
        }

        @Override
        public void setTransactionIsolation(int level) {
            isolation = level;
        }

        @Override
        public boolean isReadOnly() {
            return readOnly;
        }

        @Override
        public void setReadOnly(boolean readOnly) {
            this.readOnly = readOnly;
        }

        @Override
        public boolean getAutoCommit() {
            return autoCommit;
        }

        @Override
        public void setAutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
        }
    }

    public static class CreationAttemptsListener implements AgroalDataSourceListener {

        private final AtomicInteger creationCount = new AtomicInteger();

        @Override
        public void beforeConnectionCreation() {
            creationCount.incrementAndGet();
        }

        @Override
        public void onInfo(String message) {
            logger.info( message );
        }
    }
}

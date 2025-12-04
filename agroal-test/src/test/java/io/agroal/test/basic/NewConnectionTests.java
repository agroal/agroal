// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.AgroalSecurityProvider;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.NONE;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.READ_COMMITTED;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.READ_UNCOMMITTED;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.REPEATABLE_READ;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.SERIALIZABLE;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    // --- //

    public static class WarningsAgroalListener implements AgroalDataSourceListener {

        private final AtomicInteger warnings = new AtomicInteger(), failures = new AtomicInteger();

        @Override
        public void onWarning(String message) {
            warnings.getAndIncrement();
            logger.warning( "Unexpected WARN: " + message );
        }

        @Override
        public void onWarning(Throwable throwable) {
            warnings.getAndIncrement();
            logger.warning( "Unexpected WARN" + throwable.getMessage() );
        }

        @Override
        public void onConnectionCreationFailure(SQLException sqlException) {
            failures.getAndIncrement();
            logger.info( "Expected callback " + sqlException.getMessage() );
        }

        public int warningCount() {
            return warnings.get();
        }

        public int failuresCount() {
            return failures.get();
        }
    }

    public static class NoWarningsAgroalListener implements AgroalDataSourceListener {

        @Override
        public void onWarning(String message) {
            fail( "Unexpected warning " + message );
        }

        @Override
        public void onWarning(Throwable throwable) {
            fail( "Unexpected warning " + throwable.getMessage() );
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
        public boolean getAutoCommit() {
            return autoCommit;
        }

        @Override
        public void setAutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
        }
    }
}

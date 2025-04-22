// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
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
import java.sql.SQLWarning;
import java.util.logging.Logger;

import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.NONE;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.READ_COMMITTED;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.READ_UNCOMMITTED;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.REPEATABLE_READ;
import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.SERIALIZABLE;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter.emptyExceptionSorter;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter.fatalExceptionSorter;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class ConnectionResetTests {

    private static final Logger logger = getLogger( ConnectionResetTests.class.getName() );

    @BeforeAll
    static void setupMockDriver() {
        registerMockDriver( FakeConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Test connection isolation remains the same after being changed" )
    void isolationTest() throws SQLException {
        isolation( NONE, Connection.TRANSACTION_NONE );
        isolation( READ_UNCOMMITTED, Connection.TRANSACTION_READ_UNCOMMITTED );
        isolation( READ_COMMITTED, Connection.TRANSACTION_READ_COMMITTED );
        isolation( REPEATABLE_READ, Connection.TRANSACTION_REPEATABLE_READ );
        isolation( SERIALIZABLE, Connection.TRANSACTION_SERIALIZABLE );
    }

    private static void isolation(AgroalConnectionFactoryConfiguration.TransactionIsolation isolation, int level) throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration(
                cp -> cp.maxSize( 1 ).connectionFactoryConfiguration( cf -> cf.jdbcTransactionIsolation( isolation ) )
        ) ) ) {
            Connection connection = dataSource.getConnection();
            assertEquals( connection.getTransactionIsolation(), level );
            connection.setTransactionIsolation( Connection.TRANSACTION_NONE );
            connection.close();

            connection = dataSource.getConnection();
            assertEquals( connection.getTransactionIsolation(), level );
            logger.info( format( "Got isolation \"{0}\" from {1}", connection.getTransactionIsolation(), connection ) );
            connection.close();
        }
    }

    // --- //

    @Test
    @DisplayName( "Test connection with custom transaction isolation level" )
    void customIsolationTest() throws SQLException {
        int level = 42;
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration(
                cp -> cp.maxSize( 1 ).connectionFactoryConfiguration( cf -> cf.jdbcTransactionIsolation( level ) )
        ) ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                assertEquals( connection.getTransactionIsolation(), level );
            }
        }
    }

    // --- //

    @Test
    @DisplayName( "Test connection reset with default (driver) transaction isolation level" )
    void defaultIsolationResetTest() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration(
                cp -> cp.maxSize( 1 )
        ) ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                assertEquals( connection.getTransactionIsolation(), FakeConnection.DEFAULT_ISOLATION );
                connection.setTransactionIsolation( Connection.TRANSACTION_SERIALIZABLE );
                assertEquals( connection.getTransactionIsolation(), Connection.TRANSACTION_SERIALIZABLE );
            }
            try ( Connection connection = dataSource.getConnection() ) {
                assertEquals( connection.getTransactionIsolation(), FakeConnection.DEFAULT_ISOLATION );
            }
        }
    }

    // --- //

    @Test
    @DisplayName( "Test connection reset with default read-only" )
    void defaultReadOnlyTest() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration(
                cp -> cp.maxSize( 1 ) ) ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                 assertFalse( connection.isReadOnly() );
                 connection.setReadOnly( true );
                 assertTrue( connection.isReadOnly() );
            }
            try ( Connection connection = dataSource.getConnection() ) {
                assertFalse( connection.isReadOnly() );
            }
        }
    }

    @Test
    @DisplayName( "Test read-only connection reset" )
    void readOnlyTest() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration(
                cp -> cp.maxSize( 1 ).connectionFactoryConfiguration( AgroalConnectionFactoryConfigurationSupplier::readOnly ) ) ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                assertTrue( connection.isReadOnly() );
                assertThrows( SQLException.class, () -> connection.setReadOnly( false ) );
                assertTrue( connection.isReadOnly() );
            }
            try ( Connection connection = dataSource.getConnection() ) {
                assertTrue( connection.isReadOnly() );
            }
        }
    }

    // --- //

    @Test
    @DisplayName( "Test connection autoCommit status remains the same after being changed" )
    void autoCommitTest() throws SQLException {
        autocommit( false );
        autocommit( true );
    }

    private static void autocommit(boolean autoCommit) throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration(
                cp -> cp.maxSize( 1 ).connectionFactoryConfiguration( cf -> cf.autoCommit( autoCommit ) )
        ) ) ) {
            Connection connection = dataSource.getConnection();
            assertEquals( connection.getAutoCommit(), autoCommit );
            connection.setAutoCommit( !autoCommit );
            connection.close();

            connection = dataSource.getConnection();
            assertEquals( connection.getAutoCommit(), autoCommit );
            logger.info( format( "Got autoCommit \"{0}\" from {1}", connection.getAutoCommit(), connection ) );
            connection.close();
        }
    }

    // --- //

    @Test
    @DisplayName( "Test connection with warnings" )
    void warningsTest() throws SQLException {
        warnings( false );
        warnings( true );
    }

    private static void warnings(boolean fatal) throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().metricsEnabled()
                .connectionPoolConfiguration( cp -> cp.maxSize( 1 ).exceptionSorter( fatal ? fatalExceptionSorter() : emptyExceptionSorter() ) )
        ) ) {
            Connection connection = dataSource.getConnection();
            assertNotNull( connection.getWarnings() );
            connection.close();

            connection = dataSource.getConnection();
            assertEquals( fatal ? 2 : 1, dataSource.getMetrics().creationCount() );
            assertEquals( fatal, connection.getWarnings() != null ); // checks if warnings were cleared
            connection.close();
        }
    }

    // --- //

    @Test
    @DisplayName( "Test exception during reset" )
    void resetExceptionTest() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().metricsEnabled().connectionPoolConfiguration(
                        cp -> cp.maxSize( 1 ).connectionFactoryConfiguration( cf -> cf.connectionProviderClass( SneakyDataSource.class ) ) ) ) ) {
            try ( Connection c = dataSource.getConnection() ) {
                assertThrows( SQLException.class, c::getWarnings );
            }
        }
    }

    // --- //

    public static class FakeConnection implements MockConnection {

        private static final int DEFAULT_ISOLATION = 99;

        private int isolation = DEFAULT_ISOLATION;
        private boolean autoCommit;
        private boolean readOnly;
        private boolean warnings = true;

        @Override
        @SuppressWarnings( "MagicConstant" )
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

        @Override
        public boolean isReadOnly() throws SQLException {
            return readOnly;
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            this.readOnly = readOnly;
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return warnings ? new SQLWarning( "SQL Warning" ) : null;
        }

        @Override
        public void clearWarnings() throws SQLException {
            warnings = false;
        }
    }

    public static class SneakyDataSource implements MockDataSource {

        @Override
        public Connection getConnection() throws SQLException {
            return new SneakyConnection();
        }

    }

    public static class SneakyConnection implements MockConnection {
        @Override
        public SQLWarning getWarnings() throws SQLException {
            // getWarnings method is called on connection return. Need to make sure the pool is usable in that scenario.
            throw new SQLException("This one is sneaky!");
        }
    }

}

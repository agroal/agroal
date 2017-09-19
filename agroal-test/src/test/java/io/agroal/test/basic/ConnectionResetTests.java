// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
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

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class ConnectionResetTests {

    private static final Logger logger = getLogger( ConnectionResetTests.class.getName() );

    @BeforeAll
    public static void setupMockDriver() {
        registerMockDriver( FakeConnection.class );
    }

    @AfterAll
    public static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Test connection isolation remains the same after being changed" )
    public void isolationTest() throws SQLException {
        isolation( NONE, Connection.TRANSACTION_NONE );
        isolation( READ_UNCOMMITTED, Connection.TRANSACTION_READ_UNCOMMITTED );
        isolation( READ_COMMITTED, Connection.TRANSACTION_READ_COMMITTED );
        isolation( REPEATABLE_READ, Connection.TRANSACTION_REPEATABLE_READ );
        isolation( SERIALIZABLE, Connection.TRANSACTION_SERIALIZABLE );
    }

    private void isolation(AgroalConnectionFactoryConfiguration.TransactionIsolation isolation, int level) throws SQLException {
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

    @Test
    @DisplayName( "Test connection autoCommit status remains the same after being changed" )
    public void autoCommitTest() throws SQLException {
        autocommit( false );
        autocommit( true );
    }

    private void autocommit(boolean autoCommit) throws SQLException {
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

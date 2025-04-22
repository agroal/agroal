// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockConnection;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
class ReadOnlyTests {

    private static final Logger logger = getLogger( ReadOnlyTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver( ReadOnlyAwareConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Test default read-only with transaction" )
    void readOnlyDefaultTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                 logger.info( format( "Got connection {0} without tx", connection ) );

                 assertFalse( connection.isReadOnly() );
                 connection.setReadOnly( true );
                 assertTrue( connection.isReadOnly() );
                 connection.setReadOnly( false );
                 assertFalse( connection.isReadOnly() );
            }

            txManager.begin();

            try ( Connection connection = dataSource.getConnection() ) {
                logger.info( format( "Got connection {0} with tx", connection ) );

                assertFalse( connection.isReadOnly() );
                assertThrows( SQLException.class, () -> connection.setReadOnly( true ) );
                assertThrows( SQLException.class, () -> connection.setReadOnly( false ) );
                assertFalse( connection.isReadOnly() );
            }

            txManager.commit();
        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    @Test
    @DisplayName( "Test read-only connection with transaction" )
    void readOnlyTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                        .connectionFactoryConfiguration( AgroalConnectionFactoryConfigurationSupplier::readOnly )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                logger.info( format( "Got connection {0} without tx", connection ) );

                assertTrue( connection.isReadOnly() );
                assertThrows( SQLException.class, () -> connection.setReadOnly( false ) );
                assertTrue( connection.isReadOnly() );
                connection.setReadOnly( true );
                assertTrue( connection.isReadOnly() );
            }

            txManager.begin();

            try ( Connection connection = dataSource.getConnection() ) {
                logger.info( format( "Got connection {0} with tx", connection ) );

                assertTrue( connection.isReadOnly() );
                assertThrows( SQLException.class, () -> connection.setReadOnly( true ) );
                assertThrows( SQLException.class, () -> connection.setReadOnly( false ) );
                assertTrue( connection.isReadOnly() );
            }

            txManager.commit();
        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    // --- //

    public static class ReadOnlyAwareConnection implements MockConnection {

        private boolean autoCommit;
        private boolean readOnly;


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

    }

}

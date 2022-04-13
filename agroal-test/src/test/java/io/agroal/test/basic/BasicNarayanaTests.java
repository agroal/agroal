// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.TransactionRequirement.STRICT;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.TransactionRequirement.WARN;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class BasicNarayanaTests {

    static final Logger logger = getLogger( BasicNarayanaTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver();
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Connection acquire test" )
    @SuppressWarnings( "JDBCResourceOpenedButNotSafelyClosed" )
    void basicConnectionAcquireTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf.autoCommit( true ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            txManager.begin();

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            assertAll( () -> {
                assertThrows( SQLException.class, () -> connection.setAutoCommit( true ) );
                assertFalse( connection.getAutoCommit(), "Expect connection to have autocommit not set" );
                // TODO: comparing toString is brittle. Find a better way to make sure the underlying physical connection is the same.
                assertEquals( connection.toString(), dataSource.getConnection().toString(), "Expect the same connection under the same transaction" );
            } );

            txManager.commit();

            assertTrue( connection.isClosed() );
        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    @Test
    @DisplayName( "Basic rollback test" )
    void basicRollbackTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            txManager.begin();

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            txManager.rollback();

            assertTrue( connection.isClosed() );
        } catch ( NotSupportedException | SystemException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    @Test
    @DisplayName( "Multiple close test" )
    void multipleCloseTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {

            // there is a call to connection#close in the try-with-resources block and another on the callback from the transaction#commit()
            try ( Connection connection = dataSource.getConnection() ) {
                logger.info( format( "Got connection {0}", connection ) );
                try {
                    txManager.begin();
                    txManager.commit();
                } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
                    fail( "Exception: " + e.getMessage() );
                }
            }
        }
    }

    @Test
    @DisplayName( "Transaction required tests" )
    void transactionRequiredTests() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                ), new NoWarningsListener() ) ) {
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection " + c );
            }
        }

        WarningListener warningListener = new WarningListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                        .transactionRequirement( WARN )
                ), warningListener ) ) {
            try ( Connection c = dataSource.getConnection() ) {
                assertEquals( 1, warningListener.getWarnings(), "Expected a warning message" );
                logger.info( "Got connection with warning :" + c );
            }
        }

        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                        .transactionRequirement( STRICT )
                ) ) ) {
            assertThrows( SQLException.class, dataSource::getConnection );

            // Make sure connection is available after getConnection() throws
            txManager.begin();
            try ( Connection c = dataSource.getConnection() ) {
                logger.info( "Got connection with tx :" + c );
            }
            txManager.rollback();
        } catch ( SystemException | NotSupportedException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    // --- //
    private static class NoWarningsListener implements AgroalDataSourceListener {

        @SuppressWarnings( "WeakerAccess" )
        NoWarningsListener() {
        }

        @Override
        public void onWarning(String message) {
            fail( "Got warning: " + message );
        }

        @Override
        public void onWarning(Throwable throwable) {
            fail( "Got warning: " + throwable.getMessage() );
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    private static class WarningListener implements AgroalDataSourceListener {

        private int warnings;

        WarningListener() {
        }

        @Override
        public void onWarning(Throwable throwable) {
            logger.warning( throwable.getMessage() );
            warnings++;
        }

        int getWarnings() {
            return warnings;
        }
    }
}

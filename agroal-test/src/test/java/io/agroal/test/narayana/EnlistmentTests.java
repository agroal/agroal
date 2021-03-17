// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.BaseXAResource;
import io.agroal.narayana.ConnectableLocalXAResource;
import io.agroal.narayana.LocalXAResource;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockXADataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
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
public class EnlistmentTests {

    private static final Logger logger = getLogger( EnlistmentTests.class.getName() );

    @BeforeAll
    public static void setup() {
        registerMockDriver();
    }

    @AfterAll
    public static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Enroll connection after previous connection close test" )
    public void enrollConnectionCloseTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 10 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .autoCommit( true ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            txManager.begin();

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );
            String connectionToString = connection.toString();
            connection.close();

            Connection secondConnection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", secondConnection ) );

            // TODO: comparing toString is brittle. Find a better way to make sure the underlying physical connection is the same.
            assertEquals( connectionToString, secondConnection.toString(), "Expect the same connection under the same transaction" );
            assertFalse( secondConnection.getAutoCommit(), "AutoCommit temporarily disabled in enlisted connection" );
            secondConnection.close();

            txManager.commit();

            assertTrue( connection.isClosed() );
            assertTrue( secondConnection.isClosed() );
        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    @Test
    @DisplayName( "Connection outside the scope of a transaction test" )
    public void connectionOutsideTransactionTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );
            connection.close();
            assertTrue( connection.isClosed() );
        }
    }

    @Test
    @DisplayName( "Deferred enlistment test" )
    public void deferredEnlistmentTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            try {
                txManager.begin();
                assertThrows( SQLException.class, connection::createStatement );
                logger.info( format( "Call to a method on the connection thrown a SQLException" ) );
                txManager.commit();

                assertFalse( connection.isClosed(), "Not expecting the connection to be close since it was not enrolled into the transaction" );
            } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
                fail( "Exception: " + e.getMessage() );
            }
        }
    }

    @Test
    @DisplayName( "De-enlistment test" )
    public void deEnlistmentTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {

            try {
                txManager.begin();
                Connection connection = dataSource.getConnection();
                logger.info( format( "Got connection {0}", connection ) );
                connection.createStatement().close();

                Transaction transaction = txManager.suspend();
                assertThrows( SQLException.class, connection::createStatement );
                logger.info( format( "Call to a method on the connection thrown a SQLException" ) );
                txManager.resume( transaction );
                
                assertFalse( connection.isClosed(), "Not expecting the connection to be close since the transaction didn't complete" );
                connection.createStatement().close();
                txManager.commit();
            } catch ( NotSupportedException | SystemException | HeuristicMixedException | HeuristicRollbackException | RollbackException | InvalidTransactionException e ) {
                fail( "Exception: " + e.getMessage() );
            }
        }
    }

    @Test
    @DisplayName( "Test the type of enlisted resource" )
    public void enlistedResourceTypeTest() throws SQLException, SystemException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfiguration regularConfiguration = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, null, false ) )
                ).get();

        AgroalDataSourceConfiguration connectableConfiguration = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, null, true ) )
                ).get();

        AgroalDataSourceConfiguration xaConfiguration = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, null, true ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( MockXADataSource.Empty.class )
                        )
                ).get();

        verifyEnlistedResourceType( regularConfiguration, txManager, "regular", LocalXAResource.class );

        verifyEnlistedResourceType( connectableConfiguration, txManager, "connectable", ConnectableLocalXAResource.class );

        verifyEnlistedResourceType( xaConfiguration, txManager, "xa", BaseXAResource.class );
    }

    private void verifyEnlistedResourceType(AgroalDataSourceConfiguration configuration, TransactionManager txManager, String type, Class<?> resourceClass) throws SQLException, SystemException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configuration ) ) {
            txManager.begin();
            try ( Connection connection = dataSource.getConnection() ) {
                logger.info( format( "Got {0} connection {1}", type, connection ) );
                com.arjuna.ats.jta.transaction.Transaction tx = (com.arjuna.ats.jta.transaction.Transaction) txManager.getTransaction();
                assertEquals( 1, tx.getResources().size(), "Expected only one enlisted resource" );
                assertTrue( tx.getResources().keySet().stream().allMatch( resourceClass::isInstance ) );
            }
            txManager.commit();
        } catch ( HeuristicMixedException | NotSupportedException | HeuristicRollbackException | RollbackException e ) {
            txManager.rollback();
            fail( "Exception: " + e.getMessage() );
        }
    }
}

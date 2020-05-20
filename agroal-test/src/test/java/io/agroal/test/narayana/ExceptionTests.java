// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.transaction.TransactionAware;
import io.agroal.narayana.NarayanaTransactionIntegration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.transaction.xa.XAResource;
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
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class ExceptionTests {

    private static final Logger logger = getLogger( ExceptionTests.class.getName() );

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
    @DisplayName( "transaction integration get throws test" )
    public void testGetThrows() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new GetThrows( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            txManager.begin();
            try ( Connection connection = dataSource.getConnection() ) {
                fail( "Unexpected got connection: " + connection );
                txManager.commit();
            } catch ( SQLException e ) {
                txManager.rollback();
                
                assertEquals( 0, dataSource.getMetrics().acquireCount() );
                assertEquals( 0, dataSource.getMetrics().activeCount() );
                assertEquals( 0, dataSource.getMetrics().availableCount() );
                assertEquals( 0, dataSource.getMetrics().creationCount() );
            }
        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    @Test
    @DisplayName( "transaction integration associate throws test" )
    public void testAssociateThrows() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new AssociateThrows( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            txManager.begin();
            try ( Connection connection = dataSource.getConnection() ) {
                fail( "Unexpected got connection: " + connection );
                txManager.commit();
            } catch ( SQLException e ) {
                txManager.rollback();
                
                assertEquals( 0, dataSource.getMetrics().acquireCount() );
                assertEquals( 0, dataSource.getMetrics().activeCount() );
                assertEquals( 1, dataSource.getMetrics().availableCount() );
                assertEquals( 1, dataSource.getMetrics().creationCount() );
            }
        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    @Test
    @DisplayName( "transaction integration disassociate throws test" )
    public void testDisassociateThrows() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new DisssociateThrows( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            txManager.begin();
            try ( Connection connection = dataSource.getConnection() ) {
                logger.info( format( "Got connection {0}", connection ) );
                txManager.commit();
            } catch ( SQLException e ) {
                fail( "Should got a connection" );
                txManager.rollback();
            }

            assertEquals( 1, dataSource.getMetrics().acquireCount() );
            assertEquals( 0, dataSource.getMetrics().activeCount() );
            assertEquals( 1, dataSource.getMetrics().availableCount() );
            assertEquals( 1, dataSource.getMetrics().creationCount() );

        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    @Test
    @DisplayName( "test leak when getConnection() in rollback state" )
    public void testBogusApp() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            txManager.begin();
            txManager.setRollbackOnly();
            try ( Connection connection = dataSource.getConnection() ) {
                fail( "Should not have got a connection, but got " + connection );
            } catch ( SQLException e ) {
                assertEquals( 0, dataSource.getMetrics().acquireCount() );
                assertEquals( 0, dataSource.getMetrics().activeCount() );
                assertEquals( 1, dataSource.getMetrics().availableCount() );
                assertEquals( 1, dataSource.getMetrics().creationCount() );
            }
            txManager.rollback();
        } catch ( NotSupportedException | SystemException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    // --- //

    private static class GetThrows extends NarayanaTransactionIntegration {

        public GetThrows(TransactionManager transactionManager, TransactionSynchronizationRegistry transactionSynchronizationRegistry) {
            super( transactionManager, transactionSynchronizationRegistry );
        }

        @Override
        public TransactionAware getTransactionAware() throws SQLException {
            throw new SQLException();
        }
    }

    private static class AssociateThrows extends NarayanaTransactionIntegration {

        public AssociateThrows(TransactionManager transactionManager, TransactionSynchronizationRegistry transactionSynchronizationRegistry) {
            super( transactionManager, transactionSynchronizationRegistry );
        }

        @Override
        public void associate(TransactionAware transactionAware, XAResource xaResource) throws SQLException {
            throw new SQLException();
        }
    }

    private static class DisssociateThrows extends NarayanaTransactionIntegration {

        public DisssociateThrows(TransactionManager transactionManager, TransactionSynchronizationRegistry transactionSynchronizationRegistry) {
            super( transactionManager, transactionSynchronizationRegistry );
        }

        @Override
        public boolean disassociate(TransactionAware connection) throws SQLException {
            throw new SQLException();
        }
    }

}

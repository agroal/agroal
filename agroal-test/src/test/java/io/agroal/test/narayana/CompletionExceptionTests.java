// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockConnection;
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

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class CompletionExceptionTests {

    private static final Logger logger = getLogger( CompletionExceptionTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver( CompletionExceptionConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Commit exception test" )
    void testCommitException() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                ) ) ) {
            txManager.begin();

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            txManager.commit();
            fail( "Expected exception on commit" );
        } catch ( NotSupportedException | SystemException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Unexpected Exception: " + e.getMessage() );
        } catch ( RollbackException e ) {
            logger.info( "Got expected RollbackException: " + e.getMessage() );

            assertThrows( IllegalStateException.class, txManager::commit, "Expect no transaction" );
        }
    }

    @Test
    @DisplayName( "Rollback exception test" )
    void testRollbackException() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                ) ) ) {
            txManager.begin();

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            txManager.rollback();
            logger.info( "Rollback succeeds on connection exception" );
        } catch ( NotSupportedException | SystemException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    // --- //

    public static class CompletionExceptionConnection implements MockConnection {

        @Override
        public void commit() throws SQLException {
            throw new SQLException( "Can't commit!" );
        }

        @Override
        public void rollback() throws SQLException {
            throw new SQLException( "Can't rollback!" );
        }
    }
}

// Copyright (C) 2026 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.transaction.NotSupportedException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests that acquiring a connection while the transaction is in {@code TRANSACTION_ROLLING_BACK}
 * throws {@link SQLException} instead of silently returning a non-enlisted connection with autocommit enabled.
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class RollingBackEnlistmentTests {

    private static final Logger logger = getLogger( RollingBackEnlistmentTests.class.getName() );

    // Latches used to coordinate between the reaper thread (performing rollback) and the test thread
    static final CountDownLatch rollbackStarted = new CountDownLatch( 1 );
    static final CountDownLatch rollbackProceed = new CountDownLatch( 1 );

    @BeforeAll
    static void setup() {
        registerMockDriver( SlowRollbackConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Connection acquired during TRANSACTION_ROLLING_BACK should throw instead of returning non-enlisted connection" )
    void connectionDuringRollingBackTest() {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 2 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .autoCommit( true ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new SilentWarningsListener() ) ) {
            txManager.setTransactionTimeout( 1 );
            txManager.begin();

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got enlisted connection {0}", connection ) );
            assertFalse( connection.getAutoCommit(), "Enlisted connection should have autocommit disabled" );

            // Wait for the reaper to timeout the transaction and start rolling back.
            // The mock connection's rollback() signals rollbackStarted and then blocks on rollbackProceed,
            // keeping the transaction in TRANSACTION_ROLLING_BACK while we attempt to acquire another connection.
            logger.info( "Waiting for the reaper thread to start rolling back the transaction..." );
            if ( !rollbackStarted.await( 5, TimeUnit.SECONDS ) ) {
                fail( "Timed out waiting for rollback to start" );
            }

            // The transaction is now in TRANSACTION_ROLLING_BACK.
            // Before the fix, getTransactionAware() returned null for this state (it was mapped to TRANSACTION_COMPLETING),
            // causing the pool to hand out a brand new, non-enlisted connection with autocommit=true.
            // After the fix, getTransactionAware() throws SQLException for TRANSACTION_ROLLING_BACK.
            logger.info( "Transaction is rolling back, attempting to acquire a second connection..." );
            assertThrows( SQLException.class, dataSource::getConnection,
                    "Should not be able to acquire a connection while the transaction is rolling back" );
        } catch ( SystemException | NotSupportedException | SQLException e ) {
            fail( e );
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            fail( e );
        } finally {
            // Allow the rollback to complete so the reaper thread is not stuck
            rollbackProceed.countDown();
            try {
                txManager.rollback();
            } catch ( SystemException e ) {
                // transaction may have already completed
            }
        }
    }

    // --- //

    /**
     * Mock connection that blocks during {@code rollback()} to keep the transaction in {@code STATUS_ROLLING_BACK},
     * giving the test thread a window to attempt acquiring another connection in that state.
     */
    public static class SlowRollbackConnection implements MockConnection {

        private boolean autoCommit = true;

        @Override
        public boolean getAutoCommit() throws SQLException {
            return autoCommit;
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            this.autoCommit = autoCommit;
        }

        @Override
        public void rollback() throws SQLException {
            logger.info( "SlowRollbackConnection.rollback() - signalling rollback started and waiting..." );
            rollbackStarted.countDown();
            try {
                // Block until the test thread signals that it's done checking
                if ( !rollbackProceed.await( 10, TimeUnit.SECONDS ) ) {
                    throw new SQLException( "Timed out waiting for test to proceed in rollback()" );
                }
            } catch ( InterruptedException e ) {
                Thread.currentThread().interrupt();
                throw new SQLException( "Interrupted during rollback", e );
            }
            logger.info( "SlowRollbackConnection.rollback() - proceeding" );
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            rollback();
        }
    }

    private static class SilentWarningsListener implements AgroalDataSourceListener {

        @SuppressWarnings( "WeakerAccess" )
        SilentWarningsListener() {
        }

        @Override
        public void onWarning(String message) {
            logger.info( "Warning: " + message );
        }

        @Override
        public void onWarning(Throwable throwable) {
            logger.info( "Warning: " + throwable.getMessage() );
        }
    }
}


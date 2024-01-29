// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockConnection;
import io.agroal.test.MockStatement;
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
import java.sql.Statement;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class ReapTests {

    static final Logger logger = getLogger( ReapTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver( TrackedConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Reaper thread rolls back transaction" )
    void reaperRollback() {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
            txManager.setTransactionTimeout( 1 );
            txManager.begin();

            try ( Connection connection = dataSource.getConnection() ) {
                TrackedConnection tracked = connection.unwrap( TrackedConnection.class );

                assertFalse( connection.isClosed(), "Expected working connection" );
                assertFalse( tracked.isCommit() && tracked.isRollback(), "Unexpected completion" );

                logger.info( "waiting 2 seconds to so that the transaction times out" );
                Thread.sleep( 2 * 1000 );

                assertTrue( connection.isClosed(), "Expected connection closed by the reaper thread" );
                assertFalse( tracked.isCommit(), "Unexpected connection commit" );
                assertTrue( tracked.isRollback(), "Expected connection rollback by the reaper thread" );
            } catch ( InterruptedException e ) {
                fail( e );
            }
        } catch ( SystemException | NotSupportedException | SQLException e) {
            fail ( e );
        } finally {
            try {
                txManager.rollback();
            } catch ( SystemException e ) {
                fail( e );
            }
        }
    }

    @Test
    @DisplayName( "Reaper thread cancels the open Statement" )
    void reaperStatementCancelRollback() {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                );
        WarningsRequiredAgroalDatasourceListener listener = new WarningsRequiredAgroalDatasourceListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            txManager.setTransactionTimeout( 1 );
            txManager.begin();

            try ( Connection connection = dataSource.getConnection() ) {
                try ( Statement statement = connection.createStatement() ) {
                    TrackedStatement trackedStatement = statement.unwrap( TrackedStatement.class );

                    assertFalse( connection.isClosed(), "Expected working connection" );
                    assertFalse( trackedStatement.isClosed(), "Expected open statement" );

                    logger.info( "waiting 2 seconds to so that the transaction times out" );
                    Thread.sleep( 2 * 1000 );

                    assertTrue( connection.isClosed(), "Expected connection closed by the reaper thread" );
                    assertTrue( trackedStatement.isClosed(), "Expected statement closed by the reaper thread" );
                    assertTrue( trackedStatement.isCanceled(), "Expected statement canceled on transaction timeout" );

                    assertTrue( listener.seenWarning(), "Not seen the expected warning" );
                }
            } catch ( InterruptedException e ) {
                fail( e );
            }
        } catch ( SystemException | NotSupportedException | SQLException e) {
            fail ( e );
        } finally {
            try {
                txManager.rollback();
            } catch ( SystemException e ) {
                fail( e );
            }
        }
    }

    // --- //
    
    public static class TrackedConnection implements MockConnection {

        private boolean commit, rollback;

        @Override
        public void commit() throws SQLException {
            commit = true;
        }

        @Override
        public void rollback() throws SQLException {
            rollback = true;
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            rollback = true;
        }

        public boolean isCommit() {
            return commit;
        }

        public boolean isRollback() {
            return rollback;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return new TrackedStatement();
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public <T> T unwrap(Class<T> target) throws SQLException {
            return (T) this;
        }
    }

    private static class TrackedStatement implements MockStatement {

        private boolean closed, canceled;

        @Override
        public void close() throws SQLException {
            closed = true;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed;
        }

        @Override
        public void cancel() throws SQLException {
            canceled = true;
        }

        public boolean isCanceled() throws SQLException {
            return canceled;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public <T> T unwrap(Class<T> target) throws SQLException {
            return (T) this;
        }
    }

    private static class WarningsAgroalDatasourceListener implements AgroalDataSourceListener {

        @SuppressWarnings( "WeakerAccess" )
        WarningsAgroalDatasourceListener() {
        }

        @Override
        public void onWarning(String message) {
            fail( "Unexpected warning: " + message );
        }

        @Override
        public void onWarning(Throwable throwable) {
            fail( "Unexpected warning", throwable );
        }
    }

    private static class WarningsRequiredAgroalDatasourceListener implements AgroalDataSourceListener {

        private boolean seenWarning;

        @SuppressWarnings( "WeakerAccess" )
        WarningsRequiredAgroalDatasourceListener() {
        }

        @Override
        public void onWarning(String message) {
            seenWarning = true;
        }

        @Override
        public void onWarning(Throwable throwable) {
            seenWarning = true;
        }

        public boolean seenWarning() {
            return seenWarning;
        }

    }
}

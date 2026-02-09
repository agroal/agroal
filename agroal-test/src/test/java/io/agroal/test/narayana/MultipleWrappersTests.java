// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import com.arjuna.ats.jta.common.JTAEnvironmentBean;
import com.arjuna.ats.jta.common.jtaPropertyManager;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockConnection;
import io.agroal.test.MockXADataSource;
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
import java.sql.Savepoint;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.time.Duration.ofSeconds;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class MultipleWrappersTests {

    static final Logger logger = getLogger( MultipleWrappersTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver( CommitTrackerConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Test concurrent Synchronization" )
    void testConcurrentSynchronizations() throws SQLException {
        JTAEnvironmentBean jta = jtaPropertyManager.getJTAEnvironmentBean();
        TransactionManager txManager = jta.getTransactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = jta.getTransactionSynchronizationRegistry();

        AgroalDataSourceConfiguration configuration = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .leakTimeout( ofSeconds( 10 ) )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) ) ).get();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configuration, new WarningsAgroalDatasourceListener() ) ) {
            txManager.begin();

            Connection connection = null;
            for ( int i = 0; i < 3; i++ ) {
                connection = dataSource.getConnection();
            }
            assertFalse( connection.isClosed() );
            txManager.commit();

            assertTrue( connection.isClosed() );
            assertEquals( 1, dataSource.getMetrics().availableCount() );
        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        }

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configuration, new WarningsAgroalDatasourceListener() ) ) {
            txManager.begin();

            Connection connection = null;
            for ( int i = 0; i < 3; i++ ) {
                connection = dataSource.getConnection();
            }
            assertFalse( connection.isClosed() );
            txManager.rollback();

            assertTrue( connection.isClosed() );
            assertEquals( 1, dataSource.getMetrics().availableCount() );
        } catch ( NotSupportedException | SystemException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    @Test
    @DisplayName( "Close after completion test" )
    void closeAfterCompletionTest() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();
        ReturnCounterListener returnCounter = new ReturnCounterListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, "", false ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( MockXADataSource.Empty.class ) )
                ), returnCounter ) ) {

            logger.info( "Test for closing XA connection after completion (commit) on datasource " + dataSource );

            txManager.begin();
            Connection connectionCommit = dataSource.getConnection();
            assertEquals( 0, returnCounter.getAttemptsCount(), "Early return of Connection" );
            txManager.commit();
            assertEquals( 1, returnCounter.getAttemptsCount(), "Connection not returned when expected" );
            assertEquals( 1, returnCounter.getReturnCount(), "Connection not returned when expected" );
            connectionCommit.close();
            assertEquals( 1, returnCounter.getAttemptsCount(), "Multiple return of Connection" );
            assertEquals( 1, returnCounter.getWarningCount(), "Did not get a warning indicating the connection was closed by Agroal" );

            returnCounter.reset();
            logger.info( "Test for closing XA connection after completion (rollback) on datasource " + dataSource );

            txManager.begin();
            Connection connectionRollback = dataSource.getConnection();
            assertEquals( 0, returnCounter.getAttemptsCount(), "Early return of Connection" );
            txManager.rollback();
            assertEquals( 1, returnCounter.getAttemptsCount(), "Connection not returned when expected" );
            assertEquals( 1, returnCounter.getReturnCount(), "Connection not returned when expected" );
            connectionRollback.close();
            assertEquals( 1, returnCounter.getAttemptsCount(), "Multiple return of Connection" );
            // AG-168 - This assertion may be revisited in the future. Read the issue for further details.
            assertEquals( 0, returnCounter.getWarningCount(), "Got a warning indicating the connection was closed by Agroal on rollback" );
        } catch ( SystemException | NotSupportedException | HeuristicRollbackException | HeuristicMixedException | RollbackException e ) {
            fail( "Transaction exception", e );
        }
    }

    // --- //

    private static class ReturnCounterListener implements AgroalDataSourceListener {

        private int attemptsCount, returnCount, warningCount;

        ReturnCounterListener() {
        }

        @Override
        public void beforeConnectionReturn(Connection connection) {
            attemptsCount++;
        }

        @Override
        public void onConnectionReturn(Connection connection) {
            returnCount++;
        }

        @Override
        public void onWarning(String message) {
            warningCount++;
            logger.warning( message );
        }

        public int getAttemptsCount() {
            return attemptsCount;
        }

        public int getReturnCount() {
            return returnCount;
        }

        public int getWarningCount() {
            return warningCount;
        }

        public void reset() {
            attemptsCount = returnCount = warningCount = 0;
        }
    }

    private static class WarningsAgroalDatasourceListener implements AgroalDataSourceListener {

        @SuppressWarnings( "WeakerAccess" )
        WarningsAgroalDatasourceListener() {
        }

        @Override
        public void onWarning(String message) {
            if ( !message.contains( "open connection(s) prior to commit" ) ) {
                fail( "Unexpected warning: " + message );
            }
        }

        @Override
        public void onWarning(Throwable throwable) {
            fail( "Unexpected warning", throwable );
        }
    }

    @SuppressWarnings( "ObjectToString" )
    public static class CommitTrackerConnection implements MockConnection {

        @Override
        public void close() throws SQLException {
            logger.info( "Close " + this );
        }

        @Override
        public void commit() throws SQLException {
            logger.info( "Commit " + this );
        }

        @Override
        public void rollback() throws SQLException {
            logger.info( "Rollback " + this );
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            logger.info( "Rollback " + this );
        }
    }
}

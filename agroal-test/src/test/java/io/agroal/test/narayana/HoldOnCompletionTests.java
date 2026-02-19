// Copyright (C) 2026 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockConnection;
import io.agroal.test.MockResultSet;
import io.agroal.test.MockStatement;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
public class HoldOnCompletionTests {

    private static final Logger logger = getLogger( HoldOnCompletionTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver( ClosableConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    static Stream<Arguments> provideParams() {
        return Stream.of( true, false ).flatMap( b -> Stream.of( AgroalDataSourceConfiguration.DataSourceImplementation.values() ).map( i -> Arguments.of( b, i ) ) );
    }

    // --- //

    @ParameterizedTest
    @MethodSource("provideParams")
    @DisplayName( "Hold on completion not enlisted connection" )
    void testNotEnlisted(boolean closeConnection, AgroalDataSourceConfiguration.DataSourceImplementation implementation) throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .dataSourceImplementation( implementation )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) ) );

        AgroalDataSourceListener warningListener = new WarningListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningListener ) ) {
            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            connection.setAutoCommit( false );
            connection.setHoldability( ResultSet.HOLD_CURSORS_OVER_COMMIT );

            connection.commit();

            assertFalse( connection.isClosed(), "In non-JTA mode, commit() should not return connection to pool" );
            assertEquals( 1, dataSource.getMetrics().activeCount(), "Connection should still be active in the pool" );

            assertDoesNotThrow( connection::getSchema, "Should be able to use connection after local commit" );

            txManager.begin(); // verify lazy enlistment
            assertThrows(SQLException.class, connection::getSchema, "Should not able to use connection in a tx" );
            txManager.rollback();

            assertEquals( 1, dataSource.getMetrics().activeCount(), "Should not return to pool after some tx rollback" );

            if ( closeConnection) {
                connection.close();
                assertEquals( 0, dataSource.getMetrics().activeCount(), "Connection must return to pool after close()" );
            } else {
                connection.setHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT );
                assertEquals( 1, dataSource.getMetrics().activeCount(), "Connection must not return to pool without close()" );
            }

        } catch ( NotSupportedException | SystemException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    @ParameterizedTest
    @MethodSource("provideParams")
    @DisplayName( "Hold on completion enlisted connection" )
    void testEnlisted(boolean closeConnection, AgroalDataSourceConfiguration.DataSourceImplementation implementation) throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( implementation )
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) ) );

        WarningListener warningListener = new WarningListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningListener ) ) {
            txManager.begin();

            Connection connectionOne = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connectionOne ) );

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            connection.setHoldability( ResultSet.HOLD_CURSORS_OVER_COMMIT );
            assertEquals( 0, dataSource.getMetrics().heldCount(), "The metric for held connections should not be incremented before commit" );

            txManager.commit();

            assertEquals( 1, dataSource.getMetrics().heldCount(), "The metric for held connections was not incremented on commit" );
            assertTrue( connectionOne.isClosed(), "Connection should not be open" );
            assertFalse( connection.isClosed(), "Connection should be open" );
            assertThrows( SQLException.class, connection::getSchema, "Connection operation should throw exception" );

            txManager.begin();

            assertDoesNotThrow( connection::getSchema, "Should be able to use connection after local commit" );
            assertEquals( 1, warningListener.getWarnings(), "Got unexpected warning" );

            if ( closeConnection ) {
                connection.close();
            } else {
                connection.setHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT );
            }

            txManager.commit();

            assertTrue( connection.isClosed(), "Connection should be closed" );

            assertEquals( closeConnection ? 1 : 2, warningListener.getWarnings(), "Wrong number of warnings" );
            assertEquals( 0, dataSource.getMetrics().activeCount(), "Connection should have returned to the pool" );
        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        }
    }

    @ParameterizedTest
    @MethodSource("provideParams")
    @DisplayName( "Hold result set" )
    public void testHoldableReenlistment(boolean statementAutoClose, AgroalDataSourceConfiguration.DataSourceImplementation implementation) throws Exception {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .dataSourceImplementation( implementation )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) ) );

        WarningListener warningListener = new WarningListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningListener ) ) {

            txManager.begin();

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            Statement statement = connection.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT );
            ResultSet resultSet = statement.executeQuery( null );

            try ( ResultSet otherResultSet = statement.getResultSet() ) {
                assertFalse( resultSet.isClosed() || otherResultSet.isClosed(), "Two ResultSets should be open" );
            }
            assertFalse( statement.isClosed(), "Statement should remain open after otherResultSet closes" );


            if ( statementAutoClose ) {
                statement.closeOnCompletion();
            }

            txManager.commit();

            assertThrows( SQLException.class, resultSet::next, "Call outside the tx should fail" );

            txManager.begin();

            assertDoesNotThrow( resultSet::next, "Should be able to iterate the ResultSet" );
            resultSet.close(); // If closeOnCompletion then the statement is closed as well

            txManager.commit();

            assertTrue( resultSet.isClosed(), "ResultSet should be closed after commit" );

            if ( statementAutoClose ) {
                assertTrue( statement.isClosed(), "Statement should been closed on resultSet completion" );
                assertTrue( connection.isClosed(), "Connection should be closed after commit" );
            } else {
                assertFalse( statement.isClosed(), "Statement should not be closed" );
                assertFalse( connection.isClosed(), "Connection should remain open" );
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideParams")
    @DisplayName( "Test mixed holdability" )
    public void testMixedHoldability(boolean statementClose, AgroalDataSourceConfiguration.DataSourceImplementation implementation) throws Exception {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .dataSourceImplementation( implementation )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) ) );

        WarningListener warningListener = new WarningListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningListener ) ) {

            txManager.begin();

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            Statement holdStatement = connection.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT );
            ResultSet holdResultSet = holdStatement.executeQuery( null );

            Statement closeStatement = connection.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT );
            ResultSet closeResultSet = closeStatement.executeQuery( null );

            txManager.commit();

            assertFalse( connection.isClosed(), "Holdable Connection should remain open after commit" );
            assertFalse( holdStatement.isClosed(), "Holdable Statement should remain open after commit" );
            assertFalse( holdResultSet.isClosed(), "Holdable ResultSet should remain open after commit" );

            assertTrue( closeStatement.isClosed(), "Non-holdable Statement should be closed after commit" );
            assertTrue( closeResultSet.isClosed(), "Non-holdable ResultSet should be closed after commit" );

            if ( statementClose ) {
                holdStatement.close();

                assertTrue( holdResultSet.isClosed(), "Holdable ResultSet should remain open after statement close" );
                assertEquals( 0, dataSource.getMetrics().availableCount(), "Connection should not return to pool when the statement is closed" );
            }

            connection.close();
            assertEquals( 1, dataSource.getMetrics().availableCount(), "Connection should return to pool after holdable statement is closed" );
        }
    }

    @Test
    public void testHoldabilityRollback() throws Exception {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) ) );

        WarningListener warningListener = new WarningListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningListener ) ) {

            txManager.begin();

            Connection connection = dataSource.getConnection();
            connection.setHoldability( ResultSet.HOLD_CURSORS_OVER_COMMIT );
            logger.info( format( "Got connection {0}", connection ) );

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery( null );

            txManager.rollback();

            assertEquals( 1, warningListener.getWarnings() );
            assertTrue( connection.isClosed(), "Held Statement should be closed after rollback" );
            assertTrue( statement.isClosed(), "Held Statement should be closed after rollback" );
            assertTrue( resultSet.isClosed(), "Held ResultSet should be closed after rollback" );
            assertEquals( 1, dataSource.getMetrics().availableCount(), "Connection should return to pool after rollback" );
        }
    }

    @Test
    public void testNonHoldableResultSetLeak() throws Exception {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) ) );

        WarningListener warningListener = new WarningListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, warningListener ) ) {

            txManager.begin();

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery( null );

            txManager.commit();

            assertEquals( 2, warningListener.getWarnings() );
            assertTrue( connection.isClosed(), "Non-holdable Statement should be closed after commit" );
            assertTrue( statement.isClosed(), "Non-holdable Statement should be closed after commit" );
            assertTrue( resultSet.isClosed(), "Non-holdable ResultSet should be closed after commit" );
            assertEquals( 1, dataSource.getMetrics().availableCount(), "Connection should return to pool after commit" );
        }
    }

    // --- //

    public static class ClosableConnection implements MockConnection {

        private boolean commit, closed, rollback;

        @Override
        public void close() throws SQLException {
            closed = true;
        }

        @Override
        public void commit() throws SQLException {
            commit = true;
        }

        @Override
        public void rollback() throws SQLException {
            rollback = true;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return new CloseableStatement();
        }

        @Override
        public int getHoldability() throws SQLException {
            throw new SQLException( "Make it look like holdability is not supported" );
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            fail( "Unwrap on the connection should not be called" );
            return iface.cast( this );
        }

        public boolean isCompleted() {
            return commit || rollback;
        }
    }

    private static class CloseableStatement implements MockStatement {

        @Override
        public ResultSet getResultSet() throws SQLException {
            return new CloseableResultSet();
        }
    }

    private static class CloseableResultSet implements MockResultSet {

        private boolean closed;

        @Override
        public void close() throws SQLException {
            closed = true;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed;
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    private static class WarningListener implements AgroalDataSourceListener {

        private final AtomicInteger warning = new AtomicInteger();

        WarningListener() {
        }

        @Override
        public void onWarning(String message) {
            logger.info( message );
            warning.incrementAndGet();
        }

        @Override
        public void onWarning(Throwable throwable) {
            logger.info( throwable.getClass().getName() + " with message " + throwable.getMessage() );
            warning.incrementAndGet();
        }

        private int getWarnings() {
            return warning.get();
        }
    }
}

// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import com.arjuna.ats.jta.common.JTAEnvironmentBean;
import com.arjuna.ats.jta.common.jtaPropertyManager;
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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
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

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .leakTimeout( ofSeconds( 10 ) )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
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

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
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

    // --- //

    private static class WarningsAgroalDatasourceListener implements AgroalDataSourceListener {

        @SuppressWarnings( "WeakerAccess" )
        WarningsAgroalDatasourceListener() {
        }

        @Override
        public void onWarning(String message) {
            if ( !message.startsWith( "Closing open connection" ) ) {
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

// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import com.arjuna.ats.jta.common.jtaPropertyManager;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDataSource;
import io.agroal.test.MockPreparedStatement;
import io.agroal.test.MockResultSet;
import io.agroal.test.MockXAResource;
import io.agroal.test.SimpleMapContext;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import org.jboss.tm.ConnectableResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static java.text.MessageFormat.format;
import static java.util.List.of;
import static java.util.logging.Logger.getLogger;
import static javax.naming.Context.INITIAL_CONTEXT_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class CommitMarkableResourceTest {

    private static final String JNDI_NAME = CommitMarkableResourceTest.class.getName();
    private static final Logger logger = getLogger( CommitMarkableResourceTest.class.getName() );

    @BeforeAll
    @SuppressWarnings( "AccessOfSystemProperties" )
    static void setup() throws NamingException, ClassNotFoundException {
        System.setProperty( INITIAL_CONTEXT_FACTORY, SimpleMapContextFactory.class.getName() );
    }

    @AfterAll
    static void teardown() throws NamingException {
        System.clearProperty( INITIAL_CONTEXT_FACTORY );
    }

    // --- //

    @ParameterizedTest
    @DisplayName( "Test Last Commit Resource Optimization (LRCO)" )
    @ValueSource( booleans = {false, true} )
    void lastResourceCommitOptimizationTest(boolean performImmediateCleanupOfBranches) throws SQLException {
        jtaPropertyManager.getJTAEnvironmentBean().setLastResourceOptimisationInterface( ConnectableResource.class );
        jtaPropertyManager.getJTAEnvironmentBean().setPerformImmediateCleanupOfCommitMarkableResourceBranches( performImmediateCleanupOfBranches );
        jtaPropertyManager.getJTAEnvironmentBean().setCommitMarkableResourceJNDINames( of( JNDI_NAME ) );

        if ( performImmediateCleanupOfBranches ) {
            logger.info( "Performing immediate cleanup of branches is not supported" );
            // AG-214 for further details
            return;
        } else {
            logger.info( "Performing delayed cleanup of branches" );
        }

        com.arjuna.ats.arjuna.common.recoveryPropertyManager.getRecoveryEnvironmentBean().setRecoveryBackoffPeriod( 1 );
        RecoveryManager recoveryManager = RecoveryManager.manager( RecoveryManager.DIRECT_MANAGEMENT );

        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry, JNDI_NAME, true ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( ConnectableDataSource.class )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            jndiBind( dataSource );

            txManager.begin();
            txManager.getTransaction().enlistResource( new MockXAResource.Empty() ); // force two phase commit
            LoggingPreparedStatement.queryCount = LoggingPreparedStatement.updateCount = 0;

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );
            connection.close();

            assertEquals( 0, LoggingPreparedStatement.updateCount );

            txManager.commit();

            // perform INSERT and, if immediate cleanup, perform DELETE as well
            assertEquals( performImmediateCleanupOfBranches ? 2 : 1, LoggingPreparedStatement.updateCount );
            assertEquals( 0, LoggingPreparedStatement.queryCount );

            recoveryManager.scan();

            // RecoveryManager performs DELETE of xids in memory followed by SELECT (eventually also DELETE, if there are xids in the database)
            assertEquals( 1, LoggingPreparedStatement.queryCount );
            assertEquals( 2, LoggingPreparedStatement.updateCount );
        } catch ( NotSupportedException | SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e ) {
            fail( "Exception: " + e.getMessage() );
        } finally {
            jndiBind( null );
        }
    }

    private static void jndiBind(Object value) {
        Context jndiContext = null;
        try {
            jndiContext = new InitialContext();
            if ( value == null ) {
                logger.info( "Binding datasource to " + JNDI_NAME );
                jndiContext.unbind( JNDI_NAME );
            } else {
                logger.info( "Unbinding " + JNDI_NAME );
                jndiContext.bind( JNDI_NAME, value );
            }
        } catch ( NamingException e ) {
            fail( "Exception retrieving InitialContext: " + e );
        } finally {
            if ( jndiContext != null ) {
                try {
                    jndiContext.close();
                } catch ( NamingException e ) {
                    fail( "Failure to close InitialContext: " + e );
                }
            }
        }
    }

    // --- //

    public static class ConnectableDataSource implements MockDataSource {

        @Override
        public Connection getConnection() throws SQLException {
            return new MockConnectableConnection();
        }
    }

    public static class MockConnectableConnection implements MockConnection {

        @Override
        public void commit() throws SQLException {
            logger.info( "MockConnectableConnection.commit()" );
        }

        @Override
        public void rollback() throws SQLException {
            logger.info( "MockConnectableConnection.rollback()" );
        }

        @Override
        public Statement createStatement() throws SQLException {
            return new LoggingPreparedStatement();
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return new LoggingPreparedStatement( sql );
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            return new LoggingPreparedStatement( sql );
        }
    }

    @SuppressWarnings( "PublicField" )
    public static class LoggingPreparedStatement implements MockPreparedStatement {

        public static int queryCount, updateCount;
        private String statementSQL;

        public LoggingPreparedStatement() {
        }

        public LoggingPreparedStatement(String sql) {
            statementSQL = sql;
        }

        @Override
        public ResultSet executeQuery(String sql) throws SQLException {
            logger.info( "Executing query: " + sql );
            queryCount++;
            return new MockResultSet.Empty();
        }

        @Override
        public int executeUpdate(String sql) throws SQLException {
            logger.info( "Executing update: " + sql );
            updateCount++;
            return 1;
        }

        @Override
        public int executeUpdate() throws SQLException {
            logger.info( "Executing update: " + statementSQL );
            updateCount++;
            return 1;
        }
    }

    // --- //

    public static class SimpleMapContextFactory implements InitialContextFactory {

        private static final Context INSTANCE = new SimpleMapContext();

        @Override
        public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
            return INSTANCE;
        }
    }
}

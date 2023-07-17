// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import com.arjuna.ats.arjuna.common.arjPropertyManager;
import com.arjuna.ats.arjuna.objectstore.StoreManager;
import com.arjuna.ats.arjuna.objectstore.jdbc.JDBCAccess;
import com.arjuna.ats.internal.arjuna.objectstore.ShadowNoFileLockStore;
import com.arjuna.ats.internal.arjuna.objectstore.jdbc.JDBCStore;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDatabaseMetaData;
import io.agroal.test.MockPreparedStatement;
import io.agroal.test.MockResultSet;
import io.agroal.test.MockXAConnection;
import io.agroal.test.MockXADataSource;
import io.agroal.test.MockXAResource;
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

import javax.sql.XAConnection;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static java.text.MessageFormat.format;
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
public class JDBCStoreTests {

    private static final Logger logger = getLogger( JDBCStoreTests.class.getName() );

    @BeforeAll
    static void setup() throws NoSuchFieldException, IllegalAccessException {
        logger.info( "JDBCStore config setup" );
        arjPropertyManager.getObjectStoreEnvironmentBean().setObjectStoreType( JDBCStore.class.getTypeName() );
        arjPropertyManager.getObjectStoreEnvironmentBean().setJdbcAccess( AgroalJDBCAccess.class.getTypeName() );
        StoreManager.shutdown();
    }

    @AfterAll
    static void teardown() {
        logger.info( "JDBCStore config teardown" );
        arjPropertyManager.getObjectStoreEnvironmentBean().setObjectStoreType( ShadowNoFileLockStore.class.getName() );
        StoreManager.shutdown();
    }

    // --- //

    @Test
    @DisplayName( "Test JDBCStore with transactional datasource" )
    void testJDBCStore() throws SystemException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 2 ) // need at least 2 connections, otherwise the pool starves (deadlock / timeout)
                        .acquisitionTimeout( ofSeconds( 2 ) )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( JDBCStoreXADataSource.class )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            AgroalJDBCAccess.dataSource = dataSource;
            assertFalse( AgroalJDBCAccess.initialized, "JDBCStore initialized too soon" );

            StoreManager.getTxLog(); // Enforce start of JDBCStore to make sure it's using our datasource
            assertTrue( AgroalJDBCAccess.initialized, "JDBCStore not properly initialized" );

            txManager.begin();
            txManager.getTransaction().enlistResource( new MockXAResource.Empty() ); // force two phase commit
            LoggingPreparedStatement.updateCount = 0;

            Connection connection = dataSource.getConnection();
            logger.info( format( "Got connection {0}", connection ) );

            txManager.commit();

            assertEquals( 2, LoggingPreparedStatement.updateCount, "Unexpected number of statements executed on commit" );
        } catch ( NotSupportedException | SystemException | HeuristicMixedException | HeuristicRollbackException | SQLException e ) {
            fail( "Exception: " + e.getMessage() );
        } catch ( RollbackException e ) {
            throw new RuntimeException( e );
        } finally {
            try {
                txManager.rollback();
            } catch ( IllegalStateException e ) {
                // ignore
            }
        }
    }

    // --- //

    @SuppressWarnings( "PublicField" )
    public static class AgroalJDBCAccess implements JDBCAccess {

        public static AgroalDataSource dataSource;
        public static boolean initialized;

        @Override
        public Connection getConnection() throws SQLException {
            if ( dataSource != null ) {
                return dataSource.getConnection();
            } else {
                fail( "Creating new connection before datasource is set" );
                return new MockJDBCStoreConnection();
            }
        }

        @Override
        @SuppressWarnings( "UseOfStringTokenizer" )
        public void initialise(StringTokenizer stringTokenizer) {
            logger.info( "JDBCStore initialized" );
            initialized = true;
        }
    }

    // --- //

    public static class JDBCStoreXADataSource implements MockXADataSource {

        @Override
        @SuppressWarnings( {"ReturnOfInnerClass", "AnonymousInnerClassMayBeStatic"} )
        public XAConnection getXAConnection() throws SQLException {
            return new MockXAConnection() {

                @Override
                public Connection getConnection() throws SQLException {
                    return new MockJDBCStoreConnection();
                }
            };
        }
    }

    public static class MockJDBCStoreConnection implements MockConnection {

        @Override
        @SuppressWarnings( {"ReturnOfInnerClass", "AnonymousInnerClassMayBeStatic"} )
        public DatabaseMetaData getMetaData() throws SQLException {
            return new MockDatabaseMetaData() {
                @Override
                public String getDriverName() throws SQLException {
                    return "h2"; // one of the available drivers in com.arjuna.ats.internal.arjuna.objectstore.jdbc.drivers
                }
            };
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

        public static int updateCount;
        private String statementSQL;

        public LoggingPreparedStatement() {
        }

        public LoggingPreparedStatement(String sql) {
            statementSQL = sql;
        }

        @Override
        public ResultSet executeQuery(String sql) throws SQLException {
            logger.info( "Executing query: " + sql );
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
}

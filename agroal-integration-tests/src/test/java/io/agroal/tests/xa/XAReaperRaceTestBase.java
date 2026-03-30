package io.agroal.tests.xa;


import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.jboss.byteman.contrib.bmunit.WithByteman;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.tests.Datasources;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;


@WithByteman
@BMUnitConfig(debug = true)
abstract class XAReaperRaceTestBase {

    private static final Logger logger = Logger.getLogger( XAReaperRaceTestBase.class.getName() );

    // Set by Byteman rule when rollback() completes successfully on the reaper thread
    static volatile boolean rollbackSucceeded;

    public static void markRollbackSucceeded() {
        rollbackSucceeded = true;
    }

    abstract String xaDataSourceClassName();
    abstract JdbcDatabaseContainer container();

    abstract String slowSQL();

    /**
     * Whether this driver throws from the slow SQL when the reaper fires mid-execution.
     *
     * Most drivers (PostgreSQL, MySQL, MariaDB) let the slow SQL complete normally
     * because end(TMFAIL) does not interrupt the in-flight statement.
     *
     * MSSQL throws because MSDTC actively cancels the distributed transaction
     * on the server side, independently of the Java-level XA resource.
     */
    boolean slowSQLThrowsOnReaperTimeout() {
        return false;
    }

    String createTableDDL() {
        return "CREATE TABLE IF NOT EXISTS xa_reaper_test ( id INT PRIMARY KEY, val VARCHAR(100) )";
    }

    String truncateTableSQL() {
        return "DELETE FROM xa_reaper_test";
    }


    protected void verifyXASupported( AgroalDataSource dataSource ) {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        try {
            txManager.setTransactionTimeout( 10 );
            txManager.begin();
            dataSource.getConnection().close();
            txManager.rollback();
        } catch ( Exception e ) {
            try { txManager.rollback(); } catch ( Exception ignore ) {}
            assumeTrue( false, "XA not supported: " + e.getMessage() );
        }
    }

    @Test
    @BMScript("reaper")
    public void testInterleave() throws Exception {

        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();

        try ( AgroalDataSource dataSource = Datasources.createXADataSource(container(), xaDataSourceClassName()) ) {
            verifyXASupported( dataSource );

            // Create table using plain Statement (not PreparedStatement)
            // so the Byteman rule on PreparedStatementWrapper.execute() does not fire
            try ( Connection setup = dataSource.getConnection() ) {
                setup.createStatement().execute( createTableDDL() );
                setup.createStatement().execute( truncateTableSQL() );
            }

            rollbackSucceeded = false;

            // Short timeout — the real TransactionReaper will fire after this
            txManager.setTransactionTimeout( 2 );
            txManager.begin();
            Connection connection = dataSource.getConnection();

            // Prepare INSERT but do not execute yet.
            // Byteman activates interleave sync AFTER this call returns.
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO xa_reaper_test (id, val) VALUES (1, 'interleave-test')" );

            // The app thread calls execute() — Byteman holds it at AT ENTRY
            // until the TransactionReaper fires (~2s) and reaches rollback()
            // AT ENTRY.  The reaper is then held at rollback() while the app
            // thread attempts the INSERT.
            //
            // The effect of the INSERT depends on the driver's end() implementation:
            //   - MSSQL:      end() disassociates at server level; INSERT runs in
            //                 auto-commit and is immediately committed (data leak)
            //   - Oracle:     end() disassociates; INSERT starts a local TX that
            //                 blocks the subsequent rollback (XAER_PROTO)
            //   - PostgreSQL: end() only updates JDBC driver state, not the server;
            //                 INSERT is still inside the DB transaction and is
            //                 covered by the subsequent rollback (no data leak)

            try {
                ps.execute();
                logger.warning( "INSERT must be rejected after end(TMFAIL) — connection should be poisoned" );
            } catch ( SQLException e ) {
                logger.info( "INSERT correctly rejected: " + e.getMessage() );
                e.printStackTrace();
            }

            // Detach the timed-out TX from the app thread.
            // Byteman releases the reaper here (AT INVOKE suspend) so it can
            // proceed with the actual rollback.
            try { txManager.suspend(); } catch ( SystemException ignore ) { }

            // getConnection() blocks until the reaper finishes (rollback + connection return)
            // because pool maxSize=1.  The pool has a 10s acquisitionTimeout so that a
            // failed rollback (e.g. Oracle XAER_PROTO) that leaves the connection stuck
            // does not hang the test forever.
            Connection verify = null;
            try {
                verify = dataSource.getConnection();
                assertTrue( rollbackSucceeded, "Reaper's rollback() must have completed successfully" );
            } catch ( SQLException e ) {
                // Acquisition timed out — the connection is stuck because rollback failed.
                // Increase pool size to get a fresh connection and still verify data leaks.
                logger.warning( "Connection stuck after rollback failure, expanding pool: " + e.getMessage() );
                dataSource.getConfiguration().connectionPoolConfiguration().setMaxSize( 2 );
                verify = dataSource.getConnection();
            }

            try ( Connection c = verify ) {
                ResultSet rs = c.createStatement()
                        .executeQuery( "SELECT COUNT(*) FROM xa_reaper_test" );
                rs.next();
                int count = rs.getInt( 1 );
                if ( count != 0 ) {
                    fail( "Data leak: xa_reaper_test has " + count + " rows but should have 0" );
                }
            }
        }
    }
}

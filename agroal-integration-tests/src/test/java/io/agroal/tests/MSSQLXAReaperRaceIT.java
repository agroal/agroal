package io.agroal.tests;

import io.agroal.api.AgroalDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mssqlserver.MSSQLServerContainer;

import jakarta.transaction.TransactionManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * XA reaper race integration test against MSSQL using Testcontainers.
 * Uses WAITFOR DELAY for slow SQL that holds the read lock past the transaction timeout.
 *
 * MSSQL XA requires the JDBC XA stored procedures to be installed.
 * The init script (mssql-xa-init.sql) installs them via sp_sqljdbc_xa_install.
 */
@Tag( "testcontainers" )
@Testcontainers
class MSSQLXAReaperRaceIT extends XAReaperRaceITBase {

    private static final Logger logger = Logger.getLogger( MSSQLXAReaperRaceIT.class.getName() );

    @Container
    static MSSQLServerContainer mssql = new MSSQLServerContainer( "mcr.microsoft.com/mssql/server:2022-latest" )
            .acceptLicense()
            .withInitScript( "mssql-xa-init.sql" );

    @Override
    String xaDataSourceClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerXADataSource";
    }

    @Override
    String jdbcUrl() {
        return mssql.getJdbcUrl();
    }

    @Override
    String username() {
        return mssql.getUsername();
    }

    @Override
    String password() {
        return mssql.getPassword();
    }

    @Override
    String slowSQL() {
        return "WAITFOR DELAY '00:00:04'";
    }

    /**
     * MSSQL's MSDTC actively cancels the distributed transaction when the
     * reaper fires, causing the in-flight WAITFOR DELAY to throw:
     * "The Microsoft Distributed Transaction Coordinator (MS DTC) has cancelled the distributed transaction."
     *
     * This differs from PostgreSQL/MySQL/MariaDB where the XAConnectionLock
     * read lock blocks end(TMFAIL) and the slow SQL completes normally.
     */
    @Override
    boolean slowSQLThrowsOnReaperTimeout() {
        return true;
    }

    @Override
    String createTableDDL() {
        return "IF OBJECT_ID('xa_reaper_test', 'U') IS NULL " +
               "CREATE TABLE xa_reaper_test ( id INT PRIMARY KEY, val VARCHAR(100) )";
    }

    @Override
    String truncateTableSQL() {
        return "IF OBJECT_ID('xa_reaper_test', 'U') IS NOT NULL DELETE FROM xa_reaper_test";
    }

    /**
     * MSDTC-specific race: MSDTC cancels the SQL server-side, releasing the read lock
     * BEFORE the reaper's end(TMFAIL) acquires the write lock and poisons the connection.
     *
     * Timeline:
     *   T=0s  App starts WAITFOR DELAY '00:00:04' (read lock held)
     *   T=2s  MSDTC timeout fires → SQL Server cancels WAITFOR → app gets SQLException
     *         → StatementWrapper releases read lock
     *   T=2s+ Reaper calls end(TMFAIL) → acquires write lock → calls xp_sqljdbc_xa_end
     *         → XAER_NOTA (branch already gone) → poisons connection
     *
     *   WINDOW: between "read lock released" and "markPoisoned()", the app could
     *   issue SQL on the non-XA, non-poisoned connection.
     *
     * This test verifies that an immediate retry after the MSDTC cancellation is
     * still rejected (either by the lock poisoning or by the connection/statement
     * being closed by Agroal's transactionEnd processing).
     *
     * It also verifies that no data leaks: if the immediate retry somehow succeeds,
     * the row would persist outside XA — the COUNT(*) assertion catches this.
     */
    @Test
    @DisplayName( "MSDTC race: immediate retry after MSDTC cancellation must not persist data" )
    @Timeout( 60 )
    void msdtcCancellationWindowMustNotLeakData() {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();

        try ( AgroalDataSource dataSource = createXADataSource() ) {
            verifyXASupported( dataSource );

            try ( Connection setup = dataSource.getConnection(); Statement s = setup.createStatement() ) {
                s.execute( createTableDDL() );
                s.executeUpdate( truncateTableSQL() );
            }

            txManager.setTransactionTimeout( 2 );
            txManager.begin();

            try ( Connection connection = dataSource.getConnection() ) {
                Statement stmt = connection.createStatement();

                // This INSERT is inside the XA branch — will be rolled back by MSDTC
                stmt.executeUpdate( "INSERT INTO xa_reaper_test (id, val) VALUES (1, 'inside-xa')" );

                // This WAITFOR will be cancelled by MSDTC after ~2 seconds
                try {
                    stmt.execute( "WAITFOR DELAY '00:00:04'" );
                } catch ( SQLException msdtcCancellation ) {
                    // MSDTC cancelled the SQL. The read lock has been released.
                    // The reaper's end(TMFAIL) may or may not have poisoned the connection yet.
                    // Immediately attempt an INSERT — this is the critical window.
                    logger.info( "MSDTC cancelled WAITFOR: " + msdtcCancellation.getMessage() );
                }

                assertThatThrownBy( () -> stmt.executeUpdate(
                        "INSERT INTO xa_reaper_test (id, val) VALUES (3, 'msdtc-window-LEAKED')" ) )
                        .as( "INSERT in MSDTC cancellation window must be rejected by XAConnectionLock" )
                        .isInstanceOf( SQLException.class );
            }

            try { txManager.rollback(); } catch ( Exception ignore ) {}

            Thread.sleep( 2000 );

            try ( Connection verify = dataSource.getConnection();
                  Statement s = verify.createStatement();
                  ResultSet rs = s.executeQuery( "SELECT COUNT(*) FROM xa_reaper_test" ) ) {
                rs.next();
                assertThat( rs.getInt( 1 ) )
                        .as( "No rows must persist — XA rolled back row 1, lock rejected row 3" )
                        .isEqualTo( 0 );
            }
        } catch ( Exception e ) {
            fail( "Unexpected: " + e.getMessage(), e );
        }
    }
}

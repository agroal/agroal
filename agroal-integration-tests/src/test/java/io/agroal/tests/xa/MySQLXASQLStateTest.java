package io.agroal.tests.xa;

import io.agroal.api.AgroalDataSource;
import io.agroal.tests.Datasources;
import jakarta.transaction.TransactionManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * AG-314: Verifies that when a transaction times out while a query is blocked,
 * the resulting SQLException preserves the driver's original SQLState.
 *
 * Uses row-level lock contention: a raw JDBC connection holds an exclusive lock
 * on a row, and the XA connection tries to acquire the same lock. When the
 * Narayana reaper fires TMFAIL, the blocked query must be interrupted with an
 * exception that carries a non-null SQLState.
 */
@Tag( "testcontainers" )
@Testcontainers
class MySQLXASQLStateTest {

    private static final Logger logger = Logger.getLogger( MySQLXASQLStateTest.class.getName() );

    @Container
    static MySQLContainer mysql = new MySQLContainer( System.getProperty( "mysql.testcontainer.image", "mysql:8.0" ) );

    private void verifyXASupported( AgroalDataSource dataSource ) {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        try {
            txManager.setTransactionTimeout( 10 );
            txManager.begin();
            dataSource.getConnection().close();
            txManager.rollback();
        } catch ( Exception e ) {
            try { txManager.rollback(); } catch ( Exception ignore ) { }
            assumeTrue( false, "XA not supported: " + e.getMessage() );
        }
    }

    @Test
    @DisplayName( "AG-314 TMFAIL preserves SQLState on transaction timeout" )
    void tmfailPreservesSQLState() throws Exception {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();

        try ( AgroalDataSource dataSource = Datasources.createXADataSource( mysql, "com.mysql.cj.jdbc.MysqlXADataSource" ) ) {
            verifyXASupported( dataSource );

            // Create table and insert the row we'll lock
            try ( Connection setup = dataSource.getConnection() ) {
                setup.createStatement().execute(
                        "CREATE TABLE IF NOT EXISTS sqlstate_test ( id INT PRIMARY KEY ) ENGINE=InnoDB" );
                setup.createStatement().execute(
                        "INSERT IGNORE INTO sqlstate_test VALUES (1)" );
            }

            // Hold an exclusive lock on row id=1 from a raw JDBC connection
            try ( Connection lockHolder = DriverManager.getConnection(
                    mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword() ) ) {
                lockHolder.setAutoCommit( false );
                lockHolder.createStatement().executeQuery(
                        "SELECT * FROM sqlstate_test WHERE id = 1 FOR UPDATE" );

                // Start XA transaction with short timeout
                txManager.setTransactionTimeout( 2 );
                txManager.begin();

                Connection connection = dataSource.getConnection();
                Statement stmt = connection.createStatement();

                // This blocks waiting for the lock held by lockHolder.
                // The Narayana reaper fires after ~2s and calls end(TMFAIL),
                // which triggers Statement.cancel() via transactionBeforeCompletion(false)
                // → closeAllAutocloseableElements → StatementWrapper.beforeClose() → cancel().
                //
                // The MySQL driver's KILL QUERY interrupts the blocked SELECT,
                // producing a MySQLStatementCancelledException. The SQLState must
                // be preserved (not null) so that frameworks like Spring can
                // translate the exception correctly (e.g. QueryTimeoutException).
                SQLException ex = assertThrows( SQLException.class,
                        () -> stmt.executeQuery( "SELECT * FROM sqlstate_test WHERE id = 1 FOR UPDATE" ),
                        "Blocked query must be interrupted by transaction timeout" );

                logger.info( "Caught: " + ex.getClass().getName()
                        + " SQLState=" + ex.getSQLState()
                        + " msg=" + ex.getMessage() );

                assertNotNull( ex.getSQLState(),
                        "SQLState must not be null — AG-314" );
            }
        } finally {
            try { txManager.rollback(); } catch ( Exception ignore ) { }
        }
    }
}

package io.agroal.tests;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.agroal.api.transaction.XAConnectionLock;
import io.agroal.narayana.NarayanaTransactionIntegration;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Base class for XA reaper race integration tests against real databases.
 *
 * Each test would fail if the XAConnectionLock is removed:
 *   - reaperRaceWithRealDatabase: post-reaper SQL would succeed outside XA
 *   - postReaperInsertMustNotPersist: post-reaper INSERT would auto-commit
 *   - concurrentReaperAndJdbc: write lock would not block, poisoning would not happen
 *
 * Driver behavior when the reaper fires during a slow SQL (4s SQL, 2s timeout):
 *
 *   PostgreSQL : slowSQL completes normally, post-reaper SQL throws (lock poison)
 *   MySQL      : slowSQL completes normally, post-reaper SQL throws (lock poison)
 *   MariaDB    : slowSQL completes normally, post-reaper SQL throws (lock poison)
 *   MSSQL      : slowSQL throws (MSDTC cancels the distributed transaction),
 *                post-reaper SQL throws (lock poison)
 *
 * Subclasses declare the expected driver behavior via {@link #slowSQLThrowsOnReaperTimeout()}.
 */
abstract class XAReaperRaceITBase {

    private static final Logger logger = Logger.getLogger( XAReaperRaceITBase.class.getName() );

    abstract String xaDataSourceClassName();
    abstract String jdbcUrl();
    abstract String username();
    abstract String password();
    abstract String slowSQL();

    /**
     * Whether this driver throws from the slow SQL when the reaper fires mid-execution.
     *
     * Most drivers (PostgreSQL, MySQL, MariaDB) let the slow SQL complete normally
     * because the XAConnectionLock read lock blocks end(TMFAIL) until the SQL finishes.
     *
     * MSSQL throws because MSDTC actively cancels the distributed transaction
     * independently of the Java-level lock.
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

    protected AgroalDataSource createXADataSource() throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry =
                new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        return AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClassName( xaDataSourceClassName() )
                                .jdbcUrl( jdbcUrl() )
                                .principal( new NamePrincipal( username() ) )
                                .credential( new SimplePassword( password() ) )
                        )
                ), new LoggingListener() );
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
    @DisplayName( "Post-reaper SQL is rejected by XAConnectionLock" )
    @Timeout( 60 )
    void reaperRaceWithRealDatabase() {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();

        try ( AgroalDataSource dataSource = createXADataSource() ) {
            verifyXASupported( dataSource );

            try ( Connection setup = dataSource.getConnection(); Statement s = setup.createStatement() ) {
                s.execute( createTableDDL() );
            }

            txManager.setTransactionTimeout( 2 );
            txManager.begin();

            try ( Connection connection = dataSource.getConnection() ) {
                Statement stmt = connection.createStatement();

                if ( slowSQLThrowsOnReaperTimeout() ) {
                    assertThatThrownBy( () -> stmt.execute( slowSQL() ) )
                            .as( dbName() + ": slowSQL must throw (driver cancels on reaper timeout)" )
                            .isInstanceOf( SQLException.class );
                } else {
                    try {
                        stmt.execute( slowSQL() );
                    } catch ( SQLException e ) {
                        fail( dbName() + ": slowSQL must NOT throw for this driver, but threw: " + e.getMessage(), e );
                    }
                }

                Thread.sleep( 2000 );

                assertThatThrownBy( () -> stmt.executeUpdate(
                        "INSERT INTO xa_reaper_test (id, val) VALUES (42, 'post-reaper')" ) )
                        .as( dbName() + ": post-reaper SQL must be rejected by XAConnectionLock" )
                        .isInstanceOf( SQLException.class );
            }
        } catch ( Exception e ) {
            fail( dbName() + ": " + e.getMessage(), e );
        } finally {
            rollbackQuietly( txManager );
        }
    }

    @Test
    @DisplayName( "INSERT after reaper does not persist — atomicity preserved" )
    @Timeout( 60 )
    void postReaperInsertMustNotPersist() {
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
                stmt.executeUpdate( "INSERT INTO xa_reaper_test (id, val) VALUES (1, 'inside-xa')" );

                Thread.sleep( 4000 );

                assertThatThrownBy( () -> stmt.executeUpdate(
                        "INSERT INTO xa_reaper_test (id, val) VALUES (2, 'post-reaper-LEAKED')" ) )
                        .as( dbName() + ": post-reaper INSERT must be rejected by XAConnectionLock" )
                        .isInstanceOf( SQLException.class );
            }

            rollbackQuietly( txManager );

            try ( Connection verify = dataSource.getConnection();
                  Statement s = verify.createStatement();
                  ResultSet rs = s.executeQuery( "SELECT COUNT(*) FROM xa_reaper_test" ) ) {
                rs.next();
                assertThat( rs.getInt( 1 ) )
                        .as( dbName() + ": row count must be 0 — row A rolled back, row B blocked by lock" )
                        .isEqualTo( 0 );
            }
        } catch ( Exception e ) {
            fail( dbName() + ": " + e.getMessage(), e );
        }
    }

    @Test
    @DisplayName( "Write lock blocks until read lock released, then poisons" )
    @Timeout( 30 )
    void concurrentReaperAndJdbc() throws Exception {
        try ( AgroalDataSource dataSource = createXADataSource() ) {
            verifyXASupported( dataSource );

            try ( Connection setup = dataSource.getConnection(); Statement s = setup.createStatement() ) {
                s.execute( createTableDDL() );
                s.executeUpdate( truncateTableSQL() );
            }

            XAConnectionLock lock = new XAConnectionLock();
            CountDownLatch readLockHeld = new CountDownLatch( 1 );
            CountDownLatch poisonDone = new CountDownLatch( 1 );
            AtomicBoolean secondAcquireThrew = new AtomicBoolean( false );
            AtomicReference<Throwable> threadError = new AtomicReference<>();
            long[] reaperBlockedMs = new long[1];

            Thread appThread = new Thread( () -> {
                try ( Connection conn = dataSource.getConnection();
                      Statement stmt = conn.createStatement() ) {
                    lock.acquireForJdbc();
                    try {
                        readLockHeld.countDown();
                        stmt.executeUpdate( "INSERT INTO xa_reaper_test (id, val) VALUES (10, 'under-read-lock')" );
                        Thread.sleep( 500 );
                    } finally {
                        lock.releaseForJdbc();
                    }

                    poisonDone.await( 5, TimeUnit.SECONDS );

                    try {
                        lock.acquireForJdbc();
                        lock.releaseForJdbc();
                    } catch ( SQLException e ) {
                        secondAcquireThrew.set( true );
                    }
                } catch ( Exception e ) {
                    threadError.set( e );
                }
            }, "app-thread" );

            Thread reaperThread = new Thread( () -> {
                try {
                    readLockHeld.await( 5, TimeUnit.SECONDS );
                    Thread.sleep( 100 );

                    long t0 = System.nanoTime();
                    lock.acquireForXaEnd();
                    reaperBlockedMs[0] = ( System.nanoTime() - t0 ) / 1_000_000;
                    try {
                        lock.markPoisoned( "XA branch ended with TMFAIL (test)" );
                    } finally {
                        lock.releaseForXaEnd();
                    }
                    poisonDone.countDown();
                } catch ( Exception e ) {
                    threadError.set( e );
                    poisonDone.countDown();
                }
            }, "reaper-thread" );

            appThread.start();
            reaperThread.start();
            appThread.join( 15_000 );
            reaperThread.join( 15_000 );

            assertThat( threadError.get() ).as( "no thread errors" ).isNull();
            assertThat( lock.isPoisoned() ).as( "lock must be poisoned" ).isTrue();
            assertThat( secondAcquireThrew.get() ).as( "acquireForJdbc must throw after poison" ).isTrue();
            assertThat( reaperBlockedMs[0] ).as( "write lock must have blocked while read lock was held" ).isGreaterThan( 300 );
        }
    }

    private String dbName() {
        return getClass().getSimpleName().replace( "XAReaperRaceIT", "" );
    }

    private static void rollbackQuietly( TransactionManager txManager ) {
        try { txManager.rollback(); } catch ( Exception ignore ) {}
    }

    private static class LoggingListener implements AgroalDataSourceListener {
        @Override public void onWarning(String message) { logger.warning( "Agroal: " + message ); }
        @Override public void onWarning(Throwable throwable) { logger.warning( "Agroal: " + throwable.getMessage() ); }
    }
}

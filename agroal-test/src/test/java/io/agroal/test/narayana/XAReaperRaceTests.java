// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.narayana;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.api.transaction.XAConnectionLock;
import io.agroal.test.MockConnection;
import io.agroal.test.MockXAConnection;
import io.agroal.test.MockXADataSource;
import io.agroal.test.MockXAResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for the XA reaper race condition fix using XAConnectionLock.
 *
 * Problem: When Narayana's TransactionReaper calls XAResource.end(TMFAIL) from
 * the reaper thread while the application thread is executing SQL, the connection
 * silently exits XA mode. Subsequent SQL executes as unmanaged local operations.
 *
 * Fix: A ReadWriteLock (XAConnectionLock) on ConnectionHandler serializes JDBC
 * execute operations (read lock) with XA branch termination (write lock).
 *
 * These tests use CountDownLatch as rendezvous points to create deterministic
 * thread interleaving that reproduces the race condition. The rendezvous pattern
 * is equivalent to Byteman's createRendezvous/rendezvous mechanism:
 *
 *   Byteman equivalent for SlowMockConnection.executeUpdate():
 *     @BMRule( name = "pause inside JDBC execute",
 *              targetClass = "io.agroal.test.narayana.XAReaperRaceTests$SlowMockConnection",
 *              targetMethod = "executeUpdate(String)",
 *              targetLocation = "AT ENTRY",
 *              action = "rendezvous(\"jdbc-in-flight\")" )
 *
 *   Byteman equivalent for BaseXAResource.end() completion:
 *     @BMRule( name = "signal reaper completed",
 *              targetClass = "io.agroal.narayana.BaseXAResource",
 *              targetMethod = "end",
 *              targetLocation = "AT EXIT",
 *              action = "rendezvous(\"reaper-completed\")" )
 *
 * @author Red Hat
 */
@Tag( FUNCTIONAL )
@Tag( TRANSACTION )
public class XAReaperRaceTests {

    static final Logger logger = getLogger( XAReaperRaceTests.class.getName() );

    @BeforeAll
    static void setup() {
        registerMockDriver( SlowMockConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Reaper end(TMFAIL) blocks until in-flight JDBC completes, then poisons connection" )
    @Timeout( 30 )
    void reaperBlocksUntilJdbcCompletes() {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new SilentAgroalListener() ) ) {
            // 1-second timeout. The reaper fires on its own thread.
            txManager.setTransactionTimeout( 1 );
            txManager.begin();

            Connection connection = dataSource.getConnection();
            SlowMockConnection tracked = connection.unwrap( SlowMockConnection.class );

            // Configure the mock: executeUpdate() will block on this latch,
            // simulating a slow SQL statement that holds the read lock.
            // This is the equivalent of a Byteman rendezvous("jdbc-in-flight").
            CountDownLatch jdbcMayContinue = new CountDownLatch( 1 );
            CountDownLatch jdbcIsBlocked = new CountDownLatch( 1 );
            tracked.setSlowExecuteLatches( jdbcIsBlocked, jdbcMayContinue );

            // Release the mock after the reaper has had time to fire.
            // The reaper will call end(TMFAIL) which blocks on the write lock
            // until executeUpdate releases the read lock.
            Thread releaser = new Thread( () -> {
                try {
                    // Wait until the main thread is blocked in executeUpdate
                    jdbcIsBlocked.await( 5, TimeUnit.SECONDS );
                    logger.info( "RELEASER: main thread is blocked in executeUpdate, read lock held" );

                    // Wait for the reaper to fire (1-second timeout + margin)
                    Thread.sleep( 3000 );
                    logger.info( "RELEASER: reaper should have fired by now, releasing mock" );

                    // Release the mock so executeUpdate completes and read lock is released.
                    // The reaper's end(TMFAIL) will then acquire the write lock and poison.
                    jdbcMayContinue.countDown();
                } catch ( Exception e ) {
                    logger.warning( "RELEASER: " + e.getMessage() );
                    jdbcMayContinue.countDown();
                }
            }, "releaser-thread" );
            releaser.start();

            Statement statement = connection.createStatement();

            // First execute: blocks inside the mock while read lock is held.
            // The reaper fires during this time, calls end(TMFAIL), blocks on write lock.
            // The releaser thread releases the mock after the reaper fires.
            logger.info( "MAIN: calling executeUpdate (will block in mock)" );
            statement.executeUpdate( "INSERT INTO test VALUES (1)" );
            logger.info( "MAIN: first executeUpdate completed" );

            // Wait for the reaper to finish processing end(TMFAIL) + poison
            Thread.sleep( 2000 );

            // Second execute: should throw because the connection is poisoned.
            logger.info( "MAIN: calling second executeUpdate (should throw)" );
            assertThrows( SQLException.class, () -> statement.executeUpdate( "INSERT INTO test VALUES (2)" ),
                    "Second executeUpdate should throw because connection is poisoned after reaper end(TMFAIL)" );
            logger.info( "MAIN: second executeUpdate threw as expected" );

            releaser.join( 5_000 );

        } catch ( Exception e ) {
            fail( e );
        } finally {
            try {
                txManager.rollback();
            } catch ( Exception e ) {
                // ignore — transaction may already be rolled back by the reaper
            }
        }
    }

    @Test
    @DisplayName( "XAConnectionLock: write lock blocks until read lock released, then poisons" )
    @Timeout( 10 )
    void lockSemanticsDirectly() throws Exception {

        XAConnectionLock lock = new XAConnectionLock();

        AtomicInteger eventOrder = new AtomicInteger( 0 );
        int[] jdbcReleased = new int[1];
        int[] reaperAcquired = new int[1];

        // Rendezvous: app thread signals when it holds the read lock
        CountDownLatch jdbcHoldsLock = new CountDownLatch( 1 );
        CountDownLatch reaperDone = new CountDownLatch( 1 );

        // Thread A: holds read lock (simulates JDBC in flight)
        Thread appThread = new Thread( () -> {
            try {
                lock.acquireForJdbc();
                try {
                    jdbcHoldsLock.countDown();
                    // Simulate slow SQL — hold the read lock for 300ms
                    Thread.sleep( 300 );
                    jdbcReleased[0] = eventOrder.incrementAndGet();
                } finally {
                    lock.releaseForJdbc();
                }
            } catch ( Exception e ) {
                fail( "App thread should not throw", e );
            }
        }, "app-thread" );

        // Thread B: tries write lock (simulates reaper calling end(TMFAIL))
        Thread reaperThread = new Thread( () -> {
            try {
                jdbcHoldsLock.await();
                // Small delay to ensure the read lock is definitely held
                Thread.sleep( 50 );

                lock.acquireForXaEnd();
                try {
                    reaperAcquired[0] = eventOrder.incrementAndGet();
                    lock.markPoisoned( "XA branch ended with TMFAIL" );
                } finally {
                    lock.releaseForXaEnd();
                }
                reaperDone.countDown();
            } catch ( Exception e ) {
                fail( "Reaper thread should not throw", e );
            }
        }, "reaper-thread" );

        appThread.start();
        reaperThread.start();

        assertTrue( reaperDone.await( 5, TimeUnit.SECONDS ), "Reaper should complete" );
        appThread.join( 5_000 );

        // JDBC released read lock BEFORE reaper acquired write lock
        assertTrue( jdbcReleased[0] < reaperAcquired[0],
                "JDBC must release before reaper acquires. jdbcReleased=" + jdbcReleased[0] + " reaperAcquired=" + reaperAcquired[0] );

        // Connection is poisoned
        assertTrue( lock.isPoisoned(), "Connection should be poisoned after end(TMFAIL)" );

        // Subsequent JDBC calls throw
        SQLException ex = assertThrows( SQLException.class, lock::acquireForJdbc, "acquireForJdbc should throw on poisoned connection" );
        assertTrue( ex.getMessage().contains( "not usable" ), "Error should indicate connection is not usable" );
    }

    @Test
    @DisplayName( "XAConnectionLock: reset clears poison for connection reuse" )
    void resetClearsPoison() throws Exception {
        XAConnectionLock lock = new XAConnectionLock();

        // Poison
        lock.acquireForXaEnd();
        lock.markPoisoned( "XA branch ended with TMFAIL" );
        lock.releaseForXaEnd();
        assertTrue( lock.isPoisoned() );
        assertThrows( SQLException.class, lock::acquireForJdbc );

        // Reset (simulates XAResource.start() on connection reuse)
        lock.reset();
        assertFalse( lock.isPoisoned() );

        // JDBC should succeed again
        lock.acquireForJdbc();
        lock.releaseForJdbc();
    }

    // --- //

    private static void assertNull(Object value) {
        if ( value != null ) {
            fail( "Expected null but got: " + value );
        }
    }

    // --- Mocks //

    /**
     * Mock connection whose executeUpdate() blocks on a CountDownLatch,
     * simulating a slow SQL statement. This holds the XAConnectionLock
     * read lock for the duration of the block, exercising the race
     * between the application thread and the reaper thread.
     *
     * In a Byteman test, this blocking behavior would be injected via:
     *   @BMRule( targetMethod = "executeUpdate(String)",
     *            targetLocation = "AT ENTRY",
     *            action = "rendezvous(\"jdbc-in-flight\")" )
     */
    public static class SlowMockConnection implements MockConnection {

        private volatile CountDownLatch isBlocked;
        private volatile CountDownLatch mayContinue;
        private boolean commit, rollback;

        public void setSlowExecuteLatches(CountDownLatch isBlocked, CountDownLatch mayContinue) {
            this.isBlocked = isBlocked;
            this.mayContinue = mayContinue;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return new SlowStatement( this );
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return null;
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
        public void rollback(Savepoint savepoint) throws SQLException {
            rollback = true;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public <T> T unwrap(Class<T> target) throws SQLException {
            return (T) this;
        }

        void slowExecute() throws SQLException {
            CountDownLatch blocked = this.isBlocked;
            CountDownLatch cont = this.mayContinue;
            if ( blocked != null && cont != null ) {
                blocked.countDown();
                try {
                    if ( !cont.await( 10, TimeUnit.SECONDS ) ) {
                        throw new SQLException( "Timed out waiting for release in mock" );
                    }
                } catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                    throw new SQLException( "Interrupted in mock", e );
                }
                // Clear latches so subsequent calls don't block
                this.isBlocked = null;
                this.mayContinue = null;
            }
        }
    }

    private static class SlowStatement implements io.agroal.test.MockStatement {

        private final SlowMockConnection mockConnection;

        SlowStatement(SlowMockConnection conn) {
            this.mockConnection = conn;
        }

        @Override
        public int executeUpdate(String sql) throws SQLException {
            mockConnection.slowExecute();
            return 1;
        }

        @Override
        public boolean execute(String sql) throws SQLException {
            mockConnection.slowExecute();
            return false;
        }

        @Override
        public ResultSet executeQuery(String sql) throws SQLException {
            mockConnection.slowExecute();
            return null;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public <T> T unwrap(Class<T> target) throws SQLException {
            return (T) this;
        }
    }

    private static class SilentAgroalListener implements AgroalDataSourceListener {

        @SuppressWarnings( "WeakerAccess" )
        SilentAgroalListener() {
        }

        @Override
        public void onWarning(String message) {
            logger.warning( "Agroal warning: " + message );
        }

        @Override
        public void onWarning(Throwable throwable) {
            logger.warning( "Agroal warning: " + throwable.getMessage() );
        }
    }
}

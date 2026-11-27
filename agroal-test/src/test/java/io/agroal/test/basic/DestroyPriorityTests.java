// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.lang.System.nanoTime;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Reproducer for https://github.com/agroal/agroal/issues/216 (AG-276 follow-up).
 * <p>
 * Connection creation is dispatched to the single housekeeping thread with {@code executeNow()} (high priority) while
 * {@code DestroyConnectionTask} is dispatched with {@code execute()} (low priority), unless {@code flushOnClose} is set.
 * The high priority queue is fully drained on {@code beforeExecute()}, so every pending creation runs before a single
 * destruction, even when the destruction was enqueued first. Physical connections therefore pile up on the database
 * side while the application side pool stays within max-size.
 * <p>
 * AG-329 also covers {@code ConnectionPool.close()}, which recovers the pending {@code DestroyConnectionTask} from
 * {@code shutdownNow()}: those came back wrapped in a {@code Future}, so the {@code instanceof} test never matched and
 * the connections still queued for destruction were leaked.
 */
@Tag( FUNCTIONAL )
public class DestroyPriorityTests {

    private static final Logger logger = getLogger( DestroyPriorityTests.class.getName() );

    // simulated cost of establishing a connection on the database
    private static final long CONNECT_LATENCY_MS = 50;

    private static final AtomicInteger liveConnections = new AtomicInteger();
    private static final AtomicInteger peakLiveConnections = new AtomicInteger();

    @BeforeAll
    static void setup() {
        registerMockDriver( new SlowDriver() );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Destruction is starved by connection creation on flush" )
    void destroyStarvedByCreationTest() throws SQLException {
        int POOL_SIZE = 10;

        liveConnections.set( 0 );
        peakLiveConnections.set( 0 );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( POOL_SIZE )
                        .minSize( POOL_SIZE )
                        .maxSize( POOL_SIZE )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            await( () -> dataSource.getMetrics().creationCount() == POOL_SIZE, "initial fill" );
            logger.info( format( "Pool filled: {0} connections live on the database", liveConnections.get() ) );

            peakLiveConnections.set( liveConnections.get() );

            // FlushTask enqueues all the DestroyConnectionTask (low priority) and only then the refill (high priority)
            dataSource.flush( AgroalDataSource.FlushMode.GRACEFUL );

            await( () -> dataSource.getMetrics().creationCount() == 2L * POOL_SIZE, "refill after flush" );

            int peakDuringRefill = peakLiveConnections.get();
            long destroyed = dataSource.getMetrics().destroyCount();
            logger.info( format( "Refill complete: peak of {0} physical connections (max-size {1}), {2} of {3} old connections destroyed",
                    peakDuringRefill, POOL_SIZE, destroyed, POOL_SIZE ) );

            assertTrue( peakDuringRefill <= POOL_SIZE, format(
                    "AG-276/#216: {0} physical connections open on the database while max-size is {1}. "
                            + "Destruction was enqueued before the refill but runs at a lower priority.",
                    peakDuringRefill, POOL_SIZE ) );
        }
    }

    @Test
    @DisplayName( "Pending destruction is recovered when the pool is closed" )
    void pendingDestroyOnCloseTest() throws SQLException {
        int POOL_SIZE = 10;

        liveConnections.set( 0 );
        peakLiveConnections.set( 0 );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( POOL_SIZE )
                        .minSize( POOL_SIZE )
                        .maxSize( POOL_SIZE )
                );

        CountDownLatch firstDestroy = new CountDownLatch( 1 );
        AtomicBoolean blockDestroy = new AtomicBoolean( true );

        // hold the housekeeping thread on the first destruction, so that the others are still queued when the pool closes
        AgroalDataSourceListener listener = new AgroalDataSourceListener() {
            @Override
            public void beforeConnectionDestroy(Connection connection) {
                if ( blockDestroy.compareAndSet( true, false ) ) {
                    firstDestroy.countDown();
                    LockSupport.parkNanos( ofMillis( 5_000 ).toNanos() );
                }
            }
        };

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            await( () -> dataSource.getMetrics().creationCount() == POOL_SIZE, "initial fill" );
            assertEquals( POOL_SIZE, liveConnections.get(), "Unexpected number of physical connections after fill" );

            dataSource.flush( AgroalDataSource.FlushMode.GRACEFUL );
            await( () -> firstDestroy.getCount() == 0, "first connection to be destroyed" );
            await( () -> dataSource.getMetrics().flushCount() == POOL_SIZE, "flush" );
        }

        // the destruction that was already in flight completes on the (interrupted) housekeeping thread
        await( () -> liveConnections.get() == 0, "pending destruction to be recovered on close" );
        logger.info( "After close: no physical connection left open on the database" );
    }

    private static void await(BooleanSupplier condition, String stage) {
        long deadline = nanoTime() + ofMillis( 10_000 ).toNanos();
        while ( !condition.getAsBoolean() ) {
            if ( nanoTime() > deadline ) {
                fail( format( "Timeout waiting for {0}", stage ) );
            }
            LockSupport.parkNanos( ofMillis( 5 ).toNanos() );
        }
    }

    // --- //

    public static class SlowDriver implements MockDriver {

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            LockSupport.parkNanos( ofMillis( CONNECT_LATENCY_MS ).toNanos() ); // simulated database round trip
            int live = liveConnections.incrementAndGet();
            peakLiveConnections.accumulateAndGet( live, Math::max );
            return new TrackedConnection();
        }
    }

    private static class TrackedConnection implements MockConnection {

        private boolean closed;

        @Override
        public void close() throws SQLException {
            if ( !closed ) {
                closed = true;
                liveConnections.decrementAndGet();
            }
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed;
        }
    }
}

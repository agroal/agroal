// Copyright (C) 2026 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.CONCURRENCY;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AG-328 - A connection returned between the shared cache scan and the blocking poll in
 * waitAvailableHandler must not be lost: the waiting thread has to pick it up right away
 * instead of sleeping for the whole acquisition timeout.
 *
 * @author <a href="gegastaldi@gmail.com">George Gastaldi</a>
 */
@Tag( FUNCTIONAL )
@Tag( CONCURRENCY )
public class LostWakeupTests {

    private static final Logger logger = getLogger( LostWakeupTests.class.getName() );

    @BeforeEach
    void setup() {
        registerMockDriver();
    }

    @AfterEach
    void teardown() {
        deregisterMockDriver();
    }

    @Test
    @DisplayName( "Connection returned just before the pool blocks wakes the waiting thread" )
    void returnBetweenScanAndWaitTest() throws SQLException, InterruptedException {
        // Generously above the one second the pool now waits before scanning again, so that the
        // assertion below fails on a lost wakeup (full timeout) and not on scheduling noise
        int ACQUISITION_TIMEOUT_MS = 6000;

        // Released by the acquiring thread once it is inside beforePoolBlock, i.e. after it scanned
        // the pool and before it enters TransferQueue.poll()
        CountDownLatch aboutToBlock = new CountDownLatch( 1 );
        // Released by the holding thread once the connection is back in the pool
        CountDownLatch connectionReturned = new CountDownLatch( 1 );
        // Armed only once the single connection is checked out, so that the hook is not consumed by
        // the beforePoolBlock fired while the pool is still growing
        AtomicBoolean armed = new AtomicBoolean( false );

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( 1 )
                        .minSize( 0 )
                        .maxSize( 1 )
                        .acquisitionTimeout( ofMillis( ACQUISITION_TIMEOUT_MS ) )
                );

        AgroalDataSourceListener listener = new AgroalDataSourceListener() {
            @Override
            public void beforePoolBlock(long timeout) {
                if ( armed.compareAndSet( true, false ) ) {
                    aboutToBlock.countDown();
                    try {
                        // Hold the acquiring thread here until the connection is actually back in the
                        // pool, so that tryTransfer() runs while no consumer is blocked in poll()
                        connectionReturned.await( ACQUISITION_TIMEOUT_MS, MILLISECONDS );
                    } catch ( InterruptedException e ) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };

        ExecutorService executor = newSingleThreadExecutor();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            Connection held = dataSource.getConnection();
            armed.set( true );

            long[] elapsed = new long[1];
            Throwable[] failure = new Throwable[1];
            CountDownLatch acquired = new CountDownLatch( 1 );

            executor.submit( () -> {
                long start = System.nanoTime();
                try ( Connection connection = dataSource.getConnection() ) {
                    elapsed[0] = System.nanoTime() - start;
                    logger.info( format( "Acquired connection {0} after {1}ms", connection,
                            NANOSECONDS.toMillis( elapsed[0] ) ) );
                } catch ( Throwable t ) {
                    elapsed[0] = System.nanoTime() - start;
                    failure[0] = t;
                } finally {
                    acquired.countDown();
                }
            } );

            // Wait until the acquiring thread is past the scan and about to block
            assertTrue( aboutToBlock.await( ACQUISITION_TIMEOUT_MS, MILLISECONDS ), "Pool never blocked" );
            held.close();
            connectionReturned.countDown();

            assertTrue( acquired.await( 2 * ACQUISITION_TIMEOUT_MS, MILLISECONDS ), "Acquisition never completed" );

            if ( failure[0] != null ) {
                logger.info( format( "Acquisition failed after {0}ms with {1}",
                        NANOSECONDS.toMillis( elapsed[0] ), failure[0] ) );
            }
            assertTrue( failure[0] == null,
                    "Acquisition failed although a connection was idle in the pool: " + failure[0] );
            assertTrue( NANOSECONDS.toMillis( elapsed[0] ) < ACQUISITION_TIMEOUT_MS / 2,
                    format( "Lost wakeup: acquisition took {0}ms of the {1}ms acquisition timeout with an idle connection in the pool",
                            NANOSECONDS.toMillis( elapsed[0] ), ACQUISITION_TIMEOUT_MS ) );
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName( "Connection returned just before the pool blocks is not lost with no acquisition timeout" )
    void returnBetweenScanAndWaitWithoutTimeoutTest() throws SQLException, InterruptedException {
        int GRACE_MS = 2000;

        CountDownLatch aboutToBlock = new CountDownLatch( 1 );
        CountDownLatch connectionReturned = new CountDownLatch( 1 );
        AtomicBoolean armed = new AtomicBoolean( false );

        // acquisitionTimeout defaults to ZERO, meaning the pool blocks indefinitely
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( 1 )
                        .minSize( 0 )
                        .maxSize( 1 )
                );

        AgroalDataSourceListener listener = new AgroalDataSourceListener() {
            @Override
            public void beforePoolBlock(long timeout) {
                if ( armed.compareAndSet( true, false ) ) {
                    aboutToBlock.countDown();
                    try {
                        connectionReturned.await( GRACE_MS, MILLISECONDS );
                    } catch ( InterruptedException e ) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };

        ExecutorService executor = newSingleThreadExecutor();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            Connection held = dataSource.getConnection();
            armed.set( true );

            CountDownLatch acquired = new CountDownLatch( 1 );
            executor.submit( () -> {
                try ( Connection connection = dataSource.getConnection() ) {
                    logger.info( format( "Acquired connection {0}", connection ) );
                } catch ( Throwable t ) {
                    logger.info( format( "Acquisition failed with {0}", t ) );
                } finally {
                    acquired.countDown();
                }
            } );

            assertTrue( aboutToBlock.await( GRACE_MS, MILLISECONDS ), "Pool never blocked" );
            held.close();
            connectionReturned.countDown();

            assertTrue( acquired.await( GRACE_MS, MILLISECONDS ),
                    "Lost wakeup: thread is still blocked although a connection is idle in the pool" );
        } finally {
            executor.shutdownNow();
        }
    }
}

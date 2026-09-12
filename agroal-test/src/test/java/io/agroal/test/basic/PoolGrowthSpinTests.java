// Copyright (C) 2026 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.basic.BasicConcurrencyTests.SlowDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.CONCURRENCY;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AG-320 - Threads waiting for a connection must not busy-spin while the pool grows.
 *
 * @author <a href="gegastaldi@gmail.com">George Gastaldi</a>
 */
@Tag( FUNCTIONAL )
@Tag( CONCURRENCY )
public class PoolGrowthSpinTests {

    private static final Logger logger = getLogger( PoolGrowthSpinTests.class.getName() );

    @AfterEach
    void teardown() {
        deregisterMockDriver();
    }

    @Test
    @DisplayName( "No busy-spin while the pool grows with slow connection establishment" )
    void noSpinWhileGrowingTest() throws SQLException, InterruptedException {
        registerMockDriver( new SlowDriver( ofMillis( 500 ) ) );
        assertTrue( acquisitionLoopCount() < 500, "Acquisition loop spins while connections are being created" );
    }

    @Test
    @DisplayName( "No busy-spin while the pool grows with fast connection establishment" )
    void noSpinWhileGrowingFastTest() throws SQLException, InterruptedException {
        registerMockDriver();
        assertTrue( acquisitionLoopCount() < 500, "Acquisition loop spins while connections are being created" );
    }

    // --- //

    private long acquisitionLoopCount() throws SQLException, InterruptedException {
        int MAX_SIZE = 4, CONCURRENCY = 16, TIMEOUT_MS = 10_000;
        CountDownLatch startLatch = new CountDownLatch( 1 ), doneLatch = new CountDownLatch( CONCURRENCY );
        LongAdder cpuNanos = new LongAdder();
        PoolBlockCountListener listener = new PoolBlockCountListener();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( 0 )
                        .minSize( 0 )
                        .maxSize( MAX_SIZE )
                        .acquisitionTimeout( ofMillis( TIMEOUT_MS ) )
                );

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ExecutorService executor = newFixedThreadPool( CONCURRENCY );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            for ( int i = 0; i < CONCURRENCY; i++ ) {
                executor.submit( () -> {
                    try {
                        startLatch.await();
                        long cpuStart = threadMXBean.getCurrentThreadCpuTime();
                        try ( Connection connection = dataSource.getConnection() ) {
                            Thread.sleep( 50 );
                        } finally {
                            cpuNanos.add( threadMXBean.getCurrentThreadCpuTime() - cpuStart );
                        }
                    } catch ( SQLException e ) {
                        logger.warning( "Unexpected SQLException: " + e.getMessage() );
                    } catch ( InterruptedException e ) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                } );
            }

            long start = System.nanoTime();
            startLatch.countDown();
            assertTrue( doneLatch.await( TIMEOUT_MS * 2L, MILLISECONDS ), "Threads did not complete" );
            long elapsedMillis = MILLISECONDS.convert( System.nanoTime() - start, NANOSECONDS );

            logger.info( format( "{0} acquisitions in {1}ms consumed {2}ms of CPU and looped {3} times",
                    CONCURRENCY, elapsedMillis, cpuNanos.sum() / 1_000_000, listener.getCount() ) );
            return listener.getCount();
        } finally {
            executor.shutdownNow();
        }
    }

    // --- //

    private static class PoolBlockCountListener implements AgroalDataSourceListener {

        private final LongAdder count = new LongAdder();

        @Override
        public void beforePoolBlock(long timeout) {
            count.increment();
        }

        long getCount() {
            return count.sum();
        }
    }
}

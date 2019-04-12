// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class LifetimeTests {

    private static final Logger logger = getLogger( LifetimeTests.class.getName() );

    private static final String FAKE_SCHEMA = "skeema";

    @BeforeAll
    public static void setupMockDriver() {
        registerMockDriver( FakeSchemaConnection.class );
    }

    @AfterAll
    public static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Lifetime Test" )
    public void basicLifetimeTest() throws SQLException {
        int MIN_POOL_SIZE = 40, MAX_POOL_SIZE = 100, MAX_LIFETIME_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .maxLifetime( ofMillis( MAX_LIFETIME_MS ) )
                );

        CountDownLatch allLatch = new CountDownLatch( MAX_POOL_SIZE );
        CountDownLatch destroyLatch = new CountDownLatch( MAX_POOL_SIZE );
        LongAdder flushCount = new LongAdder();

        AgroalDataSourceListener listener = new MaxLifetimeListener( allLatch, flushCount, destroyLatch );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            try {
                logger.info( format( "Awaiting creation of all the {0} connections on the pool", MAX_POOL_SIZE ) );
                if ( !allLatch.await( MAX_LIFETIME_MS, MILLISECONDS ) ) {
                    fail( format( "{0} connections not created for maxLifetime", allLatch.getCount() ) );
                }
                assertEquals( MAX_POOL_SIZE, dataSource.getMetrics().creationCount(), "Unexpected number of connections on the pool" );

                logger.info( format( "Waiting for removal of {0} connections ", MAX_POOL_SIZE ) );
                if ( !destroyLatch.await( 2L * MAX_LIFETIME_MS, MILLISECONDS ) ) {
                    fail( format( "{0} old connections not sent for destruction", destroyLatch.getCount() ) );
                }
                assertEquals( MAX_POOL_SIZE, flushCount.longValue(), "Unexpected number of old connections" );
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
        }
    }

    @Test
    @DisplayName( "Connection in use reaches maxLifetime" )
    public void inUseLifetimeTest() throws SQLException {
        int MAX_LIFETIME_MS = 200;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .maxLifetime( ofMillis( MAX_LIFETIME_MS ) )
                );

        CountDownLatch allLatch = new CountDownLatch( 1 );
        CountDownLatch destroyLatch = new CountDownLatch( 1 );
        LongAdder flushCount = new LongAdder();

        AgroalDataSourceListener listener = new MaxLifetimeListener( allLatch, flushCount, destroyLatch );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                connection.getSchema();

                logger.info( format( "Waiting for {0}ms (twice the maxLifetime)", 2 * MAX_LIFETIME_MS ) );
                LockSupport.parkNanos( ofMillis( 2 * MAX_LIFETIME_MS ).toNanos() );

                assertFalse( connection.isClosed() );
                assertEquals( 1, dataSource.getMetrics().creationCount(), "Unexpected number of connections on the pool" );
                assertEquals( 0, flushCount.longValue(), "Unexpected number of flushed connections" );
            }

            try {
                logger.info( format( "Waiting for removal of {0} connections", 1 ) );
                if ( !destroyLatch.await( MAX_LIFETIME_MS, MILLISECONDS ) ) {
                    fail( format( "{0} old connections not sent for destruction", destroyLatch.getCount() ) );
                }
                assertEquals( 1, flushCount.longValue(), "Unexpected number of old connections" );
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
        }
    }

    // --- //

    private static class MaxLifetimeListener implements AgroalDataSourceListener {

        private final CountDownLatch allLatch;
        private final LongAdder flushCount;
        private final CountDownLatch destroyLatch;

        public MaxLifetimeListener(CountDownLatch allLatch, LongAdder flushCount, CountDownLatch destroyLatch) {
            this.allLatch = allLatch;
            this.flushCount = flushCount;
            this.destroyLatch = destroyLatch;
        }

        @Override
        public void onConnectionPooled(Connection connection) {
            allLatch.countDown();
        }

        @Override
        public void onConnectionFlush(Connection connection) {
            flushCount.increment();
        }

        @Override
        public void beforeConnectionDestroy(Connection connection) {
            destroyLatch.countDown();
        }
    }

    // --- //

    public static class FakeSchemaConnection implements MockConnection {

        private boolean closed = false;

        @Override
        public String getSchema() throws SQLException {
            return FAKE_SCHEMA;
        }

        @Override
        public void close() throws SQLException {
            if ( closed ) {
                fail( "Double close on connection" );
            } else {
                closed = true;
            }
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed;
        }
    }
}

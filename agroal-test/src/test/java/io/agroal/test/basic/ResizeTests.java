// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.cache.LocalConnectionCache;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.lang.Integer.max;
import static java.text.MessageFormat.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class ResizeTests {

    private static final Logger logger = getLogger( ResizeTests.class.getName() );

    @BeforeAll
    static void setupMockDriver() {
        registerMockDriver();
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @SuppressWarnings( "AnonymousInnerClassMayBeStatic" )
    @Test
    @DisplayName( "resize Max" )
    void resizeMax() throws SQLException {
        int INITIAL_SIZE = 10, MAX_SIZE = 6, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( INITIAL_SIZE )
                        .maxSize( MAX_SIZE )
                );

        CountDownLatch creationLatch = new CountDownLatch( INITIAL_SIZE );
        CountDownLatch destroyLatch = new CountDownLatch( INITIAL_SIZE - MAX_SIZE );
        AgroalDataSourceListener listener = new AgroalDataSourceListener() {
            @Override
            public void onConnectionPooled(Connection connection) {
                creationLatch.countDown();
            }

            @Override
            public void onConnectionDestroy(Connection connection) {
                destroyLatch.countDown();
            }
        };

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            logger.info( format( "Awaiting fill of all the {0} initial connections on the pool", INITIAL_SIZE ) );
            if ( !creationLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", creationLatch.getCount() ) );
            }
            assertEquals( INITIAL_SIZE, dataSource.getMetrics().availableCount(), "Pool not initialized correctly" );

            for ( int i = INITIAL_SIZE; i > 0; i-- ) {
                assertEquals( max( MAX_SIZE, i ), dataSource.getMetrics().availableCount(), "Pool not resized" );

                try ( Connection c = dataSource.getConnection() ) {
                    assertNotNull( c );
                }
            }

            logger.info( format( "Waiting for destruction of {0} connections ", INITIAL_SIZE - MAX_SIZE ) );
            if ( !destroyLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} flushed connections not sent for destruction", destroyLatch.getCount() ) );
            }

            assertEquals( MAX_SIZE, dataSource.getMetrics().availableCount(), "Pool not resized" );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    // --- //

    @Test
    @DisplayName( "resize Min" )
    void resizeMin() throws SQLException {
        int INITIAL_SIZE = 10, NEW_MIN_SIZE = 15, MAX_SIZE = 35, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .connectionCache( LocalConnectionCache.single() ) // this test expects thread local cache
                        .maxSize( MAX_SIZE )
                        .initialSize( INITIAL_SIZE )
                );

        CountDownLatch creationLatch = new CountDownLatch( INITIAL_SIZE );
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new ReadyDataSourceListener( creationLatch ) ) ) {
            logger.info( format( "Awaiting fill of all the {0} initial connections on the pool", INITIAL_SIZE ) );
            if ( !creationLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", creationLatch.getCount() ) );
            }

            assertEquals( INITIAL_SIZE, dataSource.getMetrics().availableCount(), "Pool not initialized correctly" );
            dataSource.getConfiguration().connectionPoolConfiguration().setMinSize( NEW_MIN_SIZE );

            // This should be a new connection and not one from the initial
            try ( Connection c = dataSource.getConnection() ) {
                assertNotNull( c );
            }
            assertEquals( INITIAL_SIZE + 1, dataSource.getMetrics().availableCount(), "Pool not resized" );

            // This will come from thread local cache, and unfortunately not increase the size of the pool
            try ( Connection c = dataSource.getConnection() ) {
                assertNotNull( c );
            }
            assertEquals( INITIAL_SIZE + 1, dataSource.getMetrics().availableCount(), "Pool not resized" );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    private static class ReadyDataSourceListener implements AgroalDataSourceListener {
        private final CountDownLatch creationLatch;

        @SuppressWarnings( "WeakerAccess" )
        ReadyDataSourceListener(CountDownLatch creationLatch) {
            this.creationLatch = creationLatch;
        }

        @Override
        public void onConnectionPooled(Connection connection) {
            creationLatch.countDown();
        }
    }
}

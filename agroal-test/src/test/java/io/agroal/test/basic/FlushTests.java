// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.text.MessageFormat.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class FlushTests {

    private static final Logger logger = getLogger( FlushTests.class.getName() );

    @BeforeAll
    public static void setupMockDriver() {
        registerMockDriver( FakeConnection.class );
    }

    @AfterAll
    public static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "FlushMode.ALL" )
    public void modeAll() throws SQLException {
        int MIN_POOL_SIZE = 40, MAX_POOL_SIZE = 100, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                );

        FlushListener listener = new FlushListener();
        listener.creationLatch = new CountDownLatch( MAX_POOL_SIZE );
        listener.flushLatch = new CountDownLatch( MAX_POOL_SIZE );
        listener.destroyLatch = new CountDownLatch( MAX_POOL_SIZE );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {

            logger.info( format( "Awaiting fill of all the {0} initial connections on the pool", MAX_POOL_SIZE ) );
            if ( !listener.creationLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", listener.creationLatch.getCount() ) );
            }
            listener.creationLatch = new CountDownLatch( MIN_POOL_SIZE );

            Connection connection = dataSource.getConnection();
            assertFalse( connection.isClosed() );

            dataSource.flush( AgroalDataSource.FlushMode.ALL );

            logger.info( format( "Awaiting flush of all the {0} connections on the pool", MAX_POOL_SIZE ) );
            if ( !listener.flushLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not flush", listener.flushLatch.getCount() ) );
            }
            logger.info( format( "Waiting for destruction of {0} connections ", MAX_POOL_SIZE - MIN_POOL_SIZE ) );
            if ( !listener.destroyLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} flushed connections not sent for destruction", listener.destroyLatch.getCount() ) );
            }
            logger.info( format( "Awaiting fill of all the {0} min connections on the pool", MIN_POOL_SIZE ) );
            if ( !listener.creationLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", listener.creationLatch.getCount() ) );
            }

            assertAll( () -> {
                assertTrue( connection.isClosed(), "Expecting connection closed after forced flush" );

                assertEquals( MAX_POOL_SIZE, listener.flushCount.longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE, listener.destroyCount.longValue(), "Unexpected number of destroyed connections" );
                assertEquals( MAX_POOL_SIZE + MIN_POOL_SIZE, listener.creationCount.longValue(), "Unexpected number of created connections" );
            } );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    @Test
    @DisplayName( "FlushMode.GRACEFUL" )
    public void modeGraceful() throws SQLException {
        int MIN_POOL_SIZE = 10, MAX_POOL_SIZE = 30, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                );

        FlushListener listener = new FlushListener();
        listener.creationLatch = new CountDownLatch( MAX_POOL_SIZE );
        listener.flushLatch = new CountDownLatch( MAX_POOL_SIZE - 1 );
        listener.destroyLatch = new CountDownLatch( MAX_POOL_SIZE - 1 );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {

            logger.info( format( "Awaiting fill of all the {0} initial connections on the pool", MAX_POOL_SIZE ) );
            if ( !listener.creationLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", listener.creationLatch.getCount() ) );
            }

            listener.creationLatch = new CountDownLatch( MIN_POOL_SIZE - 1 );

            Connection connection = dataSource.getConnection();
            assertFalse( connection.isClosed() );

            dataSource.flush( AgroalDataSource.FlushMode.GRACEFUL );

            logger.info( format( "Awaiting flush of the {0} connections on the pool", MAX_POOL_SIZE - 1 ) );
            if ( !listener.flushLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not flush", listener.flushLatch.getCount() ) );
            }
            logger.info( format( "Waiting for destruction of {0} connections ", MAX_POOL_SIZE - 1 ) );
            if ( !listener.destroyLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} flushed connections not sent for destruction", listener.destroyLatch.getCount() ) );
            }
            if ( !listener.creationLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created after flush", listener.creationLatch.getCount() ) );
            }

            listener.flushLatch = new CountDownLatch( 1 );
            listener.destroyLatch = new CountDownLatch( 1 );

            assertAll( () -> {
                assertEquals( MAX_POOL_SIZE, listener.flushCount.longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE - 1, listener.destroyCount.longValue(), "Unexpected number of destroy connections" );
                assertEquals( MAX_POOL_SIZE + MIN_POOL_SIZE - 1, listener.creationCount.longValue(), "Unexpected number of created connections" );
                assertEquals( MIN_POOL_SIZE, dataSource.getMetrics().availableCount(), "Pool not fill to min" );

                assertFalse( connection.isClosed(), "Expecting connection open after graceful flush" );
            } );

            connection.close();

            logger.info( format( "Awaiting flush of one remaining connection" ) );
            if ( !listener.flushLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not flush", listener.flushLatch.getCount() ) );
            }
            logger.info( format( "Waiting for destruction of one remaining connection" ) );
            if ( !listener.destroyLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} flushed connections not sent for destruction", listener.destroyLatch.getCount() ) );
            }

            assertAll( () -> {
                assertEquals( MAX_POOL_SIZE, listener.destroyCount.longValue(), "Unexpected number of destroy connections" );
                assertEquals( MAX_POOL_SIZE + MIN_POOL_SIZE, listener.creationCount.longValue(), "Unexpected number of created connections" );
                assertEquals( MIN_POOL_SIZE, dataSource.getMetrics().availableCount(), "Pool not fill to min" );
            } );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    @Test
    @DisplayName( "FlushMode.INVALID" )
    public void modeValid() throws SQLException {
        int MIN_POOL_SIZE = 10, MAX_POOL_SIZE = 30, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .connectionValidator( AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidator() )
                );

        FlushListener listener = new FlushListener();
        listener.creationLatch = new CountDownLatch( MAX_POOL_SIZE );
        listener.validationLatch = new CountDownLatch( MAX_POOL_SIZE );
        listener.destroyLatch = new CountDownLatch( MAX_POOL_SIZE );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {

            logger.info( format( "Awaiting fill of all the {0} initial connections on the pool", MAX_POOL_SIZE ) );
            if ( !listener.creationLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", listener.creationLatch.getCount() ) );
            }

            listener.creationLatch = new CountDownLatch( MIN_POOL_SIZE );

            dataSource.flush( AgroalDataSource.FlushMode.INVALID );

            logger.info( format( "Awaiting for validation of all the {0} connections on the pool", MAX_POOL_SIZE ) );
            if ( !listener.validationLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "Missed validation of {0} connections", listener.validationLatch.getCount() ) );
            }
            logger.info( format( "Waiting for destruction of {0} connections ", MAX_POOL_SIZE - 1 ) );
            if ( !listener.destroyLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} invalid connections not sent for destruction", listener.destroyLatch.getCount() ) );
            }
            if ( !listener.creationLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created after flush", listener.creationLatch.getCount() ) );
            }

            assertAll( () -> {
                assertEquals( MAX_POOL_SIZE, listener.flushCount.longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE, listener.destroyCount.longValue(), "Unexpected number of destroy connections" );
                assertEquals( MAX_POOL_SIZE + MIN_POOL_SIZE, listener.creationCount.longValue(), "Unexpected number of created connections" );
            } );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    @Test
    @DisplayName( "FlushMode.IDLE" )
    public void modeIdle() throws SQLException {
        int MIN_POOL_SIZE = 25, MAX_POOL_SIZE = 50, CALLS = 30, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .connectionValidator( AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidator() )
                );

        FlushListener listener = new FlushListener();
        listener.destroyLatch = new CountDownLatch( MAX_POOL_SIZE - max( MIN_POOL_SIZE, CALLS ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            Collection<Connection> connections = new ArrayList<>();
            for ( int i = 0; i < CALLS; i++ ) {
                connections.add( dataSource.getConnection() );
            }

            // Flush to CALLS
            dataSource.flush( AgroalDataSource.FlushMode.IDLE );

            logger.info( format( "Waiting for destruction of {0} connections ", MAX_POOL_SIZE - max( MIN_POOL_SIZE, CALLS ) ) );
            if ( !listener.destroyLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} invalid connections not sent for destruction", listener.destroyLatch.getCount() ) );
            }

            assertAll( () -> {
                assertEquals( MAX_POOL_SIZE - max( MIN_POOL_SIZE, CALLS ), listener.destroyCount.longValue(), "Unexpected number of destroy connections" );

                assertEquals( MAX_POOL_SIZE, listener.flushCount.longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE, listener.creationCount.longValue(), "Unexpected number of created connections" );
            } );

            for ( Connection connection : connections ) {
                connection.close();
            }

            listener.destroyLatch = new CountDownLatch( max( MIN_POOL_SIZE, CALLS ) - min( MIN_POOL_SIZE, CALLS ) );

            // Flush to MIN_SIZE
            dataSource.flush( AgroalDataSource.FlushMode.IDLE );

            logger.info( format( "Waiting for destruction of {0} remaining connection", max( MIN_POOL_SIZE, CALLS ) - min( MIN_POOL_SIZE, CALLS ) ) );
            if ( !listener.destroyLatch.await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} flushed connections not sent for destruction", listener.destroyLatch.getCount() ) );
            }

            assertAll( () -> {
                assertEquals( MAX_POOL_SIZE - MIN_POOL_SIZE, listener.destroyCount.longValue(), "Unexpected number of destroy connections" );

                assertEquals( MAX_POOL_SIZE + max( MIN_POOL_SIZE, CALLS ), listener.flushCount.longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE, listener.creationCount.longValue(), "Unexpected number of created connections" );
            } );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    // --- //

    private static class FlushListener implements AgroalDataSourceListener {

        private final LongAdder creationCount = new LongAdder(), flushCount = new LongAdder(), destroyCount = new LongAdder();
        private CountDownLatch creationLatch, validationLatch, flushLatch, destroyLatch;

        @Override
        public void beforeConnectionCreation() {
            creationCount.increment();
        }

        @Override
        public void onConnectionCreation(Connection connection) {
            try {
                creationLatch.countDown();
            } catch ( NullPointerException npe ) {
                // ignore
            }
        }

        @Override
        public void beforeConnectionValidation(Connection connection) {
            try {
                validationLatch.countDown();
            } catch ( NullPointerException npe ) {
                // ignore
            }
        }

        @Override
        public void beforeConnectionFlush(Connection connection) {
            flushCount.increment();
        }

        @Override
        public void onConnectionFlush(Connection connection) {
            try {
                flushLatch.countDown();
            } catch ( NullPointerException npe ) {
                // ignore
            }
        }

        @Override
        public void beforeConnectionDestroy(Connection connection) {
            destroyCount.increment();
        }

        @Override
        public void onConnectionDestroy(Connection connection) {
            try {
                destroyLatch.countDown();
            } catch ( NullPointerException npe ) {
                // ignore
            }
        }
    }

    // --- //

    public static class FakeConnection implements MockConnection {

        private boolean closed = false;

        @Override
        public void close() throws SQLException {
            closed = true;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed;
        }
    }
}

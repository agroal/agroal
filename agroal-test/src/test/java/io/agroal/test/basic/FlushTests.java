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
import static java.time.Duration.ofMillis;
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
    static void setupMockDriver() {
        registerMockDriver( FakeConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "FlushMode.ALL" )
    void modeAll() throws SQLException {
        int MIN_POOL_SIZE = 40, MAX_POOL_SIZE = 100, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                );

        FlushListener listener = new FlushListener( new CountDownLatch( MAX_POOL_SIZE ), new CountDownLatch( MAX_POOL_SIZE ), new CountDownLatch( MAX_POOL_SIZE ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {

            logger.info( format( "Awaiting fill of all the {0} initial connections on the pool", MAX_POOL_SIZE ) );
            if ( !listener.getCreationLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", listener.getCreationLatch().getCount() ) );
            }
            listener.resetCreationLatch( MIN_POOL_SIZE );

            Connection connection = dataSource.getConnection();
            assertFalse( connection.isClosed() );

            dataSource.flush( AgroalDataSource.FlushMode.ALL );

            logger.info( format( "Awaiting flush of all the {0} connections on the pool", MAX_POOL_SIZE ) );
            if ( !listener.getFlushLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not flush", listener.getFlushLatch().getCount() ) );
            }
            logger.info( format( "Waiting for destruction of {0} connections ", MAX_POOL_SIZE - MIN_POOL_SIZE ) );
            if ( !listener.getDestroyLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} flushed connections not sent for destruction", listener.getDestroyLatch().getCount() ) );
            }
            logger.info( format( "Awaiting fill of all the {0} min connections on the pool", MIN_POOL_SIZE ) );
            if ( !listener.getCreationLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", listener.getCreationLatch().getCount() ) );
            }

            assertAll( () -> {
                assertTrue( connection.isClosed(), "Expecting connection closed after forced flush" );

                assertEquals( MAX_POOL_SIZE, listener.getFlushCount().longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE, listener.getDestroyCount().longValue(), "Unexpected number of destroyed connections" );
                assertEquals( MAX_POOL_SIZE + MIN_POOL_SIZE, listener.getCreationCount().longValue(), "Unexpected number of created connections" );
            } );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    @Test
    @DisplayName( "FlushMode.GRACEFUL" )
    void modeGraceful() throws SQLException {
        int MIN_POOL_SIZE = 10, MAX_POOL_SIZE = 30, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                );

        FlushListener listener = new FlushListener( new CountDownLatch( MAX_POOL_SIZE ), new CountDownLatch( MAX_POOL_SIZE - 1 ), new CountDownLatch( MAX_POOL_SIZE - 1 ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {

            logger.info( format( "Awaiting fill of all the {0} initial connections on the pool", MAX_POOL_SIZE ) );
            if ( !listener.getCreationLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", listener.getCreationLatch().getCount() ) );
            }

            listener.resetCreationLatch( MIN_POOL_SIZE - 1 );

            Connection connection = dataSource.getConnection();
            assertFalse( connection.isClosed() );

            dataSource.flush( AgroalDataSource.FlushMode.GRACEFUL );

            logger.info( format( "Awaiting flush of the {0} connections on the pool", MAX_POOL_SIZE - 1 ) );
            if ( !listener.getFlushLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not flush", listener.getFlushLatch().getCount() ) );
            }
            logger.info( format( "Waiting for destruction of {0} connections ", MAX_POOL_SIZE - 1 ) );
            if ( !listener.getDestroyLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} flushed connections not sent for destruction", listener.getDestroyLatch().getCount() ) );
            }
            if ( !listener.getCreationLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created after flush", listener.getCreationLatch().getCount() ) );
            }

            listener.resetCreationLatch( 1 );
            listener.resetFlushLatch( 1 );
            listener.resetDestroyLatch( 1 );

            assertAll( () -> {
                assertEquals( MAX_POOL_SIZE, listener.getFlushCount().longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE - 1, listener.getDestroyCount().longValue(), "Unexpected number of destroy connections" );
                assertEquals( MAX_POOL_SIZE + MIN_POOL_SIZE - 1, listener.getCreationCount().longValue(), "Unexpected number of created connections" );

                assertEquals( MIN_POOL_SIZE - 1, dataSource.getMetrics().availableCount(), "Pool not fill to min" );
                assertEquals( 1, dataSource.getMetrics().activeCount(), "Incorrect active count" );

                assertFalse( connection.isClosed(), "Expecting connection open after graceful flush" );
            } );

            connection.close();

            logger.info( format( "Awaiting flush of one remaining connection" ) );
            if ( !listener.getFlushLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not flush", listener.getFlushLatch().getCount() ) );
            }
            logger.info( format( "Waiting for destruction of one remaining connection" ) );
            if ( !listener.getDestroyLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} flushed connections not sent for destruction", listener.getDestroyLatch().getCount() ) );
            }
            logger.info( format( "Awaiting creation of one additional connections" ) );
            if ( !listener.getCreationLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", listener.getCreationLatch().getCount() ) );
            }

            assertAll( () -> {
                assertEquals( MAX_POOL_SIZE, listener.getDestroyCount().longValue(), "Unexpected number of destroy connections" );
                assertEquals( MAX_POOL_SIZE + MIN_POOL_SIZE, listener.getCreationCount().longValue(), "Unexpected number of created connections" );
                assertEquals( 0, dataSource.getMetrics().activeCount(), "Incorrect active count" );
                assertEquals( MIN_POOL_SIZE, dataSource.getMetrics().availableCount(), "Pool not fill to min" );
            } );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    @Test
    @DisplayName( "FlushMode.INVALID" )
    void modeValid() throws SQLException {
        int MIN_POOL_SIZE = 10, MAX_POOL_SIZE = 30, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .connectionValidator( AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidator() )
                );

        FlushListener listener = new FlushListener( new CountDownLatch( MAX_POOL_SIZE ), new CountDownLatch( MAX_POOL_SIZE ), new CountDownLatch( MAX_POOL_SIZE ) );
        listener.resetValidationLatch( MAX_POOL_SIZE );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {

            logger.info( format( "Awaiting fill of all the {0} initial connections on the pool", MAX_POOL_SIZE ) );
            if ( !listener.getCreationLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created", listener.getCreationLatch().getCount() ) );
            }

            listener.resetCreationLatch( MIN_POOL_SIZE );

            dataSource.flush( AgroalDataSource.FlushMode.INVALID );

            logger.info( format( "Awaiting for validation of all the {0} connections on the pool", MAX_POOL_SIZE ) );
            if ( !listener.getValidationLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "Missed validation of {0} connections", listener.getValidationLatch().getCount() ) );
            }
            logger.info( format( "Waiting for destruction of {0} connections ", MAX_POOL_SIZE - 1 ) );
            if ( !listener.getDestroyLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} invalid connections not sent for destruction", listener.getDestroyLatch().getCount() ) );
            }
            if ( !listener.getCreationLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created after flush", listener.getCreationLatch().getCount() ) );
            }

            assertAll( () -> {
                assertEquals( MAX_POOL_SIZE, listener.getFlushCount().longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE, listener.getDestroyCount().longValue(), "Unexpected number of destroy connections" );
                assertEquals( MAX_POOL_SIZE + MIN_POOL_SIZE, listener.getCreationCount().longValue(), "Unexpected number of created connections" );
            } );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    @Test
    @DisplayName( "FlushMode.IDLE" )
    @SuppressWarnings( "ConstantConditions" )
    void modeIdle() throws SQLException {
        int MIN_POOL_SIZE = 25, MAX_POOL_SIZE = 50, CALLS = 30, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .connectionValidator( AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidator() )
                );

        FlushListener listener = new FlushListener(
                new CountDownLatch( MAX_POOL_SIZE ),
                new CountDownLatch( MAX_POOL_SIZE ),
                new CountDownLatch( MAX_POOL_SIZE - max( MIN_POOL_SIZE, CALLS ) ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            Collection<Connection> connections = new ArrayList<>();
            for ( int i = 0; i < CALLS; i++ ) {
                connections.add( dataSource.getConnection() );
            }

            // Flush to CALLS
            dataSource.flush( AgroalDataSource.FlushMode.IDLE );

            logger.info( format( "Waiting for destruction of {0} connections ", MAX_POOL_SIZE - max( MIN_POOL_SIZE, CALLS ) ) );
            if ( !listener.getDestroyLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} invalid connections not sent for destruction", listener.getDestroyLatch().getCount() ) );
            }

            assertAll( () -> {
                assertEquals( MAX_POOL_SIZE - max( MIN_POOL_SIZE, CALLS ), listener.getDestroyCount().longValue(), "Unexpected number of destroy connections" );

                assertEquals( MAX_POOL_SIZE, listener.getFlushCount().longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE, listener.getCreationCount().longValue(), "Unexpected number of created connections" );
            } );

            for ( Connection connection : connections ) {
                connection.close();
            }

            int remaining = max( MIN_POOL_SIZE, CALLS ) - min( MIN_POOL_SIZE, CALLS );
            listener.resetDestroyLatch( remaining );

            // Flush to MIN_SIZE
            dataSource.flush( AgroalDataSource.FlushMode.IDLE );

            logger.info( format( "Waiting for destruction of {0} remaining connection", remaining ) );
            if ( !listener.getDestroyLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} flushed connections not sent for destruction", listener.getDestroyLatch().getCount() ) );
            }

            assertAll( () -> {
                assertEquals( MAX_POOL_SIZE - MIN_POOL_SIZE, listener.getDestroyCount().longValue(), "Unexpected number of destroy connections" );

                assertEquals( MAX_POOL_SIZE + max( MIN_POOL_SIZE, CALLS ), listener.getFlushCount().longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE, listener.getCreationCount().longValue(), "Unexpected number of created connections" );
            } );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    @Test
    @DisplayName( "FlushMode.LEAK" )
    @SuppressWarnings( "ConstantConditions" )
    void modeLeak() throws SQLException {
        int MIN_POOL_SIZE = 25, MAX_POOL_SIZE = 50, CALLS = 30, LEAK_MS = 100, TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .leakTimeout( ofMillis( LEAK_MS ) )
                );

        FlushListener listener = new FlushListener(
                new CountDownLatch( MIN_POOL_SIZE + CALLS ),
                new CountDownLatch( MAX_POOL_SIZE ),
                new CountDownLatch( min( MAX_POOL_SIZE, CALLS ) ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            Collection<Connection> connections = new ArrayList<>();
            for ( int i = 0; i < CALLS; i++ ) {
                connections.add( dataSource.getConnection() );
            }

            Thread.sleep( LEAK_MS << 1 ); // 2 * LEAK_MS

            // Flush to CALLS
            dataSource.flush( AgroalDataSource.FlushMode.LEAK );

            logger.info( format( "Waiting for destruction of {0} connections ", MAX_POOL_SIZE ) );
            if ( !listener.getDestroyLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} leak connections not sent for destruction", listener.getDestroyLatch().getCount() ) );
            }
            if ( !listener.getCreationLatch().await( TIMEOUT_MS, MILLISECONDS ) ) {
                fail( format( "{0} connections not created after flush", listener.getCreationLatch().getCount() ) );
            }

            assertAll( () -> {
                assertEquals( CALLS, listener.getDestroyCount().longValue(), "Unexpected number of destroy connections" );

                assertEquals( MAX_POOL_SIZE, listener.getFlushCount().longValue(), "Unexpected number of beforeFlushConnection" );
                assertEquals( MAX_POOL_SIZE + ( CALLS - MIN_POOL_SIZE ), listener.getCreationCount().longValue(), "Unexpected number of created connections" );

                assertTrue( connections.stream().allMatch( c -> {
                    try {
                        return c.isClosed();
                    } catch ( SQLException ignore ) {
                        return false;
                    }
                } ) );
            } );
        } catch ( InterruptedException e ) {
            fail( "Test fail due to interrupt" );
        }
    }

    // --- //

    @SuppressWarnings( "WeakerAccess" )
    private static class FlushListener implements AgroalDataSourceListener {

        private final LongAdder creationCount = new LongAdder(), flushCount = new LongAdder(), destroyCount = new LongAdder();
        private CountDownLatch creationLatch, validationLatch, flushLatch, destroyLatch;

        FlushListener(CountDownLatch creationLatch, CountDownLatch flushLatch, CountDownLatch destroyLatch) {
            this.creationLatch = creationLatch;
            this.flushLatch = flushLatch;
            this.destroyLatch = destroyLatch;
        }

        @Override
        public void beforeConnectionCreation() {
            creationCount.increment();
        }

        @Override
        public void onConnectionPooled(Connection connection) {
            creationLatch.countDown();
        }

        @Override
        public void beforeConnectionValidation(Connection connection) {
            validationLatch.countDown();
        }

        @Override
        public void beforeConnectionFlush(Connection connection) {
            flushCount.increment();
        }

        @Override
        public void onConnectionFlush(Connection connection) {
            flushLatch.countDown();
        }

        @Override
        public void beforeConnectionDestroy(Connection connection) {
            destroyCount.increment();
        }

        @Override
        public void onConnectionDestroy(Connection connection) {
            destroyLatch.countDown();
        }

        // --- //

        void resetCreationLatch(int count) {
            creationLatch = new CountDownLatch( count );
        }

        void resetValidationLatch(int count) {
            validationLatch = new CountDownLatch( count );
        }

        @SuppressWarnings( "SameParameterValue" )
        void resetFlushLatch(int count) {
            flushLatch = new CountDownLatch( count );
        }

        void resetDestroyLatch(int count) {
            destroyLatch = new CountDownLatch( count );
        }

        // --- //

        LongAdder getCreationCount() {
            return creationCount;
        }

        LongAdder getFlushCount() {
            return flushCount;
        }

        LongAdder getDestroyCount() {
            return destroyCount;
        }

        CountDownLatch getCreationLatch() {
            return creationLatch;
        }

        CountDownLatch getValidationLatch() {
            return validationLatch;
        }

        CountDownLatch getFlushLatch() {
            return flushLatch;
        }

        CountDownLatch getDestroyLatch() {
            return destroyLatch;
        }
    }

    // --- //

    public static class FakeConnection implements MockConnection {

        private boolean closed;

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

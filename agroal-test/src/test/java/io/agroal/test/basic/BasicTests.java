// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockConnection;
import io.agroal.test.MockStatement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.MultipleAcquisitionAction.STRICT;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.MultipleAcquisitionAction.WARN;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class BasicTests {

    static final Logger logger = getLogger( BasicTests.class.getName() );

    private static final String FAKE_SCHEMA = "skeema";

    @BeforeAll
    static void setupMockDriver() {
        registerMockDriver( FakeSchemaConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Mock driver providing fake connections" )
    void basicConnectionAcquireTest() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ) ) ) {
            Connection connection = dataSource.getConnection();
            assertEquals( connection.getSchema(), FAKE_SCHEMA );
            logger.info( format( "Got schema \"{0}\" from {1}", connection.getSchema(), connection ) );
            connection.close();
        }
    }

    @Test
    @DisplayName( "DataSource in closed state" )
    @SuppressWarnings( "AnonymousInnerClassMayBeStatic" )
    void basicDataSourceCloseTest() throws SQLException {
        AtomicBoolean warning = new AtomicBoolean( false );

        AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 2 ) ), new AgroalDataSourceListener() {
            @Override
            public void onWarning(String message) {
                warning.set( true );
            }

            @Override
            public void onWarning(Throwable throwable) {
                warning.set( true );
            }
        } );

        Connection connection = dataSource.getConnection();
        Connection leaked = dataSource.getConnection();
        assertAll( () -> {
            assertFalse( connection.isClosed(), "Expected open connection, but it's closed" );
            assertNotNull( connection.getSchema(), "Expected non null value" );
        } );
        connection.close();

        dataSource.close();

        assertAll( () -> {
            assertThrows( SQLException.class, dataSource::getConnection );
            assertTrue( leaked.isClosed(), "Expected closed connection, but it's open" );
            assertFalse( warning.get(), "Unexpected warning" );
        } );
    }

    @Test
    @DisplayName( "Acquisition timeout" )
    @SuppressWarnings( "JDBCResourceOpenedButNotSafelyClosed" )
    void basicAcquisitionTimeoutTest() throws SQLException {
        int MAX_POOL_SIZE = 100, ACQUISITION_TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( MAX_POOL_SIZE )
                        .acquisitionTimeout( ofMillis( ACQUISITION_TIMEOUT_MS ) )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {

            for ( int i = 0; i < MAX_POOL_SIZE; i++ ) {
                Connection connection = dataSource.getConnection();
                assertNotNull( connection.getSchema(), "Expected non null value" );
                //connection.close();
            }
            logger.info( format( "Holding all {0} connections from the pool and requesting a new one", MAX_POOL_SIZE ) );

            long start = nanoTime(), timeoutBound = (long) ( ACQUISITION_TIMEOUT_MS * 1.1 );
            assertTimeoutPreemptively( ofMillis( timeoutBound ), () -> assertThrows( SQLException.class, dataSource::getConnection ), "Expecting acquisition timeout" );

            long elapsed = NANOSECONDS.toMillis( nanoTime() - start );
            logger.info( format( "Acquisition timeout after {0}ms - Configuration is {1}ms", elapsed, ACQUISITION_TIMEOUT_MS ) );
            assertTrue( elapsed >= ACQUISITION_TIMEOUT_MS, "Acquisition timeout before time" );
        }
    }

    @Test
    @DisplayName( "Leak detection" )
    @SuppressWarnings( "JDBCResourceOpenedButNotSafelyClosed" )
    void basicLeakDetectionTest() throws SQLException {
        int MAX_POOL_SIZE = 100, LEAK_DETECTION_MS = 1000;
        Thread leakingThread = currentThread();

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .leakTimeout( ofMillis( LEAK_DETECTION_MS ) )
                        .acquisitionTimeout( ofMillis( LEAK_DETECTION_MS ) )
                );
        CountDownLatch latch = new CountDownLatch( MAX_POOL_SIZE );

        AgroalDataSourceListener listener = new LeakDetectionListener( leakingThread, latch );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            for ( int i = 0; i < MAX_POOL_SIZE; i++ ) {
                Connection connection = dataSource.getConnection();
                assertNotNull( connection.getSchema(), "Expected non null value" );
                //connection.close();
            }
            try {
                logger.info( format( "Holding all {0} connections from the pool and waiting for leak notifications", MAX_POOL_SIZE ) );
                if ( !latch.await( 3L * LEAK_DETECTION_MS, MILLISECONDS ) ) {
                    fail( format( "Missed detection of {0} leaks", latch.getCount() ) );
                }
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
        }
    }

    @Test
    @DisplayName( "Connection Validation" )
    void basicValidationTest() throws SQLException {
        int MAX_POOL_SIZE = 100, CALLS = 1000, VALIDATION_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .validationTimeout( ofMillis( VALIDATION_MS ) )
                );

        CountDownLatch latch = new CountDownLatch( MAX_POOL_SIZE );

        AgroalDataSourceListener listener = new ValidationListener( latch );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            for ( int i = 0; i < CALLS; i++ ) {
                Connection connection = dataSource.getConnection();
                assertNotNull( connection.getSchema(), "Expected non null value" );
                connection.close();
            }
            try {
                logger.info( format( "Awaiting for validation of all the {0} connections on the pool", MAX_POOL_SIZE ) );
                if ( !latch.await( 3L * VALIDATION_MS, MILLISECONDS ) ) {
                    fail( format( "Validation of {0} connections", latch.getCount() ) );
                }
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
        }
    }

    @Test
    @DisplayName( "Idle Connection Validation" )
    void basicIdleValidationTest() throws SQLException {
        int CALLS = 10, IDLE_VALIDATION_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 2 )
                        .idleValidationTimeout( ofMillis( IDLE_VALIDATION_MS ) )
                );

        CountDownLatch latch = new CountDownLatch( 1 );

        AgroalDataSourceListener listener = new ValidationListener( latch );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            for ( int i = 0; i < CALLS; i++ ) {
                Connection connection = dataSource.getConnection();
                assertNotNull( connection.getSchema(), "Expected non null value" );
                connection.close();
            }
            assertEquals( 1, latch.getCount(), "Not expected validation to occur before " + IDLE_VALIDATION_MS );
            Executors.newSingleThreadScheduledExecutor().schedule( () -> {
                try ( Connection connection = dataSource.getConnection() ) {
                    assertNotNull( connection.getSchema(), "Expected non null value" );
                } catch ( SQLException e ) {
                    fail( e );
                }
            }, IDLE_VALIDATION_MS, MILLISECONDS );

            try {
                logger.info( format( "Awaiting validation of idle connection" ) );
                if ( !latch.await( 3L * IDLE_VALIDATION_MS, MILLISECONDS ) ) {
                    fail( format( "Did not validate idle connection", latch.getCount() ) );
                }
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
        }
    }

    @Test
    @DisplayName( "Connection Reap" )
    void basicReapTest() throws SQLException {
        int MIN_POOL_SIZE = 40, MAX_POOL_SIZE = 100, CALLS = 1000, REAP_TIMEOUT_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .initialSize( MAX_POOL_SIZE )
                        .minSize( MIN_POOL_SIZE )
                        .maxSize( MAX_POOL_SIZE )
                        .reapTimeout( ofMillis( REAP_TIMEOUT_MS ) )
                );

        CountDownLatch allLatch = new CountDownLatch( MAX_POOL_SIZE );
        CountDownLatch destroyLatch = new CountDownLatch( MAX_POOL_SIZE - MIN_POOL_SIZE );
        LongAdder reapCount = new LongAdder();

        AgroalDataSourceListener listener = new ReapListener( allLatch, reapCount, destroyLatch );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            for ( int i = 0; i < CALLS; i++ ) {
                Connection connection = dataSource.getConnection();
                assertNotNull( connection.getSchema(), "Expected non null value" );
                connection.close();
            }
            try {
                logger.info( format( "Awaiting test of all the {0} connections on the pool", MAX_POOL_SIZE ) );
                if ( !allLatch.await( 3L * REAP_TIMEOUT_MS, MILLISECONDS ) ) {
                    fail( format( "{0} connections not tested for reap", allLatch.getCount() ) );
                }
                logger.info( format( "Waiting for reaping of {0} connections ", MAX_POOL_SIZE - MIN_POOL_SIZE ) );
                if ( !destroyLatch.await( 2L * REAP_TIMEOUT_MS, MILLISECONDS ) ) {
                    fail( format( "{0} idle connections not sent for destruction", destroyLatch.getCount() ) );
                }
                assertEquals( MAX_POOL_SIZE - MIN_POOL_SIZE, reapCount.longValue(), "Unexpected number of idle connections " );
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
        }
    }

    @Test
    @DisplayName( "Enhanced leak report" )
    void enhancedLeakReportTest() throws SQLException {
        int LEAK_DETECTION_MS = 1000;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 10 )
                        .leakTimeout( ofMillis( LEAK_DETECTION_MS ) )
                        .acquisitionTimeout( ofMillis( LEAK_DETECTION_MS ) )
                        .enhancedLeakReport()
                );
        CountDownLatch latch = new CountDownLatch( 1 );

        LeakDetectionListener listener = new LeakDetectionListener( currentThread(), latch );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            Connection connection = dataSource.getConnection();
            assertNotNull( connection.getSchema(), "Expected non null value" );
            connection.unwrap( Connection.class ).close();

            try {
                logger.info( format( "Holding connection from the pool and waiting for leak notification" ) );
                if ( !latch.await( 3L * LEAK_DETECTION_MS, MILLISECONDS ) ) {
                    fail( format( "Missed detection of {0} leaks", latch.getCount() ) );
                }
                Thread.sleep( 100 ); // hold for a bit to allow for enhanced info
                assertEquals( 3 + 1, listener.getInfoCount(), "Not enough info on extended leak report" );
                assertEquals( 1, listener.getWarningCount(), "Not enough info on extended leak report" );
            } catch ( InterruptedException e ) {
                fail( "Test fail due to interrupt" );
            }
        }
    }

    @Test
    @SuppressWarnings( "JDBCResourceOpenedButNotSafelyClosed" )
    @DisplayName( "Initial SQL test" )
    void initialSQLTest() throws SQLException {
        int MAX_POOL_SIZE = 10;

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( MAX_POOL_SIZE )
                        .connectionFactoryConfiguration( cf -> cf.initialSql( "Initial SQL" ) )
                );
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            for ( int i = 0; i < MAX_POOL_SIZE; i++ ) {
                assertEquals( "Initial SQL", dataSource.getConnection().unwrap( FakeSchemaConnection.class ).initialSQL(), "Connection not initialized" );
                //connection.close();
            }
        }
    }

    @Test
    @DisplayName( "Single acquisition" )
    @SuppressWarnings( {"JDBCResourceOpenedButNotSafelyClosed", "ObjectAllocationInLoop"} )
    void basicSingleAcquisitionTest() throws SQLException {
        int MAX_POOL_SIZE = 10;

        AgroalDataSourceConfigurationSupplier offConfigurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( MAX_POOL_SIZE )
                );
        AgroalDataSourceConfigurationSupplier warnConfigurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( MAX_POOL_SIZE )
                        .multipleAcquisition( WARN ) );
        AgroalDataSourceConfigurationSupplier strictConfigurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .metricsEnabled()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( MAX_POOL_SIZE )
                        .multipleAcquisition( STRICT ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( offConfigurationSupplier, new NoWarningsAgroalListener() ) ) {
            for ( int i = 0; i < MAX_POOL_SIZE; i++ ) {
                Connection connection = dataSource.getConnection();
                assertNotNull( connection.getSchema(), "Expected non null value" );
                //connection.close();
            }
        }
        WarningsAgroalListener warningsAgroalListener = new WarningsAgroalListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( warnConfigurationSupplier, warningsAgroalListener ) ) {
            for ( int i = 0; i < MAX_POOL_SIZE; i++ ) {
                Connection connection = dataSource.getConnection();
                assertNotNull( connection.getSchema(), "Expected non null value" );
                //connection.close();
            }
            assertEquals( MAX_POOL_SIZE - 1, warningsAgroalListener.getWarningCount(), "Wrong number of warning messages" );
        }
        try ( AgroalDataSource dataSource = AgroalDataSource.from( strictConfigurationSupplier, new NoWarningsAgroalListener() ) ) {
            for ( int i = 0; i < MAX_POOL_SIZE; i++ ) {
                if ( i == 0 ) {
                    Connection connection = dataSource.getConnection();
                    assertNotNull( connection.getSchema(), "Expected non null value" );
                    //connection.close();
                } else {
                    assertThrows( SQLException.class, dataSource::getConnection, "Expected exception on multiple acquisition" );
                    assertEquals( 1, dataSource.getMetrics().acquireCount() );
                    assertEquals( 1, dataSource.getMetrics().activeCount() );
                }
            }
        }
    }

    // --- //

    @SuppressWarnings( "WeakerAccess" )
    private static class LeakDetectionListener implements AgroalDataSourceListener {
        private final Thread leakingThread;
        private final CountDownLatch latch;
        private int infoCount, warningCount;

        LeakDetectionListener(Thread leakingThread, CountDownLatch latch) {
            this.leakingThread = leakingThread;
            this.latch = latch;
        }

        @Override
        public void onConnectionLeak(Connection connection, Thread thread) {
            assertEquals( leakingThread, thread, "Wrong thread reported" );
            latch.countDown();
        }

        @Override
        public void onWarning(String message) {
            warningCount++;
            logger.warning( message );
        }

        @Override
        public void onInfo(String message) {
            infoCount++;
            logger.info( message );
        }

        int getInfoCount() {
            return infoCount;
        }

        int getWarningCount() {
            return warningCount;
        }
    }

    private static class ValidationListener implements AgroalDataSourceListener {
        private final CountDownLatch latch;

        @SuppressWarnings( "WeakerAccess" )
        ValidationListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void beforeConnectionValidation(Connection connection) {
            latch.countDown();
        }
    }

    private static class ReapListener implements AgroalDataSourceListener {

        private final CountDownLatch allLatch;
        private final LongAdder reapCount;
        private final CountDownLatch destroyLatch;

        @SuppressWarnings( "WeakerAccess" )
        ReapListener(CountDownLatch allLatch, LongAdder reapCount, CountDownLatch destroyLatch) {
            this.allLatch = allLatch;
            this.reapCount = reapCount;
            this.destroyLatch = destroyLatch;
        }

        @Override
        public void beforeConnectionReap(Connection connection) {
            allLatch.countDown();
        }

        @Override
        public void onConnectionReap(Connection connection) {
            reapCount.increment();
        }

        @Override
        public void beforeConnectionDestroy(Connection connection) {
            destroyLatch.countDown();
        }
    }

    private static class NoWarningsAgroalListener implements AgroalDataSourceListener {

        @SuppressWarnings( "WeakerAccess" )
        NoWarningsAgroalListener() {
        }

        @Override
        public void onWarning(String message) {
            fail( "Unexpected warn message: " + message );
        }

        @Override
        public void onWarning(Throwable throwable) {
            fail( "Unexpected warn throwable: " + throwable.getMessage() );
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    private static class WarningsAgroalListener implements AgroalDataSourceListener {
        private int warningCount;

        WarningsAgroalListener() {
        }

        @Override
        public void onWarning(String message) {
            warningCount++;
            logger.warning( message );
        }

        @Override
        public void onWarning(Throwable t) {
            warningCount++;
            logger.warning( t.getMessage() );

        }

        int getWarningCount() {
            return warningCount;
        }
    }


    // --- //

    public static class FakeSchemaConnection implements MockConnection {

        private boolean closed;
        private final RecordingStatement statement = new RecordingStatement();

        @Override
        public String getSchema() throws SQLException {
            return FAKE_SCHEMA;
        }

        @Override
        public void close() throws SQLException {
            closed = true;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return statement;
        }

        @SuppressWarnings( "WeakerAccess" )
        String initialSQL() {
            return statement.getInitialSQL();
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public <T> T unwrap(Class<T> target) throws SQLException {
            return (T) this;
        }

        private static class RecordingStatement implements MockStatement {

            private String initialSQL;

            @SuppressWarnings( "WeakerAccess" )
            RecordingStatement(){
            }

            @Override
            public boolean execute(String sql) throws SQLException {
                initialSQL = sql;
                return true;
            }

            String getInitialSQL() {
                return initialSQL;
            }
        }
    }
}

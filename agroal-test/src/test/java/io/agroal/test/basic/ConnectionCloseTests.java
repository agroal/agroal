// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.pool.wrapper.StatementWrapper;
import io.agroal.test.MockConnection;
import io.agroal.test.MockStatement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class ConnectionCloseTests {

    private static final Logger logger = getLogger( ConnectionCloseTests.class.getName() );

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
    @DisplayName( "Connection wrapper in closed state" )
    void basicConnectionCloseTest() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ) ) ) {
            Connection connection = dataSource.getConnection();

            assertAll( () -> {
                assertFalse( connection.isClosed(), "Expected open connection, but it's closed" );
                assertNotNull( connection.getSchema(), "Expected non null value" );
            } );

            connection.close();

            assertAll( () -> {
                assertThrows( SQLException.class, connection::getSchema );
                assertTrue( connection.isClosed(), "Expected closed connection, but it's open" );
            } );

            connection.close();
        }
    }

    @Test
    @DisplayName( "Connection closes Statements and ResultSets" )
    void statementCloseTest() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ) ) ) {
            Connection connection = dataSource.getConnection();
            logger.info( format( "Creating 2 Statements on Connection {0}", connection ) );

            Statement statementOne = connection.createStatement();
            Statement statementTwo = connection.createStatement();
            ResultSet setOne = statementOne.getResultSet();
            ResultSet setTwo = statementTwo.getResultSet();
            statementTwo.close();

            assertAll( () -> {
                assertNotNull( setOne, "Expected non null value" );
                assertFalse( statementOne.isClosed(), "Expected open Statement, but it's closed" );
                assertThrows( SQLException.class, statementTwo::getResultSet, "Expected SQLException on closed Connection" );
                assertTrue( statementTwo.isClosed(), "Expected closed Statement, but it's open" );
                assertTrue( setTwo.isClosed(), "Expected closed ResultSet, but it's open" );
            } );

            statementTwo.close();
            connection.close();

            assertAll( () -> {
                assertThrows( SQLException.class, statementOne::getResultSet, "Expected SQLException on closed Connection" );
                assertTrue( statementOne.isClosed(), "Expected closed Statement, but it's open" );
                assertTrue( setOne.isClosed(), "Expected closed ResultSet, but it's open" );
            } );

            connection.close();
        }
    }

    @Test
    @DisplayName( "Connection closed multiple times" )
    @SuppressWarnings( "RedundantExplicitClose" )
    void multipleCloseTest() throws SQLException {
        ReturnListener returnListener = new ReturnListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().metricsEnabled().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ), returnListener ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                // Explicit close. This is a try-with-resources so there is another call to connection.close() after this one.
                connection.close();
            }

            assertAll( () -> {
                assertEquals( 1, returnListener.getReturnCount().longValue(), "Expecting connection to be returned once to the pool" );
                assertEquals( 0, dataSource.getMetrics().activeCount(), "Expecting 0 active connections" );
            } );
        }
    }

    @Test
    @DisplayName( "Flush on close" )
    void flushOnCloseTest() throws Exception {
        OnWarningListener warningListener = new OnWarningListener();
        OnDestroyListener destroyListener = new OnDestroyListener( 1 );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().metricsEnabled().connectionPoolConfiguration( cp -> cp.maxSize( 1 ).flushOnClose() ), warningListener, destroyListener ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                assertFalse( connection.isClosed() );
            }

            assertTrue( destroyListener.awaitSeconds( 1 ) );

            assertAll( () -> {
                assertFalse( warningListener.getWarning().get(), "Unexpected warning on close connection" );
                assertEquals( 1, dataSource.getMetrics().destroyCount(), "Expecting 1 destroyed connection" );
                assertEquals( 0, dataSource.getMetrics().activeCount(), "Expecting 0 active connections" );
            } );
        }
    }

    @Test
    @DisplayName( "Statement close" )
    void statementClose() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ) ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                try ( Statement statement = connection.createStatement() ) {
                    Statement underlyingStatement = statement.unwrap( ClosableStatement.class );
                    statement.close();
                    assertTrue( underlyingStatement.isClosed() );
                }
            }
        }
    }

    @Test
    @DisplayName( "ResultSet leak" )
    void resultSetLeak() throws SQLException {
        ResultSet resultSet;
        OnWarningListener listener = new OnWarningListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ), listener ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                Statement statement = connection.createStatement();
                resultSet = statement.getResultSet();
                statement.close();
            }
        }
        assertTrue( resultSet.isClosed(), "Leaked ResultSet not closed" );
        assertTrue( listener.getWarning().get(), "No warning message on ResultSet leak" );
        resultSet.close();
    }

    @Test
    @DisplayName( "JDBC resources tracking disabled" )
    @SuppressWarnings( "InstanceofConcreteClass" )
    void jdbcResourcesTrackingDisabled() throws SQLException {
        Statement statement;
        OnWarningListener listener = new OnWarningListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().metricsEnabled().connectionPoolConfiguration( cp -> cp.maxSize( 1 ).connectionFactoryConfiguration( cf -> cf.trackJdbcResources( false ) ) ), listener ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                statement = connection.createStatement();
                assertTrue( statement instanceof ClosableStatement, "Wrapped Statement when tracking is disabled" );
                assertThrows( ClassCastException.class, () -> statement.unwrap( StatementWrapper.class ), "Wrapped Statement when tracking is disabled" );
            }
        }
        assertFalse( listener.getWarning().get(), "Leak warning when tracking is disabled " );
        assertFalse( statement.isClosed(), "Tracking is disabled, but acted to clean leak!" );
    }

    @SuppressWarnings( "WeakerAccess" )
    private static class ReturnListener implements AgroalDataSourceListener {

        private final LongAdder returnCount = new LongAdder();

        ReturnListener() {
        }

        @Override
        public void beforeConnectionReturn(Connection connection) {
            returnCount.increment();
        }

        LongAdder getReturnCount() {
            return returnCount;
        }
    }

    // --- //

    public static class FakeSchemaConnection implements MockConnection {

        private static final String FAKE_SCHEMA = "skeema";

        @Override
        public String getSchema() {
            return FAKE_SCHEMA;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return new ClosableStatement();
        }
    }

    public static class ClosableStatement implements MockStatement {

        private boolean closed;

        @Override
        public void close() throws SQLException {
            closed = true;
        }

        @Override
        public boolean isClosed() throws SQLException {
            return closed;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return iface.cast( this );
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    private static class OnWarningListener implements AgroalDataSourceListener {

        private final AtomicBoolean warning = new AtomicBoolean( false );

        OnWarningListener() {
        }

        @Override
        public void onWarning(String message) {
            warning.set( true );
        }

        @Override
        public void onWarning(Throwable throwable) {
            warning.set( true );
        }

        AtomicBoolean getWarning() {
            return warning;
        }
    }

    @SuppressWarnings( {"WeakerAccess", "SameParameterValue"} )
    private static class OnDestroyListener implements AgroalDataSourceListener {

        private final CountDownLatch latch;

        OnDestroyListener(int count) {
            latch = new CountDownLatch( count );
        }

        @Override
        public void onConnectionDestroy(Connection connection) {
            latch.countDown();
        }

        boolean awaitSeconds(int timeout) throws InterruptedException {
            return latch.await( timeout, TimeUnit.SECONDS );
        }
    }
}

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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    public static void setupMockDriver() {
        registerMockDriver( FakeSchemaConnection.class );
    }

    @AfterAll
    public static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "Connection wrapper in closed state" )
    public void basicConnectionCloseTest() throws SQLException {
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
    public void statementCloseTest() throws SQLException {
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
    public void multipleCloseTest() throws SQLException {
        ReturnListener returnListener = new ReturnListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().metricsEnabled().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ), returnListener ) ) {
            try (Connection connection = dataSource.getConnection() ) {
                // Explicit close. This is a try-with-resources so there is another call to connection.close() after this one.
                connection.close();
            }

            assertAll( () -> {
                assertEquals( 1, returnListener.returnCount.longValue(), "Expecting connection to be returned once to the pool" );
                assertEquals( 0, dataSource.getMetrics().activeCount(), "Expecting 0 active connections" );
            } );
        }
    }

    private static class ReturnListener implements AgroalDataSourceListener {

        private LongAdder returnCount = new LongAdder();

        @Override
        public void beforeConnectionReturn(Connection connection) {
            returnCount.increment();
        }
    }

    // --- //

    public static class FakeSchemaConnection implements MockConnection {

        private static final String FAKE_SCHEMA = "skeema";

        @Override
        public String getSchema() {
            return FAKE_SCHEMA;
        }
    }
}

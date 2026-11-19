// Copyright (C) 2026 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that the pool itself drives the JDBC 4.3 Request Boundaries API: {@code beginRequest()} on checkout and
 * {@code endRequest()} on return, as recommended for connection pooling managers by the JDBC specification,
 * and that {@code useConnectionRequestBoundaries} turns that off.
 *
 * @author <a href="gegastaldi@gmail.com">George Gastaldi</a>
 */
@Tag( FUNCTIONAL )
public class PoolRequestBoundariesTests {

    @BeforeAll
    static void setupMockDriver() {
        registerMockDriver( RequestBoundariesConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    @BeforeEach
    void resetCounters() {
        RequestBoundariesConnection.beginRequestCount.set( 0 );
        RequestBoundariesConnection.endRequestCount.set( 0 );
        RequestBoundariesConnection.failOnBeginRequest = false;
        RequestBoundariesConnection.failOnEndRequest = false;
    }

    // --- //

    @Test
    @DisplayName( "Pool calls beginRequest() on checkout and endRequest() on return" )
    void poolDrivesRequestBoundaries() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ) ) ) {
            Connection connection = dataSource.getConnection();

            assertAll( () -> {
                assertEquals( 1, RequestBoundariesConnection.beginRequestCount.get(), "Pool should call beginRequest() on checkout" );
                assertEquals( 0, RequestBoundariesConnection.endRequestCount.get(), "endRequest() should not be called while the connection is in use" );
            } );

            connection.close();

            assertAll( () -> {
                assertEquals( 1, RequestBoundariesConnection.beginRequestCount.get(), "beginRequest() should not be called again on return" );
                assertEquals( 1, RequestBoundariesConnection.endRequestCount.get(), "Pool should call endRequest() on return" );
            } );
        }
    }

    @Test
    @DisplayName( "Each borrow cycle produces a matching begin/end pair" )
    void requestBoundariesPerBorrowCycle() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ) ) ) {
            for ( int i = 0; i < 3; i++ ) {
                dataSource.getConnection().close();
            }

            assertAll( () -> {
                assertEquals( 3, RequestBoundariesConnection.beginRequestCount.get(), "Expected one beginRequest() per borrow" );
                assertEquals( 3, RequestBoundariesConnection.endRequestCount.get(), "Expected one endRequest() per return" );
            } );
        }
    }

    // --- useConnectionRequestBoundaries --- //

    @Test
    @DisplayName( "useConnectionRequestBoundaries is enabled by default" )
    void requestBoundariesEnabledByDefault() {
        assertTrue( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) )
                        .get().connectionPoolConfiguration().useConnectionRequestBoundaries(),
                "Request boundaries should be driven by the pool by default" );
    }

    @Test
    @DisplayName( "Pool does not drive request boundaries when useConnectionRequestBoundaries is false" )
    void requestBoundariesDisabled() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp.maxSize( 1 ).useConnectionRequestBoundaries( false ) ) ) ) {
            for ( int i = 0; i < 3; i++ ) {
                dataSource.getConnection().close();
            }

            assertAll( () -> {
                assertEquals( 0, RequestBoundariesConnection.beginRequestCount.get(), "Pool should not call beginRequest() when request boundaries are disabled" );
                assertEquals( 0, RequestBoundariesConnection.endRequestCount.get(), "Pool should not call endRequest() when request boundaries are disabled" );
            } );
        }
    }

    @Test
    @DisplayName( "A driver that throws from beginRequest() fails acquisition when the pool drives request boundaries" )
    void failingBeginRequestFailsAcquisitionByDefault() throws SQLException {
        RequestBoundariesConnection.failOnBeginRequest = true;

        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration( cp -> cp.maxSize( 1 ) ) ) ) {
            SQLException se = assertThrows( SQLException.class, dataSource::getConnection, "Acquisition should fail when the driver throws from beginRequest()" );
            assertEquals( "beginRequest() not supported", se.getMessage() );
        }
    }

    @Test
    @DisplayName( "A driver that throws from beginRequest() is harmless when useConnectionRequestBoundaries is false" )
    void failingBeginRequestIsHarmlessWhenDisabled() throws SQLException {
        RequestBoundariesConnection.failOnBeginRequest = true;
        RequestBoundariesConnection.failOnEndRequest = true;

        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp.maxSize( 1 ).useConnectionRequestBoundaries( false ) ) ) ) {
            assertDoesNotThrow( () -> {
                for ( int i = 0; i < 3; i++ ) {
                    dataSource.getConnection().close();
                }
            }, "Acquisition should not be affected by a driver that throws from the request boundary methods" );
        }
    }

    @Test
    @DisplayName( "An explicit beginRequest() from the application still reaches the driver when the pool does not drive boundaries" )
    void explicitRequestBoundariesAreNotSuppressed() throws SQLException {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp.maxSize( 1 ).useConnectionRequestBoundaries( false ) ) ) ) {
            try ( Connection connection = dataSource.getConnection() ) {
                assertEquals( 0, RequestBoundariesConnection.beginRequestCount.get(), "Pool should not have called beginRequest()" );

                connection.beginRequest();
                assertEquals( 1, RequestBoundariesConnection.beginRequestCount.get(), "An explicit beginRequest() must be delegated to the driver" );

                connection.endRequest();
                assertEquals( 1, RequestBoundariesConnection.endRequestCount.get(), "An explicit endRequest() must be delegated to the driver" );
            }

            assertEquals( 1, RequestBoundariesConnection.endRequestCount.get(), "Pool should not have called endRequest() on return" );
        }
    }

    // --- //

    public static class RequestBoundariesConnection implements MockConnection {

        private static final AtomicInteger beginRequestCount = new AtomicInteger();
        private static final AtomicInteger endRequestCount = new AtomicInteger();

        private static volatile boolean failOnBeginRequest;
        private static volatile boolean failOnEndRequest;

        @Override
        public void beginRequest() throws SQLException {
            if ( failOnBeginRequest ) {
                throw new SQLException( "beginRequest() not supported" );
            }
            beginRequestCount.incrementAndGet();
        }

        @Override
        public void endRequest() throws SQLException {
            if ( failOnEndRequest ) {
                throw new SQLException( "endRequest() not supported" );
            }
            endRequestCount.incrementAndGet();
        }
    }
}

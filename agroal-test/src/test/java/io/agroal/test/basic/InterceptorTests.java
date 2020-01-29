// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalPoolInterceptor;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.util.Arrays.asList;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class InterceptorTests {

    private static final Logger logger = getLogger( InterceptorTests.class.getName() );

    @BeforeAll
    public static void setupMockDriver() {
        registerMockDriver( FakeSchemaConnection.class );
    }

    @AfterAll
    public static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    public static void setSchema(String value, Connection connection) {
        try {
            connection.setSchema( value );
        } catch ( SQLException e ) {
            fail();
        }
    }

    public static void assertSchema(String expected, Connection connection) {
        try {
            assertEquals( expected, connection.getSchema() );
        } catch ( SQLException e ) {
            fail();
        }
    }

    // --- //

    @Test
    @DisplayName( "Interceptor basic test" )
    public void basicInterceptorTest() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new InterceptorListener() ) ) {
            dataSource.setPoolInterceptors( asList( new LowPriorityInterceptor(), new MainInterceptor() ) );

            try ( Connection c = dataSource.getConnection() ) {
                assertSchema( "during", c );
            }
        }
    }

    @Test
    @DisplayName( "Negative priority test" )
    public void negativePriorityTest() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new InterceptorListener() ) ) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> dataSource.setPoolInterceptors( asList( new LowPriorityInterceptor(), new MainInterceptor(), new NegativePriorityInterceptor() ) ),
                    "Interceptors with negative priority throw an IllegalArgumentException as negative priority values are reserved." );
        }
    }

    // --- //

    private static class InterceptorListener implements AgroalDataSourceListener {

        @Override
        public void onConnectionPooled(Connection connection) {
            assertSchema( "before", connection );
        }

        @Override
        public void beforeConnectionDestroy(Connection connection) {
            assertSchema( "after", connection );
        }

        @Override
        public void onInfo(String message) {
            logger.info( message );
        }
    }

    private static class MainInterceptor implements AgroalPoolInterceptor {
        @Override
        public void onConnectionAcquire(Connection connection) {
            setSchema( "during", connection );
        }

        @Override
        public void onConnectionReturn(Connection connection) {
            setSchema( "after", connection );
        }
    }

    // This interceptor should be "inner" of the main one because has lower priority.
    private static class LowPriorityInterceptor implements AgroalPoolInterceptor {
        @Override
        public void onConnectionAcquire(Connection connection) {
            assertSchema( "during", connection );
        }

        @Override
        public void onConnectionReturn(Connection connection) {
            assertSchema( "during", connection );
        }

        @Override
        public int getPriority() {
            return 1;
        }
    }

    // This interceptor is invalid because priority is negative.
    private static class NegativePriorityInterceptor implements AgroalPoolInterceptor {
        @Override
        public int getPriority() {
            return -1;
        }
    }

    // --- //

    public static class FakeSchemaConnection implements MockConnection {

        private String schema = "before";

        @Override
        public String getSchema() throws SQLException {
            return schema;
        }

        @Override
        public void setSchema(String schema) {
            this.schema = schema;
        }
    }
}

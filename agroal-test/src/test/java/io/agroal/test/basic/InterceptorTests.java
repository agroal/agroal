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

import static io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static java.lang.Integer.toHexString;
import static java.lang.System.identityHashCode;
import static java.util.Arrays.asList;
import static java.util.List.of;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class InterceptorTests {

    static final Logger logger = getLogger( InterceptorTests.class.getName() );

    @BeforeAll
    static void setupMockDriver() {
        registerMockDriver( FakeSchemaConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    protected static void setSchema(String value, Connection connection) {
        try {
            connection.setSchema( value );
        } catch ( SQLException e ) {
            fail();
        }
    }

    protected static void assertSchema(String expected, Connection connection) {
        try {
            assertEquals( expected, connection.getSchema() );
        } catch ( SQLException e ) {
            fail();
        }
    }

    // --- //

    @Test
    @DisplayName( "Interceptor basic test" )
    void basicInterceptorTest() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 ) );

        InterceptorListener listener = new InterceptorListener();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, listener ) ) {
            dataSource.setPoolInterceptors( asList( new LowPriorityInterceptor(), new MainInterceptor() ) );

            try ( Connection c = dataSource.getConnection() ) {
                assertSchema( "during", c );
            }

            assertEquals( 2, listener.interceptors );
        }
    }

    @Test
    @DisplayName( "Negative priority test" )
    void negativePriorityTest() throws SQLException {
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

    @Test
    @DisplayName( "Pool interceptor test" )
    void poolInterceptorTest() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 ) );

        InvocationCountInterceptor countInterceptor = new InvocationCountInterceptor();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            dataSource.setPoolInterceptors( of( countInterceptor ) );

            assertEquals( 0, countInterceptor.created, "Expected connection not created" );
            assertEquals( 0, countInterceptor.acquired, "Expected connection not acquired" );

            try ( Connection ignored = dataSource.getConnection() ) {
                assertEquals( 1, countInterceptor.created, "Expected one connection created" );
                assertEquals( 1, countInterceptor.acquired, "Expected one connection acquired" );
            }
            assertEquals( 1, countInterceptor.returned, "Expected one connection returned" );

            try ( Connection ignored = dataSource.getConnection() ) {
                assertEquals( 1, countInterceptor.created, "Expected one connection created" );
                assertEquals( 2, countInterceptor.acquired, "Expected two connection acquired" );
            }
            assertEquals( 2, countInterceptor.returned, "Expected two connection returned" );
        }
        assertEquals( 1, countInterceptor.destroy, "Expected one connection destroyed" );
    }

    @Test
    @DisplayName( "Poolless interceptor test" )
    void poollessInterceptorTest() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .dataSourceImplementation( AGROAL_POOLLESS )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 ) );

        InvocationCountInterceptor countInterceptor = new InvocationCountInterceptor();
        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            dataSource.setPoolInterceptors( of( countInterceptor ) );

            assertEquals( 0, countInterceptor.created, "Expected connection not created" );
            assertEquals( 0, countInterceptor.acquired, "Expected connection not acquired" );

            try ( Connection ignored = dataSource.getConnection() ) {
                assertEquals( 1, countInterceptor.created, "Expected one connection created" );
                assertEquals( 1, countInterceptor.acquired, "Expected one connection acquired" );
            }
            assertEquals( 1, countInterceptor.returned, "Expected one connection returned" );
            assertEquals( 1, countInterceptor.destroy, "Expected one connection destroyed" );
        }
    }

    // --- //

    private static class InterceptorListener implements AgroalDataSourceListener {

        private int interceptors;

        @SuppressWarnings( "WeakerAccess" )
        InterceptorListener() {
        }

        @Override
        public void onConnectionPooled(Connection connection) {
            assertSchema( "before", connection );
        }

        @Override
        public void beforeConnectionDestroy(Connection connection) {
            assertSchema( "after", connection );
        }

        @Override
        @SuppressWarnings( "SingleCharacterStringConcatenation" )
        public void onPoolInterceptor(AgroalPoolInterceptor interceptor) {
            onInfo( interceptor.getClass().getName() + "@" + toHexString( identityHashCode( interceptor ) ) + " (priority " + interceptor.getPriority() + ")" );
            interceptors++;
        }

        @Override
        public void onInfo(String message) {
            logger.info( message );
        }
    }

    private static class MainInterceptor implements AgroalPoolInterceptor {

        @SuppressWarnings( "WeakerAccess" )
        MainInterceptor() {
        }

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

        @SuppressWarnings( "WeakerAccess" )
        LowPriorityInterceptor() {
        }

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

        @SuppressWarnings( "WeakerAccess" )
        NegativePriorityInterceptor() {
        }

        @Override
        public int getPriority() {
            return -1;
        }
    }

    // --- //

    private static class InvocationCountInterceptor implements AgroalPoolInterceptor {

        private int created, acquired, returned, destroy;

        @Override
        public void onConnectionCreate(Connection connection) {
            created++;
        }

        @Override
        public void onConnectionAcquire(Connection connection) {
            acquired++;
        }

        @Override
        public void onConnectionReturn(Connection connection) {
            returned++;
        }

        @Override
        public void onConnectionDestroy(Connection connection) {
            destroy++;
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

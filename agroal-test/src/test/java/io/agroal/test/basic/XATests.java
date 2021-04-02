// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockXAConnection;
import io.agroal.test.MockXADataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.sql.XAConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class XATests {

    static final Logger logger = getLogger( XATests.class.getName() );

    // --- //

    @Test
    @DisplayName( "XAConnection close test" )
    void xaConnectionCloseTests() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                // using pooless datasource as it closes connection on the calling thread
                .dataSourceImplementation( AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS )
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( RequiresCloseXADataSource.class ) ) );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier ) ) {
            try ( Connection c = dataSource.getConnection() ) {
                c.getSchema();
            }
            // ensure close() is called on the xaConnection object and not in the xaConnection.getConnection() object
            assertEquals( 1, RequiresCloseXADataSource.getClosed(), "XAConnection not closed" );
        }
    }

    // --- //

    public static class RequiresCloseXADataSource implements MockXADataSource {

        private static int closed;

        static void incrementClosed() {
            closed++;
        }

        @SuppressWarnings( "WeakerAccess" )
        static int getClosed() {
            return closed;
        }

        @Override
        public XAConnection getXAConnection() throws SQLException {
            return new MyMockXAConnection();
        }

        private static class MyMockXAConnection implements MockXAConnection {
            MyMockXAConnection() {
            }

            @Override
            @SuppressWarnings( "ObjectToString" )
            public void close() throws SQLException {
                logger.info( "Closing XAConnection " + this );
                incrementClosed();
            }
        }
    }
}

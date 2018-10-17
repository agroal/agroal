// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.test.MockDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class DriverTests {

    private static final Logger logger = getLogger( DriverTests.class.getName() );

    // --- //

    @Test
    @DisplayName( "Driver does not accept the provided URL" )
    public void basicUnacceptableURL() throws SQLException {
        AgroalDataSourceConfigurationSupplier configuration = new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration(
                cp -> cp.maxSize( 1 ).connectionFactoryConfiguration(
                        cf -> cf.connectionProviderClass( UnacceptableURLDriver.class ).jdbcUrl( "jdbc:unacceptableURL" )
                ) );

        DriverAgroalDataSourceListener listener = new DriverAgroalDataSourceListener();

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configuration, listener ) ) {
            dataSource.getConnection();
            fail( "Should thrown SQLException" );
        } catch ( SQLException e ) {
            logger.info( "Expected SQLException: " + e.getMessage() );
        }
        assertTrue( listener.hasWarning(), "An warning message should be issued" );
    }

    // --- //

    private static class DriverAgroalDataSourceListener implements AgroalDataSourceListener {

        private boolean warning = false;

        @Override
        public void onWarning(String message) {
            logger.info( "EXPECTED WARNING: " + message );
            warning = true;
        }

        @Override
        public void onWarning(Throwable throwable) {
            logger.info( "EXPECTED WARNING: " + throwable.getMessage() );
            warning = true;
        }

        public boolean hasWarning() {
            return warning;
        }
    }

    public static class UnacceptableURLDriver implements MockDriver {
        @Override
        public boolean acceptsURL(String url) throws SQLException {
            return false;
        }
    }
}

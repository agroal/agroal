// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api;

import io.agroal.api.configuration.AgroalDataSourceConfiguration;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.function.Supplier;

import static java.util.ServiceLoader.load;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalDataSource extends AutoCloseable, DataSource, Serializable {

    static AgroalDataSource from(Supplier<AgroalDataSourceConfiguration> configurationSupplier, AgroalDataSourceListener... listeners) throws SQLException {
        return from( configurationSupplier.get(), listeners );
    }

    static AgroalDataSource from(AgroalDataSourceConfiguration configuration, AgroalDataSourceListener... listeners) throws SQLException {
        for ( AgroalDataSourceProvider provider : load( AgroalDataSourceProvider.class ) ) {
            AgroalDataSource implementation = provider.getDataSource( configuration, listeners );
            if ( implementation != null ) {
                return implementation;
            }
        }
        throw new SQLException( "Unable to find the required implementation" );
    }

    // --- //

    AgroalDataSourceConfiguration getConfiguration();

    AgroalDataSourceMetrics getMetrics();

    @Deprecated
    void addListener(AgroalDataSourceListener listener);

    @Override
    void close();
}

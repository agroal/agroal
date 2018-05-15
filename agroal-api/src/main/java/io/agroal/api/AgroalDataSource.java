// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api;

import io.agroal.api.configuration.AgroalDataSourceConfiguration;

import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
        for ( AgroalDataSourceProvider provider : load( AgroalDataSourceProvider.class, AgroalDataSourceProvider.class.getClassLoader() ) ) {
            AgroalDataSource implementation = provider.getDataSource( configuration, listeners );
            if ( implementation != null ) {
                return implementation;
            }
        }

        // Fall back to load the implementation using reflection
        try {
            Class<? extends AgroalDataSource> dataSourceClass = AgroalDataSource.class.getClassLoader().loadClass( configuration.dataSourceImplementation().className() ).asSubclass( AgroalDataSource.class );
            Constructor<? extends AgroalDataSource> dataSourceConstructor = dataSourceClass.getConstructor( AgroalDataSourceConfiguration.class, AgroalDataSourceListener[].class );
            return dataSourceConstructor.newInstance( configuration, listeners );
        } catch ( ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e ) {
            throw new SQLException( "Could not load the required implementation", e );
        }
    }

    // --- //

    AgroalDataSourceConfiguration getConfiguration();

    AgroalDataSourceMetrics getMetrics();

    void flush(FlushMode mode);

    @Override
    void close();

    // --- //

    enum FlushMode {
        ALL, IDLE, INVALID, GRACEFUL, FILL
    }
}

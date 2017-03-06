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

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalDataSource extends AutoCloseable, DataSource, Serializable {

    static AgroalDataSource from(Supplier<AgroalDataSourceConfiguration> configurationSupplier) throws SQLException {
        return from( configurationSupplier.get() );
    }

    static AgroalDataSource from(AgroalDataSourceConfiguration configuration) throws SQLException {
        try {
            ClassLoader classLoader = AgroalDataSource.class.getClassLoader();
            Class<?> dataSourceClass = classLoader.loadClass( configuration.dataSourceImplementation().className() );
            Constructor<?> dataSourceConstructor = dataSourceClass.getConstructor( AgroalDataSourceConfiguration.class );
            return (AgroalDataSource) dataSourceConstructor.newInstance( configuration );
        } catch ( ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e ) {
            throw new SQLException( "Could not load Data Source class", e );
        }
    }

    // --- //

    AgroalDataSourceConfiguration getConfiguration();

    AgroalDataSourceMetrics getMetrics();

    void addListener(AgroalDataSourceListener listener);

    @Override
    void close();
}

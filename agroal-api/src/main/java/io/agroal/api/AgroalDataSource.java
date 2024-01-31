// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api;

import io.agroal.api.configuration.AgroalDataSourceConfiguration;

import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static java.util.ServiceLoader.load;

/**
 * Extension of the DataSource interface that exposes some of its internals.
 * The Agroal project is all about providing a good (reliable, fast, easy to use maintain and understand) implementation of this interface.
 *
 * Agroal - the natural database connection pool!
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalDataSource extends AutoCloseable, DataSource, Serializable {

    /**
     * Create an AgroalDataSource from a supplier of the configuration.
     */
    static AgroalDataSource from(Supplier<AgroalDataSourceConfiguration> configurationSupplier, AgroalDataSourceListener... listeners) throws SQLException {
        return from( configurationSupplier.get(), listeners );
    }

    /**
     * Create an AgroalDataSource from configuration.
     */
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

    /**
     * Allows access to the used jdbcUrl. DataSource derivation in spring boot's LiquibaseAutoConfiguration may need this.
     */
    String getUrl();

    /**
     * Allows inspection of the configuration. Some properties allow read / write.
     */
    AgroalDataSourceConfiguration getConfiguration();

    /**
     * Allows access to metrics. If metrics are not enabled, returns default values.
     */
    AgroalDataSourceMetrics getMetrics();

    /**
     * Performs a flush action on the connections of the pool.
     */
    void flush(FlushMode mode);

    /**
     * Sets pool interceptors.
     */
    void setPoolInterceptors(Collection<? extends AgroalPoolInterceptor> interceptors);

    /**
     * Get the list of pool interceptors. Interceptors are sorted from high to low priority.
     */
    List<AgroalPoolInterceptor> getPoolInterceptors();

    /**
     * Performs a health check. The newConnection parameter determines that a new database connection is established for this purpose, otherwise attempts to get a connection from the pool.
     *
     * WARNING: Using a new connection may cause the size of the pool to go over max-size.
     */
    default boolean isHealthy(boolean newConnection) throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void close();

    // --- //

    /**
     * Modes supported on the flush operation.
     */
    enum FlushMode {

        /**
         * All connections are flushed right away.
         */
        ALL,

        /**
         * Idle connections are flushed.
         */
        IDLE,

        /**
         * Performs on-demand validation of idle connections.
         */
        INVALID,

        /**
         * Active connections are flushed on return. Idle connections are flushed immediately.
         */
        GRACEFUL,

        /**
         * Flushes connections that have been in use for longer than the specified leak timeout.
         */
        LEAK,

        /**
         * Creates connections to met the minimum size of the pool.
         * Used after and increase of minimum size, to make that change effective immediately.
         */
        FILL
    }
}

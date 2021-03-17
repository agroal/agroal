// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.MetricsEnabledListener;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Builder of AgroalDataSourceConfiguration.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@SuppressWarnings( {"PackageVisibleField", "WeakerAccess"} )
public class AgroalDataSourceConfigurationSupplier implements Supplier<AgroalDataSourceConfiguration> {

    AgroalConnectionPoolConfiguration connectionPoolConfiguration = null;

    DataSourceImplementation dataSourceImplementation = DataSourceImplementation.AGROAL;
    volatile boolean metrics = false;
    volatile MetricsEnabledListener metricsEnabledListener;

    private volatile boolean lock;

    private AgroalConnectionPoolConfigurationSupplier connectionPoolConfigurationSupplier = new AgroalConnectionPoolConfigurationSupplier();

    public AgroalDataSourceConfigurationSupplier() {
        lock = false;
    }

    private void checkLock() {
        if ( lock ) {
            throw new IllegalStateException( "Attempt to modify an immutable configuration" );
        }
    }

    /**
     * Sets the configuration of the connection pool.
     */
    public AgroalDataSourceConfigurationSupplier connectionPoolConfiguration(AgroalConnectionPoolConfiguration configuration) {
        checkLock();
        connectionPoolConfigurationSupplier = new AgroalConnectionPoolConfigurationSupplier( configuration );
        return this;
    }

    /**
     * Sets the configuration of the connection pool.
     */
    public AgroalDataSourceConfigurationSupplier connectionPoolConfiguration(Supplier<AgroalConnectionPoolConfiguration> supplier) {
        return connectionPoolConfiguration( supplier.get() );
    }

    /**
     * Modifies the configuration of the connection pool.
     */
    public AgroalDataSourceConfigurationSupplier connectionPoolConfiguration(Function<AgroalConnectionPoolConfigurationSupplier, AgroalConnectionPoolConfigurationSupplier> function) {
        return connectionPoolConfiguration( function.apply( connectionPoolConfigurationSupplier ) );
    }

    /**
     * Allows access to the configuration builder for the connection pool.
     */
    public AgroalConnectionPoolConfigurationSupplier connectionPoolConfiguration() {
        return connectionPoolConfigurationSupplier;
    }

    // --- //

    /**
     * Selects the AgroalDataSource implementation. The default is AGROAL.
     */
    public AgroalDataSourceConfigurationSupplier dataSourceImplementation(DataSourceImplementation implementation) {
        checkLock();
        dataSourceImplementation = implementation;
        return this;
    }

    /**
     * Enables the collection of metrics.
     */
    public AgroalDataSourceConfigurationSupplier metricsEnabled() {
        return metricsEnabled( true );
    }

    /**
     * Enables or disables the collection of metrics. The default is false.
     */
    public AgroalDataSourceConfigurationSupplier metricsEnabled(boolean metricsEnabled) {
        checkLock();
        metrics = metricsEnabled;
        return this;
    }

    // --- //

    private void validate() {
        if ( connectionPoolConfigurationSupplier == null ) {
            throw new IllegalArgumentException( "Connection pool configuration not defined" );
        }
        connectionPoolConfiguration = connectionPoolConfigurationSupplier.get();
    }

    @Override
    @SuppressWarnings( "ReturnOfInnerClass" )
    public AgroalDataSourceConfiguration get() {
        validate();
        lock = true;

        return new AgroalDataSourceConfiguration() {

            @Override
            public AgroalConnectionPoolConfiguration connectionPoolConfiguration() {
                return connectionPoolConfiguration;
            }

            @Override
            public DataSourceImplementation dataSourceImplementation() {
                return dataSourceImplementation;
            }

            @Override
            public boolean metricsEnabled() {
                return metrics;
            }

            @Override
            public void setMetricsEnabled(boolean metricsEnabled) {
                metrics = metricsEnabled;
                if ( metricsEnabledListener != null ) {
                    metricsEnabledListener.onMetricsEnabled( metricsEnabled );
                }
            }

            @Override
            public void registerMetricsEnabledListener(MetricsEnabledListener listener) {
                metricsEnabledListener = listener;
            }
        };
    }
}

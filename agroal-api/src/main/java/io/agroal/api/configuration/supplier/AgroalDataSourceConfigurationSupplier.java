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
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalDataSourceConfigurationSupplier implements Supplier<AgroalDataSourceConfiguration> {

    private volatile boolean lock;

    private AgroalConnectionPoolConfigurationSupplier connectionPoolConfigurationSupplier = new AgroalConnectionPoolConfigurationSupplier();
    private DataSourceImplementation dataSourceImplementation = DataSourceImplementation.AGROAL;

    private volatile boolean metrics = false;
    private volatile MetricsEnabledListener metricsEnabledListener;

    public AgroalDataSourceConfigurationSupplier() {
        this.lock = false;
    }

    private void checkLock() {
        if ( lock ) {
            throw new IllegalStateException( "Attempt to modify an immutable configuration" );
        }
    }

    public AgroalDataSourceConfigurationSupplier connectionPoolConfiguration(AgroalConnectionPoolConfiguration configuration) {
        checkLock();
        connectionPoolConfigurationSupplier = new AgroalConnectionPoolConfigurationSupplier( configuration );
        return this;
    }

    public AgroalDataSourceConfigurationSupplier connectionPoolConfiguration(Supplier<AgroalConnectionPoolConfiguration> supplier) {
        return connectionPoolConfiguration( supplier.get() );
    }

    public AgroalDataSourceConfigurationSupplier connectionPoolConfiguration(Function<AgroalConnectionPoolConfigurationSupplier, AgroalConnectionPoolConfigurationSupplier> function) {
        return connectionPoolConfiguration( function.apply( connectionPoolConfigurationSupplier ) );
    }

    public AgroalConnectionPoolConfigurationSupplier connectionPoolConfiguration() {
        return connectionPoolConfigurationSupplier;
    }

    // --- //

    public AgroalDataSourceConfigurationSupplier dataSourceImplementation(DataSourceImplementation implementation) {
        checkLock();
        dataSourceImplementation = implementation;
        return this;
    }

    public AgroalDataSourceConfigurationSupplier metricsEnabled() {
        return metricsEnabled( true );
    }

    public AgroalDataSourceConfigurationSupplier metricsEnabled(boolean metricsEnabled) {
        checkLock();
        metrics = metricsEnabled;
        return this;
    }

    // --- //

    private void validate() {
        if ( connectionPoolConfigurationSupplier == null ) {
            throw new IllegalArgumentException( "Connection poll configuration not defined" );
        }
    }

    @Override
    @SuppressWarnings( "ReturnOfInnerClass" )
    public AgroalDataSourceConfiguration get() {
        validate();
        this.lock = true;

        return new AgroalDataSourceConfiguration() {

            @Override
            public AgroalConnectionPoolConfiguration connectionPoolConfiguration() {
                return connectionPoolConfigurationSupplier.get();
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

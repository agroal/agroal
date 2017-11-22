// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.MetricsEnabledListener;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalDataSourceConfigurationSupplier implements Supplier<AgroalDataSourceConfiguration> {

    private volatile boolean lock;

    @Deprecated
    private String jndiName = "";
    private AgroalConnectionPoolConfiguration connectionPoolConfiguration = new AgroalConnectionPoolConfigurationSupplier().get();
    private DataSourceImplementation dataSourceImplementation = DataSourceImplementation.AGROAL;
    private boolean xa;

    private volatile boolean metrics = false;
    private volatile MetricsEnabledListener metricsEnabledListener;

    public AgroalDataSourceConfigurationSupplier() {
        this.lock = false;
    }

    private AgroalDataSourceConfigurationSupplier applySetting(Consumer<AgroalDataSourceConfigurationSupplier> consumer) {
        if ( lock ) {
            throw new IllegalStateException( "Attempt to modify an immutable configuration" );
        }
        consumer.accept( this );
        return this;
    }

    public AgroalDataSourceConfigurationSupplier connectionPoolConfiguration(AgroalConnectionPoolConfiguration configuration) {
        return applySetting( c -> c.connectionPoolConfiguration = configuration );
    }

    public AgroalDataSourceConfigurationSupplier connectionPoolConfiguration(Supplier<AgroalConnectionPoolConfiguration> supplier) {
        return connectionPoolConfiguration( supplier.get() );
    }

    public AgroalDataSourceConfigurationSupplier connectionPoolConfiguration(Function<AgroalConnectionPoolConfigurationSupplier, AgroalConnectionPoolConfigurationSupplier> function) {
        return connectionPoolConfiguration( function.apply( new AgroalConnectionPoolConfigurationSupplier( connectionPoolConfiguration ) ) );
    }

    // --- //

    public AgroalDataSourceConfigurationSupplier dataSourceImplementation(DataSourceImplementation implementation) {
        return applySetting( c -> c.dataSourceImplementation = implementation );
    }

    @Deprecated
    public AgroalDataSourceConfigurationSupplier jndiName(String name) {
        return applySetting( c -> c.jndiName = name );
    }

    public AgroalDataSourceConfigurationSupplier xa(boolean xaEnabled) {
        return applySetting( c -> c.xa = xaEnabled );
    }

    public AgroalDataSourceConfigurationSupplier metricsEnabled(boolean metricsEnabled) {
        return applySetting( c -> c.metrics = metricsEnabled );
    }

    // --- //

    private void validate() {
        if ( connectionPoolConfiguration == null ) {
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
            public String jndiName() {
                return jndiName;
            }

            @Override
            public AgroalConnectionPoolConfiguration connectionPoolConfiguration() {
                return connectionPoolConfiguration;
            }

            @Override
            public DataSourceImplementation dataSourceImplementation() {
                return dataSourceImplementation;
            }

            @Override
            public boolean isXA() {
                return xa;
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

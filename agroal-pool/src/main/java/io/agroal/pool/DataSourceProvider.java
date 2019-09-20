// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalDataSourceProvider;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;

import static io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL;
import static io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation.AGROAL_POOLLESS;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class DataSourceProvider implements AgroalDataSourceProvider {

    @Override
    public AgroalDataSource getDataSource(AgroalDataSourceConfiguration config, AgroalDataSourceListener... listeners) {
        return config.dataSourceImplementation() == AGROAL || config.dataSourceImplementation() == AGROAL_POOLLESS ? new DataSource( config, listeners ) : null;
    }
}

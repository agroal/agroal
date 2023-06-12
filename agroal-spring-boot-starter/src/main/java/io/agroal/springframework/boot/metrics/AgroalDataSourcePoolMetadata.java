// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.metrics;

import io.agroal.api.AgroalDataSource;
import org.springframework.boot.jdbc.metadata.AbstractDataSourcePoolMetadata;


public class AgroalDataSourcePoolMetadata extends AbstractDataSourcePoolMetadata<AgroalDataSource> {

    public AgroalDataSourcePoolMetadata(AgroalDataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Integer getActive() {
        return (int) getDataSource().getMetrics().activeCount();
    }

    @Override
    public Integer getIdle() {
        return (int) getDataSource().getMetrics().availableCount();
    }

    @Override
    public Integer getMax() {
        return getDataSource().getConfiguration().connectionPoolConfiguration().maxSize();
    }

    @Override
    public Integer getMin() {
        return getDataSource().getConfiguration().connectionPoolConfiguration().minSize();
    }

    @Override
    public String getValidationQuery() {
        return null;
    }

    @Override
    public Boolean getDefaultAutoCommit() {
        return getDataSource().getConfiguration().connectionPoolConfiguration().connectionFactoryConfiguration().autoCommit();
    }
}

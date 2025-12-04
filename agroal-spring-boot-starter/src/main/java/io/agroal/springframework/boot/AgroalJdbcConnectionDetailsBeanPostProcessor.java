// Copyright (C) 2024 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.transaction.jta.JtaTransactionManager;

/**
 * @author <a href="benjamin.graf@gmx.net">Benjamin Graf</a>
 */
class AgroalJdbcConnectionDetailsBeanPostProcessor implements BeanPostProcessor, PriorityOrdered {

    private final ObjectProvider<JdbcConnectionDetails> connectionDetailsProvider;
    private final ObjectProvider<JtaTransactionManager> jtaPlatformProvider;

    public AgroalJdbcConnectionDetailsBeanPostProcessor(
            ObjectProvider<JdbcConnectionDetails> connectionDetailsProvider,
            ObjectProvider<JtaTransactionManager> jtaPlatformProvider ) {
        this.connectionDetailsProvider = connectionDetailsProvider;
        this.jtaPlatformProvider = jtaPlatformProvider;
    }

    @Override
    public Object postProcessBeforeInitialization( Object bean, String beanName ) throws BeansException {
        if ( AgroalDataSource.class.isAssignableFrom( bean.getClass() ) && "dataSource".equals( beanName ) ) {
            JdbcConnectionDetails connectionDetails = connectionDetailsProvider.getIfAvailable();
            if ( connectionDetails != null ) {
                return processDataSource( (AgroalDataSource) bean, connectionDetails );
            }
        }
        return bean;
    }

    private Object processDataSource( AgroalDataSource dataSource, JdbcConnectionDetails connectionDetails ) {
        dataSource.setUrl( connectionDetails.getJdbcUrl() );
        dataSource.setUsername( connectionDetails.getUsername() );
        dataSource.setPassword( connectionDetails.getPassword() );
        JtaTransactionManager jtaPlatform = jtaPlatformProvider.getIfAvailable();
        if ( jtaPlatform != null && jtaPlatform.getTransactionManager() != null && jtaPlatform.getTransactionSynchronizationRegistry() != null ) {
            dataSource.setDriverClassName( connectionDetails.getXaDataSourceClassName() );
        } else {
            dataSource.setDriverClassName( connectionDetails.getDriverClassName() );
        }
        return dataSource;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 2;
    }
}

// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot;

import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.AgroalDataSourceMetrics;
import io.agroal.api.AgroalPoolInterceptor;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.agroal.narayana.NarayanaTransactionIntegration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.jta.JtaTransactionManager;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalDataSource implements io.agroal.api.AgroalDataSource, InitializingBean {

    private final Log logger = LogFactory.getLog( AgroalDataSource.class );

    private final AgroalDataSourceConfigurationSupplier datasourceConfiguration;
    private final AgroalConnectionPoolConfigurationSupplier connectionPoolConfiguration;
    private final AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfiguration;

    private io.agroal.api.AgroalDataSource delegate;
    private String datasourceName = "<default>";

    public AgroalDataSource() {
        datasourceConfiguration = new AgroalDataSourceConfigurationSupplier();
        connectionPoolConfiguration = new AgroalConnectionPoolConfigurationSupplier();
        connectionFactoryConfiguration = new AgroalConnectionFactoryConfigurationSupplier();

        connectionPoolConfiguration.maxSize( 10 );
    }

    @Override
    public void afterPropertiesSet() throws SQLException {
        connectionPoolConfiguration.connectionFactoryConfiguration( connectionFactoryConfiguration );
        datasourceConfiguration.connectionPoolConfiguration( connectionPoolConfiguration );

        delegate = io.agroal.api.AgroalDataSource.from( datasourceConfiguration, new LoggingListener( datasourceName ) );
        logger.info( "Started DataSource " + datasourceName + " connected to " + getConfiguration().connectionPoolConfiguration().connectionFactoryConfiguration().jdbcUrl() );
    }

    // --- //

    public void setName(String name) {
        datasourceName = name;
    }

    public void setImplementation(String name) {
        datasourceConfiguration.dataSourceImplementation( AgroalDataSourceConfiguration.DataSourceImplementation.valueOf( name ) );
    }

    // --- //

    public void setMaxSize(int size) {
        connectionPoolConfiguration.maxSize( size );
    }

    public void setMinSize(int size) {
        connectionPoolConfiguration.minSize( size );
    }

    public void setInitialSize(int size) {
        connectionPoolConfiguration.initialSize( size );
    }

    public void setAcquisitionTimeout(int timeout) {
        connectionPoolConfiguration.acquisitionTimeout( Duration.ofSeconds( timeout ) );
    }

    public void setForegroundValidationTimeout(int timeout) {
        connectionPoolConfiguration.idleValidationTimeout( Duration.ofSeconds( timeout ) );
    }

    public void setIdleTimeout(int timeout) {
        connectionPoolConfiguration.reapTimeout( Duration.ofSeconds( timeout ) );
    }

    public void setLeakTimeout(int timeout) {
        connectionPoolConfiguration.leakTimeout( Duration.ofSeconds( timeout ) );
    }

    public void setLifetimeTimeout(int timeout) {
        connectionPoolConfiguration.maxLifetime( Duration.ofSeconds( timeout ) );
    }

    public void setValidationTimeout(int timeout) {
        connectionPoolConfiguration.validationTimeout( Duration.ofSeconds( timeout ) );
    }

    public void setJtaTransactionIntegration(JtaTransactionManager jtaPlatform) {
        connectionPoolConfiguration.transactionIntegration( new NarayanaTransactionIntegration( jtaPlatform.getTransactionManager(), jtaPlatform.getTransactionSynchronizationRegistry() ) );
    }

    // --- //

    public void setUrl(String url) {
        connectionFactoryConfiguration.jdbcUrl( url );
    }

    public void setDriverClass(Class<? extends DataSource> driver) {
        connectionFactoryConfiguration.connectionProviderClass( driver );
    }

    public void setDriverClassName(String driver) {
        connectionFactoryConfiguration.connectionProviderClassName( driver );
    }

    public void setUsername(String username) {
        connectionFactoryConfiguration.principal( new NamePrincipal( username ) );
    }

    public void setPassword(String password) {
        connectionFactoryConfiguration.credential( new SimplePassword( password ) );
    }

    public void setInitialSql(String initialSql) {
        connectionFactoryConfiguration.initialSql( initialSql );
    }

    public void setAutoCommit(boolean autoCommit) {
        connectionFactoryConfiguration.autoCommit( autoCommit );
    }

    public void setTrackResources(boolean track) {
        connectionFactoryConfiguration.trackJdbcResources( track );
    }

    public void setRecoveryUsername(String username) {
        connectionFactoryConfiguration.recoveryPrincipal( new NamePrincipal( username ) );
    }

    public void setRecoveryPassword(String password) {
        connectionFactoryConfiguration.recoveryCredential( new SimplePassword( password ) );
    }

    // --- //

    @Override
    public AgroalDataSourceConfiguration getConfiguration() {
        return delegate.getConfiguration();
    }

    @Override
    public AgroalDataSourceMetrics getMetrics() {
        return delegate.getMetrics();
    }

    public void setMetrics(boolean metrics) {
        datasourceConfiguration.metricsEnabled();
    }

    @Override
    public void flush(FlushMode mode) {
        delegate.flush( mode );
    }

    @Override
    public List<AgroalPoolInterceptor> getPoolInterceptors() {
        return delegate.getPoolInterceptors();
    }

    @Override
    public void setPoolInterceptors(Collection<? extends AgroalPoolInterceptor> interceptors) {
        delegate.setPoolInterceptors( interceptors );
    }

    @Override
    public void close() {
        logger.debug( "Closing DataSource " + datasourceName );
        delegate.close();
        delegate = null;
    }

    // --- //

    @Override
    public Connection getConnection() throws SQLException {
        return delegate.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return delegate.getConnection( username, password );
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return delegate.unwrap( iface );
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return delegate.isWrapperFor( iface );
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        delegate.setLogWriter( out );
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return delegate.getLoginTimeout();
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        delegate.setLoginTimeout( seconds );
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

    // --- //

    private static class LoggingListener implements AgroalDataSourceListener {

        private final Log logger;

        private LoggingListener(String name) {
            logger = LogFactory.getLog( io.agroal.api.AgroalDataSource.class.getName() + ".'" + name + "'" );
        }

        @Override
        public void onWarning(String message) {
            logger.warn( message );
        }

        @Override
        public void onWarning(Throwable throwable) {
            logger.warn( throwable );
        }

        @Override
        public void onInfo(String message) {
            logger.info( message );
        }

        @Override
        public void onConnectionCreation(Connection connection) {
            if ( logger.isDebugEnabled() ) {
                logger.debug( "Created connection " + connection );
            }
        }

        @Override
        public void onConnectionAcquire(Connection connection) {
            if ( logger.isDebugEnabled() ) {
                logger.debug( "Connection acquired " + connection );
            }
        }

        @Override
        public void onConnectionReturn(Connection connection) {
            if ( logger.isDebugEnabled() ) {
                logger.debug( "Connection return " + connection );
            }
        }

        @Override
        public void onConnectionLeak(Connection connection, Thread thread) {
            logger.info( "Connection " + connection + "leak! Acquired by " + thread.getName() );
            logger.info( Arrays.stream( thread.getStackTrace() ).map( StackTraceElement::toString ).collect( Collectors.joining( System.lineSeparator() ) ) );
        }

        @Override
        public void beforeConnectionValidation(Connection connection) {
            if ( logger.isDebugEnabled() ) {
                logger.debug( "Performing validation of " + connection );
            }
        }

        @Override
        public void onConnectionInvalid(Connection connection) {
            if ( logger.isDebugEnabled() ) {
                logger.debug( "Connection invalid " + connection );
            }
        }

        @Override
        public void onConnectionReap(Connection connection) {
            if ( logger.isDebugEnabled() ) {
                logger.debug( "Connection reap " + connection );
            }
        }

        @Override
        public void onConnectionDestroy(Connection connection) {
            if ( logger.isDebugEnabled() ) {
                logger.debug( "Connection destroy " + connection );
            }
        }
    }
}

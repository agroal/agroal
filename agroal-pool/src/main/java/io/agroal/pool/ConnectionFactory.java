// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.security.AgroalSecurityProvider;
import io.agroal.api.transaction.TransactionIntegration.ResourceRecoveryFactory;
import io.agroal.pool.util.PropertyInjector;
import io.agroal.pool.util.XAConnectionAdaptor;

import javax.sql.XAConnection;
import java.lang.reflect.InvocationTargetException;
import java.security.Principal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.UNDEFINED;
import static io.agroal.pool.util.ListenerHelper.fireOnWarning;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class ConnectionFactory implements ResourceRecoveryFactory {

    private static final String URL_PROPERTY_NAME = "url";

    private static final Properties EMPTY_PROPERTIES = new Properties();

    private final AgroalConnectionFactoryConfiguration configuration;
    private final AgroalDataSourceListener[] listeners;
    private final Properties jdbcProperties = new Properties(); // backup of jdbcProperties for DRIVER mode
    private final Mode factoryMode;

    // these are the sources for connections, that will be used depending on the mode
    private java.sql.Driver driver;
    private javax.sql.DataSource dataSource;
    private javax.sql.XADataSource xaDataSource;
    private javax.sql.XADataSource xaRecoveryDataSource;

    private PropertyInjector injector;
    private Integer defaultIsolationLevel;

    public ConnectionFactory(AgroalConnectionFactoryConfiguration configuration, AgroalDataSourceListener... listeners) {
        this.configuration = configuration;
        this.listeners = listeners;

        factoryMode = Mode.fromClass( configuration.connectionProviderClass() );
        switch ( factoryMode ) {
            case XA_DATASOURCE:
                injector = new PropertyInjector( configuration.connectionProviderClass() );
                Properties xaProperties = configuration.xaProperties().isEmpty() ? configuration.jdbcProperties() : configuration.xaProperties();
                xaDataSource = newXADataSource( xaProperties );
                xaRecoveryDataSource = newXADataSource( xaProperties );
                break;
            case DATASOURCE:
                injector = new PropertyInjector( configuration.connectionProviderClass() );
                dataSource = newDataSource( configuration.jdbcProperties() );
                break;
            case DRIVER:
                driver = newDriver();
                jdbcProperties.putAll( configuration.jdbcProperties() );
                break;
        }
    }

    public int defaultJdbcIsolationLevel() {
        return defaultIsolationLevel == null ? UNDEFINED.level() : defaultIsolationLevel;
    }

    @SuppressWarnings( "StringConcatenation" )
    private javax.sql.XADataSource newXADataSource(Properties properties) {
        javax.sql.XADataSource newDataSource;
        try {
            newDataSource = configuration.connectionProviderClass().asSubclass( javax.sql.XADataSource.class ).getDeclaredConstructor().newInstance();
        } catch ( IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e ) {
            throw new RuntimeException( "Unable to instantiate javax.sql.XADataSource", e );
        }

        if ( configuration.jdbcUrl() != null && !configuration.jdbcUrl().isEmpty() ) {
            injectUrlProperty( newDataSource, URL_PROPERTY_NAME, configuration.jdbcUrl() );
        }
        try {
            newDataSource.setLoginTimeout( (int) configuration.loginTimeout().getSeconds() );
        } catch ( SQLException e ) {
            fireOnWarning( listeners, "Unable to set login timeout: " + e.getMessage() );
        }

        injectJdbcProperties( newDataSource, properties );
        return newDataSource;
    }

    @SuppressWarnings( "StringConcatenation" )
    private javax.sql.DataSource newDataSource(Properties properties) {
        javax.sql.DataSource newDataSource;
        try {
            newDataSource = configuration.connectionProviderClass().asSubclass( javax.sql.DataSource.class ).getDeclaredConstructor().newInstance();
        } catch ( IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e ) {
            throw new RuntimeException( "Unable to instantiate javax.sql.DataSource", e );
        }

        if ( configuration.jdbcUrl() != null && !configuration.jdbcUrl().isEmpty() ) {
            injectUrlProperty( newDataSource, URL_PROPERTY_NAME, configuration.jdbcUrl() );
        }
        try {
            newDataSource.setLoginTimeout( (int) configuration.loginTimeout().getSeconds() );
        } catch ( SQLException e ) {
            fireOnWarning( listeners, "Unable to set login timeout: " + e.getMessage() );
        }

        injectJdbcProperties( newDataSource, properties );
        return newDataSource;
    }

    @SuppressWarnings( "StringConcatenation" )
    private java.sql.Driver newDriver() {
        DriverManager.setLoginTimeout( (int) configuration.loginTimeout().getSeconds() );

        if ( configuration.connectionProviderClass() == null ) {
            try {
                return driver = DriverManager.getDriver( configuration.jdbcUrl() );
            } catch ( SQLException sql ) {
                throw new RuntimeException( "Unable to get java.sql.Driver from DriverManager", sql );
            }
        } else {
            try {
                driver = configuration.connectionProviderClass().asSubclass( java.sql.Driver.class ).getDeclaredConstructor().newInstance();
                if ( !driver.acceptsURL( configuration.jdbcUrl() ) ) {
                    fireOnWarning( listeners, "Driver does not support the provided URL: " + configuration.jdbcUrl() );
                }
                return driver;
            } catch ( IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e ) {
                throw new RuntimeException( "Unable to instantiate java.sql.Driver", e );
            } catch ( SQLException e ) {
                throw new RuntimeException( "Unable to verify that the java.sql.Driver supports the provided URL", e );
            }
        }
    }

    @SuppressWarnings( "StringConcatenation" )
    private void injectUrlProperty(Object target, String propertyName, String propertyValue) {
        try {
            injector.inject( target, propertyName, propertyValue );
        } catch ( IllegalArgumentException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e ) {
            // AG-134 - Some drivers have setURL() instead of setUrl(), so we retry with upper case.
            // AG-228 - Not all (XA)DataSource's require an url to be set
            if ( propertyName.chars().anyMatch( Character::isLowerCase ) ) {
                injectUrlProperty( target, propertyName.toUpperCase( Locale.ROOT ), propertyValue );
            }
        }
    }

    @SuppressWarnings( {"ObjectAllocationInLoop", "StringConcatenation"} )
    private void injectJdbcProperties(Object target, Properties properties) {
        boolean ignoring = false;
        for ( String propertyName : properties.stringPropertyNames() ) {
            try {
                injector.inject( target, propertyName, properties.getProperty( propertyName ) );
            } catch ( IllegalArgumentException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e ) {
                fireOnWarning( listeners, "Ignoring property '" + propertyName + "': " + e.getMessage() );
                ignoring = true;
            }
        }
        if ( ignoring ) {
            fireOnWarning( listeners, "Available properties " + Arrays.toString( injector.availableProperties().toArray() ) );
        }
    }

    // --- //

    private Properties jdbcProperties() {
        Properties properties = new Properties();
        properties.putAll( jdbcProperties );
        properties.putAll( securityProperties( configuration.principal(), configuration.credentials() ) );
        return properties;
    }

    private Properties recoveryProperties() {
        Properties properties = new Properties();
        // use the main credentials when recovery credentials are not provided
        if ( configuration.recoveryPrincipal() == null && ( configuration.recoveryCredentials() == null || configuration.recoveryCredentials().isEmpty() ) ) {
            properties.putAll( securityProperties( configuration.principal(), configuration.credentials() ) );
        } else {
            properties.putAll( securityProperties( configuration.recoveryPrincipal(), configuration.recoveryCredentials() ) );
        }
        return properties;
    }

    private Properties securityProperties(Principal principal, Iterable<Object> credentials) {
        Properties properties = new Properties();
        properties.putAll( securityProperties( principal ) );
        for ( Object credential : credentials ) {
            properties.putAll( securityProperties( credential ) );
        }
        return properties;
    }

    private Properties securityProperties(Object securityObject) {
        if ( securityObject == null ) {
            return EMPTY_PROPERTIES;
        }

        for ( AgroalSecurityProvider provider : configuration.securityProviders() ) {
            Properties properties = provider.getSecurityProperties( securityObject );
            if ( properties != null ) {
                return properties;
            }
        }
        fireOnWarning( listeners, "Unknown security object of type: " + securityObject.getClass().getName() );
        return EMPTY_PROPERTIES;
    }

    // --- //

    public XAConnection createConnection() throws SQLException {
        switch ( factoryMode ) {
            case DRIVER:
                return new XAConnectionAdaptor( connectionSetup( driver.connect( configuration.jdbcUrl(), jdbcProperties() ) ) );
            case DATASOURCE:
                injectJdbcProperties( dataSource, securityProperties( configuration.principal(), configuration.credentials() ) );
                return new XAConnectionAdaptor( connectionSetup( dataSource.getConnection() ) );
            case XA_DATASOURCE:
                injectJdbcProperties( xaDataSource, securityProperties( configuration.principal(), configuration.credentials() ) );
                return xaConnectionSetup( xaDataSource.getXAConnection() );
            default:
                throw new SQLException( "Unknown connection factory mode" );
        }
    }

    @SuppressWarnings( "MagicConstant" )
    private Connection connectionSetup(Connection connection) throws SQLException {
        if ( connection == null ) {
            // AG-90: Driver can return null if the URL is not supported (see java.sql.Driver#connect() documentation)
            throw new SQLException( "Driver does not support the provided URL: " + configuration.jdbcUrl() );
        }

        connection.setAutoCommit( configuration.autoCommit() );
        if ( configuration.jdbcTransactionIsolation().isDefined() ) {
            connection.setTransactionIsolation( configuration.jdbcTransactionIsolation().level() );
        } else if ( defaultIsolationLevel == null ) {
            defaultIsolationLevel = connection.getTransactionIsolation();
        }
        if ( configuration.initialSql() != null && !configuration.initialSql().isEmpty() ) {
            try ( Statement statement = connection.createStatement() ) {
                statement.execute( configuration.initialSql() );
            }
        }
        return connection;
    }

    private XAConnection xaConnectionSetup(XAConnection xaConnection) throws SQLException {
        if ( xaConnection.getXAResource() == null ) {
            // Make sure that XAConnections are not processed as non-XA connections by the pool
            xaConnection.close();
            throw new SQLException( "null XAResource from XADataSource" );
        }
        try ( Connection connection = xaConnection.getConnection() ) {
            connectionSetup( connection );
        }
        return xaConnection;
    }

    // --- //

    @Override
    @SuppressWarnings( "StringConcatenation" )
    public XAConnection getRecoveryConnection() {
        try {
            if ( factoryMode == Mode.XA_DATASOURCE ) {
                injectJdbcProperties( xaRecoveryDataSource, recoveryProperties() );
                return xaRecoveryDataSource.getXAConnection();
            }
            fireOnWarning( listeners, "Recovery connections are only available for XADataSource" );
        } catch ( SQLException e ) {
            fireOnWarning( listeners, "Unable to get recovery connection: " + e.getMessage() );
        }
        return null;
    }

    // --- //

    private enum Mode {

        DRIVER, DATASOURCE, XA_DATASOURCE;

        @SuppressWarnings( "WeakerAccess" )
        static Mode fromClass(Class<?> providerClass) {
            if ( providerClass == null ) {
                return DRIVER;
            } else if ( javax.sql.XADataSource.class.isAssignableFrom( providerClass ) ) {
                return XA_DATASOURCE;
            } else if ( javax.sql.DataSource.class.isAssignableFrom( providerClass ) ) {
                return DATASOURCE;
            } else if ( java.sql.Driver.class.isAssignableFrom( providerClass ) ) {
                return DRIVER;
            } else {
                throw new IllegalArgumentException( "Unable to create ConnectionFactory from providerClass " + providerClass.getName() );
            }
        }
    }
}

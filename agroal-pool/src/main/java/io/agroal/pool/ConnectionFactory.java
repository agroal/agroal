// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.agroal.pool.util.PropertyInjector;
import io.agroal.pool.util.XAConnectionAdaptor;

import javax.sql.XAConnection;
import java.security.Principal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class ConnectionFactory {

    private static final String URL_PROPERTY_NAME = "url";
    private static final String USER_PROPERTY_NAME = "user";
    private static final String PASSWORD_PROPERTY_NAME = "password";

    private final AgroalConnectionFactoryConfiguration configuration;
    private final Properties jdbcProperties;
    private final Mode factoryMode;

    // these are the sources for connections, that will be used depending on the mode
    private java.sql.Driver driver;
    private javax.sql.DataSource dataSource;
    private javax.sql.XADataSource xaDataSource;

    public ConnectionFactory(AgroalConnectionFactoryConfiguration configuration, boolean isXA) {
        this.configuration = configuration;
        this.jdbcProperties = new Properties( configuration.jdbcProperties() );
        setupSecurity( configuration );

        Mode mode;
        if ( isXA ) {
            setupXA();
            mode = Mode.XADATASOURCE;
        } else {
            try {
                setupDataSource();
                mode = Mode.DATASOURCE;
            } catch ( Throwable t ) {
                setupDriver();
                mode = Mode.DRIVER;
            }
        }
        factoryMode = mode;
    }

    @SuppressWarnings( "unchecked" )
    private void setupXA() {
        try {
            ClassLoader driverLoader = configuration.classLoaderProvider().getClassLoader( configuration.driverClassName() );
            Class<javax.sql.XADataSource> xaDataSourceClass = (Class<javax.sql.XADataSource>) driverLoader.loadClass( configuration.driverClassName() );
            this.xaDataSource = xaDataSourceClass.newInstance();
        } catch ( IllegalAccessException | InstantiationException | ClassNotFoundException e ) {
            throw new RuntimeException( "Unable to load XA DataSource class", e );
        }

        PropertyInjector.inject( xaDataSource, URL_PROPERTY_NAME, configuration.jdbcUrl() );
        for ( String property : jdbcProperties.stringPropertyNames() ) {
            PropertyInjector.inject( xaDataSource, property, jdbcProperties.getProperty( property ) );
        }
    }

    @SuppressWarnings( "unchecked" )
    private void setupDataSource() {
        try {
            ClassLoader driverLoader = configuration.classLoaderProvider().getClassLoader( configuration.driverClassName() );
            Class<javax.sql.DataSource> dataSourceClass = (Class<javax.sql.DataSource>) driverLoader.loadClass( configuration.driverClassName() );
            this.dataSource = dataSourceClass.newInstance();
        } catch ( IllegalAccessException | InstantiationException | ClassNotFoundException e ) {
            throw new RuntimeException( "Unable to load DataSource class", e );
        }

        PropertyInjector.inject( dataSource, URL_PROPERTY_NAME, configuration.jdbcUrl() );
        for ( String property : jdbcProperties.stringPropertyNames() ) {
            PropertyInjector.inject( dataSource, property, jdbcProperties.getProperty( property ) );
        }
    }

    @SuppressWarnings( "unchecked" )
    private void setupDriver() {
        try {
            ClassLoader driverLoader = configuration.classLoaderProvider().getClassLoader( configuration.driverClassName() );
            Class<java.sql.Driver> driverClass = (Class<java.sql.Driver>) driverLoader.loadClass( configuration.driverClassName() );
            this.driver = driverClass.newInstance();
        } catch ( IllegalAccessException | InstantiationException | ClassNotFoundException e ) {
            try {
                // Fallback to load the Driver through the DriverManager
                this.driver = java.sql.DriverManager.getDriver( configuration.jdbcUrl() );
            } catch ( SQLException ig ) {
                throw new RuntimeException( "Unable to load Driver class", e );
            }
        }
    }

    // --- //

    private void setupSecurity(AgroalConnectionFactoryConfiguration configuration) {
        Principal principal = configuration.principal();

        if ( principal == null ) {
            // skip!
        } else if ( principal instanceof NamePrincipal ) {
            jdbcProperties.put( USER_PROPERTY_NAME, principal.getName() );
        }

        // Add other principal types here

        else {
            throw new IllegalArgumentException( "Unknown Principal type: " + principal.getClass().getName() );
        }

        for ( Object credential : configuration.credentials() ) {
            if ( credential instanceof SimplePassword ) {
                jdbcProperties.put( PASSWORD_PROPERTY_NAME, ( (SimplePassword) credential ).getWord() );
            }

            // Add other credential types here

            else {
                throw new IllegalArgumentException( "Unknown Credential type: " + credential.getClass().getName() );
            }
        }
    }

    // --- //

    public XAConnection createConnection() throws SQLException {
        switch ( factoryMode ) {
            case DRIVER:
                return new XAConnectionAdaptor( connectionSetup( driver.connect( configuration.jdbcUrl(), jdbcProperties ) ) );
            case DATASOURCE:
                return new XAConnectionAdaptor( connectionSetup( dataSource.getConnection() ) );
            case XADATASOURCE:
                XAConnection xaConnection = xaDataSource.getXAConnection();
                connectionSetup( xaConnection.getConnection() );
                if ( xaConnection.getXAResource() == null ) {
                    // this ensures that XAConnections are not processed as non-XA connections by the pool
                    throw new SQLException( "null XAResource from XADataSource" );
                }
                return xaConnection;
        }
        throw new SQLException( "Unknown connection factory mode" );
    }

    private Connection connectionSetup(Connection connection) throws SQLException {
        connection.setAutoCommit( configuration.autoCommit() );
        if ( configuration.jdbcTransactionIsolation().isDefined() ) {
            connection.setTransactionIsolation( configuration.jdbcTransactionIsolation().level() );
        }
        if ( configuration.initialSql() != null && !configuration.initialSql().isEmpty() ) {
            try ( Statement statement = connection.createStatement() ) {
                statement.execute( configuration.initialSql() );
            }
        }
        return connection;
    }

    // --- //

    private enum Mode {
        DRIVER, DATASOURCE, XADATASOURCE
    }
}

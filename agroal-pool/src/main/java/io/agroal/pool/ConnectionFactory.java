// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.pool;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;

import java.security.Principal;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class ConnectionFactory {

    private static final String USER_PROPERTY_NAME = "user";
    private static final String PASSWORD_PROPERTY_NAME = "password";

    private AgroalConnectionFactoryConfiguration configuration;
    private Driver driver;
    private Properties jdbcProperties;

    @SuppressWarnings( "unchecked" )
    public ConnectionFactory(AgroalConnectionFactoryConfiguration configuration) {
        try {
            this.configuration = configuration;
            this.jdbcProperties = configuration.jdbcProperties();
            ClassLoader driverLoader = configuration.classLoaderProvider().getClassLoader( configuration.driverClassName() );
            Class<Driver> driverClass = (Class<Driver>) driverLoader.loadClass( configuration.driverClassName() );
            driver = driverClass.newInstance();
            setupSecurity( configuration );
        } catch ( IllegalAccessException | InstantiationException | ClassNotFoundException e ) {
            try {
                // Fallback to load the Driver through the DriverManager
                driver = DriverManager.getDriver( configuration.jdbcUrl() );
            } catch ( SQLException ig ) {
                throw new RuntimeException( "Unable to load driver class", e );
            }
        }
    }

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

    public Connection createConnection() throws SQLException {
        Connection connection = driver.connect( configuration.jdbcUrl(), jdbcProperties );
        connection.setAutoCommit( configuration.autoCommit() );
        if ( configuration.initialSql() != null && !configuration.initialSql().isEmpty() ) {
            try ( Statement statement = connection.createStatement() ) {
                statement.execute( configuration.initialSql() );
            }
        }
        return connection;
    }
}

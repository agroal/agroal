// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;
import io.agroal.api.configuration.ClassLoaderProvider;
import io.agroal.api.configuration.InterruptProtection;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.READ_COMMITTED;
import static io.agroal.api.configuration.ClassLoaderProvider.systemClassloader;
import static io.agroal.api.configuration.InterruptProtection.none;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalConnectionFactoryConfigurationSupplier implements Supplier<AgroalConnectionFactoryConfiguration> {

    private static final String USERNAME_PROPERTY_NAME = "username";
    private static final String PASSWORD_PROPERTY_NAME = "password";

    private volatile boolean lock;

    private boolean autoCommit = false;
    private String jdbcUrl = "";
    private String initialSql = "";
    private String driverClassName = "";
    private ClassLoaderProvider classLoaderProvider = systemClassloader();
    private TransactionIsolation transactionIsolation = READ_COMMITTED;
    private InterruptProtection interruptProtection = none();
    private Principal principal;
    private Collection<Object> credentials = new ArrayList<>();
    private Properties jdbcProperties = new Properties();

    public AgroalConnectionFactoryConfigurationSupplier() {
        this.lock = false;
    }

    public AgroalConnectionFactoryConfigurationSupplier(AgroalConnectionFactoryConfiguration existingConfiguration) {
        this();
        if ( existingConfiguration == null ) {
            return;
        }
        this.autoCommit = existingConfiguration.autoCommit();
        this.jdbcUrl = existingConfiguration.jdbcUrl();
        this.initialSql = existingConfiguration.initialSql();
        this.driverClassName = existingConfiguration.driverClassName();
        this.classLoaderProvider = existingConfiguration.classLoaderProvider();
        this.transactionIsolation = existingConfiguration.jdbcTransactionIsolation();
        this.interruptProtection = existingConfiguration.interruptProtection();
        this.principal = existingConfiguration.principal();
        this.credentials = existingConfiguration.credentials();
        this.jdbcProperties = existingConfiguration.jdbcProperties();
    }

    private AgroalConnectionFactoryConfigurationSupplier applySetting(Consumer<AgroalConnectionFactoryConfigurationSupplier> consumer) {
        if ( lock ) {
            throw new IllegalStateException( "Attempt to modify an immutable configuration" );
        }
        consumer.accept( this );
        return this;
    }

    // --- //

    public AgroalConnectionFactoryConfigurationSupplier autoCommit(boolean autoCommit) {
        return applySetting( c -> c.autoCommit = autoCommit );
    }

    public AgroalConnectionFactoryConfigurationSupplier jdbcUrl(String jdbcUrl) {
        return applySetting( c -> c.jdbcUrl = jdbcUrl );
    }

    public AgroalConnectionFactoryConfigurationSupplier initialSql(String initialSql) {
        return applySetting( c -> c.initialSql = initialSql );
    }

    public AgroalConnectionFactoryConfigurationSupplier driverClassName(String driverClassName) {
        return applySetting( c -> c.driverClassName = driverClassName );
    }

    public AgroalConnectionFactoryConfigurationSupplier classLoaderProvider(ClassLoaderProvider provider) {
        return applySetting( c -> c.classLoaderProvider = provider );
    }

    public AgroalConnectionFactoryConfigurationSupplier classLoader(ClassLoader classLoader) {
        return applySetting( c -> c.classLoaderProvider = className -> classLoader );
    }

    public AgroalConnectionFactoryConfigurationSupplier jdbcTransactionIsolation(TransactionIsolation transactionIsolation) {
        return applySetting( c -> c.transactionIsolation = transactionIsolation );
    }

    public AgroalConnectionFactoryConfigurationSupplier interruptHandlingMode(InterruptProtection interruptProtection) {
        return applySetting( c -> c.interruptProtection = interruptProtection );
    }

    public AgroalConnectionFactoryConfigurationSupplier principal(Principal principal) {
        return applySetting( c -> c.principal = principal );
    }

    public AgroalConnectionFactoryConfigurationSupplier credential(Object credential) {
        return applySetting( c -> c.credentials.add( credential ) );
    }

    public AgroalConnectionFactoryConfigurationSupplier jdbcProperty(String key, String value) {
        return applySetting( c -> c.jdbcProperties.put( key, value ) );
    }

    // --- //

    private void validateJdbcProperty(String key) {
        if ( USERNAME_PROPERTY_NAME.equalsIgnoreCase( key ) ) {
            throw new IllegalArgumentException( "Invalid property '" + key + "': use principal instead." );
        }
        if ( PASSWORD_PROPERTY_NAME.equalsIgnoreCase( key ) ) {
            throw new IllegalArgumentException( "Invalid property '" + key + "': use credential instead." );
        }
    }

    // --- //

    private void validate() {
        if ( jdbcProperties.containsKey( USERNAME_PROPERTY_NAME ) ) {
            throw new IllegalArgumentException( "Invalid JDBC property '" + USERNAME_PROPERTY_NAME + "': use principal instead." );
        }
        if ( jdbcProperties.containsKey( PASSWORD_PROPERTY_NAME ) ) {
            throw new IllegalArgumentException( "Invalid property '" + PASSWORD_PROPERTY_NAME + "': use credential instead." );
        }
    }

    @Override
    public AgroalConnectionFactoryConfiguration get() {
        validate();
        this.lock = true;

        return new AgroalConnectionFactoryConfiguration() {

            @Override
            public boolean autoCommit() {
                return autoCommit;
            }

            @Override
            public String jdbcUrl() {
                return jdbcUrl;
            }

            @Override
            public String initialSql() {
                return initialSql;
            }

            @Override
            public String driverClassName() {
                return driverClassName;
            }

            @Override
            public ClassLoaderProvider classLoaderProvider() {
                return classLoaderProvider;
            }

            @Override
            public TransactionIsolation jdbcTransactionIsolation() {
                return transactionIsolation;
            }

            @Override
            public InterruptProtection interruptProtection() {
                return interruptProtection;
            }

            @Override
            public Principal principal() {
                return principal;
            }

            @Override
            public Collection<Object> credentials() {
                return credentials;
            }

            @Override
            public Properties jdbcProperties() {
                return jdbcProperties;
            }
        };
    }
}
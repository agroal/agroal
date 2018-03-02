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

import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.UNDEFINED;
import static io.agroal.api.configuration.ClassLoaderProvider.systemClassloader;
import static io.agroal.api.configuration.InterruptProtection.none;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalConnectionFactoryConfigurationSupplier implements Supplier<AgroalConnectionFactoryConfiguration> {

    private static final String USER_PROPERTY_NAME = "user";
    private static final String PASSWORD_PROPERTY_NAME = "password";

    private volatile boolean lock;

    private boolean autoCommit = false;
    private String jdbcUrl = "";
    private String initialSql = "";
    private Class<?> connectionProviderClass;
    @Deprecated
    private String driverClassName = "";
    @Deprecated
    private ClassLoaderProvider classLoaderProvider = systemClassloader();
    private TransactionIsolation transactionIsolation = UNDEFINED;
    @Deprecated
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
        this.connectionProviderClass = existingConfiguration.connectionProviderClass();
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

    public AgroalConnectionFactoryConfigurationSupplier autoCommit(boolean autoCommitEnabled) {
        return applySetting( c -> c.autoCommit = autoCommitEnabled );
    }

    public AgroalConnectionFactoryConfigurationSupplier jdbcUrl(String jdbcUrlString) {
        return applySetting( c -> c.jdbcUrl = jdbcUrlString );
    }

    public AgroalConnectionFactoryConfigurationSupplier initialSql(String initialSqlString) {
        return applySetting( c -> c.initialSql = initialSqlString );
    }

    public AgroalConnectionFactoryConfigurationSupplier connectionProviderClass(Class<?> connectionProvider) {
        return applySetting( c -> c.connectionProviderClass = connectionProvider );
    }

    public AgroalConnectionFactoryConfigurationSupplier connectionProviderClassName(String connectionProviderName) {
        try {
            Class<?> connectionProvider = connectionProviderName == null ? null : Class.forName( connectionProviderName );
            return applySetting( c -> c.connectionProviderClass = connectionProvider );
        } catch ( ClassNotFoundException e ) {
            throw new IllegalArgumentException( "Unable to load class " + connectionProviderName, e );
        }
    }

    @Deprecated
    public AgroalConnectionFactoryConfigurationSupplier driverClassName(String driverClass) {
        return applySetting( c -> c.driverClassName = driverClass );
    }

    @Deprecated
    public AgroalConnectionFactoryConfigurationSupplier classLoaderProvider(ClassLoaderProvider provider) {
        return applySetting( c -> c.classLoaderProvider = provider );
    }

    @Deprecated
    public AgroalConnectionFactoryConfigurationSupplier classLoader(ClassLoader classLoader) {
        return applySetting( c -> c.classLoaderProvider = className -> classLoader );
    }

    public AgroalConnectionFactoryConfigurationSupplier jdbcTransactionIsolation(TransactionIsolation transactionIsolationLevel) {
        return applySetting( c -> c.transactionIsolation = transactionIsolationLevel );
    }

    @Deprecated
    public AgroalConnectionFactoryConfigurationSupplier interruptHandlingMode(InterruptProtection interruptProtectionEnabled) {
        return applySetting( c -> c.interruptProtection = interruptProtectionEnabled );
    }

    public AgroalConnectionFactoryConfigurationSupplier principal(Principal loginPrincipal) {
        return applySetting( c -> c.principal = loginPrincipal );
    }

    public AgroalConnectionFactoryConfigurationSupplier credential(Object credential) {
        return applySetting( c -> c.credentials.add( credential ) );
    }

    public AgroalConnectionFactoryConfigurationSupplier jdbcProperty(String key, String value) {
        return applySetting( c -> c.jdbcProperties.put( key, value ) );
    }

    // --- //

    private void validate() {
        if ( jdbcProperties.containsKey( USER_PROPERTY_NAME ) ) {
            throw new IllegalArgumentException( "Invalid JDBC property '" + USER_PROPERTY_NAME + "': use principal instead." );
        }
        if ( jdbcProperties.containsKey( PASSWORD_PROPERTY_NAME ) ) {
            throw new IllegalArgumentException( "Invalid property '" + PASSWORD_PROPERTY_NAME + "': use credential instead." );
        }
    }

    @Override
    @SuppressWarnings( "ReturnOfInnerClass" )
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
            public Class<?> connectionProviderClass() {
                return connectionProviderClass;
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
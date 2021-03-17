// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.IsolationLevel;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;
import io.agroal.api.security.AgroalDefaultSecurityProvider;
import io.agroal.api.security.AgroalKerberosSecurityProvider;
import io.agroal.api.security.AgroalSecurityProvider;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Supplier;

import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.UNDEFINED;

/**
 * Builder of AgroalConnectionFactoryConfiguration.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@SuppressWarnings( {"PackageVisibleField", "WeakerAccess"} )
public class AgroalConnectionFactoryConfigurationSupplier implements Supplier<AgroalConnectionFactoryConfiguration> {

    private static final String USER_PROPERTY_NAME = "user";
    private static final String PASSWORD_PROPERTY_NAME = "password";

    boolean autoCommit = true;
    boolean trackJdbcResources = true;
    String jdbcUrl = "";
    String initialSql = "";
    Class<?> connectionProviderClass;
    IsolationLevel transactionIsolation = UNDEFINED;
    Collection<AgroalSecurityProvider> securityProviders = new ArrayList<>();
    Principal principal;
    Collection<Object> credentials = new ArrayList<>();
    Principal recoveryPrincipal;
    Collection<Object> recoveryCredentials = new ArrayList<>();
    Properties jdbcProperties = new Properties();

    private volatile boolean lock;

    public AgroalConnectionFactoryConfigurationSupplier() {
        lock = false;
        securityProviders.add( new AgroalDefaultSecurityProvider() );
        securityProviders.add( new AgroalKerberosSecurityProvider() );
    }

    public AgroalConnectionFactoryConfigurationSupplier(AgroalConnectionFactoryConfiguration existingConfiguration) {
        this();
        if ( existingConfiguration == null ) {
            return;
        }
        autoCommit = existingConfiguration.autoCommit();
        jdbcUrl = existingConfiguration.jdbcUrl();
        initialSql = existingConfiguration.initialSql();
        connectionProviderClass = existingConfiguration.connectionProviderClass();
        transactionIsolation = existingConfiguration.jdbcTransactionIsolation();
        principal = existingConfiguration.principal();
        credentials = existingConfiguration.credentials();
        recoveryPrincipal = existingConfiguration.recoveryPrincipal();
        recoveryCredentials = existingConfiguration.recoveryCredentials();
        jdbcProperties = existingConfiguration.jdbcProperties();
        securityProviders = existingConfiguration.securityProviders();
        trackJdbcResources = existingConfiguration.trackJdbcResources();
    }

    private void checkLock() {
        if ( lock ) {
            throw new IllegalStateException( "Attempt to modify an immutable configuration" );
        }
    }

    // --- //

    /**
     * Sets the value of auto-commit for connections on the pool. Default is true.
     */
    public AgroalConnectionFactoryConfigurationSupplier autoCommit(boolean autoCommitEnabled) {
        checkLock();
        autoCommit = autoCommitEnabled;
        return this;
    }

    /**
     * Sets if JBDC resources are tracked to be closed if leaked. Default is true.
     */
    public AgroalConnectionFactoryConfigurationSupplier trackJdbcResources(boolean trackJdbcResourcesEnabled) {
        checkLock();
        trackJdbcResources = trackJdbcResourcesEnabled;
        return this;
    }

    /**
     * Sets the database URL to connect to. Default is ""
     */
    public AgroalConnectionFactoryConfigurationSupplier jdbcUrl(String jdbcUrlString) {
        checkLock();
        jdbcUrl = jdbcUrlString;
        return this;
    }

    /**
     * Sets the SQL command to be executed when a connection is created.
     */
    public AgroalConnectionFactoryConfigurationSupplier initialSql(String initialSqlString) {
        checkLock();
        initialSql = initialSqlString;
        return this;
    }

    /**
     * Sets a class from the JDBC driver to be used as a supplier of connections.
     * Default is null, meaning the driver will be obtained from the URL (using the {@link java.sql.DriverManager#getDriver(String)} mechanism).
     */
    public AgroalConnectionFactoryConfigurationSupplier connectionProviderClass(Class<?> connectionProvider) {
        checkLock();
        connectionProviderClass = connectionProvider;
        return this;
    }

    /**
     * Attempts to load a JDBC driver class using its fully qualified name. The classloader used is the one of this class.
     * This method throws Exception if the class can't be loaded.
     */
    public AgroalConnectionFactoryConfigurationSupplier connectionProviderClassName(String connectionProviderName) {
        try {
            checkLock();
            connectionProviderClass = connectionProviderName == null ? null : Class.forName( connectionProviderName );
            return this;
        } catch ( ClassNotFoundException e ) {
            throw new IllegalArgumentException( "Unable to load class " + connectionProviderName, e );
        }
    }

    /**
     * Sets the transaction isolation level. Default is UNDEFINED, meaning that the default isolation level for the JDBC driver is used.
     */
    public AgroalConnectionFactoryConfigurationSupplier jdbcTransactionIsolation(TransactionIsolation transactionIsolationLevel) {
        checkLock();
        transactionIsolation = transactionIsolationLevel;
        return this;
    }

    /**
     * Allows setting a custom transaction isolation level.
     */
    public AgroalConnectionFactoryConfigurationSupplier jdbcTransactionIsolation(int customValue) {
        checkLock();
        transactionIsolation = new CustomIsolationLevel( customValue );
        return this;
    }

    /**
     * Allows setting additional {@link AgroalSecurityProvider} to deal with custom principal/credential types.
     * Default is to have {@link AgroalDefaultSecurityProvider} and {@link AgroalKerberosSecurityProvider} available by default.
     */
    public AgroalConnectionFactoryConfigurationSupplier addSecurityProvider(AgroalSecurityProvider provider) {
        checkLock();
        securityProviders.add( provider );
        return this;
    }

    /**
     * Sets the principal to be authenticated in the database. Default is to don't perform authentication.
     */
    public AgroalConnectionFactoryConfigurationSupplier principal(Principal loginPrincipal) {
        checkLock();
        principal = loginPrincipal;
        return this;
    }

    /**
     * Sets credentials to use in order to authenticate to the database. Default is to don't provide any credentials.
     */
    public AgroalConnectionFactoryConfigurationSupplier credential(Object credential) {
        checkLock();
        credentials.add( credential );
        return this;
    }

    /**
     * Allows setting a different principal for recovery connections.
     */
    public AgroalConnectionFactoryConfigurationSupplier recoveryPrincipal(Principal loginPrincipal) {
        checkLock();
        recoveryPrincipal = loginPrincipal;
        return this;
    }

    /**
     * Allows providing a different set of credentials for recovery connections.
     */
    public AgroalConnectionFactoryConfigurationSupplier recoveryCredential(Object credential) {
        checkLock();
        recoveryCredentials.add( credential );
        return this;
    }

    /**
     * Allows setting other, unspecified, properties to be passed to the JDBC driver when creating new connections.
     * NOTE: username and password properties are not allowed, these have to be set using the principal / credentials mechanism.
     */
    public AgroalConnectionFactoryConfigurationSupplier jdbcProperty(String key, String value) {
        checkLock();
        jdbcProperties.setProperty( key, value );
        return this;
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
        lock = true;

        return new AgroalConnectionFactoryConfiguration() {

            @Override
            public boolean autoCommit() {
                return autoCommit;
            }

            @Override
            public boolean trackJdbcResources() {
                return trackJdbcResources;
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
            public IsolationLevel jdbcTransactionIsolation() {
                return transactionIsolation;
            }

            @Override
            public Collection<AgroalSecurityProvider> securityProviders() {
                return securityProviders;
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
            public Principal recoveryPrincipal() {
                return recoveryPrincipal;
            }

            @Override
            public Collection<Object> recoveryCredentials() {
                return recoveryCredentials;
            }

            @Override
            public Properties jdbcProperties() {
                return jdbcProperties;
            }
        };
    }

    // --- //

    private static class CustomIsolationLevel implements IsolationLevel {

        private final int customValue;

        public CustomIsolationLevel(int customValue) {
            this.customValue = customValue;
        }

        @Override
        public boolean isDefined() {
            return true;
        }

        @Override
        public int level() {
            return customValue;
        }

        @Override
        public String toString() {
            return "CUSTOM";
        }
    }
}

// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.IsolationLevel;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Supplier;

import static io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation.UNDEFINED;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalConnectionFactoryConfigurationSupplier implements Supplier<AgroalConnectionFactoryConfiguration> {

    private static final String USER_PROPERTY_NAME = "user";
    private static final String PASSWORD_PROPERTY_NAME = "password";

    private volatile boolean lock;

    private boolean autoCommit = true;
    private String jdbcUrl = "";
    private String initialSql = "";
    private Class<?> connectionProviderClass;
    private IsolationLevel transactionIsolation = UNDEFINED;
    private Principal principal;
    private Collection<Object> credentials = new ArrayList<>();
    private Principal recoveryPrincipal;
    private Collection<Object> recoveryCredentials = new ArrayList<>();
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
        this.transactionIsolation = existingConfiguration.jdbcTransactionIsolation();
        this.principal = existingConfiguration.principal();
        this.credentials = existingConfiguration.credentials();
        this.recoveryPrincipal = existingConfiguration.recoveryPrincipal();
        this.recoveryCredentials = existingConfiguration.recoveryCredentials();
        this.jdbcProperties = existingConfiguration.jdbcProperties();
    }

    private void checkLock() {
        if ( lock ) {
            throw new IllegalStateException( "Attempt to modify an immutable configuration" );
        }
    }

    // --- //

    public AgroalConnectionFactoryConfigurationSupplier autoCommit(boolean autoCommitEnabled) {
        checkLock();
        autoCommit = autoCommitEnabled;
        return this;
    }

    public AgroalConnectionFactoryConfigurationSupplier jdbcUrl(String jdbcUrlString) {
        checkLock();
        jdbcUrl = jdbcUrlString;
        return this;
    }

    public AgroalConnectionFactoryConfigurationSupplier initialSql(String initialSqlString) {
        checkLock();
        initialSql = initialSqlString;
        return this;
    }

    public AgroalConnectionFactoryConfigurationSupplier connectionProviderClass(Class<?> connectionProvider) {
        checkLock();
        connectionProviderClass = connectionProvider;
        return this;
    }

    public AgroalConnectionFactoryConfigurationSupplier connectionProviderClassName(String connectionProviderName) {
        try {
            checkLock();
            connectionProviderClass = connectionProviderName == null ? null : Class.forName( connectionProviderName );
            return this;
        } catch ( ClassNotFoundException e ) {
            throw new IllegalArgumentException( "Unable to load class " + connectionProviderName, e );
        }
    }

    public AgroalConnectionFactoryConfigurationSupplier jdbcTransactionIsolation(TransactionIsolation transactionIsolationLevel) {
        checkLock();
        transactionIsolation = transactionIsolationLevel;
        return this;
    }

    public AgroalConnectionFactoryConfigurationSupplier jdbcTransactionIsolation(int customValue) {
        checkLock();
        transactionIsolation = new CustomIsolationLevel( customValue );
        return this;
    }

    public AgroalConnectionFactoryConfigurationSupplier principal(Principal loginPrincipal) {
        checkLock();
        principal = loginPrincipal;
        return this;
    }

    public AgroalConnectionFactoryConfigurationSupplier credential(Object credential) {
        checkLock();
        credentials.add( credential );
        return this;
    }

    public AgroalConnectionFactoryConfigurationSupplier recoveryPrincipal(Principal loginPrincipal) {
        checkLock();
        recoveryPrincipal = loginPrincipal;
        return this;
    }

    public AgroalConnectionFactoryConfigurationSupplier recoveryCredential(Object credential) {
        checkLock();
        recoveryCredentials.add( credential );
        return this;
    }

    public AgroalConnectionFactoryConfigurationSupplier jdbcProperty(String key, String value) {
        checkLock();
        jdbcProperties.put( key, value );
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
            public IsolationLevel jdbcTransactionIsolation() {
                return transactionIsolation;
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

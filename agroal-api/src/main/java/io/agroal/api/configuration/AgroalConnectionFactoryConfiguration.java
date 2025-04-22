// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration;

import io.agroal.api.security.AgroalSecurityProvider;

import java.security.Principal;
import java.sql.Connection;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

/**
 * The configuration of the connection factory.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalConnectionFactoryConfiguration {

    /**
     * If connections should have the auto-commit mode on by default. The transaction integration may disable auto-commit when a connection in enrolled in a transaction.
     */
    boolean autoCommit();

    /**
     * If pooled connections are kept in read-only state, according to {@link java.sql.Connection#setReadOnly(boolean)}}. The JDBC driver is responsible for enforcing.
     */
    boolean readOnly();

    /**
     * If JDBC resources ({@link java.sql.Statement} and {@link java.sql.ResultSet}) should be tracked to be closed if leaked.
     */
    boolean trackJdbcResources();

    /**
     * Maximum time to wait while attempting to connect to a database. Resolution in seconds.
     */
    Duration loginTimeout();

    /**
     * The database URL to connect to.
     */
    String jdbcUrl();

    /**
     * A SQL command to be executed when a connection is created.
     */
    String initialSql();

    /**
     * JDBC driver class to use as a supplier of connections. Must be an implementation of {@link java.sql.Driver}, {@link javax.sql.DataSource} or {@link javax.sql.XADataSource}.
     * Can be null, in which case the driver will be obtained from the URL (using the {@link java.sql.DriverManager#getDriver(String)} mechanism).
     */
    Class<?> connectionProviderClass();

    /**
     * The isolation level between database transactions.
     */
    IsolationLevel jdbcTransactionIsolation();

    /**
     * A collection of providers that are capable of handling principal / credential objects
     */
    Collection<AgroalSecurityProvider> securityProviders();

    /**
     * Entity to be authenticated in the database.
     */
    Principal principal();

    /**
     * Collection of evidence used for authentication.
     */
    Collection<Object> credentials();

    /**
     * Retrieve recovery connections from the pool.
     */
    boolean poolRecovery();

    /**
     * Entity to be authenticated in the database for recovery connections. If not set, the principal will be used.
     */
    Principal recoveryPrincipal();

    /**
     * Collection of evidence used for authentication for recovery connections. If not set, the credentials will be used.
     */
    Collection<Object> recoveryCredentials();

    /**
     * Other unspecified properties to be passed into the JDBC driver when creating new connections.
     * NOTE: username and password properties are not allowed, these have to be set using the principal / credentials mechanism.
     */
    Properties jdbcProperties();

    /**
     * Override of JDBC properties used for XA drivers. If left empty, the regular JDBC properties are used for both XA and non-XA.
     */
    Properties xaProperties();

    // --- //

    /**
     * The default transaction isolation levels, defined in {@link Connection}.
     */
    enum TransactionIsolation implements IsolationLevel {
        UNDEFINED, NONE, READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE;

        public static TransactionIsolation fromLevel(int level) {
            switch ( level ) {
                case Connection.TRANSACTION_NONE:
                    return NONE;
                case Connection.TRANSACTION_READ_UNCOMMITTED:
                    return READ_UNCOMMITTED;
                case Connection.TRANSACTION_READ_COMMITTED:
                    return READ_COMMITTED;
                case Connection.TRANSACTION_REPEATABLE_READ:
                    return REPEATABLE_READ;
                case Connection.TRANSACTION_SERIALIZABLE:
                    return SERIALIZABLE;
                default:
                    return UNDEFINED;
            }
        }

        public int level() {
            switch ( this ) {
                case READ_UNCOMMITTED:
                    return Connection.TRANSACTION_READ_UNCOMMITTED;
                case READ_COMMITTED:
                    return Connection.TRANSACTION_READ_COMMITTED;
                case REPEATABLE_READ:
                    return Connection.TRANSACTION_REPEATABLE_READ;
                case SERIALIZABLE:
                    return Connection.TRANSACTION_SERIALIZABLE;
                case NONE:
                    return Connection.TRANSACTION_NONE;
                default:
                    return -1;
            }
        }

        public boolean isDefined() {
            return this != UNDEFINED;
        }
    }

    /**
     * Interface to define the transaction isolation level.
     */
    interface IsolationLevel {

        /**
         * If a level is not defined it will not be set by the pool (it will use the JDBC driver default).
         */
        boolean isDefined();

        /**
         * The value for transaction isolation level.
         */
        int level();
    }
}

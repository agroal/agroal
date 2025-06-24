// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration;

import io.agroal.api.cache.ConnectionCache;
import io.agroal.api.transaction.TransactionIntegration;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

/**
 * The configuration of the connection pool.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalConnectionPoolConfiguration {

    /**
     * Configuration for the connection factory.
     */
    AgroalConnectionFactoryConfiguration connectionFactoryConfiguration();

    /**
     * The connection validation method. Allows customization of the validation operation.
     */
    ConnectionValidator connectionValidator();

    /**
     * Allows a custom exception sorter. This determines if a connection is still usable after an exception.
     */
    ExceptionSorter exceptionSorter();

    /**
     * Customizable strategy for connection caching.
     */
    ConnectionCache connectionCache();

    /**
     * The transaction layer integration to use.
     */
    TransactionIntegration transactionIntegration();

    /**
     * Requires connections to be enlisted into a transaction.
     */
    TransactionRequirement transactionRequirement();

    /**
     * Validates connections before being acquired (foreground validation) overriding any idle timeout.
     * Because of the overhead of performing validation on every call, it's recommended to rely on idle valdation instead.
     */
    boolean validateOnBorrow();

    /**
     * Connections idle for longer than this time period are validated before being acquired (foreground validation).
     * A duration of {@link Duration#ZERO} means that this feature is disabled.
     */
    Duration idleValidationTimeout();

    /**
     * Connections acquired for longer than this time period may be reported as leaking.
     * A duration of {@link Duration#ZERO} means that this feature is disabled.
     */
    Duration leakTimeout();

    /**
     * Connections idle for longer than this time period are validated (background validation).
     * A duration of {@link Duration#ZERO} means that this feature is disabled.
     */
    Duration validationTimeout();

    /**
     * Connections idle for longer than this time period are flushed from the pool.
     * A duration of {@link Duration#ZERO} means that this feature is disabled.
     */
    Duration reapTimeout();

    /**
     * Connections that are older than this time period are flushed from the pool.
     * A duration of {@link Duration#ZERO} means that this feature is disabled.
     */
    Duration maxLifetime();

    /**
     * Provides detailed insights of the connection status when it's reported as a leak (as INFO messages on AgroalDataSourceListener).
     */
    boolean enhancedLeakReport();

    /**
     * If connections should be flushed when returning to the pool.
     */
    boolean flushOnClose();

    /**
     * If connections from this pool should be used during recovery. For XA pools it is vital that recovery is enabled
     * on at least one connection pool to the same database and therefore the default is true.
     * <p>
     * Normally a transaction manager will call xa_recover () on a connection during recovery to obtain a list of
     * transaction branches that are currently in a prepared or heuristically completed state. However, it can
     * happen that multiple XA connections connect to the same database which would all return the same set of branches
     * and for performance reasons only one should be used for recover() calls.
     * <p>
     * @return true if enabled
     */
    default boolean recoveryEnable() {
        return true;
    }

    /**
     * Behaviour when a thread tries to acquire multiple connections.
     */
    MultipleAcquisitionAction multipleAcquisition();

    /**
     * The number of connections to be created when the pool starts. Can be smaller than min or bigger than max.
     */
    int initialSize();

    // --- Mutable attributes //

    /**
     * The minimum number of connections on the pool. If the pool has to flush connections it may create connections to keep this amount.
     */
    int minSize();

    /**
     * Sets a new minimum number of connections on the pool. When this value increase the pool may temporarily have less connections than the minimum.
     */
    void setMinSize(int size);

    /**
     * The maximum number of connections on the pool. When the number of acquired connections is equal to this value, further requests will block.
     */
    int maxSize();

    /**
     * Sets a new maximum number of connections on the pool. When this value decreases the pool may temporarily have more connections than the maximum.
     */
    void setMaxSize(int size);

    /**
     * The maximum amount of time a thread may be blocked waiting for a connection. If this time expires and still no connection is available, an exception is thrown.
     * A duration of {@link Duration#ZERO} means that a thread will wait indefinitely.
     * In Pool-less this timeout can add to {@link AgroalConnectionFactoryConfiguration#loginTimeout()}.
     */
    Duration acquisitionTimeout();

    /**
     * Sets a new amount of time a thread may be blocked. Threads already blocked when this value changes do not see the new value when they unblock.
     * A duration of {@link Duration#ZERO} means that a thread will wait indefinitely.
     */
    void setAcquisitionTimeout(Duration timeout);

    /**
     * When set to {@code false}, Agroal will not be updating essential pool usage stats such as active count, available count, max used
     * count. Those stats will stay at zero and should not be relied on.
     * Disabling these stats is generally not recommended, but could be useful when these stats
     * are not being used, for example when these are duplicating other means to get such details.
     *
     * @return {@code true} unless overriden
     */
    default boolean poolStatisticsEnabled() {
        return true;
    };

    // --- //

    /**
     * Modes available for transaction requirement.
     */
    enum TransactionRequirement {
        /**
         * Enlistment not required.
         */
        OFF,
        /**
         * Warn if not enlisted.
         */
        WARN,
        /**
         * Throw exception if not enlisted.
         */
        STRICT
    }

    /**
     * Action to perform on acquisition of multiple connections by the same thread.
     */
    enum MultipleAcquisitionAction {
        /**
         * No restriction.
         */
        OFF,
        /**
         * Warn if thread already holds a connection.
         */
        WARN,
        /**
         * Enforces single connection by throwing exception.
         */
        STRICT
    }

    // --- //

    /**
     * Interface for custom connection validation strategies.
     */
    interface ConnectionValidator {

        /**
         * The default validation strategy {@link Connection#isValid(int)}
         */
        static ConnectionValidator defaultValidator() {
            return new ConnectionValidator() {
                @Override
                public boolean isValid(Connection connection) {
                    try {
                        return connection.isValid( 0 );
                    } catch ( Exception t ) {
                        return false;
                    }
                }
            };
        }

        /**
         * The default validation strategy with a timeout (in seconds).
         * If the timeout period expires before the operation completes, the connection is invalidated.
         */
        static ConnectionValidator defaultValidatorWithTimeout(int timeout) {
            return new ConnectionValidator() {
                @Override
                public boolean isValid(Connection connection) {
                    try {
                        return connection.isValid( timeout );
                    } catch ( Exception t ) {
                        return false;
                    }
                }
            };
        }

        /**
         * A validator that never invalidates connections.
         */
        static ConnectionValidator emptyValidator() {
            return new ConnectionValidator() {
                @Override
                public boolean isValid(Connection connection) {
                    return true;
                }
            };
        }

        // --- //

        /**
         * @return true if a connection is valid, false otherwise
         */
        boolean isValid(Connection connection);
    }

    // --- //

    /**
     * Interface for custom exception sorter strategies. Determines if a connection is still usable after an exception occurs.
     */
    interface ExceptionSorter {

        /**
         * Default exception sorter. Does not treat any exception as fatal.
         */
        static ExceptionSorter defaultExceptionSorter() {
            return new ExceptionSorter() {
                @Override
                public boolean isFatal(SQLException se) {
                    return false;
                }
            };
        }

        /**
         * Never treat an exception as fatal.
         */
        static ExceptionSorter emptyExceptionSorter() {
            return new ExceptionSorter() {
                @Override
                public boolean isFatal(SQLException se) {
                    return false;
                }
            };
        }

        /**
         * Treats every exception as fatal.
         */
        static ExceptionSorter fatalExceptionSorter() {
            return new ExceptionSorter() {
                @Override
                public boolean isFatal(SQLException se) {
                    return true;
                }
            };
        }

        // --- //

        /**
         * @return true if an exception is considered fatal and the connection is not able for further use, false otherwise
         */
        boolean isFatal(SQLException se);
    }
}

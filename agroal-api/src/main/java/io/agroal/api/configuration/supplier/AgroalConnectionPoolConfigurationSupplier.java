// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.cache.ConnectionCache;
import io.agroal.api.cache.LocalConnectionCache;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.MultipleAcquisitionAction;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.TransactionRequirement;
import io.agroal.api.transaction.TransactionIntegration;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ConnectionValidator.emptyValidator;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter.emptyExceptionSorter;
import static io.agroal.api.transaction.TransactionIntegration.none;
import static java.lang.Integer.MAX_VALUE;
import static java.time.Duration.ZERO;

/**
 * Builder of AgroalConnectionPoolConfiguration.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@SuppressWarnings( {"PackageVisibleField", "WeakerAccess"} )
public class AgroalConnectionPoolConfigurationSupplier implements Supplier<AgroalConnectionPoolConfiguration> {

    AgroalConnectionFactoryConfiguration connectionFactoryConfiguration;

    ConnectionCache connectionCache = LocalConnectionCache.single();
    TransactionIntegration transactionIntegration = none();
    TransactionRequirement transactionRequirement = TransactionRequirement.OFF;
    MultipleAcquisitionAction multipleAcquisitionAction = MultipleAcquisitionAction.OFF;
    boolean enhancedLeakReport;
    boolean flushOnClose;
    int initialSize;
    volatile int minSize;
    volatile int maxSize = MAX_VALUE;
    AgroalConnectionPoolConfiguration.ConnectionValidator connectionValidator = emptyValidator();
    AgroalConnectionPoolConfiguration.ExceptionSorter exceptionSorter = emptyExceptionSorter();
    Duration idleValidationTimeout = ZERO;
    Duration leakTimeout = ZERO;
    Duration validationTimeout = ZERO;
    Duration reapTimeout = ZERO;
    Duration maxLifetime = ZERO;
    volatile Duration acquisitionTimeout = ZERO;

    private volatile boolean lock;

    private AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfigurationSupplier = new AgroalConnectionFactoryConfigurationSupplier();

    public AgroalConnectionPoolConfigurationSupplier() {
        lock = false;
    }

    public AgroalConnectionPoolConfigurationSupplier(AgroalConnectionPoolConfiguration existingConfiguration) {
        this();
        if ( existingConfiguration == null ) {
            return;
        }
        connectionFactoryConfigurationSupplier = new AgroalConnectionFactoryConfigurationSupplier( existingConfiguration.connectionFactoryConfiguration() );
        connectionCache = existingConfiguration.connectionCache();
        transactionIntegration = existingConfiguration.transactionIntegration();
        transactionRequirement = existingConfiguration.transactionRequirement();
        multipleAcquisitionAction = existingConfiguration.multipleAcquisition();
        flushOnClose = existingConfiguration.flushOnClose();
        enhancedLeakReport = existingConfiguration.enhancedLeakReport();
        initialSize = existingConfiguration.initialSize();
        minSize = existingConfiguration.minSize();
        maxSize = existingConfiguration.maxSize();
        connectionValidator = existingConfiguration.connectionValidator();
        exceptionSorter = existingConfiguration.exceptionSorter();
        idleValidationTimeout = existingConfiguration.idleValidationTimeout();
        leakTimeout = existingConfiguration.leakTimeout();
        validationTimeout = existingConfiguration.validationTimeout();
        reapTimeout = existingConfiguration.reapTimeout();
        maxLifetime = existingConfiguration.maxLifetime();
        acquisitionTimeout = existingConfiguration.acquisitionTimeout();
    }

    private void checkLock() {
        if ( lock ) {
            throw new IllegalStateException( "Attempt to modify an immutable configuration" );
        }
    }

    /**
     * Sets the configuration for the connection factory.
     */
    private AgroalConnectionPoolConfigurationSupplier connectionFactoryConfiguration(AgroalConnectionFactoryConfiguration configuration) {
        checkLock();
        connectionFactoryConfigurationSupplier = new AgroalConnectionFactoryConfigurationSupplier( configuration );
        return this;
    }

    /**
     * Sets the configuration for the connection factory.
     */
    public AgroalConnectionPoolConfigurationSupplier connectionFactoryConfiguration(Supplier<? extends AgroalConnectionFactoryConfiguration> supplier) {
        return connectionFactoryConfiguration( supplier.get() );
    }

    /**
     * Modifies the configuration of the connection pool.
     */
    public AgroalConnectionPoolConfigurationSupplier connectionFactoryConfiguration(Function<? super AgroalConnectionFactoryConfigurationSupplier, ? extends AgroalConnectionFactoryConfigurationSupplier> function) {
        return connectionFactoryConfiguration( function.apply( connectionFactoryConfigurationSupplier ) );
    }

    /**
     * Allows access to the configuration builder for the connection pool.
     */
    public AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfiguration() {
        return connectionFactoryConfigurationSupplier;
    }

    // --- //

    /**
     * Sets the connection cache implementation. Default is {@link LocalConnectionCache#single}.
     */
    public AgroalConnectionPoolConfigurationSupplier connectionCache(ConnectionCache cache) {
        checkLock();
        connectionCache = cache;
        return this;
    }

    /**
     * Sets the transaction integration instance to use. Default is {@link TransactionIntegration#none()}.
     */
    public AgroalConnectionPoolConfigurationSupplier transactionIntegration(TransactionIntegration integration) {
        checkLock();
        transactionIntegration = integration;
        return this;
    }

    /**
     * Sets the transaction requirements for the pool. Default is {@link TransactionRequirement#OFF}.
     */
    public AgroalConnectionPoolConfigurationSupplier transactionRequirement(TransactionRequirement requirement) {
        checkLock();
        transactionRequirement = requirement;
        return this;
    }

    /**
     * Enables enhanced leak report.
     */
    public AgroalConnectionPoolConfigurationSupplier enhancedLeakReport() {
        return enhancedLeakReport( true );
    }

    /**
     * Sets the behaviour of the pool when a thread tries to acquire multiple connections. Default is {@link MultipleAcquisitionAction#OFF}
     */
    public AgroalConnectionPoolConfigurationSupplier multipleAcquisition(MultipleAcquisitionAction action) {
        checkLock();
        multipleAcquisitionAction = action;
        return this;
    }

    /**
     * Enables or disables enhanced leak report. Default is false.
     */
    public AgroalConnectionPoolConfigurationSupplier enhancedLeakReport(boolean enhanced) {
        checkLock();
        enhancedLeakReport = enhanced;
        return this;
    }

    /**
     * Enables flushing of connections on close.
     */
    public AgroalConnectionPoolConfigurationSupplier flushOnClose() {
        return flushOnClose( true );
    }

    /**
     * Enables or disables flushing of connections on close. Default is false.
     */
    public AgroalConnectionPoolConfigurationSupplier flushOnClose(boolean flush) {
        checkLock();
        flushOnClose = flush;
        return this;
    }

    /**
     * Sets the number of connections when the pool starts. Must not be negative. Default is zero.
     */
    public AgroalConnectionPoolConfigurationSupplier initialSize(int size) {
        checkLock();
        initialSize = size;
        return this;
    }

    /**
     * Sets the minimum number of connections on the pool. Must not be negative and smaller than max. Default is zero.
     */
    public AgroalConnectionPoolConfigurationSupplier minSize(int size) {
        checkLock();
        minSize = size;
        return this;
    }

    /**
     * Sets the maximum number of connections on the pool. Must not be negative. Required.
     */
    public AgroalConnectionPoolConfigurationSupplier maxSize(int size) {
        checkLock();
        maxSize = size;
        return this;
    }

    /**
     * Sets the connection validation method. Default {@link AgroalConnectionPoolConfiguration.ConnectionValidator#emptyValidator()}
     */
    public AgroalConnectionPoolConfigurationSupplier connectionValidator(AgroalConnectionPoolConfiguration.ConnectionValidator validator) {
        checkLock();
        connectionValidator = validator;
        return this;
    }

    /**
     * Sets the exception sorter. Default {@link AgroalConnectionPoolConfiguration.ExceptionSorter#emptyExceptionSorter()}
     */
    public AgroalConnectionPoolConfigurationSupplier exceptionSorter(AgroalConnectionPoolConfiguration.ExceptionSorter sorter) {
        checkLock();
        exceptionSorter = sorter;
        return this;
    }

    /**
     * Sets the duration of the acquisition timeout. Default is {@link Duration#ZERO} meaning that a thread will wait indefinitely.
     */
    public AgroalConnectionPoolConfigurationSupplier acquisitionTimeout(Duration timeout) {
        checkLock();
        acquisitionTimeout = timeout;
        return this;
    }

    /**
     * Sets the duration of idle time for foreground validation to be executed. Default is {@link Duration#ZERO} meaning that this feature is disabled.
     */
    public AgroalConnectionPoolConfigurationSupplier idleValidationTimeout(Duration timeout) {
        checkLock();
        idleValidationTimeout = timeout;
        return this;
    }

    /**
     * Sets the duration of the leak timeout detection. Default is {@link Duration#ZERO} meaning that this feature is disabled.
     */
    public AgroalConnectionPoolConfigurationSupplier leakTimeout(Duration timeout) {
        checkLock();
        leakTimeout = timeout;
        return this;
    }

    /**
     * Sets the duration of background validation interval. Default is {@link Duration#ZERO} meaning that this feature is disabled.
     */
    public AgroalConnectionPoolConfigurationSupplier validationTimeout(Duration timeout) {
        checkLock();
        validationTimeout = timeout;
        return this;
    }

    /**
     * Sets the duration for eviction of idle connections. Default is {@link Duration#ZERO} meaning that this feature is disabled.
     */
    public AgroalConnectionPoolConfigurationSupplier reapTimeout(Duration timeout) {
        checkLock();
        reapTimeout = timeout;
        return this;
    }

    /**
     * Sets the duration for the lifetime of connections. Default is {@link Duration#ZERO} meaning that this feature is disabled.
     */
    public AgroalConnectionPoolConfigurationSupplier maxLifetime(Duration time) {
        checkLock();
        maxLifetime = time;
        return this;
    }

    // --- //

    private void validate() {
        if ( maxSize == MAX_VALUE ) {
            throw new IllegalArgumentException( "max size attribute is mandatory" );
        }
        if ( maxSize <= 0 ) {
            throw new IllegalArgumentException( "A Positive max size is required" );
        }
        if ( minSize < 0 ) {
            throw new IllegalArgumentException( "Invalid min size: smaller than 0" );
        }
        if ( minSize > maxSize ) {
            throw new IllegalArgumentException( "Invalid min size: greater than max size" );
        }
        if ( initialSize < 0 ) {
            throw new IllegalArgumentException( "Invalid value for initial size. Must not be negative, and ideally between min size and max size" );
        }
        if ( acquisitionTimeout.isNegative() ) {
            throw new IllegalArgumentException( "Acquisition timeout must not be negative" );
        }
        if ( idleValidationTimeout.isNegative() ) {
            throw new IllegalArgumentException( "Idle validation timeout must not be negative" );
        }
        if ( leakTimeout.isNegative() ) {
            throw new IllegalArgumentException( "Leak detection timeout must not be negative" );
        }
        if ( reapTimeout.isNegative() ) {
            throw new IllegalArgumentException( "Reap timeout must not be negative" );
        }
        if ( maxLifetime.isNegative() ) {
            throw new IllegalArgumentException( "Max Lifetime must not be negative" );
        }
        if ( validationTimeout.isNegative() ) {
            throw new IllegalArgumentException( "Validation timeout must not be negative" );
        }
        if ( connectionFactoryConfigurationSupplier == null ) {
            throw new IllegalArgumentException( "Connection factory configuration not defined" );
        }
        connectionFactoryConfiguration = connectionFactoryConfigurationSupplier.get();
    }

    @Override
    @SuppressWarnings( "ReturnOfInnerClass" )
    public AgroalConnectionPoolConfiguration get() {
        validate();
        lock = true;

        return new AgroalConnectionPoolConfiguration() {

            @Override
            public AgroalConnectionFactoryConfiguration connectionFactoryConfiguration() {
                return connectionFactoryConfiguration;
            }

            @Override
            public ConnectionCache connectionCache() {
                return connectionCache;
            }

            @Override
            public TransactionIntegration transactionIntegration() {
                return transactionIntegration;
            }

            @Override
            public TransactionRequirement transactionRequirement() {
                return transactionRequirement;
            }

            @Override
            public boolean enhancedLeakReport() {
                return enhancedLeakReport;
            }

            @Override
            public boolean flushOnClose() {
                return flushOnClose;
            }

            @Override
            public MultipleAcquisitionAction multipleAcquisition() {
                return multipleAcquisitionAction;
            }

            @Override
            public int initialSize() {
                return initialSize;
            }

            @Override
            public int minSize() {
                return minSize;
            }

            @Override
            public void setMinSize(int size) {
                if ( size < 0 ) {
                    throw new IllegalArgumentException( "Invalid min size: smaller than 0" );
                }
                if ( size > maxSize ) {
                    throw new IllegalArgumentException( "Invalid min size: greater than max size" );
                }
                minSize = size;
            }

            @Override
            public int maxSize() {
                return maxSize;
            }

            @Override
            public void setMaxSize(int size) {
                if ( size <= 0 ) {
                    throw new IllegalArgumentException( "A Positive max size is required" );
                }
                if ( size < minSize ) {
                    throw new IllegalArgumentException( "Invalid max size: smaller than min size" );
                }
                maxSize = size;
            }

            @Override
            public Duration acquisitionTimeout() {
                return acquisitionTimeout;
            }

            @Override
            public void setAcquisitionTimeout(Duration timeout) {
                if ( timeout.isNegative() ) {
                    throw new IllegalArgumentException( "Acquisition timeout must not be negative" );
                }
                acquisitionTimeout = timeout;
            }

            @Override
            public ConnectionValidator connectionValidator() {
                return connectionValidator;
            }

            @Override
            public ExceptionSorter exceptionSorter() {
                return exceptionSorter;
            }

            @Override
            public Duration idleValidationTimeout() {
                return idleValidationTimeout;
            }

            @Override
            public Duration leakTimeout() {
                return leakTimeout;
            }

            @Override
            public Duration validationTimeout() {
                return validationTimeout;
            }

            @Override
            public Duration reapTimeout() {
                return reapTimeout;
            }

            @Override
            public Duration maxLifetime() {
                return maxLifetime;
            }
        };
    }
}

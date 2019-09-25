// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
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
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalConnectionPoolConfigurationSupplier implements Supplier<AgroalConnectionPoolConfiguration> {

    private volatile boolean lock;

    private AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfigurationSupplier = new AgroalConnectionFactoryConfigurationSupplier();
    private AgroalConnectionFactoryConfiguration connectionFactoryConfiguration = null;

    private TransactionIntegration transactionIntegration = none();
    private boolean flushOnClose = false;
    private int initialSize = 0;
    private volatile int minSize = 0;
    private volatile int maxSize = MAX_VALUE;
    private AgroalConnectionPoolConfiguration.ConnectionValidator connectionValidator = emptyValidator();
    private AgroalConnectionPoolConfiguration.ExceptionSorter exceptionSorter = emptyExceptionSorter();
    private Duration idleValidationTimeout = ZERO;
    private Duration leakTimeout = ZERO;
    private Duration validationTimeout = ZERO;
    private Duration reapTimeout = ZERO;
    private Duration maxLifetime = ZERO;
    private volatile Duration acquisitionTimeout = ZERO;

    public AgroalConnectionPoolConfigurationSupplier() {
        this.lock = false;
    }

    public AgroalConnectionPoolConfigurationSupplier(AgroalConnectionPoolConfiguration existingConfiguration) {
        this();
        if ( existingConfiguration == null ) {
            return;
        }
        this.connectionFactoryConfigurationSupplier = new AgroalConnectionFactoryConfigurationSupplier( existingConfiguration.connectionFactoryConfiguration() );
        this.transactionIntegration = existingConfiguration.transactionIntegration();
        this.flushOnClose = existingConfiguration.flushOnClose();
        this.initialSize = existingConfiguration.initialSize();
        this.minSize = existingConfiguration.minSize();
        this.maxSize = existingConfiguration.maxSize();
        this.connectionValidator = existingConfiguration.connectionValidator();
        this.exceptionSorter = existingConfiguration.exceptionSorter();
        this.idleValidationTimeout = existingConfiguration.idleValidationTimeout();
        this.leakTimeout = existingConfiguration.leakTimeout();
        this.validationTimeout = existingConfiguration.validationTimeout();
        this.reapTimeout = existingConfiguration.reapTimeout();
        this.maxLifetime = existingConfiguration.maxLifetime();
        this.acquisitionTimeout = existingConfiguration.acquisitionTimeout();
    }

    private void checkLock() {
        if ( lock ) {
            throw new IllegalStateException( "Attempt to modify an immutable configuration" );
        }
    }

    private AgroalConnectionPoolConfigurationSupplier connectionFactoryConfiguration(AgroalConnectionFactoryConfiguration configuration) {
        checkLock();
        connectionFactoryConfigurationSupplier = new AgroalConnectionFactoryConfigurationSupplier( configuration );
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier connectionFactoryConfiguration(Supplier<AgroalConnectionFactoryConfiguration> supplier) {
        return connectionFactoryConfiguration( supplier.get() );
    }

    public AgroalConnectionPoolConfigurationSupplier connectionFactoryConfiguration(Function<AgroalConnectionFactoryConfigurationSupplier, AgroalConnectionFactoryConfigurationSupplier> function) {
        return connectionFactoryConfiguration( function.apply( connectionFactoryConfigurationSupplier ) );
    }

    public AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfiguration() {
        return connectionFactoryConfigurationSupplier;
    }

    // --- //

    public AgroalConnectionPoolConfigurationSupplier transactionIntegration(TransactionIntegration integration) {
        checkLock();
        transactionIntegration = integration;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier flushOnClose() {
        return flushOnClose( true );
    }
    
    public AgroalConnectionPoolConfigurationSupplier flushOnClose(boolean flush) {
        checkLock();
        flushOnClose = flush;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier initialSize(int size) {
        checkLock();
        initialSize = size;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier minSize(int size) {
        checkLock();
        minSize = size;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier maxSize(int size) {
        checkLock();
        maxSize = size;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier connectionValidator(AgroalConnectionPoolConfiguration.ConnectionValidator validator) {
        checkLock();
        connectionValidator = validator;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier exceptionSorter(AgroalConnectionPoolConfiguration.ExceptionSorter sorter) {
        checkLock();
        exceptionSorter = sorter;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier acquisitionTimeout(Duration timeout) {
        checkLock();
        acquisitionTimeout = timeout;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier idleValidationTimeout(Duration timeout) {
        checkLock();
        idleValidationTimeout = timeout;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier leakTimeout(Duration timeout) {
        checkLock();
        leakTimeout = timeout;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier validationTimeout(Duration timeout) {
        checkLock();
        validationTimeout = timeout;
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier reapTimeout(Duration timeout) {
        checkLock();
        reapTimeout = timeout;
        return this;
    }

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
        this.lock = true;

        return new AgroalConnectionPoolConfiguration() {

            @Override
            public AgroalConnectionFactoryConfiguration connectionFactoryConfiguration() {
                return connectionFactoryConfiguration;
            }

            @Override
            public TransactionIntegration transactionIntegration() {
                return transactionIntegration;
            }

            @Override
            public boolean flushOnClose() {
                return flushOnClose;
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

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
import static io.agroal.api.transaction.TransactionIntegration.none;
import static java.lang.Integer.MAX_VALUE;
import static java.time.Duration.ZERO;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalConnectionPoolConfigurationSupplier implements Supplier<AgroalConnectionPoolConfiguration> {

    private volatile boolean lock;

    private AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfigurationSupplier = new AgroalConnectionFactoryConfigurationSupplier();

    private TransactionIntegration transactionIntegration = none();
    private int initialSize = 0;
    private volatile int minSize = 0;
    private volatile int maxSize = MAX_VALUE;
    private AgroalConnectionPoolConfiguration.ConnectionValidator connectionValidator = emptyValidator();
    private Duration leakTimeout = ZERO;
    private Duration validationTimeout = ZERO;
    private Duration reapTimeout = ZERO;
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
        this.initialSize = existingConfiguration.initialSize();
        this.minSize = existingConfiguration.minSize();
        this.maxSize = existingConfiguration.maxSize();
        this.connectionValidator = existingConfiguration.connectionValidator();
        this.leakTimeout = existingConfiguration.leakTimeout();
        this.validationTimeout = existingConfiguration.validationTimeout();
        this.reapTimeout = existingConfiguration.reapTimeout();
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
        transactionIntegration =  integration;
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

    public AgroalConnectionPoolConfigurationSupplier acquisitionTimeout(Duration timeout) {
        checkLock();
        acquisitionTimeout = timeout;
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

    // --- //

    private void validate() {
        if ( maxSize == MAX_VALUE ) {
            throw new IllegalArgumentException( "max size attribute is mandatory" );
        }
        if ( minSize < 0 ) {
            throw new IllegalArgumentException( "Invalid min size: smaller than 0" );
        }
        if ( maxSize <= 0 ) {
            throw new IllegalArgumentException( "A Positive max size is required" );
        }
        if ( minSize > maxSize ) {
            throw new IllegalArgumentException( "Invalid min size: greater than max size" );
        }
        if ( initialSize < 0 ) {
            throw new IllegalArgumentException( "Invalid value for initial size. Must be positive, and ideally between min size and max size" );
        }
        if ( connectionFactoryConfigurationSupplier == null ) {
            throw new IllegalArgumentException( "Connection factory configuration not defined" );
        }
    }

    @Override
    @SuppressWarnings( "ReturnOfInnerClass" )
    public AgroalConnectionPoolConfiguration get() {
        validate();
        this.lock = true;

        return new AgroalConnectionPoolConfiguration() {

            @Override
            public AgroalConnectionFactoryConfiguration connectionFactoryConfiguration() {
                return connectionFactoryConfigurationSupplier.get();
            }

            @Override
            public TransactionIntegration transactionIntegration() {
                return transactionIntegration;
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
                acquisitionTimeout = timeout;
            }

            @Override
            public ConnectionValidator connectionValidator() {
                return connectionValidator;
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
        };
    }
}

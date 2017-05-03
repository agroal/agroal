// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.PreFillMode;
import io.agroal.api.configuration.ConnectionValidator;
import io.agroal.api.transaction.TransactionIntegration;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.PreFillMode.MAX;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.PreFillMode.NONE;
import static io.agroal.api.configuration.ConnectionValidator.emptyValidator;
import static io.agroal.api.transaction.TransactionIntegration.none;
import static java.lang.Integer.MAX_VALUE;
import static java.time.Duration.ZERO;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalConnectionPoolConfigurationSupplier implements Supplier<AgroalConnectionPoolConfiguration> {

    private volatile boolean lock;

    private AgroalConnectionFactoryConfiguration connectionFactoryConfiguration = new AgroalConnectionFactoryConfigurationSupplier().get();

    @Deprecated
    private PreFillMode preFillMode = NONE;
    private TransactionIntegration transactionIntegration = none();
    private int initialSize = 0;
    private volatile int minSize = 0;
    private volatile int maxSize = MAX_VALUE;
    private ConnectionValidator connectionValidator = emptyValidator();
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
        this.connectionFactoryConfiguration = existingConfiguration.connectionFactoryConfiguration();
        this.preFillMode = existingConfiguration.preFillMode();
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

    private AgroalConnectionPoolConfigurationSupplier applySetting(Consumer<AgroalConnectionPoolConfigurationSupplier> consumer) {
        if ( lock ) {
            throw new IllegalStateException( "Attempt to modify an immutable configuration" );
        }
        consumer.accept( this );
        return this;
    }

    public AgroalConnectionPoolConfigurationSupplier connectionFactoryConfiguration(AgroalConnectionFactoryConfiguration configuration) {
        return applySetting( c -> c.connectionFactoryConfiguration = configuration );
    }

    public AgroalConnectionPoolConfigurationSupplier connectionFactoryConfiguration(Supplier<AgroalConnectionFactoryConfiguration> supplier) {
        return connectionFactoryConfiguration( supplier.get() );
    }

    public AgroalConnectionPoolConfigurationSupplier connectionFactoryConfiguration(Function<AgroalConnectionFactoryConfigurationSupplier, AgroalConnectionFactoryConfigurationSupplier> function) {
        return connectionFactoryConfiguration( function.apply( new AgroalConnectionFactoryConfigurationSupplier( connectionFactoryConfiguration ) ) );
    }

    // --- //

    public AgroalConnectionPoolConfigurationSupplier transactionIntegration(TransactionIntegration integration) {
        return applySetting( c -> c.transactionIntegration = integration );
    }

    @Deprecated
    public AgroalConnectionPoolConfigurationSupplier preFillMode(PreFillMode mode) {
        return applySetting( c -> c.preFillMode = mode );
    }

    public AgroalConnectionPoolConfigurationSupplier initialSize(int size) {
        return applySetting( c -> c.initialSize = size );
    }

    public AgroalConnectionPoolConfigurationSupplier minSize(int size) {
        return applySetting( c -> c.minSize = size );
    }

    public AgroalConnectionPoolConfigurationSupplier maxSize(int size) {
        return applySetting( c -> c.maxSize = size );
    }

    public AgroalConnectionPoolConfigurationSupplier connectionValidator(ConnectionValidator connectionValidator) {
        return applySetting( c -> c.connectionValidator = connectionValidator );
    }

    public AgroalConnectionPoolConfigurationSupplier acquisitionTimeout(Duration timeout) {
        return applySetting( c -> c.acquisitionTimeout = timeout );
    }

    public AgroalConnectionPoolConfigurationSupplier leakTimeout(Duration timeout) {
        return applySetting( c -> c.leakTimeout = timeout );
    }

    public AgroalConnectionPoolConfigurationSupplier validationTimeout(Duration timeout) {
        return applySetting( c -> c.validationTimeout = timeout );
    }

    public AgroalConnectionPoolConfigurationSupplier reapTimeout(Duration timeout) {
        return applySetting( c -> c.reapTimeout = timeout );
    }

    // --- //

    private void validate() {
        if ( minSize < 0 ) {
            throw new IllegalArgumentException( "Invalid min size" );
        }
        if ( maxSize <= 0 ) {
            throw new IllegalArgumentException( "A Positive max size is required" );
        }
        if ( minSize > maxSize ) {
            throw new IllegalArgumentException( "Wrong size of min / max size" );
        }
        if ( initialSize < minSize || initialSize > maxSize ) {
            throw new IllegalArgumentException( "Invalid value for initial size. Must be between min and max." );
        }
        if ( maxSize == MAX_VALUE && MAX.equals( preFillMode ) ) {
            throw new IllegalArgumentException( "Invalid pre-fill mode MAX without specifying max size" );
        }
        if ( connectionFactoryConfiguration == null ) {
            throw new IllegalArgumentException( "Connection factory configuration not defined" );
        }
    }

    @Override
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
            public PreFillMode preFillMode() {
                return preFillMode;
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
                minSize = size;
            }

            @Override
            public int maxSize() {
                return maxSize;
            }

            @Override
            public void setMaxSize(int size) {
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

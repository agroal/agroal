// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.PreFillMode;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalPropertiesReader implements Supplier<AgroalDataSourceConfiguration> {

    private static final String IMPLEMENTATION = "implementation";
    private static final String JNDI_NAME = "jndiName";
    private static final String METRICS_ENABLED = "metricsEnabled";
    private static final String XA = "xa";

    // --- //

    private static final String MIN_SIZE = "minSize";
    private static final String MAX_SIZE = "maxSize";
    private static final String PRE_FILL_MODE = "preFillMode";
    private static final String ACQUISITION_TIMEOUT = "acquisitionTimeout";
    private static final String VALIDATION_TIMEOUT = "validationTimeout";
    private static final String LEAK_TIMEOUT = "leakTimeout";
    private static final String REAP_TIMEOUT = "reapTimeout";

    // --- //

    private static final String JDBC_URL = "jdbcUrl";
    private static final String AUTO_COMMIT = "autoCommit";
    private static final String INITIAL_SQL = "initialSQL";
    private static final String DRIVER_CLASS_NAME = "driverClassName";
    private static final String TRANSACTION_ISOLATION = "jdbcTransactionIsolation";
    private static final String PRINCIPAL = "principal";
    private static final String CREDENTIAL = "credential";
    private static final String JDBC_PROPERTIES = "jdbcProperties";

    // --- //

    private final String prefix;

    private final AgroalDataSourceConfigurationSupplier dataSourceSupplier;
    private final AgroalConnectionPoolConfigurationSupplier connectionPoolSupplier;
    private final AgroalConnectionFactoryConfigurationSupplier connectionFactorySupplier;

    public AgroalPropertiesReader(String prefix) {
        this.prefix = prefix;
        this.dataSourceSupplier = new AgroalDataSourceConfigurationSupplier();
        this.connectionPoolSupplier = new AgroalConnectionPoolConfigurationSupplier();
        this.connectionFactorySupplier = new AgroalConnectionFactoryConfigurationSupplier();
    }

    @Override
    public AgroalDataSourceConfiguration get() {
        return dataSourceSupplier.connectionPoolConfiguration( connectionPoolSupplier.connectionFactoryConfiguration( connectionFactorySupplier ) ).get();
    }

    // --- //

    public AgroalPropertiesReader readProperties(String filename) throws IOException {
        try ( InputStream inputStream = new FileInputStream( filename ) ) {
            Properties properties = new Properties();
            properties.load( inputStream );
            return readProperties( properties );
        }
    }

    private AgroalPropertiesReader readProperties(Properties properties) {
        apply( dataSourceSupplier::dataSourceImplementation, DataSourceImplementation::valueOf, properties, IMPLEMENTATION );
        apply( dataSourceSupplier::jndiName, Function.identity(), properties, JNDI_NAME );
        apply( dataSourceSupplier::metricsEnabled, Boolean::parseBoolean, properties, METRICS_ENABLED );
        apply( dataSourceSupplier::xa, Boolean::parseBoolean, properties, XA );

        apply( connectionPoolSupplier::minSize, Integer::parseInt, properties, MIN_SIZE );
        apply( connectionPoolSupplier::maxSize, Integer::parseInt, properties, MAX_SIZE );
        apply( connectionPoolSupplier::preFillMode, PreFillMode::valueOf, properties, PRE_FILL_MODE );
        apply( connectionPoolSupplier::acquisitionTimeout, Duration::parse, properties, ACQUISITION_TIMEOUT );
        apply( connectionPoolSupplier::validationTimeout, Duration::parse, properties, VALIDATION_TIMEOUT );
        apply( connectionPoolSupplier::leakTimeout, Duration::parse, properties, LEAK_TIMEOUT );
        apply( connectionPoolSupplier::reapTimeout, Duration::parse, properties, REAP_TIMEOUT );

        apply( connectionFactorySupplier::jdbcUrl, Function.identity(), properties, JDBC_URL );
        apply( connectionFactorySupplier::autoCommit, Boolean::parseBoolean, properties, AUTO_COMMIT );
        apply( connectionFactorySupplier::initialSql, Function.identity(), properties, INITIAL_SQL );
        apply( connectionFactorySupplier::driverClassName, Function.identity(), properties, DRIVER_CLASS_NAME );
        apply( connectionFactorySupplier::jdbcTransactionIsolation, TransactionIsolation::valueOf, properties, TRANSACTION_ISOLATION );
        apply( connectionFactorySupplier::principal, NamePrincipal::new, properties, PRINCIPAL );
        apply( connectionFactorySupplier::credential, SimplePassword::new, properties, CREDENTIAL );
        applyJdbcProperties( connectionFactorySupplier::jdbcProperty, properties, JDBC_PROPERTIES );
        return this;
    }

    private <T> void apply(Consumer<T> consumer, Function<String, T> converter, Properties properties, String key) {
        String value = properties.getProperty( prefix + key );
        if ( value != null ) {
            consumer.accept( converter.apply( value ) );
        }
    }

    private void applyJdbcProperties(BiConsumer<String, String> consumer, Properties properties, String key) {
        String propertiesArray = properties.getProperty( prefix + key );
        if ( propertiesArray != null ) {
            for ( String property : propertiesArray.split( ";" ) ) {
                String[] keyValue = property.split( "=" );
                consumer.accept( keyValue[0], keyValue[1] );
            }
        }
    }
}

// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.MultipleAcquisitionAction;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.TransactionRequirement;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.Long.parseLong;
import static java.util.function.Function.identity;

/**
 * Convenient way to build an Agroal configuration. This class can build a configuration from a *.properties file or a {@link Properties} object.
 * This class defines keys for all the options and also allows for a prefix when looking for that properties.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalPropertiesReader implements Supplier<AgroalDataSourceConfiguration> {

    public static final String IMPLEMENTATION = "implementation";
    public static final String METRICS_ENABLED = "metricsEnabled";

    // --- //

    public static final String MIN_SIZE = "minSize";
    public static final String MAX_SIZE = "maxSize";
    public static final String INITIAL_SIZE = "initialSize";
    public static final String FLUSH_ON_CLOSE = "flushOnClose";
    public static final String ENHANCED_LEAK_REPORT = "enhancedLeakReport";
    public static final String MULTIPLE_ACQUISITION = "multipleAcquisition";
    public static final String TRANSACTION_REQUIREMENT = "transactionRequirement";

    public static final String ACQUISITION_TIMEOUT = "acquisitionTimeout";
    public static final String ACQUISITION_TIMEOUT_MS = "acquisitionTimeout_ms";
    public static final String ACQUISITION_TIMEOUT_S = "acquisitionTimeout_s";
    public static final String ACQUISITION_TIMEOUT_M = "acquisitionTimeout_m";
    public static final String VALIDATION_TIMEOUT = "validationTimeout";
    public static final String VALIDATION_TIMEOUT_MS = "validationTimeout_ms";
    public static final String VALIDATION_TIMEOUT_S = "validationTimeout_s";
    public static final String VALIDATION_TIMEOUT_M = "validationTimeout_m";
    public static final String LEAK_TIMEOUT = "leakTimeout";
    public static final String LEAK_TIMEOUT_MS = "leakTimeout_ms";
    public static final String LEAK_TIMEOUT_S = "leakTimeout_s";
    public static final String LEAK_TIMEOUT_M = "leakTimeout_m";
    public static final String REAP_TIMEOUT = "reapTimeout";
    public static final String REAP_TIMEOUT_MS = "reapTimeout_ms";
    public static final String REAP_TIMEOUT_S = "reapTimeout_s";
    public static final String REAP_TIMEOUT_M = "reapTimeout_m";
    public static final String MAX_LIFETIME = "maxLifetime";
    public static final String MAX_LIFETIME_MS = "maxLifetime_ms";
    public static final String MAX_LIFETIME_S = "maxLifetime_s";
    public static final String MAX_LIFETIME_M = "maxLifetime_m";

    // --- //

    public static final String JDBC_URL = "jdbcUrl";
    public static final String AUTO_COMMIT = "autoCommit";
    public static final String TRACK_JDBC_RESOURCES = "trackJdbcResources";
    public static final String INITIAL_SQL = "initialSQL";
    public static final String PROVIDER_CLASS_NAME = "providerClassName";
    public static final String TRANSACTION_ISOLATION = "jdbcTransactionIsolation";
    public static final String PRINCIPAL = "principal";
    public static final String CREDENTIAL = "credential";
    public static final String RECOVERY_PRINCIPAL = "recoveryPrincipal";
    public static final String RECOVERY_CREDENTIAL = "recoveryCredential";
    public static final String JDBC_PROPERTIES = "jdbcProperties";

    // --- //

    private final String prefix;

    private final AgroalDataSourceConfigurationSupplier dataSourceSupplier;

    public AgroalPropertiesReader() {
        this( "" );
    }

    public AgroalPropertiesReader(String prefix) {
        this.prefix = prefix;
        this.dataSourceSupplier = new AgroalDataSourceConfigurationSupplier();
    }

    @Override
    public AgroalDataSourceConfiguration get() {
        return dataSourceSupplier.get();
    }

    public AgroalDataSourceConfigurationSupplier modify() {
        return dataSourceSupplier;
    }

    // --- //

    public AgroalPropertiesReader readProperties(Path path) throws IOException {
        return readProperties( path.toFile() );
    }

    public AgroalPropertiesReader readProperties(String filename) throws IOException {
        return readProperties( new File( filename ) );
    }

    public AgroalPropertiesReader readProperties(File file) throws IOException {
        try ( InputStream inputStream = new FileInputStream( file ) ) {
            Properties properties = new Properties();
            properties.load( inputStream );
            return readProperties( properties );
        }
    }

    @SuppressWarnings( "unchecked" )
    public AgroalPropertiesReader readProperties(Properties properties) {
        return readProperties( (Map) properties );
    }

    public AgroalPropertiesReader readProperties(Map<String, String> properties) {
        AgroalConnectionPoolConfigurationSupplier connectionPoolSupplier = new AgroalConnectionPoolConfigurationSupplier();
        AgroalConnectionFactoryConfigurationSupplier connectionFactorySupplier = new AgroalConnectionFactoryConfigurationSupplier();

        apply( dataSourceSupplier::dataSourceImplementation, DataSourceImplementation::valueOf, properties, IMPLEMENTATION );
        apply( dataSourceSupplier::metricsEnabled, Boolean::parseBoolean, properties, METRICS_ENABLED );

        apply( connectionPoolSupplier::minSize, Integer::parseInt, properties, MIN_SIZE );
        apply( connectionPoolSupplier::maxSize, Integer::parseInt, properties, MAX_SIZE );
        apply( connectionPoolSupplier::flushOnClose, Boolean::parseBoolean, properties, FLUSH_ON_CLOSE );
        apply( connectionPoolSupplier::initialSize, Integer::parseInt, properties, INITIAL_SIZE );
        apply( connectionPoolSupplier::enhancedLeakReport, Boolean::parseBoolean, properties, ENHANCED_LEAK_REPORT );
        apply( connectionPoolSupplier::multipleAcquisition, MultipleAcquisitionAction::valueOf, properties, MULTIPLE_ACQUISITION );
        apply( connectionPoolSupplier::transactionRequirement, TransactionRequirement::valueOf, properties, TRANSACTION_REQUIREMENT );

        apply( connectionPoolSupplier::acquisitionTimeout, Duration::parse, properties, ACQUISITION_TIMEOUT );
        apply( connectionPoolSupplier::acquisitionTimeout, this::parseDurationMs, properties, ACQUISITION_TIMEOUT_MS );
        apply( connectionPoolSupplier::acquisitionTimeout, this::parseDurationS, properties, ACQUISITION_TIMEOUT_S );
        apply( connectionPoolSupplier::acquisitionTimeout, this::parseDurationM, properties, ACQUISITION_TIMEOUT_M );

        apply( connectionPoolSupplier::validationTimeout, Duration::parse, properties, VALIDATION_TIMEOUT );
        apply( connectionPoolSupplier::validationTimeout, this::parseDurationMs, properties, VALIDATION_TIMEOUT_MS );
        apply( connectionPoolSupplier::validationTimeout, this::parseDurationS, properties, VALIDATION_TIMEOUT_S );
        apply( connectionPoolSupplier::validationTimeout, this::parseDurationM, properties, VALIDATION_TIMEOUT_M );

        apply( connectionPoolSupplier::leakTimeout, Duration::parse, properties, LEAK_TIMEOUT );
        apply( connectionPoolSupplier::leakTimeout, this::parseDurationMs, properties, LEAK_TIMEOUT_MS );
        apply( connectionPoolSupplier::leakTimeout, this::parseDurationS, properties, LEAK_TIMEOUT_S );
        apply( connectionPoolSupplier::leakTimeout, this::parseDurationM, properties, LEAK_TIMEOUT_M );

        apply( connectionPoolSupplier::reapTimeout, Duration::parse, properties, REAP_TIMEOUT );
        apply( connectionPoolSupplier::reapTimeout, this::parseDurationMs, properties, REAP_TIMEOUT_MS );
        apply( connectionPoolSupplier::reapTimeout, this::parseDurationS, properties, REAP_TIMEOUT_S );
        apply( connectionPoolSupplier::reapTimeout, this::parseDurationM, properties, REAP_TIMEOUT_M );

        apply( connectionPoolSupplier::maxLifetime, Duration::parse, properties, MAX_LIFETIME );
        apply( connectionPoolSupplier::maxLifetime, this::parseDurationMs, properties, MAX_LIFETIME_MS );
        apply( connectionPoolSupplier::maxLifetime, this::parseDurationS, properties, MAX_LIFETIME_S );
        apply( connectionPoolSupplier::maxLifetime, this::parseDurationM, properties, MAX_LIFETIME_M );

        apply( connectionFactorySupplier::jdbcUrl, identity(), properties, JDBC_URL );
        apply( connectionFactorySupplier::autoCommit, Boolean::parseBoolean, properties, AUTO_COMMIT );
        apply( connectionFactorySupplier::trackJdbcResources, Boolean::parseBoolean, properties, TRACK_JDBC_RESOURCES );
        apply( connectionFactorySupplier::initialSql, identity(), properties, INITIAL_SQL );
        apply( connectionFactorySupplier::connectionProviderClassName, identity(), properties, PROVIDER_CLASS_NAME );
        apply( connectionFactorySupplier::jdbcTransactionIsolation, TransactionIsolation::valueOf, properties, TRANSACTION_ISOLATION );
        apply( connectionFactorySupplier::principal, NamePrincipal::new, properties, PRINCIPAL );
        apply( connectionFactorySupplier::credential, SimplePassword::new, properties, CREDENTIAL );
        apply( connectionFactorySupplier::recoveryPrincipal, NamePrincipal::new, properties, RECOVERY_PRINCIPAL );
        apply( connectionFactorySupplier::recoveryCredential, SimplePassword::new, properties, RECOVERY_CREDENTIAL );
        applyJdbcProperties( connectionFactorySupplier::jdbcProperty, properties, JDBC_PROPERTIES );

        dataSourceSupplier.connectionPoolConfiguration( connectionPoolSupplier.connectionFactoryConfiguration( connectionFactorySupplier ) );
        return this;
    }

    private <T> void apply(Consumer<T> consumer, Function<String, T> converter, Map<String, String> properties, String key) {
        String value = properties.get( prefix + key );
        if ( value != null ) {
            consumer.accept( converter.apply( value ) );
        }
    }

    private void applyJdbcProperties(BiConsumer<String, String> consumer, Map<String, String> properties, String key) {
        String propertiesArray = properties.get( prefix + key );
        if ( propertiesArray != null && !propertiesArray.isEmpty() ) {
            for ( String property : propertiesArray.split( ";" ) ) {
                String[] keyValue = property.split( "=" );
                consumer.accept( keyValue[0], keyValue[1] );
            }
        }
    }

    private Duration parseDurationMs(String value) {
        return Duration.ofMillis( parseLong( value ) );
    }

    private Duration parseDurationS(String value) {
        return Duration.ofSeconds( parseLong( value ) );
    }

    private Duration parseDurationM(String value) {
        return Duration.ofMinutes( parseLong( value ) );
    }
}

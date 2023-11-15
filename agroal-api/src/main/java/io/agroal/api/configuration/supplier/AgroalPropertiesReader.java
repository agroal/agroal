// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.configuration.supplier;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ConnectionValidator;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.MultipleAcquisitionAction;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.TransactionRequirement;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.exceptionsorter.DB2ExceptionSorter;
import io.agroal.api.exceptionsorter.MSSQLExceptionSorter;
import io.agroal.api.exceptionsorter.MySQLExceptionSorter;
import io.agroal.api.exceptionsorter.OracleExceptionSorter;
import io.agroal.api.exceptionsorter.PostgreSQLExceptionSorter;
import io.agroal.api.exceptionsorter.SybaseExceptionSorter;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidator;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidatorWithTimeout;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ConnectionValidator.emptyValidator;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter.defaultExceptionSorter;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter.emptyExceptionSorter;
import static io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ExceptionSorter.fatalExceptionSorter;
import static java.lang.Long.parseLong;
import static java.util.function.Function.identity;

/**
 * Convenient way to build an Agroal configuration. This class can build a configuration from a *.properties file or a {@link Properties} object.
 * This class defines keys for all the options and also allows for a prefix when looking for that properties.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@SuppressWarnings( "WeakerAccess" )
public class AgroalPropertiesReader implements Supplier<AgroalDataSourceConfiguration> {

    public static final String IMPLEMENTATION = "implementation";
    public static final String METRICS_ENABLED = "metricsEnabled";

    // --- //

    public static final String MIN_SIZE = "minSize";
    public static final String MAX_SIZE = "maxSize";
    public static final String INITIAL_SIZE = "initialSize";
    public static final String FLUSH_ON_CLOSE = "flushOnClose";
    public static final String CONNECTION_VALIDATOR = "connectionValidator";
    public static final String ENHANCED_LEAK_REPORT = "enhancedLeakReport";
    public static final String EXCEPTION_SORTER = "exceptionSorter";
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
    public static final String LOGIN_TIMEOUT = "loginTimeout";
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

    public AgroalPropertiesReader(String readerPrefix) {
        prefix = readerPrefix;
        dataSourceSupplier = new AgroalDataSourceConfigurationSupplier();
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
        apply( connectionPoolSupplier::connectionValidator, AgroalPropertiesReader::parseConnectionValidator, properties, CONNECTION_VALIDATOR );
        apply( connectionPoolSupplier::exceptionSorter, AgroalPropertiesReader::parseExceptionSorter, properties, EXCEPTION_SORTER );
        apply( connectionPoolSupplier::enhancedLeakReport, Boolean::parseBoolean, properties, ENHANCED_LEAK_REPORT );
        apply( connectionPoolSupplier::multipleAcquisition, MultipleAcquisitionAction::valueOf, properties, MULTIPLE_ACQUISITION );
        apply( connectionPoolSupplier::transactionRequirement, TransactionRequirement::valueOf, properties, TRANSACTION_REQUIREMENT );

        apply( connectionPoolSupplier::acquisitionTimeout, Duration::parse, properties, ACQUISITION_TIMEOUT );
        apply( connectionPoolSupplier::acquisitionTimeout, AgroalPropertiesReader::parseDurationMs, properties, ACQUISITION_TIMEOUT_MS );
        apply( connectionPoolSupplier::acquisitionTimeout, AgroalPropertiesReader::parseDurationS, properties, ACQUISITION_TIMEOUT_S );
        apply( connectionPoolSupplier::acquisitionTimeout, AgroalPropertiesReader::parseDurationM, properties, ACQUISITION_TIMEOUT_M );

        apply( connectionPoolSupplier::validationTimeout, Duration::parse, properties, VALIDATION_TIMEOUT );
        apply( connectionPoolSupplier::validationTimeout, AgroalPropertiesReader::parseDurationMs, properties, VALIDATION_TIMEOUT_MS );
        apply( connectionPoolSupplier::validationTimeout, AgroalPropertiesReader::parseDurationS, properties, VALIDATION_TIMEOUT_S );
        apply( connectionPoolSupplier::validationTimeout, AgroalPropertiesReader::parseDurationM, properties, VALIDATION_TIMEOUT_M );

        apply( connectionPoolSupplier::leakTimeout, Duration::parse, properties, LEAK_TIMEOUT );
        apply( connectionPoolSupplier::leakTimeout, AgroalPropertiesReader::parseDurationMs, properties, LEAK_TIMEOUT_MS );
        apply( connectionPoolSupplier::leakTimeout, AgroalPropertiesReader::parseDurationS, properties, LEAK_TIMEOUT_S );
        apply( connectionPoolSupplier::leakTimeout, AgroalPropertiesReader::parseDurationM, properties, LEAK_TIMEOUT_M );

        apply( connectionPoolSupplier::reapTimeout, Duration::parse, properties, REAP_TIMEOUT );
        apply( connectionPoolSupplier::reapTimeout, AgroalPropertiesReader::parseDurationMs, properties, REAP_TIMEOUT_MS );
        apply( connectionPoolSupplier::reapTimeout, AgroalPropertiesReader::parseDurationS, properties, REAP_TIMEOUT_S );
        apply( connectionPoolSupplier::reapTimeout, AgroalPropertiesReader::parseDurationM, properties, REAP_TIMEOUT_M );

        apply( connectionPoolSupplier::maxLifetime, Duration::parse, properties, MAX_LIFETIME );
        apply( connectionPoolSupplier::maxLifetime, AgroalPropertiesReader::parseDurationMs, properties, MAX_LIFETIME_MS );
        apply( connectionPoolSupplier::maxLifetime, AgroalPropertiesReader::parseDurationS, properties, MAX_LIFETIME_S );
        apply( connectionPoolSupplier::maxLifetime, AgroalPropertiesReader::parseDurationM, properties, MAX_LIFETIME_M );

        apply( connectionFactorySupplier::jdbcUrl, identity(), properties, JDBC_URL );
        apply( connectionFactorySupplier::autoCommit, Boolean::parseBoolean, properties, AUTO_COMMIT );
        apply( connectionFactorySupplier::trackJdbcResources, Boolean::parseBoolean, properties, TRACK_JDBC_RESOURCES );
        apply( connectionFactorySupplier::loginTimeout, Duration::parse, properties, LOGIN_TIMEOUT );
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

    @SuppressWarnings( "StringConcatenation" )
    private <T> void apply(Consumer<? super T> consumer, Function<? super String, T> converter, Map<String, String> properties, String key) {
        String value = properties.get( prefix + key );
        if ( value != null ) {
            consumer.accept( converter.apply( value ) );
        }
    }

    @SuppressWarnings( {"SameParameterValue", "StringConcatenation"} )
    private void applyJdbcProperties(BiConsumer<? super String, ? super String> consumer, Map<String, String> properties, String key) {
        String propertiesArray = properties.get( prefix + key );
        if ( propertiesArray != null && !propertiesArray.isEmpty() ) {
            for ( String property : propertiesArray.split( ";" ) ) {
                String[] keyValue = property.split( "=" );
                consumer.accept( keyValue[0], keyValue[1] );
            }
        }
    }

    // --- //

    /**
     * Accepts the following options:
     * <ul>
     * <li>`empty` for the default {@link ConnectionValidator#emptyValidator()}</li>
     * <li>`default` for {@link ConnectionValidator#defaultValidator()}</li>
     * <li>`defaultX` for {@link ConnectionValidator#defaultValidatorWithTimeout(int)} where `X` is the timeout in seconds</li>
     * <li>the name of a class that implements {@link ConnectionValidator}</li>
     * </ul>
     */
    public static ConnectionValidator parseConnectionValidator(String connectionValidatorName) {
        if ( "empty".equalsIgnoreCase( connectionValidatorName ) ) {
            return emptyValidator();
        } else if ( "default".equalsIgnoreCase( connectionValidatorName ) ) {
            return defaultValidator();
        } else if ( connectionValidatorName.regionMatches( true, 0, "default", 0, "default".length() ) ) {
            return defaultValidatorWithTimeout( (int) parseDurationS( connectionValidatorName.substring( "default".length() ) ).toSeconds() );
        }

        try {
            Class<? extends ConnectionValidator> validatorClass = Thread.currentThread().getContextClassLoader().loadClass( connectionValidatorName ).asSubclass( ConnectionValidator.class );
            return validatorClass.getDeclaredConstructor().newInstance();
        } catch ( ClassNotFoundException e ) {
            throw new IllegalArgumentException( "Unknown connection validator " + connectionValidatorName );
        } catch ( ClassCastException e ) {
            throw new IllegalArgumentException( connectionValidatorName + " class is not a ConnectionValidator", e );
        } catch ( InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e ) {
            throw new IllegalArgumentException( "Unable to instantiate ConnectionValidator " + connectionValidatorName, e );
        }
    }

    // --- //

    /**
     * Accepts the following options:
     * <ul>
     * <li>`empty` for the default {@link ExceptionSorter#emptyExceptionSorter()}</li>
     * <li>`default` for {@link ExceptionSorter#defaultExceptionSorter()}</li>
     * <li>`fatal` for {@link ExceptionSorter#fatalExceptionSorter()}</li>
     * <li>`DB2` for the {@link DB2ExceptionSorter}</li>
     * <li>`MSSQL` for the {@link MSSQLExceptionSorter}</li>
     * <li>`MySQL` for the {@link MySQLExceptionSorter}</li>
     * <li>`Oracle` for the {@link OracleExceptionSorter}</li>
     * <li>`Postgres` or `PostgreSQL` for the {@link PostgreSQLExceptionSorter}</li>
     * <li>`Sybase` for the {@link SybaseExceptionSorter}</li>
     * <li>the name of a class that implements {@link ExceptionSorter}</li>
     * </ul>
     */
    public static ExceptionSorter parseExceptionSorter(String exceptionSorterName) {
        if ( "empty".equalsIgnoreCase( exceptionSorterName ) ) {
            return emptyExceptionSorter();
        } else if ( "default".equalsIgnoreCase( exceptionSorterName ) ) {
            return defaultExceptionSorter();
        } else if ( "fatal".equalsIgnoreCase( exceptionSorterName ) ) {
            return fatalExceptionSorter();
        } else if ( "DB2".equalsIgnoreCase( exceptionSorterName ) ) {
            return new DB2ExceptionSorter();
        } else if ( "MSSQL".equalsIgnoreCase( exceptionSorterName ) ) {
            return new MSSQLExceptionSorter();
        } else if ( "MySQL".equalsIgnoreCase( exceptionSorterName ) ) {
            return new MySQLExceptionSorter();
        } else if ( "Oracle".equalsIgnoreCase( exceptionSorterName ) ) {
            return new OracleExceptionSorter();
        } else if ( "Postgres".equalsIgnoreCase( exceptionSorterName ) || "PostgreSQL".equalsIgnoreCase( exceptionSorterName ) ) {
            return new PostgreSQLExceptionSorter();
        } else if ( "Sybase".equalsIgnoreCase( exceptionSorterName ) ) {
            return new SybaseExceptionSorter();
        }
        try {
            Class<? extends ExceptionSorter> sorterClass = Thread.currentThread().getContextClassLoader().loadClass( exceptionSorterName ).asSubclass( ExceptionSorter.class );
            return sorterClass.getDeclaredConstructor().newInstance();
        } catch ( ClassNotFoundException e ) {
            throw new IllegalArgumentException( "Unknown exception sorter " + exceptionSorterName );
        } catch ( ClassCastException e ) {
            throw new IllegalArgumentException( exceptionSorterName + " class is not a ExceptionSorter", e );
        } catch ( InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e ) {
            throw new IllegalArgumentException( "Unable to instantiate ExceptionSorter " + exceptionSorterName, e );
        }
    }

    // --- //

    private static Duration parseDurationMs(String value) {
        return Duration.ofMillis( parseLong( value ) );
    }

    private static Duration parseDurationS(String value) {
        return Duration.ofSeconds( parseLong( value ) );
    }

    private static Duration parseDurationM(String value) {
        return Duration.ofMinutes( parseLong( value ) );
    }
}

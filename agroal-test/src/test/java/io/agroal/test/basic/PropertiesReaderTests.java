// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.AgroalDataSourceConfiguration;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.agroal.api.exceptionsorter.PostgreSQLExceptionSorter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.time.LocalTime;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static java.util.logging.Logger.getLogger;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
class PropertiesReaderTests {

    private static final Logger logger = getLogger( PropertiesReaderTests.class.getName() );

    private static final Path basePath = Paths.get( "src", "test", "resources", "PropertiesReaderTests" );

    // --- //

    @Test
    @DisplayName( "Properties File" )
    void basicPropertiesReaderTest() throws IOException {
        AgroalDataSourceConfiguration configuration = new AgroalPropertiesReader().readProperties( basePath.resolve( "agroal.properties" ) ).get();

        logger.info( configuration.toString() );

        // Not an exhaustive test, just a couple properties
        Assertions.assertEquals( 1, configuration.connectionPoolConfiguration().acquisitionTimeout().getSeconds() );
        Assertions.assertEquals( 60, configuration.connectionPoolConfiguration().validationTimeout().getSeconds() );
        Assertions.assertEquals( AgroalConnectionFactoryConfiguration.TransactionIsolation.SERIALIZABLE, configuration.connectionPoolConfiguration().connectionFactoryConfiguration().jdbcTransactionIsolation() );
        Assertions.assertInstanceOf( OddHoursConnectionValidator.class, configuration.connectionPoolConfiguration().connectionValidator() );
        Assertions.assertInstanceOf( PostgreSQLExceptionSorter.class, configuration.connectionPoolConfiguration().exceptionSorter() );
    }

    @Test
    @DisplayName( "Parse custom sql query validator" )
    void sqlValidatorTest() throws NoSuchFieldException, IllegalAccessException {
        String connectionValidatorName;
        AgroalConnectionPoolConfiguration.ConnectionValidator validator;

        connectionValidatorName = "sql[select 1]";
        validator = AgroalPropertiesReader.parseConnectionValidator(connectionValidatorName);
        Assertions.assertEquals( "io.agroal.api.configuration.AgroalConnectionPoolConfiguration$ConnectionValidator$4", validator.getClass().getName() );
        Assertions.assertEquals( "select 1", getDeclaredField( validator, "val$sql" ) );
        Assertions.assertEquals( 0, getDeclaredField( validator, "val$timeoutSeconds" ) );

        connectionValidatorName = "sql[select 1]5000";
        validator = AgroalPropertiesReader.parseConnectionValidator(connectionValidatorName);
        Assertions.assertEquals( "io.agroal.api.configuration.AgroalConnectionPoolConfiguration$ConnectionValidator$4", validator.getClass().getName() );
        Assertions.assertEquals( "select 1", getDeclaredField( validator, "val$sql" ) );
        Assertions.assertEquals( 5000, getDeclaredField( validator, "val$timeoutSeconds" ) );
    }

    // --- //

    /**
     * Silly validator that drop connections on odd hours
     */
    public static class OddHoursConnectionValidator implements AgroalConnectionPoolConfiguration.ConnectionValidator {

        @Override
        public boolean isValid(Connection connection) {
            return LocalTime.now().getHour() % 2 == 0;
        }
    }

    private static Object getDeclaredField(Object obj, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field field = obj.getClass().getDeclaredField( fieldName );
        field.setAccessible( true );
        return field.get( obj );
    }
}

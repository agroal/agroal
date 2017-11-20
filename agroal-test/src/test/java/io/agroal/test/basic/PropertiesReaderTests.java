// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static java.util.logging.Logger.getLogger;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class PropertiesReaderTests {

    private static final Logger logger = getLogger( PropertiesReaderTests.class.getName() );

    private static final Path basePath = Paths.get( "src", "test", "resources", "PropertiesReaderTests" );

    // --- //

    @Test
    @DisplayName( "Properties File" )
    public void basicPropertiesReaderTest() throws IOException {
        AgroalPropertiesReader reader = new AgroalPropertiesReader().readProperties( basePath.resolve( "agroal.properties" ) );

        logger.info( reader.get().toString() );

        // Not an exhaustive test, just a couple properties
        Assertions.assertEquals( 1, reader.get().connectionPoolConfiguration().acquisitionTimeout().getSeconds() );
        Assertions.assertEquals( 60, reader.get().connectionPoolConfiguration().validationTimeout().getSeconds() );
        Assertions.assertEquals( AgroalConnectionFactoryConfiguration.TransactionIsolation.SERIALIZABLE, reader.get().connectionPoolConfiguration().connectionFactoryConfiguration().jdbcTransactionIsolation() );
    }
}

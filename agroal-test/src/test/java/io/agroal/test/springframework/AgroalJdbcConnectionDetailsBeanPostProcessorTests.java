// Copyright (C) 2024 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework;

import dev.snowdrop.boot.narayana.autoconfigure.NarayanaConfiguration;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.agroal.springframework.boot.AgroalDataSource;
import io.agroal.springframework.boot.AgroalDataSourceConfiguration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.context.annotation.UserConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.sql.Connection;

import static io.agroal.test.AgroalTestGroup.SPRING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author <a href="benjamin.graf@gmx.net">Benjamin Graf</a>
 */
@Tag(SPRING)
class AgroalJdbcConnectionDetailsBeanPostProcessorTests {

    private static final Logger LOG = LoggerFactory.getLogger( AgroalJdbcConnectionDetailsBeanPostProcessorTests.class );

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration( AutoConfigurations.of( AgroalDataSourceConfiguration.class ) );

    @Test
    void testAgroalDataSource() {
        runner.withBean( JdbcConnectionDetails.class, () -> new MockJdbcConnectionDetails() )
                .run( context -> {
                    AgroalDataSource dataSource = context.getBean( AgroalDataSource.class );
                    AgroalConnectionPoolConfiguration poolConfiguration = dataSource.getConfiguration().connectionPoolConfiguration();

                    assertThat( poolConfiguration.connectionFactoryConfiguration().principal() )
                            .isEqualTo( new NamePrincipal( "mock" ) );
                    assertThat( poolConfiguration.connectionFactoryConfiguration().credentials() )
                            .contains( new SimplePassword( "mock" ) );
                    assertThat( dataSource.getUrl() )
                            .isEqualTo( "jdbc:mock" );
                    assertThat( poolConfiguration.connectionFactoryConfiguration().connectionProviderClass().getName() )
                            .isEqualTo( "io.agroal.test.MockDriver$Empty" );

                    try ( Connection c = dataSource.getConnection() ) {
                        LOG.info( "Got connection {}", c );
                    }
                });
    }

    @Test
    void testAgroalDataSourceWithXa() {
        runner.withPropertyValues( "narayana.logDir=ObjectStore" )
                .withConfiguration( UserConfigurations.of( NarayanaConfiguration.class ) )
                .withBean( JdbcConnectionDetails.class, () -> new MockJdbcConnectionDetails() )
                .run( context -> {
                    AgroalDataSource dataSource = context.getBean( AgroalDataSource.class );
                    AgroalConnectionPoolConfiguration poolConfiguration = dataSource.getConfiguration().connectionPoolConfiguration();

                    assertThat( poolConfiguration.connectionFactoryConfiguration().principal() )
                            .isEqualTo( new NamePrincipal( "mock" ) );
                    assertThat( poolConfiguration.connectionFactoryConfiguration().credentials() )
                            .contains( new SimplePassword( "mock" ) );
                    assertThat( dataSource.getUrl() )
                            .isEqualTo( "jdbc:mock" );
                    assertThat( poolConfiguration.connectionFactoryConfiguration().connectionProviderClass().getName() )
                            .isEqualTo( "io.agroal.test.MockXADataSource$Empty" );

                    try ( Connection c = dataSource.getConnection() ) {
                        LOG.info( "Got connection {}", c );
                    }
                });
    }

    // --- //

    public static class MockJdbcConnectionDetails implements JdbcConnectionDetails {

        @Override
        public String getUsername() {
            return "mock";
        }

        @Override
        public String getPassword() {
            return "mock";
        }

        @Override
        public String getJdbcUrl() {
            return "jdbc:mock";
        }

        @Override
        public String getDriverClassName() {
            return "io.agroal.test.MockDriver$Empty";
        }

        @Override
        public String getXaDataSourceClassName() {
            return "io.agroal.test.MockXADataSource$Empty";
        }
    }
}

// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework;

import io.agroal.test.springframework.model.FruitRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.Arrays;

import static io.agroal.test.AgroalTestGroup.SPRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( SPRING )
@SpringBootTest( classes = SpringBootIntegrationTests.AgroalApplication.class )
class SpringBootIntegrationTests {

    @Autowired
    ConfigurableApplicationContext application;

    @Test
    @DisplayName( "test deployment on spring boot container" )
    void basicSpringConnectionAcquireTest() throws Exception {
        assertTrue( application.isActive() );
    }

    @SpringBootApplication(proxyBeanMethods = false)
    @PropertySource( "SpringBootIntegrationTests/application.properties" )
    @SuppressWarnings( {"HardcodedFileSeparator", "UtilityClass", "NonFinalUtilityClass"} )
    static class AgroalApplication {

        private static final Logger log = LoggerFactory.getLogger( AgroalApplication.class );

        @Bean
        @Transactional
        public static CommandLineRunner demo(FruitRepository repository, DataSource dataSource) {
            assertTrue( dataSource instanceof io.agroal.api.AgroalDataSource );
            return (args) -> {
                log.info( Arrays.toString( repository.findAll().toArray() ) );

                assertEquals( 5, repository.count() );
                assertEquals( 3, repository.findByColor( "Red" ).size() );
            };
        }
    }
}

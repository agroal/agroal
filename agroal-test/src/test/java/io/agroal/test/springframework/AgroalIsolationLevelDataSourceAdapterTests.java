// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.transaction.annotation.Transactional;
import io.agroal.springframework.boot.AgroalIsolationLevelDataSourceAdapter;
import io.agroal.test.springframework.model.FruitRepository;

import java.util.Arrays;
import javax.sql.DataSource;

import static io.agroal.test.AgroalTestGroup.SPRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="benjamin.graf@gmx.net">Benjamin Graf</a>
 */
@Tag( SPRING )
@SpringBootTest( classes = AgroalIsolationLevelDataSourceAdapterTests.AgroalApplication.class )
class AgroalIsolationLevelDataSourceAdapterTests {

    @Autowired
    ConfigurableApplicationContext application;

    @Test
    @DisplayName( "test deployment on spring boot container" )
    void basicSpringConnectionAcquireTest() {
        assertTrue( application.isActive() );
    }

    @SpringBootApplication(proxyBeanMethods = false)
    @PropertySource( "SpringBootIntegrationTests/application.properties" )
    @SuppressWarnings( {"HardcodedFileSeparator", "UtilityClass", "NonFinalUtilityClass"} )
    static class AgroalApplication {

        private static final Logger log = LoggerFactory.getLogger( AgroalApplication.class );

        @Bean
        public static BeanPostProcessor wrapDataSourceProcessor() {
            return new BeanPostProcessor() {
                @Override
                public @Nullable Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
                    if ( bean instanceof DataSource dataSource && "dataSource".equals( beanName ) ) {
                        return new AgroalIsolationLevelDataSourceAdapter( dataSource );
                    }
                    return bean;
                }
            };
        }

        @Bean
        @Transactional( readOnly = true )
        public static CommandLineRunner demo(FruitRepository repository, DataSource dataSource) {
            assertInstanceOf( AgroalIsolationLevelDataSourceAdapter.class, dataSource );
            return args -> {
                log.info( Arrays.toString( repository.findAll().toArray() ) );

                assertEquals( 5, repository.count() );
                assertEquals( 3, repository.findByColor( "Red" ).size() );
            };
        }
    }
}

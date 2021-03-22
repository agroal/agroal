// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.jta.JtaTransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.SPRING;
import static java.lang.System.identityHashCode;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static javax.transaction.Status.STATUS_ACTIVE;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( SPRING )
@ExtendWith( SpringExtension.class )
@JdbcTest
@ComponentScan( basePackageClasses = io.agroal.springframework.boot.AgroalDataSource.class ) // tests do not pick META-INF/spring.factories
@ComponentScan( basePackageClasses = me.snowdrop.boot.narayana.autoconfigure.NarayanaConfiguration.class )
@TestPropertySource( properties = {"spring.datasource.url=jdbc:irrelevant",
        "spring.datasource.driver-class-name=io.agroal.test.springframework.BasicSpringTests$FakeDriver",
        "spring.datasource.agroal.min-size=" + BasicSpringTests.MIN_SIZE,
        "spring.datasource.agroal.initial-size=" + BasicSpringTests.INITIAL_SIZE,
        "spring.datasource.agroal.max-size=" + BasicSpringTests.MAX_SIZE
} )
@AutoConfigureTestDatabase( replace = AutoConfigureTestDatabase.Replace.NONE )
public class BasicSpringTests {

    private static final Logger logger = getLogger( BasicSpringTests.class.getName() );

    public static final int MIN_SIZE = 13, INITIAL_SIZE = 7, MAX_SIZE = 37;

    @Autowired
    @SuppressWarnings( "unused" )
    private DataSource dataSource;

    @Autowired
    @SuppressWarnings( "unused" )
    private JtaTransactionManager txManager;

    @Test
    @DisplayName( "test deployment on Spring Boot container" )
    void basicSpringConnectionAcquireTest() throws Exception {
        assertTrue( dataSource instanceof AgroalDataSource );
        AgroalConnectionPoolConfiguration poolConfiguration = ( (AgroalDataSource) dataSource ).getConfiguration().connectionPoolConfiguration();

        // Check that the configuration was injected
        assertEquals( MIN_SIZE, poolConfiguration.minSize() );
        assertEquals( INITIAL_SIZE, poolConfiguration.initialSize() );
        assertEquals( MAX_SIZE, poolConfiguration.maxSize() );

        try ( Connection c = dataSource.getConnection() ) {
            assertEquals( FakeDriver.FakeConnection.FAKE_SCHEMA, c.getSchema() );
            logger.info( format( "Got connection {0}", c ) );
        }
    }

    @Test
    @SuppressWarnings( "JDBCResourceOpenedButNotSafelyClosed" )
    @DisplayName( "test transaction manager integration on Spring Boot container" )
    void springTransactionIntegrationTest() throws Exception {
        assertNotNull( txManager.getTransactionManager(), "A TransactionManager is required for this test" );
        assertNotNull( txManager.getTransactionManager().getTransaction(), "A Transaction is required for this test" );
        assertEquals( STATUS_ACTIVE , txManager.getTransactionManager().getTransaction().getStatus(), "An ACTIVE Transaction is required for this test" );

        Connection connection = dataSource.getConnection();
        logger.info( format( "Got connection {0}", connection ) );

        assertAll( () -> {
            assertThrows( SQLException.class, () -> connection.setAutoCommit( true ) );
            assertFalse( connection.getAutoCommit(), "Expect connection to have autocommit not set" );
            assertEquals( identityHashCode( connection.unwrap( Connection.class ) ), identityHashCode( dataSource.getConnection().unwrap( Connection.class )  ), "Expect the same connection under the same transaction" );
        } );

        txManager.getTransactionManager().rollback();

        assertTrue( connection.isClosed() );
    }

    // --- //

    public static class FakeDriver implements MockDriver {

        @Override
        public Connection connect(String url, Properties info) {
            return new FakeDriver.FakeConnection();
        }

        public static class FakeConnection implements MockConnection {

            private static final String FAKE_SCHEMA = "skeema";

            @Override
            public String getSchema() {
                return FAKE_SCHEMA;
            }
        }
    }

    // --- //

    @SpringBootConfiguration
    public static class TestConfiguration {
    }
}

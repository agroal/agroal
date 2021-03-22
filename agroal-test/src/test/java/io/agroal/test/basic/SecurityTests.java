// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.AgroalDataSourceListener;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.AgroalSecurityProvider;
import io.agroal.api.security.NamePrincipal;
import io.agroal.test.MockConnection;
import io.agroal.test.MockDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static java.text.MessageFormat.format;
import static java.util.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Tag( FUNCTIONAL )
public class SecurityTests {

    private static final Logger logger = getLogger( SecurityTests.class.getName() );

    private static final String DEFAULT_USER = "def";

    // --- //

    @Test
    @DisplayName( "Test password rotation" )
    @SuppressWarnings( "InstantiationOfUtilityClass" )
    void passwordRotation() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( RotationPassword.PASSWORDS.size() )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClass( CredentialsDataSource.class )
                                .principal( new NamePrincipal( DEFAULT_USER ) )
                                .credential( new RotationPassword() )
                                .addSecurityProvider( new PasswordRotationProvider() )
                        )
                );

        try ( AgroalDataSource dataSource = AgroalDataSource.from( configurationSupplier, new WarningsAgroalDatasourceListener() ) ) {
            for ( String expectedPassword : RotationPassword.PASSWORDS ) {
                Connection connection = dataSource.getConnection();
                CredentialsConnection credentialsConnection = connection.unwrap( CredentialsConnection.class );
                logger.info( format( "Got connection {0} with username {1} and password {2}", connection, credentialsConnection.getUser(), credentialsConnection.getPassword() ) );

                assertEquals( DEFAULT_USER, credentialsConnection.getUser() );
                assertEquals( expectedPassword, credentialsConnection.getPassword() );

                // Connection leak
            }
        }
    }

    // --- //

    @SuppressWarnings( {"UtilityClass", "UtilityClassWithoutPrivateConstructor"} )
    private static final class RotationPassword {

        public static final List<String> PASSWORDS = Collections.unmodifiableList( Arrays.asList( "one", "two", "secret", "unknown" ) );

        private static final AtomicInteger COUNTER = new AtomicInteger( 0 );

        @SuppressWarnings( "WeakerAccess" )
        RotationPassword() {
        }

        static Properties asProperties() {
            Properties properties = new Properties();
            properties.setProperty( "password", getWord() );
            return properties;
        }

        private static String getWord() {
            return PASSWORDS.get( COUNTER.getAndIncrement() );
        }
    }

    private static class PasswordRotationProvider implements AgroalSecurityProvider {

        @SuppressWarnings( "WeakerAccess" )
        PasswordRotationProvider() {
        }

        @Override
        @SuppressWarnings( "InstanceofConcreteClass" )
        public Properties getSecurityProperties(Object securityObject) {
            if ( securityObject instanceof RotationPassword ) {
                return RotationPassword.asProperties();
            }
            return null;
        }
    }

    private static class WarningsAgroalDatasourceListener implements AgroalDataSourceListener {

        @SuppressWarnings( "WeakerAccess" )
        WarningsAgroalDatasourceListener() {
        }

        @Override
        public void onWarning(String message) {
            fail( "Unexpected warning: " + message );
        }

        @Override
        public void onWarning(Throwable throwable) {
            fail( "Unexpected warning", throwable );
        }
    }

    public static class CredentialsDataSource implements MockDataSource {

        private String user, password;

        public void setUser(String user) {
            this.user = user;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return new CredentialsConnection( user, password );
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    private static class CredentialsConnection implements MockConnection {

        private final String user, password;

        CredentialsConnection(String user, String password) {
            this.user = user;
            this.password = password;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return iface.cast( this );
        }

        String getUser() {
            return user;
        }

        String getPassword() {
            return password;
        }
    }
}

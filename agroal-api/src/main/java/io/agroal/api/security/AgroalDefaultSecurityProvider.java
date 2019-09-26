// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.security;

import java.security.Principal;
import java.util.Properties;

/**
 * Handles objects of type {@link NamePrincipal}, {@link SimplePassword} and {@link Principal}
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalDefaultSecurityProvider implements AgroalSecurityProvider {

    @Override
    public Properties getSecurityProperties(Object securityObject) {
        if ( securityObject instanceof NamePrincipal ) {
            return ( (NamePrincipal) securityObject ).asProperties();
        }
        if ( securityObject instanceof SimplePassword ) {
            return ( (SimplePassword) securityObject ).asProperties();
        }
        if ( securityObject instanceof Principal ) {
            Properties properties = new Properties();
            properties.setProperty( "user", ( (Principal) securityObject ).getName() );
            return properties;
        }

        return null;
    }
}

// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.security;

import java.util.Properties;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalDefaultSecurityProvider implements AgroalSecurityProvider {

    private static final Properties EMPTY_PROPERTIES = new Properties();

    @Override
    public Properties getSecurityProperties(Object securityObject) {
        if ( securityObject instanceof NamePrincipal ) {
            return ( (NamePrincipal) securityObject ).asProperties();
        }
        if ( securityObject instanceof SimplePassword ) {
            return ( (SimplePassword) securityObject ).asProperties();
        }
        return EMPTY_PROPERTIES;
    }
}

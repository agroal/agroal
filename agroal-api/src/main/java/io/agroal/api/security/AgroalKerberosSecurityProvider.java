// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.security;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

import javax.security.auth.kerberos.KerberosTicket;
import java.util.Properties;

/**
 * Handle objects of type {@link GSSCredential} and {@link KerberosTicket}
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class AgroalKerberosSecurityProvider implements AgroalSecurityProvider {

    private static final Properties EMPTY_PROPERTIES = new Properties();

    private static final String KERBEROS_v5 = "1.2.840.113554.1.2.2";

    @Override
    public Properties getSecurityProperties(Object securityObject) {
        if ( securityObject instanceof GSSCredential ) {
            try {
                Properties properties = new Properties();
                properties.setProperty( "user", ( (GSSCredential) securityObject ).getName( new Oid( KERBEROS_v5 ) ).toString() );
                return properties;
            } catch ( GSSException e ) {
                // nothing we can do
                return EMPTY_PROPERTIES;
            }
        }
        if ( securityObject instanceof KerberosTicket ) {
            return EMPTY_PROPERTIES;
        }
        return null;
    }
}

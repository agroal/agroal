// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.security;

import java.io.Serializable;
import java.security.Principal;
import java.util.Properties;

/**
 * A string that identifies an user account.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class NamePrincipal implements Serializable, Principal {

    private static final long serialVersionUID = 6943668105633565329L;

    private final String name;

    public NamePrincipal(String name) {
        this.name = name;
    }

    public NamePrincipal(char[] name) {
        this.name = new String( name );
    }

    public String getName() {
        return name;
    }

    // --- //

    @Override
    public boolean equals(Object o) {
        if ( this == o ) {
            return true;
        }
        if ( !( o instanceof Principal ) ) {
            return false;
        }
        Principal p = (Principal) o;
        return name == null ? p.getName() == null : name.contentEquals( p.getName() );
    }

    @Override
    public int hashCode() {
        return name == null ? 7 : name.hashCode();
    }

    @Override
    public String toString() {
        return name;
    }

    public Properties asProperties() {
        Properties properties = new Properties();
        properties.setProperty( "user", getName() );
        return properties;
    }
}

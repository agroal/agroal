// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.springframework.boot.jndi;

import io.agroal.api.AgroalDataSource;

import javax.naming.InitialContext;
import javax.naming.NamingException;

public class DefaultAgroalDataSourceJndiBinder implements AgroalDataSourceJndiBinder {

    @Override
    public boolean bindToJndi(String jndiName, AgroalDataSource dataSource) {
        InitialContext initialContext = null;
        try {
            initialContext = new InitialContext();
            initialContext.bind( jndiName, dataSource );
            return true;
        } catch ( NamingException ignored ) {
            // no-op
        } finally {
            if ( initialContext != null ) {
                try {
                    initialContext.close();
                } catch ( NamingException ignored ) {
                    // no-op
                }
            }
        }
        return false;
    }
}

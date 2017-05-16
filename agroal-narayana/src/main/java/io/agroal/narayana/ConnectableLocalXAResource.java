// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.narayana;

import io.agroal.api.transaction.TransactionAware;
import org.jboss.tm.ConnectableResource;
import org.jboss.tm.LastResource;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class ConnectableLocalXAResource extends LocalXAResource implements ConnectableResource, LastResource {

    public ConnectableLocalXAResource(TransactionAware connection, String jndiName) {
        super( connection, jndiName );
    }
}

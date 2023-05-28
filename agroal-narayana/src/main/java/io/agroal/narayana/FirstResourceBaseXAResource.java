// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.narayana;

import io.agroal.api.transaction.TransactionAware;
import org.jboss.tm.FirstResource;

import javax.transaction.xa.XAResource;

public class FirstResourceBaseXAResource extends BaseXAResource implements FirstResource {

    public FirstResourceBaseXAResource(TransactionAware transactionAware, XAResource xaResource, String jndiName) {
        super( transactionAware, xaResource, jndiName );
    }
}

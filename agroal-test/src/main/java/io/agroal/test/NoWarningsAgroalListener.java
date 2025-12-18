// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import io.agroal.api.AgroalDataSourceListener;

import static org.junit.jupiter.api.Assertions.fail;

public class NoWarningsAgroalListener implements AgroalDataSourceListener {

    @Override
    public void onWarning(String message) {
        fail( "Unexpected warning " + message );
    }

    @Override
    public void onWarning(Throwable throwable) {
        fail( "Unexpected warning " + throwable.getMessage() );
    }
}

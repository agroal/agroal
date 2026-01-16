// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.fakeserver;

public class LoginDoesNotComplete implements ServerBehavior {

    @Override
    public byte[] sendCompleteAuthentication(byte[] packet) {
        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return packet;
    }
}

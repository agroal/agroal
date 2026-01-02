// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.fakeserver;

import java.net.ServerSocket;

public class NotAcceptConnection implements ServerBehavior {

    public AcceptResult accept(ServerSocket serverSocket) throws Exception {
        return new AcceptResult(ProceedWith.BREAK, null);
    }
}

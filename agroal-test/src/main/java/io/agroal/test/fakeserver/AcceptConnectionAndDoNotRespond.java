// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.fakeserver;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class AcceptConnectionAndDoNotRespond implements ServerBehavior {

    protected Socket socket;

    @Override
    public void start(ServerSocket server) throws IOException {
        this.socket = server.accept();
    }

    @Override
    public void close() throws Exception {
        if(this.socket != null) {
            this.socket.close();
        }
    }
}
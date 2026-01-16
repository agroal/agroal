// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.fakeserver;

import java.io.IOException;
import java.net.ServerSocket;

public class MySqlFakeServer implements AutoCloseable, Runnable {

    private final ServerSocket server;
    private final int port;
    private final ServerBehavior behavior;

    public MySqlFakeServer(ServerBehavior behavior) throws IOException {
        this.behavior = behavior;
        server = new ServerSocket( 0 );
        port = server.getLocalPort();
    }

    @Override
    public void run() {
        try {
            behavior.start( server );
        } catch ( Throwable e ) {
            throw new RuntimeException( e );
        }
    }

    public int getPort() {
        return port;
    }

    @Override
    public void close() throws Exception {
        behavior.close();
        server.close();
    }
}

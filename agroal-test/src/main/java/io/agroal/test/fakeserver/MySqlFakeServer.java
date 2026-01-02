// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.fakeserver;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.logging.Logger.getLogger;

/**
 * Fake MySQL Server that implements the connection handshake protocol
 * Logs all protocol interactions in plaintext to the console
 */
public class MySqlFakeServer implements Runnable {

    private static final Logger logger = getLogger( MySqlFakeServer.class.getName() );

    private int connectionId = 1;
    private final int port;
    private final ServerSocket serverSocket;
    private final ExecutorService connectionExecutor;
    private final List<ConnectionProcess> connectionProcesses = new ArrayList<>();

    private final ServerBehavior behavior;

    public MySqlFakeServer(ServerBehavior behavior) throws IOException {
        this.behavior = behavior;
        this.connectionExecutor = Executors.newCachedThreadPool();
        this.serverSocket = new ServerSocket(0);
        this.port = serverSocket.getLocalPort();

    }

    public void run() {
        logger.info("MySql Fake Server is listening on port: " + port);
        try {
            while (true) {
                ServerBehavior.AcceptResult acceptResult = behavior.accept(serverSocket);
                if(ServerBehavior.ProceedWith.BREAK.equals(acceptResult.proceedWith())){
                    break;
                }

                if(ServerBehavior.ProceedWith.NEXT_ACCEPT.equals(acceptResult.proceedWith())){
                    continue;
                }
                Socket clientSocket = acceptResult.clientSocket();
                logger.info("[NEW CONNECTION] Client connected from: " + clientSocket.getInetAddress() + ":" + clientSocket.getPort());

                // Handle each connection in a separate thread
                MySqlFakeConnection connection = new MySqlFakeConnection(clientSocket, connectionId++, behavior);
                Future<?> future = connectionExecutor.submit(connection);
                connectionProcesses.add(new ConnectionProcess(connection, future));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int getPort() {
        return port;
    }

    public void close() {
        if(serverSocket != null && serverSocket.isClosed() && connectionExecutor != null && connectionExecutor.isShutdown()){
            return;
        }

        for (ConnectionProcess connectionProcess : connectionProcesses) {
            connectionProcess.connection.initShutdown();
        }

        for (ConnectionProcess connectionProcess : connectionProcesses) {
            connectionProcess.connection.close();
        }

        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error during server close: " + e.getClass().getSimpleName(), e);
            }
        }

        if(connectionExecutor != null ) {
            connectionExecutor.shutdown();
        }
        logger.info("MySql Fake Server was closed");
    }

    private record ConnectionProcess(MySqlFakeConnection connection, Future<?> future){}
}

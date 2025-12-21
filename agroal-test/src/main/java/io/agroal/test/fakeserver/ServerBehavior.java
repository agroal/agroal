// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.fakeserver;

import java.net.ServerSocket;
import java.net.Socket;

public interface ServerBehavior {

    default AcceptResult accept(ServerSocket serverSocket) throws Exception {
        Socket clientSocket = serverSocket.accept();
        return new AcceptResult(ProceedWith.CREATE_CONNECTION, clientSocket);
    }

    default byte[] sendInitialHandshake(byte[] packet) {
        return packet;
    }

    default byte[] receivedInitialHandshakeResponse(byte[] packet) {
        return packet;
    }

    default byte[] sendCompleteAuthentication(byte[] packet) {
        return packet;
    }

    default Command receivedCommandRequest(Command command) {
        return command;
    }

    default byte[] sendCommandResponse(byte[] packet) {
        return packet;
    }

    record Command(int commandType, String commandName, String commandText) {}
    record AcceptResult(ProceedWith proceedWith, Socket clientSocket) {}

    enum ProceedWith {
        CREATE_CONNECTION,
        NEXT_ACCEPT,
        BREAK,
    }
}

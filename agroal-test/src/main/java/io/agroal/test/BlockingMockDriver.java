// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.util.Properties;


/**
 *  BlockingMockDriver simply blocks during the entire connection creation phase
 *  furthermore it is not interruptible from a .chancel() of the thread
 */
public class BlockingMockDriver implements MockDriver {

     @Override
     public Connection connect(String url, Properties info) {
         try(ServerSocket serverSocket = new ServerSocket(0)) {
             serverSocket.accept();
         } catch (IOException e) {
             throw new RuntimeException(e);
         }
         return new MockConnection.Empty();
     }

}

// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import io.agroal.test.fakeserver.MySqlFakeServer;
import io.agroal.test.fakeserver.ServerBehavior;
import org.junit.platform.commons.util.StringUtils;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MySQLConnectMockDriver implements MockDriver {

    private final ServerBehavior behavior;

    public MySQLConnectMockDriver(ServerBehavior behavior) {
        this.behavior = behavior;
    }

    @Override
    public Connection connect(String url, Properties info) {
        ExecutorService executor = null;
        MySqlFakeServer server = null;
        try {
            executor = Executors.newCachedThreadPool();
            server = new MySqlFakeServer(behavior);
            executor.submit(server);

            Driver driver = DriverManager.drivers().filter( d -> d.getClass().getName().startsWith( "com.mysql" ) ).findFirst().orElseThrow();
            Connection connection = driver.connect( "jdbc:mysql://" + "localhost:" + server.getPort() + "/test", info );
            server.close();

            return new MockConnection.Empty();
            //return connection;

        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }finally {
            if(server != null) {
                server.close();
            }
            if(executor != null){
                executor.shutdown();
            }
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return StringUtils.isBlank( url );
    }
}

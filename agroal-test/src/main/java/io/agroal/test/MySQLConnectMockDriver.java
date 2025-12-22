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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

public class MySQLConnectMockDriver implements MockDriver {
     private final ServerBehavior behavior;
     private final Duration connectTimeout;
     private final Duration socketTimeout;

    public MySQLConnectMockDriver(ServerBehavior behavior) {
        this(behavior, Duration.ofSeconds(0), Duration.ofSeconds(0));
    }

     public MySQLConnectMockDriver(ServerBehavior behavior, Duration connectTimeout, Duration socketTimeout) {
         this.behavior = behavior;
         this.connectTimeout = connectTimeout;
         this.socketTimeout = socketTimeout;
     }

     @Override
     public Connection connect(String url, Properties info) {
         try (var server = new MySqlFakeServer(this.behavior)) {
             Executors.newSingleThreadExecutor().execute(server);
             String hostAndPort = "localhost:" + server.getPort();

             List<String> params = new ArrayList<>();
             if (connectTimeout != null) {
                 params.add("connectTimeout="+ connectTimeout.toMillis());
             }
             if (socketTimeout != null) {
                 params.add("socketTimeout="+ socketTimeout.toMillis());
             }

             String connParams = "";
             if(!params.isEmpty()){
                 connParams = "?" + String.join("&", params);
             }

             String connUrl = "jdbc:mysql://"+hostAndPort+"/test" + connParams;
             Driver driver = DriverManager.drivers().filter(d -> d.getClass().getName().startsWith( "com.mysql" ) ).findFirst().orElseThrow();
             return driver.connect(connUrl, info);
         } catch (Exception e) {
             throw new RuntimeException(e);
         }
     }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return StringUtils.isBlank(url);
    }
}

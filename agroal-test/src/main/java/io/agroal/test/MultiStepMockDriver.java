// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class  MultiStepMockDriver implements MockDriver {

    private final List<Driver> drivers;

    public MultiStepMockDriver(List<Driver> drivers) {
        this.drivers = Collections.synchronizedList(new ArrayList<>());
        this.drivers.addAll(drivers);
    }

    @Override
    public Connection connect( String url, Properties info ) throws SQLException {
        Driver driver;
        try{
            driver = drivers.remove(0);
        }catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("No Driver found");
        }
        logger.info("Start Connect with " + driver.getClass().getSimpleName());

        var result = driver.connect(url, info);

        logger.info("Finished Connect with " + driver.getClass().getSimpleName());
        return result;
    }
}

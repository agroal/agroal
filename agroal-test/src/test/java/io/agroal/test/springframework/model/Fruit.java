// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Entity
public class Fruit {

    @Id
    @GeneratedValue
    @SuppressWarnings( "unused" )
    private Long id;

    private String name;

    private String color;

    public Fruit() {
    }

    public Fruit(String name, String color) {
        this.name = name;
        this.color = color;
    }

    @Override
    public String toString() {
        return "Fruit name:" + name + " color:" + color;
    }
}

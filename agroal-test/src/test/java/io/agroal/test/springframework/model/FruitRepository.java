// Copyright (C) 2020 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework.model;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
@Repository
public interface FruitRepository extends JpaRepository<Fruit, Long> {

    List<Fruit> findByColor(String color);
}

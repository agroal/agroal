// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.security;

import java.util.Properties;

/**
 * Interface to be implemented in order to extend Agroal with custom types of authentication.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface AgroalSecurityProvider {

    /**
     * Converts a custom principal / credential objects to properties to be passed to the JDBC driver.
     * @return null if not capable of handle the security object, otherwise return a {@link Properties} object even if empty.
     */
    Properties getSecurityProperties(Object securityObject);
}

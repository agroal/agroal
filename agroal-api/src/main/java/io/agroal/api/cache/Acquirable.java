// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.cache;

/**
 * Contract for objects that can be acquired.
 *
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface Acquirable {

    /**
     * Attempts to acquire this object.
     *
     * @return true on successful object acquisition, false otherwise.
     */
    boolean acquire();

    /**
     * This method signals if the object can't be acquired in future calls to acquire().
     *
     * @return true if this object can eventually be acquired in the future.
     */
    boolean isAcquirable();
}

// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.api.security;

import java.io.Serializable;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public class SimplePassword implements Serializable {

    private static final long serialVersionUID = -3903625768428915058L;

    private final String word;

    public SimplePassword(String password) {
        this.word = password;
    }

    public SimplePassword(char[] password) {
        this.word = new String( password );
    }

    public String getWord() {
        return word;
    }

    // --- //

    @Override
    public boolean equals(Object o) {
        if ( this == o ) {
            return true;
        }
        if ( !( o instanceof SimplePassword ) ) {
            return false;
        }

        SimplePassword that = (SimplePassword) o;
        return word == null ? that.word == null : word.contentEquals( that.word );
    }

    @Override
    public int hashCode() {
        return word == null ? 7 : word.hashCode();
    }

    @Override
    public String toString() {
        return "*** masked ***";
    }
}

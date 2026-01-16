package io.agroal.test;

import io.agroal.api.AgroalDataSourceListener;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConnectionListener implements AgroalDataSourceListener {

    AtomicInteger createdConnections = new AtomicInteger();
    AtomicInteger beforeConnectionCreation = new AtomicInteger();

    @Override
    public void onConnectionCreation(Connection connection) {
        createdConnections.getAndIncrement();
    }

    @Override
    public void beforeConnectionCreation() {
        beforeConnectionCreation.getAndIncrement();
    }

    public Integer getCreatedConnections() {
        return createdConnections.get();
    }

    public void assertConnectionCreated() {
        assertEquals( 1, getCreatedConnections() );
    }

    public void assertNoConnectionCreated() {
        assertEquals( 0, getCreatedConnections() );
    }

    public Integer getStartedConnectionCreations() {
        return beforeConnectionCreation.get();
    }

    public void assertConnectionCreationStarted() {
        assertEquals( 1, getStartedConnectionCreations() );
    }

    public void assertNoConnectionCreationStarted() {
        assertEquals( 0, getStartedConnectionCreations() );
    }

    public void reset() {
        createdConnections.set( 0 );
        beforeConnectionCreation.set( 0 );
    }
}

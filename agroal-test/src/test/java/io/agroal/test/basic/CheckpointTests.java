package io.agroal.test.basic;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.pool.DataSource;
import io.agroal.pool.Pool;
import io.agroal.test.MockConnection;
import org.crac.Resource;
import org.junit.jupiter.api.*;

import java.lang.reflect.Field;
import java.sql.Connection;

import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.MockDriver.deregisterMockDriver;
import static io.agroal.test.MockDriver.registerMockDriver;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag( FUNCTIONAL )
public class CheckpointTests {
    @BeforeAll
    static void setupMockDriver() {
        registerMockDriver( FakeConnection.class );
    }

    @AfterAll
    static void teardown() {
        deregisterMockDriver();
    }

    // --- //

    @Test
    @DisplayName( "ConnectionPool C/R" )
    void checkpointRestoreTest() throws Exception {
        try ( AgroalDataSource dataSource = AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier().connectionPoolConfiguration(cp -> cp.maxSize( 1 ) ) ) ) {
            Connection connection = dataSource.getConnection();

            assertFalse( connection.isClosed(), "Expected open connection, but it's closed" );

            Field cpField = DataSource.class.getDeclaredField("connectionPool");
            cpField.setAccessible(true);
            Pool pool = (Pool) cpField.get(dataSource);
            assertInstanceOf(Resource.class, pool);
            Resource poolResource = (Resource) pool;

            poolResource.beforeCheckpoint(null);

            assertTrue( connection.isClosed(), "Expected closed connection, but it's open" );

            poolResource.afterRestore(null);

            Connection another = dataSource.getConnection();
            assertFalse( another.isClosed(), "Expected open connection, but it's closed" );
        }
    }

    public static class FakeConnection implements MockConnection {
        private boolean closed;

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }
    }

}

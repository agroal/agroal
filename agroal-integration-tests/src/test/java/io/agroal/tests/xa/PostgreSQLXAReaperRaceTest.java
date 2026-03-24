package io.agroal.tests.xa;

import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * XA reaper race integration test against PostgreSQL using Testcontainers.
 * Uses pg_sleep() for slow SQL that holds the read lock past the transaction timeout.
 */
@Tag( "testcontainers" )
@Testcontainers
@BMUnitConfig(debug = true)
class PostgreSQLXAReaperRaceTest extends XAReaperRaceTestBase {

    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer( "postgres:17-alpine" );

    @Override
    String xaDataSourceClassName() {
        return "org.postgresql.xa.PGXADataSource";
    }

    @Override
    JdbcDatabaseContainer<PostgreSQLContainer>  container() {
        return postgres;
    }

    @Override
    String slowSQL() {
        return "SELECT pg_sleep(4)";
    }
}

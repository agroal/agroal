package io.agroal.tests;

import org.junit.jupiter.api.Tag;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * XA reaper race integration test against PostgreSQL using Testcontainers.
 * Uses pg_sleep() for slow SQL that holds the read lock past the transaction timeout.
 */
@Tag( "testcontainers" )
@Testcontainers
class PostgreSQLXAReaperRaceIT extends XAReaperRaceITBase {

    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer( "postgres:17-alpine" );

    @Override
    String xaDataSourceClassName() {
        return "org.postgresql.xa.PGXADataSource";
    }

    @Override
    String jdbcUrl() {
        return postgres.getJdbcUrl();
    }

    @Override
    String username() {
        return postgres.getUsername();
    }

    @Override
    String password() {
        return postgres.getPassword();
    }

    @Override
    String slowSQL() {
        return "SELECT pg_sleep(4)";
    }
}

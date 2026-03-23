package io.agroal.tests;

import org.junit.jupiter.api.Tag;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mariadb.MariaDBContainer;

/**
 * XA reaper race integration test against MariaDB using Testcontainers.
 * Uses SLEEP() for slow SQL that holds the read lock past the transaction timeout.
 */
@Tag( "testcontainers" )
@Testcontainers
class MariaDBXAReaperRaceIT extends XAReaperRaceITBase {

    @Container
    static MariaDBContainer mariadb = new MariaDBContainer( "mariadb:11" );

    @Override
    String xaDataSourceClassName() {
        return "org.mariadb.jdbc.MariaDbDataSource";
    }

    @Override
    String jdbcUrl() {
        return mariadb.getJdbcUrl();
    }

    @Override
    String username() {
        return mariadb.getUsername();
    }

    @Override
    String password() {
        return mariadb.getPassword();
    }

    @Override
    String slowSQL() {
        return "SELECT SLEEP(4)";
    }

    @Override
    String createTableDDL() {
        return "CREATE TABLE IF NOT EXISTS xa_reaper_test ( id INT PRIMARY KEY, val VARCHAR(100) ) ENGINE=InnoDB";
    }
}

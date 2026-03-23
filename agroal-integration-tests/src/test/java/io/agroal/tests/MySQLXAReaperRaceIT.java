package io.agroal.tests;

import org.junit.jupiter.api.Tag;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

/**
 * XA reaper race integration test against MySQL using Testcontainers.
 * Uses SLEEP() for slow SQL that holds the read lock past the transaction timeout.
 */
@Tag( "testcontainers" )
@Testcontainers
class MySQLXAReaperRaceIT extends XAReaperRaceITBase {

    @Container
    static MySQLContainer mysql = new MySQLContainer( "mysql:8.0" );

    @Override
    String xaDataSourceClassName() {
        return "com.mysql.cj.jdbc.MysqlXADataSource";
    }

    @Override
    String jdbcUrl() {
        return mysql.getJdbcUrl();
    }

    @Override
    String username() {
        return mysql.getUsername();
    }

    @Override
    String password() {
        return mysql.getPassword();
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

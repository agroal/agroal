package io.agroal.tests.xa;

import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mariadb.MariaDBContainer;

/**
 * XA reaper race integration test against MariaDB using Testcontainers.
 * Uses SLEEP() for slow SQL that holds the read lock past the transaction timeout.
 */
@Tag( "testcontainers" )
@Testcontainers
@BMUnitConfig(debug = true)
class MariaDBXAReaperRaceTest extends XAReaperRaceTestBase {

    @Container
    static MariaDBContainer mariadb = new MariaDBContainer( System.getProperty( "mariadb.testcontainer.image", "mariadb:11" ) );

    @Override
    String xaDataSourceClassName() {
        return "org.mariadb.jdbc.MariaDbDataSource";
    }

    @Override
    JdbcDatabaseContainer<MariaDBContainer> container() {
        return mariadb;
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

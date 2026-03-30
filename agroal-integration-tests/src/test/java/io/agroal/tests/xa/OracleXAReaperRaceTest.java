package io.agroal.tests.xa;

import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

/**
 * XA reaper race integration test against Oracle Free using Testcontainers.
 * Uses DBMS_SESSION.SLEEP() for slow SQL that holds the read lock past the transaction timeout.
 *
 * Oracle XA requires specific SYS grants (dba_pending_transactions, dbms_xa, etc.).
 * These are installed via an init script copied to the container entrypoint directory.
 *
 * Oracle's XAResource.end() implementation:
 *   - Sends an OCI-level xa_end() to the database
 *   - setTransactionTimeout() returns false — no server-side timeout propagation
 *   - Behavior matches PostgreSQL/MySQL/MariaDB: the read lock blocks end(TMFAIL),
 *     the slow SQL completes normally, and the connection is poisoned after end()
 */
@Tag( "testcontainers" )
@Testcontainers
@BMUnitConfig(debug = true)
class OracleXAReaperRaceTest extends XAReaperRaceTestBase {

    @Container
    static OracleContainer oracle = new OracleContainer( System.getProperty( "oracle.testcontainer.image", "gvenzl/oracle-free:slim-faststart" ) )
            .withUsername( "test" )
            .withPassword( "test" )
            .withCopyToContainer(
                    MountableFile.forClasspathResource( "oracle-xa-init.sql" ),
                    "/container-entrypoint-initdb.d/01-xa-setup.sql" );

    @Override
    String xaDataSourceClassName() {
        return "oracle.jdbc.xa.client.OracleXADataSource";
    }

    @Override
    JdbcDatabaseContainer<OracleContainer>  container() {
        return oracle;
    }

    @Override
    String slowSQL() {
        return "BEGIN DBMS_SESSION.SLEEP(4); END;";
    }

    @Override
    String createTableDDL() {
        return "BEGIN EXECUTE IMMEDIATE 'CREATE TABLE xa_reaper_test ( id NUMBER(10) PRIMARY KEY, val VARCHAR2(100) )'; " +
               "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;";
    }
}

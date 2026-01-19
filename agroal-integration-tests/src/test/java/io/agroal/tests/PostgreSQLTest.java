package io.agroal.tests;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration.ConnectionValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

@Tag("testcontainers")
@Testcontainers
public class PostgreSQLTest {

    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    AgroalDataSource datasource;

    @BeforeEach
    void setupDataSource() throws SQLException {
        this.datasource = Datasources.createDataSource(postgres);
    }

    @Test
    void shouldConnect() {
        assertThatNoException().isThrownBy(() -> datasource.getConnection().close());
    }

    @Test
    void testConnectionValidatorTest() throws SQLException {
        try (var connection = datasource.getConnection()) {
            assertThat(ConnectionValidator.sqlValidator("select pg_sleep(2)", 5).isValid(connection)).isTrue();
            assertThat(ConnectionValidator.sqlValidator("select pg_sleep(3)", 2).isValid(connection)).isFalse();
        }
    }

    @AfterEach
    void closeDataSource() {
        if (this.datasource != null) {
            this.datasource.close();
        }
    }
}

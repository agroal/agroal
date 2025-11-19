package io.agroal.tests;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.SQLException;

public class PostgreSQLTest {
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    AgroalDataSource datasource;

    @BeforeAll
    static void startPostgreSQLDB() {
        postgres.start();
    }

    @BeforeEach
    void setupDataSource() throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration(cp -> cp
                        .maxSize(1)
                        .connectionFactoryConfiguration(cf -> cf
                                .connectionProviderClass(org.postgresql.Driver.class)
                                .jdbcUrl(postgres.getJdbcUrl())
                                .principal(new NamePrincipal(postgres.getUsername()))
                                .credential(new SimplePassword(postgres.getPassword()))
                        )
                );
        this.datasource = AgroalDataSource.from(configurationSupplier);
    }


    @Test
    void shouldConnect() {
        Assertions.assertThatCode(() -> datasource.getConnection()).doesNotThrowAnyException();
    }

    @AfterEach
    void closeDataSource() {
        if (this.datasource != null) {
            this.datasource.close();
        }
    }

    @AfterAll
    static void stopPostgreSQLDB() {
        postgres.stop();
    }

}

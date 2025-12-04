package io.agroal.tests;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.sql.SQLException;

/**
 * Utility class for creating Agroal DataSources for tests
 */
class Datasources {

    private Datasources() {
        throw new IllegalAccessError("Utility class");
    }

    /**
     * Create an AgroalDataSource from a Testcontainers JDBC Database Container
     *
     * @param container
     * @return
     * @throws SQLException
     */
    public static AgroalDataSource createDataSource(JdbcDatabaseContainer container) throws SQLException {
        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration(cp -> cp
                        .maxSize(1)
                        .connectionFactoryConfiguration(cf -> cf
                                .connectionProviderClassName(container.getDriverClassName())
                                .jdbcUrl(container.getJdbcUrl())
                                .principal(new NamePrincipal(container.getUsername()))
                                .credential(new SimplePassword(container.getPassword()))
                        )
                );
        return AgroalDataSource.from(configurationSupplier);
    }
}

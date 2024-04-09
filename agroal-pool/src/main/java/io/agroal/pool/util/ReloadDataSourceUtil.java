package io.agroal.pool.util;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class ReloadDataSourceUtil {
    public static final String AGROAL_JDBC_URL = "agroal_jdbc_url";
    public static final String AGROAL_USERNAME = "agroal_principal";
    public static final String AGROAL_PASSWORD = "agroal_credential";

    public static boolean checkConnectionWithNewCredentials(String url, String username, String password) {
        AgroalDataSourceConfigurationSupplier dataSourceConfiguration = new AgroalDataSourceConfigurationSupplier();
        AgroalConnectionPoolConfigurationSupplier poolConfiguration = dataSourceConfiguration.connectionPoolConfiguration();
        AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfiguration = poolConfiguration.connectionFactoryConfiguration();

        poolConfiguration
                .initialSize(1)
                .maxSize(3)
                .minSize(1)
                .maxLifetime(Duration.of(5, ChronoUnit.MINUTES))
                .acquisitionTimeout(Duration.of(30, ChronoUnit.SECONDS));

        connectionFactoryConfiguration
                .jdbcUrl(url)
                .principal(new NamePrincipal(username))
                .credential(new SimplePassword(password));

        AgroalDataSource datasource = null;
        try {
            datasource = AgroalDataSource.from(dataSourceConfiguration.get());
            if (datasource.isHealthy(true)) {
                return true;
            }
            datasource.flush(AgroalDataSource.FlushMode.ALL);
            return false;
        } catch (Exception ex) {
            return false;
        } finally {
            if (datasource != null) {
                datasource.flush(AgroalDataSource.FlushMode.ALL);
                datasource.close();
            }
        }
    }
}
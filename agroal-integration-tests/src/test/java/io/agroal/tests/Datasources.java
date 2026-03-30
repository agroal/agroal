package io.agroal.tests;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.agroal.narayana.NarayanaTransactionIntegration;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.sql.SQLException;
import java.time.Duration;

/**
 * Utility class for creating Agroal DataSources for tests
 */
public class Datasources {

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

    /**
     * Create an XA-capable AgroalDataSource from a Testcontainers JDBC Database Container
     */
    public static AgroalDataSource createXADataSource(JdbcDatabaseContainer container, String xaDataSourceClassName) throws SQLException {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry =
                new com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple();

        return AgroalDataSource.from( new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration( cp -> cp
                        .maxSize( 1 )
                        .acquisitionTimeout( Duration.ofSeconds( 10 ) )
                        .transactionIntegration( new NarayanaTransactionIntegration( txManager, txSyncRegistry ) )
                        .connectionFactoryConfiguration( cf -> cf
                                .connectionProviderClassName( xaDataSourceClassName )
                                .jdbcUrl(container.getJdbcUrl())
                                .principal(new NamePrincipal(container.getUsername()))
                                .credential(new SimplePassword(container.getPassword()))
                        )
                ));
    }


}

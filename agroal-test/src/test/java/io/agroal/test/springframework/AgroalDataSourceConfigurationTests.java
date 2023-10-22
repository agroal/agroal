package io.agroal.test.springframework;

import io.agroal.springframework.boot.AgroalDataSource;
import io.agroal.springframework.boot.AgroalDataSourceConfiguration;
import io.agroal.springframework.boot.metrics.AgroalDataSourcePoolMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.jdbc.metadata.DataSourcePoolMetadata;
import org.springframework.boot.jdbc.metadata.DataSourcePoolMetadataProvider;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;

import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AgroalDataSourceConfigurationTests {
    private final Class<?>[] autoconfigurationsInWrongOrder = new Class<?>[]{
            DataSourceAutoConfiguration.class,
            AgroalDataSourceConfiguration.class
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(autoconfigurationsInWrongOrder));

    @DisplayName("Autoconfigurations will trigger in the correct order and the beans will be registered and created")
    @Test
    void testAutoconfigureAgroalDataSource() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(AgroalDataSource.class);

            DataSource dataSource = context.getBean(AgroalDataSource.class);
            Map<String, DataSourcePoolMetadataProvider> metadataProviders = context.getBeansOfType(DataSourcePoolMetadataProvider.class);

            DataSourcePoolMetadata metadata = metadataProviders.values().stream()
                    .map(provider -> provider.getDataSourcePoolMetadata(dataSource))
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("AgroalDataSourcePoolMetaData not provided by any DataSourcePoolMetaDataProvider"));
            assertThat(metadata).isInstanceOf(AgroalDataSourcePoolMetadata.class);
        });
    }

    @DisplayName("Autoconfiguration will not trigger when AgroalDataSource is not on the classpath")
    @Test
    void testAutoconfigureAgroalDataSourceNotPresentOnClasspath() {
        runner
                .withClassLoader(new FilteredClassLoader(AgroalDataSource.class))
                .run(context -> assertThat(context).doesNotHaveBean(AgroalDataSourceConfiguration.class));
    }

    @DisplayName("The context will not start due to a cycle when forcing the wrong order of autoconfigurations")
    @Test
    void testAutoconfigurationWithCycle() {
        assertThatThrownBy(
                () -> runner.withConfiguration(AutoConfigurations.of(WrongOrderForcingAutoConfiguration.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("AutoConfigure cycle detected");
    }

    @AutoConfiguration(after = DataSourceAutoConfiguration.class, before = AgroalDataSourceConfiguration.class)
    private static class WrongOrderForcingAutoConfiguration {

    }
}

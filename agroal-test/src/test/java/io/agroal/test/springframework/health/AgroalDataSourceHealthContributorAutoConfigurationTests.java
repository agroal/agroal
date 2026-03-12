// Copyright (C) 2023 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test.springframework.health;

import io.agroal.springframework.boot.AgroalDataSource;
import io.agroal.springframework.boot.AgroalDataSourceAutoConfiguration;
import io.agroal.springframework.boot.health.AgroalDataSourceHealthContributorAutoConfiguration;
import io.agroal.springframework.boot.health.AgroalDataSourceHealthIndicator;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.util.ReflectionUtils;

import javax.sql.DataSource;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.agroal.test.AgroalTestGroup.SPRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@Tag( SPRING )
class AgroalDataSourceHealthContributorAutoConfigurationTests {

    private static final Logger LOG = LoggerFactory.getLogger(AgroalDataSourceHealthContributorAutoConfigurationTests.class);

    private final Class<?>[] autoconfigurationsInWrongOrder = new Class<?>[]{
            DataSourceAutoConfiguration.class,
            AgroalDataSourceAutoConfiguration.class,
            AgroalDataSourceHealthContributorAutoConfiguration.class
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(autoconfigurationsInWrongOrder));

    @DisplayName("AutoConfiguration will create AgroalDataSourceHealthIndicator")
    @Test
    void testHealthCheckEnabled() {
        runner.run(context -> {
                  assertThat(context).hasSingleBean(AgroalDataSourceHealthIndicator.class);

                  AgroalDataSourceHealthIndicator agroalDataSourceHealthIndicator = context.getBean(AgroalDataSourceHealthIndicator.class);
                  Health health = agroalDataSourceHealthIndicator.health(true);
                  LOG.info("Got health {}", health);
                  assertThat(health.getStatus()).isEqualTo(Status.UP);

                  AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);
                  try (Connection c = dataSource.getConnection()) {
                     LOG.info("Got connection {}", c);
                  }
              });
    }

    @DisplayName("AutoConfiguration will create AgroalDataSourceHealthIndicator for AgroalDataSource with strict transactionRequirement")
    @Test
    void testHealthCheckEnabledWithStrictTransactionRequirement() {
        runner.withPropertyValues("spring.datasource.agroal.transactionRequirement=strict")
                .run(context -> {
                    assertThat(context).hasSingleBean(AgroalDataSourceHealthIndicator.class);

                    AgroalDataSourceHealthIndicator agroalDataSourceHealthIndicator = context.getBean(AgroalDataSourceHealthIndicator.class);
                    Health health = agroalDataSourceHealthIndicator.health(true);
                    LOG.info("Got health {}", health);
                    assertThat(health.getStatus()).isEqualTo(Status.UP);

                    AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);
                    try (Connection connection = dataSource.getConnection()) {
                        fail("Unexpected got connection: " + connection);
                    } catch (SQLException e) {
                        LOG.info("Got SQLException: {}", e.getMessage());
                        assertThat(e.getMessage()).isEqualTo("Connection acquired without transaction.");
                    }
                });
    }

    @DisplayName("AutoConfiguration will create AgroalDataSourceHealthIndicator for wrapped AgroalDataSource")
    @Test
    void testHealthCheckEnabledWithWrappedAgroalDataSources() throws SQLException {
        AgroalDataSource ds = new AgroalDataSource();
        ds.setDriverClass(JdbcDataSource.class);
        ds.setUrl("jdbc:h2:mem:ds1;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
        ds.setUsername("sa");
        ds.afterPropertiesSet();

        runner.withBean("dataSource", DataSource.class, () -> new DelegatingDataSource(ds))
                .run(context ->  {
                    assertThat(context).hasSingleBean(AgroalDataSourceHealthIndicator.class);

                    AgroalDataSourceHealthIndicator agroalDataSourceHealthIndicator = context.getBean(AgroalDataSourceHealthIndicator.class);
                    Health health = agroalDataSourceHealthIndicator.health(true);
                    LOG.info("Got health {}", health);
                    assertThat(health.getStatus()).isEqualTo(Status.UP);

                    DataSource dataSource = context.getBean(DataSource.class);
                    try (Connection c = dataSource.getConnection()) {
                        LOG.info("Got connection {}", c);
                    }
                });
    }

    @DisplayName("AutoConfiguration will create HealthIndicator instances for AgroalDataSource and plain DataSource")
    @Test
    void testHealthCheckEnabledWithMultipleDataSources() {
        AgroalDataSource ds1 = new AgroalDataSource();
        ds1.setDriverClass(JdbcDataSource.class);
        ds1.setUrl("jdbc:h2:mem:ds1;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
        ds1.setUsername("sa");
        JdbcDataSource ds2 = new JdbcDataSource();
        ds2.setUrl("jdbc:h2:mem:ds2;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
        ds2.setUser("sa");

        runner.withBean("ds1", AgroalDataSource.class, () -> ds1)
                .withBean("ds2", DataSource.class, () -> ds2)
                .run(context ->  {
                    assertThat(context).hasSingleBean(CompositeHealthContributor.class);

                    CompositeHealthContributor compositeHealthContributor = context.getBean(CompositeHealthContributor.class);
                    assertThat(compositeHealthContributor.stream().count()).isEqualTo(2);
                    compositeHealthContributor.forEach(contributor -> {
                        Health health = ((HealthIndicator) contributor.contributor()).health(true);
                        LOG.info("Got health {}", health);
                        assertThat(health.getStatus()).isEqualTo(Status.UP);
                    });

                    for (DataSource dataSource : context.getBeanProvider(DataSource.class)) {
                        try (Connection c = dataSource.getConnection()) {
                            LOG.info("Got connection {}", c);
                        }
                    }
                });
    }

    @DisplayName("AutoConfiguration won't create AgroalDataSourceHealthIndicator")
    @Test
    void testHealthCheckDisabled() {
        runner.withPropertyValues("management.health.db.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(AgroalDataSourceHealthIndicator.class);

                    AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);
                    try (Connection c = dataSource.getConnection()) {
                        LOG.info("Got connection {}", c);
                    }
                });
    }

	@DisplayName("Health indicator caches database product name - only queries once")
	@Test
	void testHealthCheckDatabaseNameCaching() throws Exception {
		final AgroalDataSource ds = createAgroalDataSourceWithoutJdbcUrl(
				"jdbc:h2:mem:cachingtest;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
		final CountingDatabaseProductNameDataSource countingDataSource = new CountingDatabaseProductNameDataSource(ds);

		final AgroalDataSourceHealthIndicator healthIndicator = new AgroalDataSourceHealthIndicator(
				countingDataSource.getDataSource());

		final Health health1 = healthIndicator.health(true);
		final Health health2 = healthIndicator.health(true);
		final Health health3 = healthIndicator.health(true);

		assertThat(health1.getDetails().get("database")).isEqualTo(health2.getDetails().get("database"));
		assertThat(health2.getDetails().get("database")).isEqualTo(health3.getDetails().get("database"));
		assertThat(health1.getDetails().get("database").toString()).isEqualTo("H2");

		assertThat(health1.getStatus()).isEqualTo(Status.UP);
		assertThat(health2.getStatus()).isEqualTo(Status.UP);
		assertThat(health3.getStatus()).isEqualTo(Status.UP);
		assertThat(countingDataSource.getDatabaseProductNameLookups()).isEqualTo(1);

		ds.close();
	}

	@DisplayName("Health indicator with jdbc-url does not use metadata fallback")
	@Test
	void testHealthCheckWithJdbcUrlDoesNotUseMetadataFallback() throws Exception {
		final AgroalDataSource ds = new AgroalDataSource();
		ds.setDriverClass(JdbcDataSource.class);
		ds.setUrl("jdbc:h2:mem:withjdbcurl;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
		ds.setUsername("sa");
		ds.afterPropertiesSet();

		final CountingDatabaseProductNameDataSource countingDataSource = new CountingDatabaseProductNameDataSource(ds);
		final AgroalDataSourceHealthIndicator healthIndicator = new AgroalDataSourceHealthIndicator(
				countingDataSource.getDataSource());

		final Health health = healthIndicator.health(true);

		assertThat(health.getStatus()).isEqualTo(Status.UP);
		assertThat(health.getDetails()).containsKey("database");
		assertThat(health.getDetails().get("database").toString()).contains("H2");
		assertThat(countingDataSource.getConnectionCalls()).isZero();
		assertThat(countingDataSource.getMetadataCalls()).isZero();
		assertThat(countingDataSource.getDatabaseProductNameLookups()).isZero();

		ds.close();
	}

	@DisplayName("Health indicator works with datasource configured WITHOUT jdbc-url (DB2-style with properties)")
	@Test
	void testHealthCheckWithoutJdbcUrl() {
		this.runner.withBean("dataSource", AgroalDataSource.class, () -> createAgroalDataSourceBeanWithoutJdbcUrl(
				"jdbc:h2:mem:nojdbcurl;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false")).run(context -> {
					assertThat(context).hasSingleBean(AgroalDataSourceHealthIndicator.class);

					final AgroalDataSourceHealthIndicator healthIndicator = context
							.getBean(AgroalDataSourceHealthIndicator.class);
					final Health health = healthIndicator.health(true);

					assertThat(health.getStatus()).isEqualTo(Status.UP);
					assertThat(health.getDetails()).containsKey("database");
					assertThat(health.getDetails().get("database").toString()).contains("H2").isNotEqualTo("UNKNOWN");

					final AgroalDataSource dataSource = context.getBean(AgroalDataSource.class);
					final String jdbcUrl = dataSource.getConfiguration().connectionPoolConfiguration()
							.connectionFactoryConfiguration().jdbcUrl();
					assertThat(jdbcUrl).isNullOrEmpty();
				});
	}

	private static AgroalDataSource createAgroalDataSourceWithoutJdbcUrl(String jdbcUrl) {
		final AgroalDataSource dataSource = createAgroalDataSourceBeanWithoutJdbcUrl(jdbcUrl);
		try {
			dataSource.afterPropertiesSet();
			return dataSource;
		} catch (final Exception e) {
			throw new IllegalStateException("Unable to initialize datasource for test", e);
		}
	}

	private static AgroalDataSource createAgroalDataSourceBeanWithoutJdbcUrl(String jdbcUrl) {
		final AgroalDataSource dataSource = new AgroalDataSource();
		dataSource.setDriverClass(JdbcDataSource.class);
		dataSource.setUsername("sa");
		dataSource.setJdbcProperties(Map.of("url", jdbcUrl));
		return dataSource;
	}

	private static final class CountingDatabaseProductNameDataSource {

		private final AtomicInteger connectionCalls = new AtomicInteger();
		private final AtomicInteger metadataCalls = new AtomicInteger();
		private final AtomicInteger databaseProductNameLookups = new AtomicInteger();
		private final io.agroal.api.AgroalDataSource dataSource;

		private CountingDatabaseProductNameDataSource(io.agroal.api.AgroalDataSource targetDataSource) {
			this.dataSource = (io.agroal.api.AgroalDataSource) Proxy.newProxyInstance(
					io.agroal.api.AgroalDataSource.class.getClassLoader(),
					new Class[]{io.agroal.api.AgroalDataSource.class}, (proxy, method, args) -> {
						if ("getConnection".equals(method.getName())) {
							this.connectionCalls.incrementAndGet();
							final Connection connection = (Connection) ReflectionUtils.invokeMethod(method,
									targetDataSource, args);
							return this.wrap(connection);
						}
						return ReflectionUtils.invokeMethod(method, targetDataSource, args);
					});
		}

		private io.agroal.api.AgroalDataSource getDataSource() {
			return this.dataSource;
		}

		private Connection wrap(Connection delegate) {
			return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class[]{Connection.class},
					(proxy, method, args) -> {
						if ("getMetaData".equals(method.getName()) && method.getParameterCount() == 0) {
							this.metadataCalls.incrementAndGet();
							final DatabaseMetaData metadata = delegate.getMetaData();
							return this.wrap(metadata);
						}
						return ReflectionUtils.invokeMethod(method, delegate, args);
					});
		}

		private DatabaseMetaData wrap(DatabaseMetaData delegate) {
			return (DatabaseMetaData) Proxy.newProxyInstance(DatabaseMetaData.class.getClassLoader(),
					new Class[]{DatabaseMetaData.class}, (proxy, method, args) -> {
						if ("getDatabaseProductName".equals(method.getName()) && method.getParameterCount() == 0) {
							this.databaseProductNameLookups.incrementAndGet();
						}
						return ReflectionUtils.invokeMethod(method, delegate, args);
					});
		}

		private int getDatabaseProductNameLookups() {
			return this.databaseProductNameLookups.get();
		}

		private int getConnectionCalls() {
			return this.connectionCalls.get();
		}

		private int getMetadataCalls() {
			return this.metadataCalls.get();
		}
	}
}

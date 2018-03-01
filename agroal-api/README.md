# Agroal

The natural database connection pool


## Sample API usage

```java
    AgroalDataSourceConfigurationSupplier configuration = new AgroalDataSourceConfigurationSupplier()
            .dataSourceImplementation( DataSourceImplementation.AGROAL )
            .metricsEnabled( false )
            .connectionPoolConfiguration( cp -> cp
                    .minSize( 5 )
                    .maxSize( 20 )
                    .initialSize( 10 )
                    .connectionValidator( defaultValidator() )
                    .acquisitionTimeout( ofSeconds( 5 ) )
                    .leakTimeout( ofSeconds( 5 ) )
                    .validationTimeout( ofSeconds( 50 ) )
                    .reapTimeout( ofSeconds( 500 ) )
                    .connectionFactoryConfiguration( cf -> cf
                            .jdbcUrl( "jdbc:h2:mem:test" )
                            .connectionProviderClassName( "org.h2.Driver" )
                            .autoCommit( false )
                            .jdbcTransactionIsolation( SERIALIZABLE )
                            .principal( new NamePrincipal( "username" ) )
                            .credential( new SimplePassword( "secret" ) )
                    )
            );

    try ( AgroalDataSource dataSource = AgroalDataSource.from( configuration ) ) {
        Connection connection = dataSource.getConnection();
        connection.close();
    } catch ( SQLException e ) {
        System.out.println( "Oops! " + e.getMessage() );
    }
```

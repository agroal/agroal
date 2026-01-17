module io.agroal.api {
    requires java.security.jgss;
    requires transitive java.sql;
    requires java.transaction.xa;

    exports io.agroal.api;
    exports io.agroal.api.cache;
    exports io.agroal.api.configuration;
    exports io.agroal.api.configuration.supplier;
    exports io.agroal.api.exceptionsorter;
    exports io.agroal.api.security;
    exports io.agroal.api.transaction;

    uses io.agroal.api.AgroalDataSourceProvider;
}

module io.agroal.pool {
    requires transitive io.agroal.api;

    requires java.logging;
    requires java.transaction.xa;

    exports io.agroal.pool;
    exports io.agroal.pool.util;
    exports io.agroal.pool.wrapper;
}

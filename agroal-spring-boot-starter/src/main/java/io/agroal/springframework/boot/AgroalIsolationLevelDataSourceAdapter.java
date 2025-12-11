package io.agroal.springframework.boot;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.jdbc.DataSourceUnwrapper;
import org.springframework.jdbc.datasource.IsolationLevelDataSourceAdapter;

import io.agroal.api.AgroalDataSource;

public class AgroalIsolationLevelDataSourceAdapter extends IsolationLevelDataSourceAdapter {

    public AgroalIsolationLevelDataSourceAdapter(DataSource targetDataSource) {
        setTargetDataSource( targetDataSource );
    }

    @Override
    protected Connection doGetConnection(@Nullable String username, @Nullable String password) throws SQLException {
        AgroalDataSource agroalDataSource = DataSourceUnwrapper.unwrap( obtainTargetDataSource(), AgroalDataSource.class );
        if ( agroalDataSource != null ) {
            Boolean readOnlyToUse = getCurrentReadOnlyFlag();
            Connection con = readOnlyToUse == null ? agroalDataSource.getConnection() : agroalDataSource.getReadOnlyConnection();
            Integer isolationLevelToUse = getCurrentIsolationLevel();
            if ( isolationLevelToUse != null ) {
                con.setTransactionIsolation( isolationLevelToUse );
            }
            return con;
        }
        return super.doGetConnection( username, password );
    }
}

// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.agroal.test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

import static java.lang.System.identityHashCode;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public interface MockDatabaseMetaData extends DatabaseMetaData {

    @Override
    default boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    default boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    @Override
    default String getURL() throws SQLException {
        return null;
    }

    @Override
    default String getUserName() throws SQLException {
        return null;
    }

    @Override
    default boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    default boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    default boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    default boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    default boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    default String getDatabaseProductName() throws SQLException {
        return null;
    }

    @Override
    default String getDatabaseProductVersion() throws SQLException {
        return null;
    }

    @Override
    default String getDriverName() throws SQLException {
        return null;
    }

    @Override
    default String getDriverVersion() throws SQLException {
        return null;
    }

    @Override
    default int getDriverMajorVersion() {
        return 0;
    }

    @Override
    default int getDriverMinorVersion() {
        return 0;
    }

    @Override
    default boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    default boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    default boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    default boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    default boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    default boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    default boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    default boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    default String getIdentifierQuoteString() throws SQLException {
        return null;
    }

    @Override
    default String getSQLKeywords() throws SQLException {
        return null;
    }

    @Override
    default String getNumericFunctions() throws SQLException {
        return null;
    }

    @Override
    default String getStringFunctions() throws SQLException {
        return null;
    }

    @Override
    default String getSystemFunctions() throws SQLException {
        return null;
    }

    @Override
    default String getTimeDateFunctions() throws SQLException {
        return null;
    }

    @Override
    default String getSearchStringEscape() throws SQLException {
        return null;
    }

    @Override
    default String getExtraNameCharacters() throws SQLException {
        return null;
    }

    @Override
    default boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    default boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    default boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    default String getSchemaTerm() throws SQLException {
        return null;
    }

    @Override
    default String getProcedureTerm() throws SQLException {
        return null;
    }

    @Override
    default String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    default boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    default String getCatalogSeparator() throws SQLException {
        return null;
    }

    @Override
    default boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    default int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    default boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    default int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    default int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    default int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    default boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    default boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    default boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    default boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    default ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getSchemas() throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getCatalogs() throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getTableTypes() throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getTypeInfo() throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    default boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    default boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    default boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    default boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    default boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    default boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    default boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    default boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    default boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    default boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    default boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    default ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default Connection getConnection() throws SQLException {
        return new MockConnection.Empty();
    }

    @Override
    default boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    default ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    default int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    default int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    default int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    default int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    default int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    default int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    default boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    default boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    default RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    @Override
    default ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    default boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    default ResultSet getClientInfoProperties() throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return new MockResultSet.Empty();
    }

    @Override
    default boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    default long getMaxLogicalLobSize() throws SQLException {
        return DatabaseMetaData.super.getMaxLogicalLobSize();
    }

    @Override
    default boolean supportsRefCursors() throws SQLException {
        return DatabaseMetaData.super.supportsRefCursors();
    }

    @Override
    default boolean supportsSharding() throws SQLException {
        return DatabaseMetaData.super.supportsSharding();
    }

    @Override
    default <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    // --- //

    class Empty implements MockDatabaseMetaData {

        @Override
        public String toString() {
            return "MockDatabaseMetaDatat@" + identityHashCode( this );
        }
    }
}

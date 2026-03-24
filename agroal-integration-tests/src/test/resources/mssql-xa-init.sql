-- Install the JDBC XA stored procedures required for XA transactions.
-- Must run as SA before any XA enlistment is attempted.
EXEC sp_sqljdbc_xa_install;

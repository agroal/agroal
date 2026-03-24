-- Grant XA transaction privileges to the test user.
-- These run as SYS via the container entrypoint init mechanism.
GRANT SELECT ON sys.dba_pending_transactions TO test;
GRANT SELECT ON sys.pending_trans$ TO test;
GRANT SELECT ON sys.dba_2pc_pending TO test;
GRANT EXECUTE ON sys.dbms_xa TO test;
GRANT EXECUTE ON sys.dbms_session TO test;

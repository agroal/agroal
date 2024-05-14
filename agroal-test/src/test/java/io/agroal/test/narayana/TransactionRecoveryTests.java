package io.agroal.test.narayana;

import com.arjuna.ats.arjuna.AtomicAction;
import com.arjuna.ats.arjuna.common.Uid;
import com.arjuna.ats.arjuna.coordinator.ActionStatus;
import com.arjuna.ats.arjuna.coordinator.ActionType;
import com.arjuna.ats.arjuna.coordinator.RecordType;
import com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome;
import com.arjuna.ats.arjuna.objectstore.RecoveryStore;
import com.arjuna.ats.arjuna.objectstore.StateStatus;
import com.arjuna.ats.arjuna.objectstore.StoreManager;
import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import com.arjuna.ats.arjuna.state.OutputObjectState;
import com.arjuna.ats.internal.arjuna.common.UidHelper;
import com.arjuna.ats.internal.jta.recovery.arjunacore.RecoveryXids;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionSynchronizationRegistryImple;
import com.arjuna.ats.jbossatx.jta.RecoveryManagerService;
import com.arjuna.ats.jta.xa.RecoverableXAConnection;
import com.arjuna.ats.jta.xa.XidImple;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.narayana.NarayanaTransactionIntegration;
import io.agroal.narayana.XAExceptionUtils;
import io.agroal.test.MockXAConnection;
import io.agroal.test.MockXADataSource;
import io.agroal.test.MockXAResource;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.TransactionSynchronizationRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import static com.arjuna.ats.arjuna.recovery.RecoveryManager.DIRECT_MANAGEMENT;
import static io.agroal.test.AgroalTestGroup.FUNCTIONAL;
import static io.agroal.test.AgroalTestGroup.TRANSACTION;
import static java.lang.System.identityHashCode;
import static java.util.logging.Logger.getLogger;
import static javax.transaction.xa.XAException.XAER_RMERR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(FUNCTIONAL)
@Tag(TRANSACTION)
class TransactionRecoveryTests {

    static final Logger logger = getLogger(TransactionRecoveryTests.class.getName());

    OutputObjectState createDummyTransaction(Uid txUid, XidImple xid) throws IOException {
        OutputObjectState os = new OutputObjectState();
        os.packStringBytes("#ARJUNA#".getBytes(StandardCharsets.UTF_8));
        UidHelper.packInto(txUid, os);
        UidHelper.packInto(new Uid(), os);
        os.packLong(Instant.now().minus(Duration.ofHours(1)).toEpochMilli());
        os.packBoolean(true);

        os.packInt(RecordType.JTA_RECORD);
        os.packInt(TwoPhaseOutcome.FINISH_OK);
        os.packBoolean(false);
        XidImple.pack(os, xid);
        os.packInt(RecoverableXAConnection.OBJECT_RECOVERY);
        os.packString("productName");
        os.packString("productVersion");
        os.packString("jndiName");
        os.packBoolean(false);
        UidHelper.packInto(new Uid(), os);
        os.packString(null);

        os.packInt(RecordType.NONE_RECORD);
        os.packInt(0);
        os.packInt(ActionStatus.COMMITTED);
        os.packInt(ActionType.TOP_LEVEL);
        os.packInt(TwoPhaseOutcome.FINISH_OK);

        return os;
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    @DisplayName( "Transaction successful commit" )
    void testSuccessfulCommit(boolean recoveryCredentials) throws Exception {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new TransactionSynchronizationRegistryImple();

        com.arjuna.ats.arjuna.common.recoveryPropertyManager.getRecoveryEnvironmentBean().setRecoveryBackoffPeriod(1);
        RecoveryManager manager = RecoveryManager.manager(DIRECT_MANAGEMENT);
        RecoveryManagerService recoveryManagerService = new RecoveryManagerService();
        recoveryManagerService.create();

        RecoveryStore recoveryStore = StoreManager.getRecoveryStore();

        Uid txUid = new Uid();
        XidImple xid = new XidImple(txUid, true, 0);

        String typeName = new AtomicAction().type();
        OutputObjectState os = createDummyTransaction(txUid, xid);
        recoveryStore.write_committed(txUid, typeName, os);
        if (recoveryStore.currentState(txUid, typeName) == StateStatus.OS_COMMITTED) {
            logger.info("Writing dummy transaction " + txUid);
        }

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration(cp -> cp
                        .maxSize(1)
                        .transactionIntegration(new NarayanaTransactionIntegration(
                                txManager, txSyncRegistry,
                                "jndiName",
                                false, false,
                                recoveryManagerService))
                        .connectionFactoryConfiguration(cf -> cf
                                .recoveryPrincipal( recoveryCredentials ? new NamePrincipal( RecoveryDataSource.RECOVERY_USER ) : null )
                                .connectionProviderClass(RecoveryDataSource.class)));

        try (AgroalDataSource dataSource = AgroalDataSource.from(configurationSupplier)) {
            logger.info("Starting recovery on DataSource " + dataSource);
            RecoveryDataSource.xids.add(xid);
            manager.scan();

            assertTrue(RecoveryDataSource.xids.isEmpty(), "Expected XAResource to have been committed");
            assertEquals(recoveryCredentials ? 1 : 0, RecoveryDataSource.closed, "Expected one connection to have been closed");
            assertEquals(StateStatus.OS_UNKNOWN, recoveryStore.currentState(txUid, typeName), "Transaction not committed successfully!");
        } finally {
            manager.terminate(true);
        }
    }


    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    @DisplayName( "Transaction successful rollback" )
    void testSuccessfulRollback(boolean recoveryCredentials) throws Exception {
        TransactionManager txManager = com.arjuna.ats.jta.TransactionManager.transactionManager();
        TransactionSynchronizationRegistry txSyncRegistry = new TransactionSynchronizationRegistryImple();

        com.arjuna.ats.arjuna.common.recoveryPropertyManager.getRecoveryEnvironmentBean().setRecoveryBackoffPeriod(5);
        RecoveryXids.setSafetyIntervalMillis(1);
        RecoveryManager manager = RecoveryManager.manager(DIRECT_MANAGEMENT);
        RecoveryManagerService recoveryManagerService = new RecoveryManagerService();
        recoveryManagerService.create();

        Uid txUid = new Uid();
        XidImple xid = new XidImple(txUid, true, 0);

        AgroalDataSourceConfigurationSupplier configurationSupplier = new AgroalDataSourceConfigurationSupplier()
                .connectionPoolConfiguration(cp -> cp
                        .maxSize(1)
                        .transactionIntegration(new NarayanaTransactionIntegration(
                                txManager, txSyncRegistry,
                                "jndiName",
                                false, false,
                                recoveryManagerService))
                        .connectionFactoryConfiguration(cf -> cf
                                .recoveryPrincipal( recoveryCredentials ? new NamePrincipal( RecoveryDataSource.RECOVERY_USER ) : null )
                                .connectionProviderClass(RecoveryDataSource.class)));

        try (AgroalDataSource dataSource = AgroalDataSource.from(configurationSupplier)) {
            logger.info("Starting recovery on DataSource " + dataSource);
            RecoveryDataSource.xids.add(xid);
            manager.scan();

            assertTrue(RecoveryDataSource.xids.isEmpty(), "Expected XAResource to have been rollback");
            assertEquals(recoveryCredentials ? 1 : 0, RecoveryDataSource.closed, "Expected one connection to have been closed");
        } finally {
            manager.terminate(true);
        }
    }

    public static class RecoveryDataSource implements MockXADataSource {

        public static final String RECOVERY_USER = "recoveryUser";

        static final Set<Xid> xids = new HashSet<>();
        static int closed = 0;

        public RecoveryDataSource() {
            xids.clear();
            closed = 0;
        }

        @Override
        public XAConnection getXAConnection() throws SQLException {
            MyMockXAConnection connection = new MyMockXAConnection();
            logger.info("Creating a new XAConnection " + connection);
            return connection;
        }

        static class MyMockXAConnection implements MockXAConnection {
            boolean closed = false;

            @Override
            public void close() throws SQLException {
                logger.info("Closing connection " + this);
                this.closed = true;
                RecoveryDataSource.closed++;
            }

            @Override
            public XAResource getXAResource() throws SQLException {
                return new MockXAResource() {

                    @Override
                    public void commit(Xid xid, boolean b) throws XAException {
                        if (closed) {
                            throw XAExceptionUtils.xaException(XAER_RMERR, new SQLException("Closed connection!"));
                        }
                        if (!xids.remove(xid)) {
                            throw XAExceptionUtils.xaException(XAER_RMERR, new SQLException("Unknown XID " + xid + " !"));
                        }
                        logger.info("Committing on XAResource " + this);
                    }

                    @Override
                    public void rollback(Xid xid) throws XAException {
                        if (closed) {
                            throw XAExceptionUtils.xaException(XAER_RMERR, new SQLException("Closed connection!"));
                        }
                        if (!xids.remove(xid)) {
                            throw XAExceptionUtils.xaException(XAER_RMERR, new SQLException("Unknown XID " + xid + " !"));
                        }
                        logger.info("Rolling back on XAResource " + this);
                    }

                    @Override
                    public Xid[] recover(int flag) throws XAException {
                        logger.info("Recovering on XAResource " + this + " " + (flag == TMSTARTRSCAN ? "TMSTARTRSCAN" : flag == TMENDRSCAN ? "TMENDRSCAN" : flag));
                        return flag == TMSTARTRSCAN ? xids.toArray(new Xid[0]) : new Xid[0];
                    }

                    @Override
                    public String toString() {
                        return "MockXAResource@" + identityHashCode(this);
                    }
                };
            }

            @Override
            public String toString() {
                return "MockXAConnection@" + identityHashCode(this);
            }
        }
    }
}

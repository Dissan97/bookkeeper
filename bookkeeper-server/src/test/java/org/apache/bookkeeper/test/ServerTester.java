package org.apache.bookkeeper.test;

import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorWithOomHandler;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.Auditor;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.replication.ReplicationWorker;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;

public  class ServerTester {
        static final Logger LOG = LoggerFactory.getLogger(ServerTester.class);
        private final ServerConfiguration conf;
        private final TestStatsProvider provider;
        private final Bookie bookie;
        private final BookieServer server;
        private final BookieSocketAddress address;
        private final MetadataBookieDriver metadataDriver;
        private final RegistrationManager registrationManager;
        private final LedgerManagerFactory lmFactory;
        private final LedgerManager ledgerManager;
        private final LedgerStorage storage;
        private final ByteBufAllocatorWithOomHandler allocator;
        private AutoRecoveryMain autoRecovery;

        public ServerTester(ServerConfiguration conf) throws Exception {
                this(conf, true);
        }
        public ServerTester(ServerConfiguration conf, boolean isCluster) throws Exception {
                this.conf = conf;
                provider = new TestStatsProvider();

                StatsLogger rootStatsLogger = provider.getStatsLogger("");
                StatsLogger bookieStats = rootStatsLogger.scope(BOOKIE_SCOPE);
                if (isCluster) {
                        metadataDriver = BookieResources.createMetadataDriver(conf, bookieStats);
                }else {
                        metadataDriver = new NullMetadataBookieDriver();
                }
                registrationManager = metadataDriver.createRegistrationManager();
                lmFactory = metadataDriver.getLedgerManagerFactory();
                ledgerManager = lmFactory.newLedgerManager();
                allocator = BookieResources.createAllocator(conf);

                if (isCluster) {
                        LegacyCookieValidation cookieValidation = new LegacyCookieValidation(
                                conf, registrationManager);
                        cookieValidation.checkCookies(Main.storageDirectoriesFromConf(conf));
                }

                DiskChecker diskChecker = BookieResources.createDiskChecker(conf);
                LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                        conf, diskChecker, bookieStats.scope(LD_LEDGER_SCOPE));
                LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                        conf, diskChecker, bookieStats.scope(LD_INDEX_SCOPE), ledgerDirsManager);

                UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);

                storage = BookieResources.createLedgerStorage(
                        conf, ledgerManager, ledgerDirsManager, indexDirsManager,
                        bookieStats, allocator);

                if (conf.isForceReadOnlyBookie()) {
                        bookie = new ReadOnlyBookie(conf, registrationManager, storage,
                                diskChecker, ledgerDirsManager, indexDirsManager,
                                bookieStats, allocator, BookieServiceInfo.NO_INFO);
                } else {
                        bookie = new BookieImpl(conf, registrationManager, storage,
                                diskChecker, ledgerDirsManager, indexDirsManager,
                                bookieStats, allocator, BookieServiceInfo.NO_INFO);
                }
                server = new BookieServer(conf, bookie, rootStatsLogger, allocator,
                        uncleanShutdownDetection);
                address = BookieImpl.getBookieAddress(conf);

                autoRecovery = null;
        }

        public ServerTester(ServerConfiguration conf, Bookie b) throws Exception {
                this.conf = conf;
                provider = new TestStatsProvider();

                metadataDriver = null;
                registrationManager = null;
                ledgerManager = null;
                lmFactory = null;
                storage = null;
                allocator = BookieResources.createAllocator(conf);
                bookie = b;
                server = new BookieServer(conf, b, provider.getStatsLogger(""),
                        allocator, new MockUncleanShutdownDetection());
                address = BookieImpl.getBookieAddress(conf);

                autoRecovery = null;
        }

        public void startAutoRecovery() throws Exception {
                if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting Auditor Recovery for the bookie: {}", address);
                }
                autoRecovery = new AutoRecoveryMain(conf);
                autoRecovery.start();
        }

        public void stopAutoRecovery() {
                if (autoRecovery != null) {
                        if (LOG.isDebugEnabled()) {
                                LOG.debug("Shutdown Auditor Recovery for the bookie: {}", address);
                        }
                        autoRecovery.shutdown();
                }
        }

        public Auditor getAuditor() {
                if (autoRecovery != null) {
                        return autoRecovery.getAuditor();
                } else {
                        return null;
                }
        }

        public ReplicationWorker getReplicationWorker() {
                if (autoRecovery != null) {
                        return autoRecovery.getReplicationWorker();
                } else {
                        return null;
                }
        }

        public ServerConfiguration getConfiguration() {
                return conf;
        }

        public BookieServer getServer() {
                return server;
        }

        public TestStatsProvider getStatsProvider() {
                return provider;
        }

        public BookieSocketAddress getAddress() {
                return address;
        }

        public AutoRecoveryMain getAutoRecovery() {
                return autoRecovery;
        }

        public void shutdown() throws Exception {
                server.shutdown();

                if (ledgerManager != null) {
                        ledgerManager.close();
                }
                if (lmFactory != null) {
                        lmFactory.close();
                }
                if (registrationManager != null) {
                        registrationManager.close();
                }
                if (metadataDriver != null) {
                        metadataDriver.close();
                }

                if (autoRecovery != null) {
                        if (LOG.isDebugEnabled()) {
                                LOG.debug("Shutdown auto recovery for bookie server: {}", address);
                        }
                        autoRecovery.shutdown();
                }
        }
}

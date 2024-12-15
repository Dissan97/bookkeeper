package org.apache.bookkeeper.mock;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.UncleanShutdownDetection;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class LedgerHandleMock {
        private static LedgerHandleMock instance = null;
        private ZooKeeperServer zkServer;
        public final static String ZK_META_URI = "zk+hierarchical://127.0.0.1:2181/ledgers";
        private List<BookieServer> bookies;

        private TempDirs tempDirs;
        private LedgerHandleMock() throws Exception {
                this.tempDirs = new TempDirs();
                File zkDir = tempDirs.createNew("zookeeper", "test");
                this.bookies = new ArrayList<>();
                this.zkServer = new ZooKeeperServer(zkDir, zkDir, 2000);
                this.zkServer.startup();

                for (int i = 0; i < 3; i++) {
                        File bkDir =tempDirs.createNew("bookie", "test");
                        ServerConfiguration conf = new ServerConfiguration();
                        conf.setMetadataServiceUri(ZK_META_URI);
                        conf.setBookiePort(PortManager.nextFreePort());
                        conf.setJournalDirName(bkDir.getPath());
                        conf.setLedgerDirNames(new String[]{bkDir.getPath()});
                        BookieServer bookieServer = createBookieServer(conf);
                        bookieServer.start();
                        bookies.add(bookieServer);
                }
        }

        public static synchronized LedgerHandleMock getInstance() throws Exception {
                if (instance == null){
                        instance = new LedgerHandleMock();
                }
                return instance;
        }

        public void close() throws Exception {
                for (BookieServer bookie: this.bookies){
                        bookie.shutdown();
                }
                this.zkServer.shutdown();
                this.tempDirs.cleanup();

        }

        private static BookieServer createBookieServer(ServerConfiguration conf) throws
                Exception {
                Bookie mockBookie = Mockito.mock(Bookie.class);
                StatsLogger statsLogger = NullStatsLogger.INSTANCE;
                ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
                UncleanShutdownDetection uncleanShutdownDetection = Mockito.mock(UncleanShutdownDetection.class);

                // Initialize BookieServer
                return new BookieServer(conf, mockBookie, statsLogger, allocator, uncleanShutdownDetection);
        }



}

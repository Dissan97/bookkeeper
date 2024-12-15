package org.apache.bookkeeper.proto;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.conf.TestConfig;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.ServerTester;
import org.apache.bookkeeper.test.TestOutcome;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.util.ByteBufList;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.*;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

@RunWith(value = Enclosed.class)
public class PerChannelBookieClientTest {

        abstract static class AbstractPerChannelBookieTest {
                protected ServerTester serverTester = null;
                protected final AtomicBoolean startFailed = new AtomicBoolean(false);
                protected PerChannelBookieClient bookieClient = null;
                protected static final byte[] MASTER_KEY = "test".getBytes(TestConfig.TEST_CHARSET);
                protected static final byte[] ENTRY = "test".getBytes(TestConfig.TEST_CHARSET);

                public AbstractPerChannelBookieTest(ServerConfiguration conf) throws Exception {
                        startBookie(conf);
                        setupBookieClient();
                        assert serverTester != null;
                        if (bookieClient == null){
                                startFailed.set(true);
                        }
                }

                /**
                 * starts a new bookie server for test
                 */
                abstract void startBookie(@NotNull ServerConfiguration conf) throws Exception;
                public abstract void doTest();

                abstract void setupBookieClient();
                @Before
                public abstract void setup();
                abstract @NotNull PerChannelBookieClient getBookieClient();

                @After
                public void tearDown() throws Exception {
                        this.serverTester.shutdown();
                }
        }

        @RunWith(value = Parameterized.class)
        public static class ReadEntryTest extends AbstractPerChannelBookieTest {


                private TestOutcome expectedOutcome;
                private long readLedgerId;
                private long readEntryId;
                private ReadEntryCallback rcb;
                private Object rCtx;
                private int readFlags;
                private byte[] rMasterKey;
                private boolean readAllowFirstFail;
                private final AtomicBoolean setupSuccess = new AtomicBoolean(false);



                public ReadEntryTest(long readEntryId) throws Exception {
                        super(TestBKConfiguration.newServerConfiguration());
                        this.readEntryId = 0;
                }

                @Parameterized.Parameters
                public static Collection<Object[]> params(){
                        return Arrays.asList(
                                new Object[][]{
                                        // null check
                                        {0}
                                }
                        );
                }

                @Override
                void startBookie(@NotNull ServerConfiguration conf) throws Exception {
                        super.serverTester = new ServerTester(conf, false);
                        super.serverTester.getServer().getBookie().getLedgerStorage().setMasterKey(
                                this.readLedgerId, AbstractPerChannelBookieTest.MASTER_KEY
                        );
                        super.serverTester.getServer().start();

                }



                @Override
                void setupBookieClient() {
                        try {
                                super.bookieClient = new PerChannelBookieClient(
                                        OrderedExecutor.newBuilder().build(),
                                        new NioEventLoopGroup(),
                                        super.serverTester.getServer().getBookieId(),
                                        BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER
                                        );
                        } catch (SecurityException | UnknownHostException e) {
                                LOG.info("hola {}", e.getClass().getName());
                        }
                }

                @Override
                @Before
                public void setup() {
                        if(this.setupSuccess.get()) {
                                ByteBuf byteBuf = Unpooled.buffer(ENTRY.length);
                                ByteBufList bufList = ByteBufList.get(byteBuf);
                                EnumSet<WriteFlag> wFlags = EnumSet.allOf(WriteFlag.class);
                                super.bookieClient.addEntry(
                                        0,
                                        AbstractPerChannelBookieTest.MASTER_KEY,
                                        0,
                                        bufList,
                                        mockedWriteCb(),
                                        new Object(),
                                        0,
                                        false,
                                        wFlags
                                        );
                        }
                }




                @Override
                @Test
                public void doTest() {
                        super.bookieClient.readEntry(
                                0,
                                0,
                                mockedReadCb(),
                                new Object(),
                                2,
                                "bad".getBytes(StandardCharsets.UTF_8),
                                true
                        );
                }

                @NotNull
                @Override
                PerChannelBookieClient getBookieClient() {
                        assert this.bookieClient != null;
                        return this.bookieClient;
                }


                private static ReadEntryCallback mockedReadCb(){
                        ReadEntryCallback readEntryCallback = mock(ReadEntryCallback.class);
                        doNothing().when(readEntryCallback).readEntryComplete(
                                isA(Integer.class), isA(Long.class), isA(Long.class), isA(ByteBuf.class),
                                isA(Object.class)
                        );
                        return readEntryCallback;
                }

                private static WriteCallback mockedWriteCb() {
                        WriteCallback writeCallback = mock(BookkeeperInternalCallbacks.WriteCallback.class);
                        doNothing().when(writeCallback).writeComplete(isA(Integer.class), isA(Long.class), isA(Long.class),
                                isA(BookieId.class), isA(Object.class));

                        return writeCallback;

                }
        }




}

package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.TestConfig;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestOutcome;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.bookkeeper.client.BookKeeper.DigestType;
@RunWith(value = Enclosed.class)
public abstract class EnclosedBookkeeperTest extends BookKeeperClusterTestCase {

        protected BookKeeper testClient;
        protected Logger log;
        public EnclosedBookkeeperTest() {
                super(TestConfig.BOOKIES);
                setupLogger();
        }

        private void setupLogger() {
                this.log = LoggerFactory.getLogger(
                        EnclosedBookkeeperTest.class.getName() +
                                '#'+this.getClass().getSimpleName()
                );
        }

        public abstract void doTest();

        @Override
        public void setUp() throws Exception {
                super.setUp();
                testClient = new BookKeeper(super.baseClientConf, super.zkc);
        }

        @Override
        public void tearDown() throws Exception {
                testClient.close();
                super.tearDown();
        }

        @RunWith(value = Parameterized.class)
        public static class CreateLedgerTest extends EnclosedBookkeeperTest {

                private final TestOutcome testOutcome;
                private final DigestType dt;
                private final byte[] password;

                public CreateLedgerTest(TestOutcome testOutcome, DigestType dt, byte[] password) {
                        this.testOutcome = testOutcome;
                        this.dt = dt;
                        this.password = password;
                }

                @Parameterized.Parameters
                public static Collection<Object[]> params(){
                        return Arrays.asList(
                                new Object[][]{
                                        {TestOutcome.NULL, null, TestConfig.LEDGER_PASSWORD},
                                        {TestOutcome.NULL, DigestType.DUMMY, null},
                                        {TestOutcome.VALID, DigestType.DUMMY, TestConfig.LEDGER_PASSWORD},
                                        {TestOutcome.VALID, DigestType.CRC32, TestConfig.LEDGER_PASSWORD},
                                        {TestOutcome.VALID, DigestType.CRC32C, TestConfig.LEDGER_PASSWORD},
                                        {TestOutcome.VALID, DigestType.MAC, TestConfig.LEDGER_PASSWORD}
                                }
                        );
                }
                @Test
                @Override
                public void doTest() {
                        boolean passed = false;
                        try {
                                LedgerHandle lh = super.testClient.createLedger(this.dt,
                                        this.password);

                                lh.close();

                                super.testClient.openLedger(
                                        0, this.dt,
                                        this.password
                                );
                                passed = this.testOutcome.equals(TestOutcome.VALID);
                        } catch (BKException e) {
                                log.info("testing exception {}",e.getCode());
                        }catch (NullPointerException e){
                                passed = this.testOutcome.equals(TestOutcome.NULL);
                        } catch (InterruptedException e) {
                                fail();
                        }
                        assertTrue(passed);
                }
        }

        @RunWith(value = Parameterized.class)
        public static class CreateLedger6ParamsTest extends EnclosedBookkeeperTest {

                private final TestOutcome testOutcome;
                private final int ensSize;
                private final int writeQSize;
                private final int ackQSize;
                private final DigestType dt;
                private final byte[] password;
                private final Map<String, byte[]> customMetadata;

                public CreateLedger6ParamsTest(TestOutcome testOutcome, int ensSize, int writeQSize, int ackQSize,
                                               DigestType dt, byte[] password,
                                               Map<String, byte[]> customMetadata) {
                        this.testOutcome = testOutcome;
                        this.ensSize = ensSize;
                        this.writeQSize = writeQSize;
                        this.ackQSize = ackQSize;
                        this.dt = dt;
                        this.password = password;
                        this.customMetadata = customMetadata;
                }

                @Parameterized.Parameters
                public static Collection<Object[]> params(){
                        return Arrays.asList(
                                new Object[][]{
                                        {
                                                TestOutcome.NULL, 3, 3, 3,
                                                null, TestConfig.LEDGER_PASSWORD,
                                                Collections.emptyMap()
                                        },
                                        {
                                                TestOutcome.NULL, 3, 3, 3,
                                                DigestType.DUMMY, null,
                                                Collections.emptyMap()
                                        },
                                        // server side exception
                                        {
                                                TestOutcome.SERVER_ISSUE, 0, 3, 3,
                                                DigestType.CRC32, TestConfig.LEDGER_PASSWORD,
                                                Collections.emptyMap()
                                        },
                                        // illegal argument exception
                                        {

                                        },
                                        // valid test
                                        {
                                                TestOutcome.VALID, 3, 3, 3,
                                                DigestType.DUMMY, TestConfig.LEDGER_PASSWORD,
                                                Collections.emptyMap()
                                        },
                                        {
                                                TestOutcome.VALID, 3, 3, 3,
                                                DigestType.MAC, TestConfig.LEDGER_PASSWORD,
                                                Collections.emptyMap()
                                        },
                                        {
                                                TestOutcome.VALID, 3, 3, 3,
                                                DigestType.CRC32, TestConfig.LEDGER_PASSWORD,
                                                Collections.emptyMap()
                                        },
                                        {
                                                TestOutcome.VALID, 3, 3, 3,
                                                DigestType.CRC32C, TestConfig.LEDGER_PASSWORD,
                                                Collections.emptyMap()
                                        }
                                }
                        );
                }

                /**
                 * There is some problem with Bookkeeper server when passing esSize < 0 or esnSize = 0
                 * and ensSize <= writeQuorumSize
                 */
                private static final long MILLIS_TO_WAIT = 10000L;
                @Test
                @Override
                public void doTest() {
                        boolean passed = false;
                        try {
                                LedgerHandle lh;
                                if (this.testOutcome.equals(TestOutcome.SERVER_ISSUE)){
                                        AtomicReference<LedgerHandle> handle = new AtomicReference<>(null);
                                        Thread watcher = getWatcher(handle);
                                        watcher.join(MILLIS_TO_WAIT);
                                        watcher.interrupt();
                                        lh = handle.get();
                                        passed = lh == null;

                                }else {
                                        lh = createLedger(
                                                super.testClient, this.ensSize,
                                                this.writeQSize,
                                                this.ackQSize,
                                                this.dt,
                                                this.password,
                                                this.customMetadata
                                        );
                                        lh.close();

                                        super.testClient.openLedger(
                                                0, this.dt,
                                                this.password
                                        );
                                        passed = this.testOutcome.equals(TestOutcome.VALID);
                                }

                        } catch (BKException e) {
                                log.info("testing exception {}",e.getCode());
                        }catch (NullPointerException e){
                                passed = this.testOutcome.equals(TestOutcome.NULL);
                        } catch (InterruptedException e) {
                                fail();
                        }
                        assertTrue(passed);
                }

                @NotNull
                private Thread getWatcher(AtomicReference<LedgerHandle> handle) throws InterruptedException {

                        return new Thread(() -> {
                                try {
                                        handle.set(createLedger(
                                                super.testClient, this.ensSize,
                                                this.writeQSize,
                                                this.ackQSize,
                                                this.dt,
                                                this.password,
                                                this.customMetadata
                                        ));

                                } catch (BKException | InterruptedException ignored) {

                                }
                        });
                }

                public static LedgerHandle createLedger(BookKeeper client,int ensSize,
                                                  int wQSize,
                                                  int ackQSize,
                                                  DigestType dt,
                                                  byte[] password,
                                                  Map<String, byte[]> customMetadata)
                        throws BKException, InterruptedException {
                        return client.createLedger(
                                ensSize, wQSize, ackQSize,
                                dt,
                                password,
                                customMetadata);
                }
        }
}

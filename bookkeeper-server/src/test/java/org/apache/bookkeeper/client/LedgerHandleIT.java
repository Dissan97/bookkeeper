package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.TestConfig;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestOutcome;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(value = Parameterized.class)
public class LedgerHandleIT extends BookKeeperClusterTestCase {

        private static final int TEST_UTIL_NUM = 5;
        private BookKeeper bkClient;
        private final TestOutcome outcome;
        private BookKeeper client;
        private LedgerHandle handle;
        // for write num of failed bookies and for read num readers
        private final int logic;
        private final boolean isRead;

        public LedgerHandleIT(TestOutcome outcome, int logic, boolean isRead) {
                super(TEST_UTIL_NUM);
                this.outcome = outcome;
                this.logic = logic;
                this.isRead = isRead;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> params() {
                return Arrays.asList(
                        new Object[][]{
                                {TestOutcome.SERVER_ISSUE, TEST_UTIL_NUM, false},
                                {TestOutcome.VALID, TEST_UTIL_NUM - 1, false},
                                {TestOutcome.VALID, 1, false},
                                {TestOutcome.VALID, 0, false},
                                {TestOutcome.VALID, TEST_UTIL_NUM, true},
                                {TestOutcome.VALID, 1, true}
                        }
                );

        }


        @Before
        public void createLedger() throws BKException, IOException, InterruptedException {

                client = new BookKeeper(super.baseClientConf, super.zkc);
                handle = client.createLedger(TEST_UTIL_NUM, TEST_UTIL_NUM, TEST_UTIL_NUM, BookKeeper.DigestType.CRC32,
                        TestConfig.LEDGER_PASSWORD);


        }

        @Test
        public void testGuaranteeFailure() {
                if (isRead) {
                        assertTrue(checkRead());
                        return;
                }
                assertTrue(checkWrite());
        }

        private boolean checkWrite() {
                boolean passed = false;

                try {
                        final byte[] data = "data".getBytes();
                        handle.addEntry(data);
                        for (int i = 0; i < logic; i++) {

                                try {
                                        super.killBookie(0);
                                } catch (Exception e) {
                                        fail();
                                }

                        }
                        LoggerFactory.getLogger(this.getClass()).info("find this entry value {}",
                                new String(handle.readLastEntry().getEntry()));
                        passed = Arrays.equals(data, handle.readLastEntry().getEntry()) &&
                                outcome.equals(TestOutcome.VALID);
                } catch (InterruptedException e) {
                        fail();
                } catch (BKException e) {
                        if (this.logic == TEST_UTIL_NUM) {
                                LoggerFactory.getLogger(this.getClass()).info("the Exception is threw check code {}",
                                        e.getCode());
                                passed = e.getCode() == BKException.Code.BookieHandleNotAvailableException &&
                                        outcome.equals(TestOutcome.SERVER_ISSUE);
                        }
                }

                return passed;

        }

        private boolean checkRead() {
                if (logic < 1){
                        fail("should be greater than 1");
                }
                final List<byte[]> bytes = IntStream.range(0, TEST_UTIL_NUM)
                        .mapToObj(i -> ("data#" + i).getBytes()).collect(Collectors.toList());
                boolean passed = true;
                for (byte[] b : bytes) {
                        try {
                                handle.addEntry(b);
                        } catch (InterruptedException | BKException e) {
                                fail();
                        }
                }


                CountDownLatch latch = new CountDownLatch(logic);
                List<AtomicInteger> checks = IntStream.range(0, TEST_UTIL_NUM)
                        .mapToObj(i -> new AtomicInteger(0)).collect(Collectors.toList());
                ExecutorService service = Executors.newFixedThreadPool(logic);
                for (int i = 0; i < logic; i++) {
                        service.execute(() -> {
                                try {
                                        BookKeeper reader = new BookKeeper(super.baseClientConf, super.zkc);
                                        LedgerHandle readerHandle = reader.openLedger(0L,
                                                BookKeeper.DigestType.CRC32,
                                                TestConfig.LEDGER_PASSWORD);
                                        Enumeration<LedgerEntry> entries =
                                                readerHandle.readEntries(0, bytes.size() - 1);
                                        int counter = 0;
                                        while (entries.hasMoreElements()) {
                                                if (Arrays.equals(bytes.get(counter),
                                                        entries.nextElement().getEntry())) {
                                                        checks.get(counter).getAndIncrement();
                                                }
                                                counter++;
                                        }
                                        latch.countDown();
                                } catch (IOException | InterruptedException | BKException e) {
                                        fail();
                                }
                        });
                }

                try {
                        latch.await();
                        service.shutdown();
                } catch (InterruptedException e) {
                        fail();
                }
                for (AtomicInteger i : checks) {
                        if (i.get() != logic) {
                                passed = false;
                                break;
                        }
                }


                return passed;
        }

}

package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.conf.TestConfig;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestOutcome;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import static org.apache.bookkeeper.client.BookKeeper.DigestType;
import static org.apache.bookkeeper.client.SyncCallbackUtils.SyncAddCallback;
import static org.apache.bookkeeper.client.LedgerHandleTestUtil.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
@RunWith(value = Enclosed.class)
public class EnclosedLedgerHandleTest {

        public static final byte[] VALID_DATA = "test-data".getBytes(TestConfig.TEST_CHARSET);
        public static final int VALID_DATA_SIZE = VALID_DATA.length;

        private static final Logger LOG = LoggerFactory.getLogger(EnclosedLedgerHandleTest.class.getName());

        public static abstract class LedgerHandleTestAbstract extends BookKeeperClusterTestCase{
                protected final static byte[][] LH_DATA = IntStream.range(0, TestConfig.LEDGER_ENTRY_FOR_TEST)
                        .mapToObj(i -> (new String(VALID_DATA) + '#' + i).getBytes(TestConfig.TEST_CHARSET))
                        .toArray(byte[][]::new);
                protected LedgerHandle targetLh;
                protected BookKeeper testClient;
                public LedgerHandleTestAbstract(){
                        super(TestConfig.BOOKIES);
                }
                @Before
                public void setUpLedger() throws BKException, IOException, InterruptedException {
                        ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
                        testClient = new BookKeeper(conf, super.zkc);
                        targetLh = testClient.createLedger(DigestType.CRC32, TestConfig.LEDGER_PASSWORD);
                }
                @Test
                public abstract void doTest();
                @After
                public void tearDownLedger() throws BKException, InterruptedException {
                        targetLh.close();
                        testClient.close();
                }

                protected static boolean readSuccessCheck(Enumeration<LedgerEntry> entries, int firstEntry){
                        int i = firstEntry;
                        while (entries.hasMoreElements()){

                                if (!Arrays.equals(
                                        entries.nextElement().getEntry(),
                                        LH_DATA[i++]
                                )){
                                        return false;

                                }
                        }
                        return true;
                }
        }

        @RunWith(value = Parameterized.class)
        public static class AddEntryTest extends LedgerHandleTestAbstract {


                private final TestOutcome expectedOutcome;
                private final byte[] data;
                private final int offset;
                private final int length;

                public AddEntryTest(TestOutcome expectedOutcome, byte[] data, int offset, int length){
                        this.expectedOutcome = expectedOutcome;
                        this.data = data;
                        this.offset = offset;
                        this.length = length;
                }

                /**
                 * parameters for testing addEntry synchronous
                 * @return Collection {TestOutCome, byte[], int, int}
                 */
                @Parameterized.Parameters
                public static Collection<Object[]> params(){
                        return Arrays.asList(
                          new Object[][]{
                                  // null check
                                  {TestOutcome.NULL, null, 0, 0},
                                  // outOfRangeChecks
                                  {TestOutcome.OUT_OF_INDEX, VALID_DATA, 0, VALID_DATA_SIZE + 1},
                                  {TestOutcome.OUT_OF_INDEX, VALID_DATA, 1, VALID_DATA_SIZE},
                                  {TestOutcome.OUT_OF_INDEX, VALID_DATA, VALID_DATA_SIZE, 1},
                                  {TestOutcome.OUT_OF_INDEX, VALID_DATA, -1, 0},
                                  {TestOutcome.OUT_OF_INDEX, VALID_DATA, VALID_DATA_SIZE, -1},
                                  //valid checks
                                  {TestOutcome.VALID, VALID_DATA, 0, VALID_DATA_SIZE},
                                  {TestOutcome.VALID, VALID_DATA, 0, 0},
                                  {TestOutcome.VALID, VALID_DATA, 1, VALID_DATA_SIZE - 1},
                                  {TestOutcome.VALID, VALID_DATA, VALID_DATA_SIZE, 0},

                          }
                        );
                }


                @Test
                @Override
                public void doTest(){
                        boolean passed = false;
                        try {
                                LOG.info("before addEntry the length {}", targetLh.length);
                                long entry = targetLh.addEntry(this.data, this.offset, this.length);
                                LOG.info("after the addEntry length {}", targetLh.length);
                                passed = entry == 0 && this.length == targetLh.length;


                        } catch (InterruptedException | BKException e) {
                                LOG.info("some exception got catch {}", e.getClass().getName());
                                fail();
                        } catch (ArrayIndexOutOfBoundsException e){
                                passed = expectedOutcome.equals(TestOutcome.OUT_OF_INDEX);
                        }catch (NullPointerException e){
                                passed = expectedOutcome.equals(TestOutcome.NULL);
                        }
                        assertTrue(passed);
                }



        }

        @RunWith(value = Parameterized.class)
        public static class AsyncAddEntryTest extends LedgerHandleTestAbstract {

                private final TestOutcome expectedOutcome;
                private final byte[] data;
                private final int offset;
                private final int length;
                private final SyncAddCallback cb;
                private final Object ctx;

                public AsyncAddEntryTest(@NotNull TestOutcome expectedOutcome, @Nullable byte[] data, int offset,
                                         int length, boolean validCallback, @Nullable Object ctx) {
                        this.expectedOutcome = expectedOutcome;
                        this.data = data;
                        this.offset = offset;
                        this.length = length;
                        this.cb = validCallback ? new SyncAddCallback() : null;
                        this.ctx = ctx;
                }

                /**
                 * parameters for testing addEntry asynchronous
                 * @return Collection {TestOutcome, byte[], int, int, cb, ctx}
                 */
                @Parameterized.Parameters
                public static Collection<Object[]> params(){
                        return Arrays.asList(
                                new Object[][]{
                                        // null test
                                        {
                                                TestOutcome.NULL, null, 0, VALID_DATA_SIZE,
                                                true, getValidCtx()
                                        },
                                        {
                                                TestOutcome.NULL, VALID_DATA, 0, VALID_DATA_SIZE,
                                                false, getValidCtx()
                                        },
                                        // outOfBoundary
                                        {
                                                TestOutcome.OUT_OF_INDEX, VALID_DATA, 0, VALID_DATA_SIZE + 1,
                                                true, getValidCtx()
                                        },
                                        {
                                                TestOutcome.OUT_OF_INDEX, VALID_DATA, -1, VALID_DATA_SIZE,
                                                true, getValidCtx()
                                        },
                                        {
                                                TestOutcome.OUT_OF_INDEX, VALID_DATA, 0, -1,
                                                true, getValidCtx()
                                        },
                                        {
                                                TestOutcome.OUT_OF_INDEX, VALID_DATA, VALID_DATA_SIZE, 1,
                                                true, getValidCtx()
                                        },
                                        //valid
                                        {
                                                TestOutcome.VALID, VALID_DATA, 0, VALID_DATA_SIZE,
                                                true, getValidCtx()
                                        },
                                        {
                                                TestOutcome.VALID, VALID_DATA, 0, VALID_DATA_SIZE,
                                                true, null // clientCtx can be null
                                        }

                                }
                        );
                }

                @Test
                @Override
                public void doTest(){
                        boolean passed = false;
                        try {
                                super.targetLh.asyncAddEntry(
                                        this.data, this.offset, this.length,
                                        this.cb, this.ctx
                                );

                                long id = SyncCallbackUtils.waitForResult(this.cb);
                                passed = id == 0 && this.expectedOutcome.equals(TestOutcome.VALID);
                                super.targetLh.close();
                        }
                        catch (ArrayIndexOutOfBoundsException ignored){
                                passed = expectedOutcome.equals(TestOutcome.OUT_OF_INDEX);
                        }catch (NullPointerException e){
                                passed = this.expectedOutcome.equals(TestOutcome.NULL);
                        } catch (BKException e) {
                                LOG.info("BKException {}", e.getMessage());
                                fail();

                        } catch (InterruptedException e) {
                                LOG.info("should not be interrupted");
                                fail();
                        }
                        assertTrue(passed);
                }
                @Override
                @After
                public void tearDownLedger() throws BKException, InterruptedException {
                        super.testClient.close();
                }
        }




        @RunWith(value = Parameterized.class)
        public static class ReadEntriesTest extends LedgerHandleTestAbstract{



                protected TestOutcome expectedOutcome;
                protected long firstEntry;
                protected long lastEntry;
                protected long lastConfirmed = -1;
                public ReadEntriesTest(TestOutcome expectedOutcome, long firstEntry, long lastEntry) {
                        this.expectedOutcome = expectedOutcome;
                        this.firstEntry = firstEntry;
                        this.lastEntry = lastEntry;
                }

                @Parameterized.Parameters
                public static Collection<Object[]> params() {
                        return Arrays.asList(
                                new Object[][]{
                                        // Incorrect parameter exception
                                        {TestOutcome.BK_INVALID_PARAMETER, -1, 0},
                                        {TestOutcome.BK_INVALID_PARAMETER, 1, 0},
                                        // outOfBoundary
                                        {TestOutcome.OUT_OF_INDEX, 0, LH_DATA.length + 1},
                                        // valid parameters
                                        {TestOutcome.VALID, 0, LH_DATA.length - 1},
                                        {TestOutcome.VALID, LH_DATA.length - 2, LH_DATA.length - 1}

                                }
                        );
                }


                protected boolean checkForErrors(Exception exception){
                        if (exception instanceof BKException) {
                                BKException e = (BKException) exception;
                                return (
                                        // when firstEntry > lastEntry || firstEntry < 0
                                        (e.getCode() == BKException.Code.IncorrectParameterException &&
                                                this.expectedOutcome.equals(TestOutcome.BK_INVALID_PARAMETER))
                                                ||
                                                // when lastEntry > lastConfirmed
                                                (e.getCode() == BKException.Code.ReadException &&
                                                        this.expectedOutcome.equals(TestOutcome.OUT_OF_INDEX) &&
                                                        this.lastEntry > this.lastConfirmed)
                                );

                        }
                        return false;
                }



                @Override
                @Before
                public void setUpLedger() throws BKException, IOException, InterruptedException {
                        super.setUpLedger();

                        for (byte[] data : LH_DATA){
                                targetLh.addEntry(data);
                        }
                        this.lastConfirmed = targetLh.getLastAddConfirmed();
                }

                @Override
                @Test
                public void doTest() {
                        boolean passed = true;
                        try {
                                Enumeration<LedgerEntry> entries = targetLh.readEntries(this.firstEntry,
                                        this.lastEntry);
                                // test passed
                                passed = LedgerHandleTestAbstract.readSuccessCheck(entries, (int) this.firstEntry);
                        } catch (Exception e){
                                passed = this.checkForErrors(e);
                        }
                        assertTrue(passed);
                }
        }


        @RunWith(value = Parameterized.class)
        public static class AsyncReadEntriesTest extends ReadEntriesTest{

                private final ReadCallback cb;
                private final Object ctx;
                private final CompletableFuture<Enumeration<LedgerEntry>> future;
                public AsyncReadEntriesTest(TestOutcome expectedOutcome, long firstEntry, long lastEntry,
                                            ReadCallback cb, Object ctx,
                                            CompletableFuture<Enumeration<LedgerEntry>> future) {
                        super(expectedOutcome, firstEntry, lastEntry);
                        this.cb = cb;
                        this.ctx = ctx;
                        this.future = future;
                }

                @Parameterized.Parameters
                public static Collection<Object[]> params() {
                        CompletableFuture<Enumeration<LedgerEntry>> future = getValidReadCompletableFuture();
                        return Arrays.asList(
                                new Object[][]{
                                        //Null
                                        {TestOutcome.NULL, 0, LH_DATA.length - 1, null, getValidCtx(), future},
                                        // Incorrect parameter exception
                                        {TestOutcome.BK_INVALID_PARAMETER, -1, 0, getValidReadCallback(future),
                                                getValidCtx(), future},
                                        {TestOutcome.BK_INVALID_PARAMETER, 1, 0, getValidReadCallback(future),
                                                getValidCtx(), future},
                                        // outOfBoundary
                                        {TestOutcome.OUT_OF_INDEX, 0, LH_DATA.length + 1, getValidReadCallback(future),
                                                getValidCtx(), future},
                                        // valid parameters
                                        {TestOutcome.VALID, 0, LH_DATA.length - 1, getValidReadCallback(future),
                                                getValidCtx(), future},
                                        {TestOutcome.VALID, LH_DATA.length - 2, LH_DATA.length - 1,
                                                getValidReadCallback(future), getValidCtx(), future},
                                        // valid parameters
                                        {TestOutcome.VALID, 0, LH_DATA.length - 1,
                                                getValidReadCallback(future), getValidCtx(), future},
                                        {TestOutcome.VALID, LH_DATA.length - 2, LH_DATA.length - 1,
                                                getValidReadCallback(future), getValidCtx(), future}

                                }
                        );
                }
                @Override
                @Test
                public void doTest() {
                        boolean passed = true;
                        try {
                                targetLh.asyncReadEntries(this.firstEntry,
                                        this.lastEntry, this.cb, this.ctx);
                                if (this.cb == null){
                                        // internally they still run cb.readComplete without throwing Exception
                                        AtomicBoolean internalCheck = new AtomicBoolean(false);
                                        launchTestThread(internalCheck).interrupt();
                                        if (!internalCheck.get()){
                                                throw new NullPointerException();
                                        }
                                }
                                Enumeration<LedgerEntry> entries = SyncCallbackUtils.waitForResult(this.future);
                                assert entries != null;
                                passed = LedgerHandleTestAbstract.readSuccessCheck(entries, (int) this.firstEntry);
                        } catch (BKException | InterruptedException e) {
                                super.checkForErrors(e);
                        } catch (NullPointerException e){
                                passed = this.expectedOutcome.equals(TestOutcome.NULL);
                        }

                        assertTrue(passed);
                }

                @NotNull
                private Thread launchTestThread(AtomicBoolean internalCheck) throws InterruptedException {
                        long timeout = (10L) * (1000); // waiting 10s
                        Thread watcher = new Thread (() -> {
                                try {
                                        SyncCallbackUtils.waitForResult(this.future);
                                        internalCheck.set(true);
                                } catch (InterruptedException | BKException ignored) {
                                }
                        });
                        watcher.start();
                        watcher.join(timeout);
                        return watcher;
                }
        }
        /**
         * Read a sequence of entries synchronously.
         *
         * @see LedgerHandle#batchReadEntries(long, int, long)
         */
        @RunWith(value = Parameterized.class)
        public static class BatchReadEntriesTest extends LedgerHandleTestAbstract{


                private static final Map<Long, Long> DATA_MAP = IntStream.range(0, LH_DATA.length)
                        .boxed()
                        .collect(Collectors.toMap(
                                i -> (long) i,
                                i -> Arrays.stream(LH_DATA, i, LH_DATA.length)
                                        .mapToLong(data -> data.length)
                                        .sum()
                        ));
                private final TestOutcome expectedOutcome;
                private final long startEntry;
                private final int maxCount;
                private final long maxSize;


                public BatchReadEntriesTest(TestOutcome expectedOutcome, long startEntry, int maxCount,
                                            long maxSize) {
                        this.expectedOutcome = expectedOutcome;
                        this.startEntry = startEntry;
                        this.maxCount = maxCount;
                        this.maxSize = maxSize;
                }

                @Parameterized.Parameters
                public static Collection<Object[]> params() {
                        return Arrays.asList(
                                new Object[][]{
                                        // Incorrect parameter exception negative not managed
                                        //{TestOutcome.BK_INVALID_PARAMETER, -1, LH_DATA.length, DATA_MAP.get(0L)},
                                        {TestOutcome.VALID, 0, LH_DATA.length + 1, DATA_MAP.get(0L)},
                                        {TestOutcome.OUT_OF_INDEX, LH_DATA.length, 0, 0L}
                                }
                        );
                }

                @Override
                @Before
                public void setUpLedger() throws BKException, IOException, InterruptedException {
                        super.setUpLedger();

                        for (byte[] data : LH_DATA){
                                targetLh.addEntry(data);
                        }
                }

                @Override
                public void doTest() {
                        boolean passed = false;
                        try {
                                Enumeration<LedgerEntry> entries = targetLh.batchReadEntries(
                                        this.startEntry, this.maxCount, this.maxSize
                                );

                                passed = LedgerHandleTestAbstract.readSuccessCheck(entries, (int) startEntry);

                        } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                        } catch (BKException e) {
                                passed = e.getCode() == BKException.Code.ReadException &&
                                        this.expectedOutcome.equals(TestOutcome.OUT_OF_INDEX);
                        }

                        assertTrue(passed);
                }
        }


}

package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.test.TestCallBackUtils;
import org.apache.bookkeeper.test.TestOutcome;
import org.apache.bookkeeper.test.DefaultValues;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;


@RunWith(Enclosed.class)
public class UnitLedgerHandleReadTest {
    @Rule
    public Timeout globalTimeout=Timeout.seconds(10L);



    abstract static class ReadEntryAbstract{
        private final ClientContext ctx;
        protected LedgerHandle lh;
        private final LhTestingEnvironment env;
        protected final List<byte[]> ledgerData;
        ReadEntryAbstract(List<byte[]> data) throws GeneralSecurityException {
            this.env = new LhTestingEnvironment(data);
            ctx = this.env.getMockCtx();
            this.ledgerData = new ArrayList<>(data);
        }

        public ReadEntryAbstract() throws GeneralSecurityException {
            this(DefaultValues.INIT_ENTRY);
        }

        @Before
        public void setUp() throws Exception {
            lh = new LedgerHandle(
                    ctx,
                    DefaultValues.LEDGER_ID,
                    env.getMockVersioned(),
                    BookKeeper.DigestType.valueOf(env.getLedgerDigestType().name()),
                    DefaultValues.PASSWORD,
                    WriteFlagsMock.getMockWriteFlags()
            );
            for (byte[] b : ledgerData) {
                lh.addEntry(b);

            }
        }

        @After
        public void tearDown() throws Exception {
            this.lh.close();
        }
        public abstract void test();
    }

    // Sync read tests
    @RunWith(Parameterized.class)
    public static class Read2ParamTest extends ReadEntryAbstract{
        private final long firstEntry;
        private final long lastEntry;
        private final TestOutcome outcome;

        public Read2ParamTest(long firstEntry, long lastEntry, TestOutcome outcome) throws GeneralSecurityException {
            super();
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.outcome = outcome;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            return Arrays.asList(new Object[][]{
                    {0, DefaultValues.INIT_ENTRY.size() - 1, TestOutcome.VALID},
                    {DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.INIT_ENTRY.size(), TestOutcome.BK_READ_EXCEPTION},
                    {-1, DefaultValues.INIT_ENTRY.size() -1, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {-1, -2, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {-1, -1, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {-1, 0, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {0, -1, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {0, 0, TestOutcome.VALID},
                    {0, 1, TestOutcome.VALID},
                    {DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.INIT_ENTRY.size() - 1,
                            TestOutcome.VALID},
                    {DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.INIT_ENTRY.size(),
                            TestOutcome.BK_READ_EXCEPTION},
                    {DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.INIT_ENTRY.size() + 1,
                            TestOutcome.BK_READ_EXCEPTION},
                    {DefaultValues.INIT_ENTRY.size(), DefaultValues.INIT_ENTRY.size() -1,
                            TestOutcome.INCORRECT_PARAMETER_EXCEPTION}
            });
        }

        @Test
        @Override
        public void test() {
            boolean success;
            try {
                final int entryToConfirm =(int)(lastEntry - firstEntry);
                LedgerEntries entries = lh.read(firstEntry, lastEntry);
                success = outcome.equals(TestOutcome.VALID);

                // check if the ledger entry added is the same
                for (int i = 0; i < entryToConfirm; i++) {
                    final String expected = new String(DefaultValues.INIT_ENTRY.get(i));
                    final String actual = new String(entries.getEntry(i).getEntryBytes());
                    if (!expected.equals(actual)){
                        success = false;
                        break;
                    }
                }
            } catch (BKException e) {
                success = (e.getCode() == (BKException.Code.IncorrectParameterException)
                        && this.outcome.equals(TestOutcome.INCORRECT_PARAMETER_EXCEPTION)) ||
                        ((e.getCode() == (BKException.Code.ReadException)
                        && this.outcome.equals(TestOutcome.BK_READ_EXCEPTION)));
            } catch (InterruptedException e) {
                success = false;
            }
            assertTrue(success);
        }
    }

    @RunWith(Parameterized.class)
    public static class ReadEntries2ParamTest extends ReadEntryAbstract{
        private final long firstEntry;
        private final long lastEntry;
        private final TestOutcome outcome;

        public ReadEntries2ParamTest(long firstEntry, long lastEntry, TestOutcome outcome)
                throws GeneralSecurityException {
            super();
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.outcome = outcome;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            return Arrays.asList(new Object[][]{
                    {0, DefaultValues.INIT_ENTRY.size() - 1, TestOutcome.VALID},
                    {DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.INIT_ENTRY.size(), TestOutcome.BK_READ_EXCEPTION},
                    {-1, DefaultValues.INIT_ENTRY.size() -1, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {-1, -2, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {-1, -1, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {-1, 0, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {0, -1, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {0, 0, TestOutcome.VALID},
                    {0, 1, TestOutcome.VALID},
                    {DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.INIT_ENTRY.size() - 1,
                            TestOutcome.VALID},
                    {DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.INIT_ENTRY.size(),
                            TestOutcome.BK_READ_EXCEPTION},
                    {DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.INIT_ENTRY.size() + 1,
                            TestOutcome.BK_READ_EXCEPTION},
                    {DefaultValues.INIT_ENTRY.size(), DefaultValues.INIT_ENTRY.size() -1,
                            TestOutcome.INCORRECT_PARAMETER_EXCEPTION}
            });
        }

        @Test
        @Override
        public void test() {
            boolean success;
            try {
                Enumeration<LedgerEntry> entries = lh.readEntries(firstEntry, lastEntry);
                // check if the ledger entry added is the same
                int i = (int) firstEntry;
                success = outcome.equals(TestOutcome.VALID);

                while (entries.hasMoreElements()){
                    final String expected = new String(DefaultValues.INIT_ENTRY.get(i++));
                    final LedgerEntry le = entries.nextElement();
                    final String actual = new String(le.getEntry());
                    if (!expected.equals(actual)){
                        success = false;
                        break;
                    }
                }


            } catch (BKException e) {
                success = (e.getCode() == (BKException.Code.IncorrectParameterException)
                        && this.outcome.equals(TestOutcome.INCORRECT_PARAMETER_EXCEPTION)) ||
                        ((e.getCode() == (BKException.Code.ReadException)
                                && this.outcome.equals(TestOutcome.BK_READ_EXCEPTION)));
            } catch (InterruptedException e) {
                success = false;
            }
            assertTrue(success);
        }
    }

    @RunWith(Parameterized.class)
    public static class BatchReadEntry3ParamTest extends ReadEntryAbstract{

        private final long startEntry;
        private final int maxCount;
        private final int maxSize;
        private final TestOutcome outcome;

        public BatchReadEntry3ParamTest(long startEntry, int maxCount, int maxSize, TestOutcome outcome)
                throws GeneralSecurityException {
            super();
            this.startEntry = startEntry;
            this.maxCount = maxCount;
            this.maxSize = maxSize;
            this.outcome = outcome;
        }


        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            return Arrays.asList(new Object[][]{
                    {0, DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.LEDGER_NETTY_FRAME_SIZE, TestOutcome.VALID},
                    {DefaultValues.INIT_ENTRY.size() + 1, DefaultValues.INIT_ENTRY.size() - 1,
                            DefaultValues.LEDGER_NETTY_FRAME_SIZE, TestOutcome.BK_READ_EXCEPTION},
                    {-1, DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.LEDGER_NETTY_FRAME_SIZE,
                            TestOutcome.BK_READ_EXCEPTION},
                    // because is calculated the min
                    {0, DefaultValues.INIT_ENTRY.size(), DefaultValues.LEDGER_NETTY_FRAME_SIZE,
                            TestOutcome.VALID},
                    {0, -1, DefaultValues.LEDGER_NETTY_FRAME_SIZE, TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    // there is no problem internally is there is a check of max size, but it should return error
                    {0, DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.LEDGER_NETTY_FRAME_SIZE + 1,
                            TestOutcome.VALID},
                    {0, DefaultValues.INIT_ENTRY.size() - 1, -1,
                            TestOutcome.VALID}
            });
        }

        @Test
        @Override
        public void test() {
            boolean success = true;
            try {
                Enumeration<LedgerEntry> entries = lh.batchReadEntries(
                        startEntry, maxCount, maxSize
                );

                // check if the ledger entry added is the same
                int i = 0;
                success = outcome.equals(TestOutcome.VALID);
                while (entries.hasMoreElements()){
                    final String expected = new String(DefaultValues.INIT_ENTRY.get(i++));
                    final LedgerEntry le = entries.nextElement();
                    final String actual = new String(le.getEntry());
                    if (!expected.equals(actual)){
                        success = false;
                        break;
                    }
                }


            } catch (BKException e) {
                success = (e.getCode() == (BKException.Code.IncorrectParameterException)
                        && this.outcome.equals(TestOutcome.INCORRECT_PARAMETER_EXCEPTION)) ||
                        ((e.getCode() == (BKException.Code.ReadException)
                                && this.outcome.equals(TestOutcome.BK_READ_EXCEPTION)));
            } catch (InterruptedException e) {
                success = false;
            } catch (IllegalArgumentException e) {
                success = this.outcome.equals(TestOutcome.INCORRECT_PARAMETER_EXCEPTION);
            } catch (NoSuchElementException e){
                success = outcome.equals(TestOutcome.BK_READ_EXCEPTION);
            }
            assertTrue(success);
        }
    }

    // async reads



    @RunWith(Parameterized.class)
    public static class AsyncReadEntries5ParamTest extends ReadEntryAbstract{
        private final long firstEntry;
        private final long lastEntry;
        private final TestOutcome outcome;
        private final AsyncCallback.ReadCallback cb;
        private final Object ctx;
        private static final CompletableFuture<Enumeration<LedgerEntry>> FUTURE = new CompletableFuture<>();
        public AsyncReadEntries5ParamTest(long firstEntry, long lastEntry,
                                          AsyncCallback.ReadCallback cb, Object ctx, TestOutcome outcome)
                throws GeneralSecurityException {
            super();
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.cb = cb;
            this.ctx = ctx;
            this.outcome = outcome;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            return Arrays.asList(new Object[][]{
                    {0, DefaultValues.INIT_ENTRY.size() - 1, new SyncCallbackUtils.SyncReadCallback(FUTURE),
                            null, TestOutcome.VALID},
                    {0, DefaultValues.INIT_ENTRY.size() - 1, new SyncCallbackUtils.SyncReadCallback(FUTURE),
                            new Object(), TestOutcome.VALID},
                    {0, DefaultValues.INIT_ENTRY.size(), new SyncCallbackUtils.SyncReadCallback(FUTURE),
                            new Object(), TestOutcome.BK_READ_EXCEPTION},
                    {-1, DefaultValues.INIT_ENTRY.size() -1, new SyncCallbackUtils.SyncReadCallback(FUTURE),
                            new Object(), TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {DefaultValues.INIT_ENTRY.size(), DefaultValues.INIT_ENTRY.size() -1,
                            new SyncCallbackUtils.SyncReadCallback(FUTURE),
                            new Object(), TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    {0, DefaultValues.INIT_ENTRY.size() - 1, new TestCallBackUtils.InvalidReadCallback(),
                            null, TestOutcome.BK_READ_EXCEPTION},
                    {0, DefaultValues.INIT_ENTRY.size() - 1, null,
                            null, TestOutcome.NULL},
            });
        }

        @Test
        @Override
        public void test() {
            boolean success = true;
            try {

                lh.asyncReadEntries(
                     this.firstEntry, this.lastEntry, this.cb, this.ctx);
                // check if the ledger entry added is the same

                Enumeration<LedgerEntry> entries = SyncCallbackUtils.waitForResult(FUTURE);
                int i = 0;
                assert entries != null;
                while (entries.hasMoreElements()){
                    final String expected = new String(DefaultValues.INIT_ENTRY.get(i++));
                    final LedgerEntry le = entries.nextElement();
                    final String actual = new String(le.getEntry());
                    if (!expected.equals(actual)){
                        success = false;
                        break;
                    }
                }

            } catch (BKException e) {
                success = (e.getCode() == (BKException.Code.IncorrectParameterException)
                        && this.outcome.equals(TestOutcome.INCORRECT_PARAMETER_EXCEPTION)) ||
                        ((e.getCode() == (BKException.Code.ReadException)
                                && this.outcome.equals(TestOutcome.BK_READ_EXCEPTION)));
            } catch (InterruptedException e) {
                success = false;
            } catch (NullPointerException e) {
                success = this.outcome.equals(TestOutcome.NULL);
            }
            assertTrue(success);
        }
    }

    @RunWith(Parameterized.class)
    public static class AsyncBatchReadEntry5ParamTest extends ReadEntryAbstract{

        private final long startEntry;
        private final int maxCount;
        private final int maxSize;
        private final AsyncCallback.ReadCallback cb;
        private final Object ctx;
        private final TestOutcome outcome;
        private static final CompletableFuture<Enumeration<LedgerEntry>> FUTURE = new CompletableFuture<>();
        public AsyncBatchReadEntry5ParamTest(long startEntry, int maxCount, int maxSize,
                                             AsyncCallback.ReadCallback cb, Object ctx,
                                             TestOutcome outcome)
                throws GeneralSecurityException {
            super();
            this.startEntry = startEntry;
            this.maxCount = maxCount;
            this.maxSize = maxSize;
            this.cb = cb;
            this.ctx = ctx;
            this.outcome = outcome;
        }


        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            return Arrays.asList(new Object[][]{
                    {0, DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.LEDGER_NETTY_FRAME_SIZE,
                            new SyncCallbackUtils.SyncReadCallback(FUTURE), null,
                            TestOutcome.VALID},
                    {0, DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.LEDGER_NETTY_FRAME_SIZE,
                            new SyncCallbackUtils.SyncReadCallback(FUTURE), new Object(),
                            TestOutcome.VALID},
                    {0, DefaultValues.INIT_ENTRY.size(), DefaultValues.LEDGER_NETTY_FRAME_SIZE,
                            new SyncCallbackUtils.SyncReadCallback(FUTURE), new Object(),
                            TestOutcome.BK_READ_EXCEPTION},
                    {0, -1, DefaultValues.LEDGER_NETTY_FRAME_SIZE, new SyncCallbackUtils.SyncReadCallback(FUTURE),
                            new Object(), TestOutcome.INCORRECT_PARAMETER_EXCEPTION},
                    // there is no problem internally is there is a check of max size, but it should return error
                    {0, DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.LEDGER_NETTY_FRAME_SIZE + 1,
                            new SyncCallbackUtils.SyncReadCallback(FUTURE), new Object(),
                            TestOutcome.VALID},
                    {0, DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.LEDGER_NETTY_FRAME_SIZE,
                            new TestCallBackUtils.InvalidReadCallback(), null,
                            TestOutcome.BK_READ_EXCEPTION},
                    {0, DefaultValues.INIT_ENTRY.size() - 1, DefaultValues.LEDGER_NETTY_FRAME_SIZE,
                            null, null, TestOutcome.NULL},
            });
        }

        @Test
        @Override
        public void test() {
            boolean success = true;
            try {
                 lh.asyncBatchReadEntries(
                        startEntry, maxCount, maxSize, this.cb,
                        this.ctx
                );

                Enumeration<LedgerEntry> entries = SyncCallbackUtils.waitForResult(FUTURE);
                assert entries != null;
                // check if the ledger entry added is the same
                int i = 0;
                while (entries.hasMoreElements()){
                    final String expected = new String(DefaultValues.INIT_ENTRY.get(i++));
                    final LedgerEntry le = entries.nextElement();
                    final String actual = new String(le.getEntry());
                    if (!expected.equals(actual)){
                        success = false;
                        break;
                    }
                }

            } catch (BKException e) {
                success = (e.getCode() == (BKException.Code.IncorrectParameterException)
                        && this.outcome.equals(TestOutcome.INCORRECT_PARAMETER_EXCEPTION)) ||
                        ((e.getCode() == (BKException.Code.ReadException)
                                && this.outcome.equals(TestOutcome.BK_READ_EXCEPTION)));
            } catch (InterruptedException e) {
                success = false;
            } catch (IllegalArgumentException e) {
                success = this.outcome.equals(TestOutcome.INCORRECT_PARAMETER_EXCEPTION);
            }catch (NullPointerException e) {
                success = this.outcome.equals(TestOutcome.NULL);
            }
            assertTrue(success);

        }

    }

    @RunWith(Parameterized.class)
    public static class ReadLastEntriesTest extends ReadEntryAbstract{

        private final ReadLast readLast;

        public ReadLastEntriesTest(ReadLast readLast) throws GeneralSecurityException {
            super();
            this.readLast = readLast;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {

            return Arrays.asList(new Object[][]{
                    {new ReadLast.ReadLastSimple(new String(
                        DefaultValues.INIT_ENTRY.get(DefaultValues.INIT_ENTRY.size() - 1)),
                        TestOutcome.VALID)},
                    {new ReadLast.ReadLastConfirmed(DefaultValues.INIT_ENTRY.size() - 1,
                            TestOutcome.VALID)}
            });
        }

        @Test
        @Override
        public void test() {
            assertTrue(readLast.readTest(this.lh));
        }

    }



}

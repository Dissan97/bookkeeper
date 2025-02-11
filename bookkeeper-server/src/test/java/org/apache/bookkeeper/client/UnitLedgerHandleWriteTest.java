package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.TestOutcome;
import org.apache.bookkeeper.test.DefaultValues;
import org.apache.bookkeeper.test.TestCallBackUtils;
import org.apache.bookkeeper.test.TestContextUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.rules.Timeout;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;


@RunWith(Enclosed.class)
public class UnitLedgerHandleWriteTest {
    static final Logger LOG = LoggerFactory.getLogger(UnitLedgerHandleWriteTest.class);

    @Rule
    public Timeout globalTimeout=Timeout.seconds(10L);
    public static final byte[] DEFAULT_VALID_ENTRY = "test-entry".getBytes();
    public static final byte[] EMPTY_ENTRY = "".getBytes();
    abstract static class AddEntryAbstract {

        private final ClientContext ctx;
        protected LedgerHandle lh;
        private final LhTestingEnvironment env;
        private final List<byte[]> data;

        AddEntryAbstract() throws GeneralSecurityException {
            env = new LhTestingEnvironment();
            ctx = env.getMockCtx();
            this.data = Collections.emptyList();
        }

        AddEntryAbstract(List<byte[]> data) throws GeneralSecurityException {
            env = new LhTestingEnvironment(data);
            this.data = data;
            ctx = env.getMockCtx();
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
            for (byte[] b : data) {
                lh.addEntry(b);
            }
        }

        public abstract void test();
    }


    /**
     * Testing class for add entry with one parameter in this testing environment
     * There are already default entries
     * @see LedgerHandle#addEntry(byte[], int, int)
     * @see DefaultValues#INIT_ENTRY
     */
    @RunWith(Parameterized.class)
    public static class AddEntry3ParamTest extends AddEntryAbstract{

        private final byte[] entry;
        private final int length;
        private final int offset;
        private final TestOutcome outcome;

        public AddEntry3ParamTest(byte[] entry, int length, int offset, TestOutcome outcome)
                throws GeneralSecurityException {
            this.entry = entry;
            this.length = length;
            this.offset = offset;
            this.outcome = outcome;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            return Arrays.asList(new Object[][]{
                    // caso valido base
                    {DEFAULT_VALID_ENTRY, 0, DEFAULT_VALID_ENTRY.length, TestOutcome.VALID},
                    {null, 0, 0, TestOutcome.NULL},
                    {EMPTY_ENTRY, 0, 0, TestOutcome.VALID},
                    // expecting failure no sense for negative offset
                    {DEFAULT_VALID_ENTRY, -1, 0, TestOutcome.ARRAY_INDEX_EXCEPTION},
                    // expecting good because no data will be inserted
                    {DEFAULT_VALID_ENTRY, 0, 0, TestOutcome.VALID},
                    {DEFAULT_VALID_ENTRY, 1, 0, TestOutcome.VALID},
                    // offset + length > data.size
                    {DEFAULT_VALID_ENTRY, DEFAULT_VALID_ENTRY.length, DEFAULT_VALID_ENTRY.length,
                            TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, DEFAULT_VALID_ENTRY.length, DEFAULT_VALID_ENTRY.length + 1,
                            TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, 0, DEFAULT_VALID_ENTRY.length + 1,
                            TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, DEFAULT_VALID_ENTRY.length + 1, DEFAULT_VALID_ENTRY.length + 1,
                            TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, DEFAULT_VALID_ENTRY.length + 1, DEFAULT_VALID_ENTRY.length + 1,
                            TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, -1, -2, TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, -1, -1, TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, -1, -0, TestOutcome.ARRAY_INDEX_EXCEPTION},

            });
        }

        @Test
        @Override
        public void test() {
            boolean success;
            try {
                super.lh.addEntry(this.entry, this.offset, this.length);
                success = this.outcome.equals(TestOutcome.VALID);
            } catch (InterruptedException | BKException e) {
                success = false;
            } catch (NullPointerException e) {
                success = this.outcome.equals(TestOutcome.NULL);
            } catch (ArrayIndexOutOfBoundsException e) {
                success = this.outcome.equals(TestOutcome.ARRAY_INDEX_EXCEPTION);
            }
            assertTrue(success);
        }
    }


    /**
     * Testing class for add entry with one parameter
     * @see LedgerHandle#asyncAddEntry(long, byte[], int, int, AsyncCallback.AddCallbackWithLatency, Object)
     */
    @RunWith(Parameterized.class)
    public static class AsyncAddEntry5ParamTest extends AddEntryAbstract {
        private final byte[] data;
        private final int offset;
        private final int length;
        private final AsyncCallback.AddCallback cb;
        private final Object ctx;
        private final TestOutcome outcome;

        public AsyncAddEntry5ParamTest(byte[] data, int offset, int length, AsyncCallback.AddCallback cb,
                                       Object ctx, TestOutcome outcome) throws GeneralSecurityException {
            super();
            this.data = data;
            this.offset = offset;
            this.length = length;
            this.cb = cb;
            this.ctx = ctx;
            this.outcome = outcome;
        }


        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument(){

            return Arrays.asList(new Object[][]{
                    {DEFAULT_VALID_ENTRY,  0, 0, new TestCallBackUtils.ValidAddCallback(), null, TestOutcome.VALID},

                    {DEFAULT_VALID_ENTRY, 0, DEFAULT_VALID_ENTRY.length, new TestCallBackUtils.ValidAddCallback(),
                            null, TestOutcome.VALID},

                    {DEFAULT_VALID_ENTRY, DEFAULT_VALID_ENTRY.length, 0, new TestCallBackUtils.ValidAddCallback(),
                            null, TestOutcome.VALID},

                    {DEFAULT_VALID_ENTRY, 0, DEFAULT_VALID_ENTRY.length,new TestCallBackUtils.ValidAddCallback(),
                            new TestContextUtils.ValidCtx(), TestOutcome.VALID},

                    {EMPTY_ENTRY, 0, 0, new TestCallBackUtils.ValidAddCallback(),
                            new TestContextUtils.ValidCtx(), TestOutcome.VALID},

                    {null, 0, DEFAULT_VALID_ENTRY.length, new TestCallBackUtils.ValidAddCallback(),
                            new Object(), TestOutcome.NULL},

                    {DEFAULT_VALID_ENTRY, 0, DEFAULT_VALID_ENTRY.length, null, new Object(), TestOutcome.NULL},

                    {DEFAULT_VALID_ENTRY, 0, -1, new TestCallBackUtils.ValidAddCallback(),
                            null, TestOutcome.ARRAY_INDEX_EXCEPTION},

                    {DEFAULT_VALID_ENTRY, 1, DEFAULT_VALID_ENTRY.length, new TestCallBackUtils.ValidAddCallback(),
                            null, TestOutcome.ARRAY_INDEX_EXCEPTION},

                    {DEFAULT_VALID_ENTRY, 0, DEFAULT_VALID_ENTRY.length, new TestCallBackUtils.InvalidAddCallback(),
                            null, TestOutcome.INVALID_CALLBACK},

                    // does not matter cause can pass all stuff is an Object
                    {DEFAULT_VALID_ENTRY, 0, DEFAULT_VALID_ENTRY.length, new TestCallBackUtils.ValidAddCallback(),
                            new TestContextUtils.InvalidCtx(), TestOutcome.VALID},

                    // array index cases
                    {DEFAULT_VALID_ENTRY, DEFAULT_VALID_ENTRY.length, DEFAULT_VALID_ENTRY.length,
                            new TestCallBackUtils.ValidAddCallback(),
                            new Object(), TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, DEFAULT_VALID_ENTRY.length, DEFAULT_VALID_ENTRY.length + 1,
                            new TestCallBackUtils.ValidAddCallback(),
                            new Object(), TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, 0, DEFAULT_VALID_ENTRY.length + 1,
                            new TestCallBackUtils.ValidAddCallback(),
                            new Object(), TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, DEFAULT_VALID_ENTRY.length + 1, DEFAULT_VALID_ENTRY.length + 1,
                            new TestCallBackUtils.ValidAddCallback(),
                            new Object(), TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, DEFAULT_VALID_ENTRY.length + 1, DEFAULT_VALID_ENTRY.length + 1,
                            new TestCallBackUtils.ValidAddCallback(),
                            new Object(), TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, -1, -2, new TestCallBackUtils.ValidAddCallback(),
                            new Object(), TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, -1, -1, new TestCallBackUtils.ValidAddCallback(),
                            new Object(), TestOutcome.ARRAY_INDEX_EXCEPTION},
                    {DEFAULT_VALID_ENTRY, -1, -0, new TestCallBackUtils.ValidAddCallback(),
                            new Object(), TestOutcome.ARRAY_INDEX_EXCEPTION},

            });
        }

        @Test
        @Override
        public void test() {
            boolean success;
            try {

                lh.asyncAddEntry(this.data, this.offset, this.length, this.cb, this.ctx);

                CompletableFuture<Long> future = this.cb instanceof TestCallBackUtils.ValidAddCallback ?
                        (TestCallBackUtils.ValidAddCallback) this.cb : (TestCallBackUtils.InvalidAddCallback) this.cb;
                long id = SyncCallbackUtils.waitForResult(future);
                success = outcome.equals(TestOutcome.VALID) && id == 0;
            } catch (NullPointerException e) {
                success = this.outcome.equals(TestOutcome.NULL);
            } catch (BKException e) {
                success = this.outcome.equals(TestOutcome.INVALID_CALLBACK) &&
                        BKException.getExceptionCode(e) == BKException.Code.UnexpectedConditionException;
            } catch (InterruptedException e) {
                success = false;
            } catch (ArrayIndexOutOfBoundsException e) {
                success = this.outcome.equals(TestOutcome.ARRAY_INDEX_EXCEPTION);
            }

            assertTrue(success);


        }
    }


}

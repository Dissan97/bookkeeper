package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.TestConfig;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestOutcome;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(value = Enclosed.class)
public class BookkeeperUnitTest extends BookKeeperClusterTestCase {
    public static final String CTX = "this is ctx";
    public static final int ENS_SIZE = 3;
    protected BookKeeper bk;
    public BookkeeperUnitTest() {
        super(ENS_SIZE);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // initialize the bookkeeper client
        this.bk = new BookKeeper(super.baseClientConf, super.zkc);
    }

    @After
    public void tearDown() throws Exception {
        this.bk.close();
        super.tearDown();
    }

    @RunWith(value = Parameterized.class)
    public static class BookkeeperCreateLedgerTest extends BookkeeperUnitTest {

        private final int wQSize;
        private final int aQSize;
        private final BookKeeper.DigestType digestType;
        private final byte[] passwd;
        private final Map<String,byte[]> customMetadata;
        private final TestOutcome outcome;

        public BookkeeperCreateLedgerTest(int wQSize, int aQSize, BookKeeper.DigestType digestType,
                                          byte[] passwd, Map<String,byte[]> customMetadata, TestOutcome outcome) {
            this.wQSize = wQSize;
            this.aQSize = aQSize;
            this.digestType = digestType;
            this.passwd = passwd;
            this.customMetadata = customMetadata;
            this.outcome = outcome;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {

            return Arrays.asList(
                    new Object[][]{
                            {
                                ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    ENS_SIZE - 1, ENS_SIZE - 1, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    ENS_SIZE - 1, ENS_SIZE - 2, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            // here there is the violation of the quorum
                            {
                                    ENS_SIZE - 1, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.INCORRECT_PARAMETER_EXCEPTION
                            },
                            {
                                    ENS_SIZE, ENS_SIZE, null, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.NULL
                            },
                            {
                                    ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.MAC, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32C, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.DUMMY, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, new byte[0],
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, null,
                                    Collections.emptyMap(), TestOutcome.NULL
                            },
                            {
                                    ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    null, TestOutcome.VALID
                            }
                    }
            );
        }
        
        

        @Test
        public void testCreateLedger() {
            boolean testPassed = false;
            try {
                bk.createLedger(
                        ENS_SIZE, wQSize, aQSize,
                        digestType, passwd, customMetadata
                );
                testPassed = outcome.equals(TestOutcome.VALID);
            }catch (IllegalArgumentException e){
                testPassed = outcome.equals(TestOutcome.INCORRECT_PARAMETER_EXCEPTION);
            } catch (BKException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                fail();
            } catch (NullPointerException e) {
                testPassed = outcome.equals(TestOutcome.NULL);
            }
            assertTrue(testPassed);
        }
    }

    @RunWith(value = Parameterized.class)
    public static class BookkeeperCreateLedgerAdvTest extends BookkeeperUnitTest {

        private final long ledgerId;
        private final int wQSize;
        private final int aQSize;
        private final BookKeeper.DigestType digestType;
        private final byte[] passwd;
        private final Map<String,byte[]> customMetadata;
        private final TestOutcome outcome;
        
        @Override
        @Before
        public void setUp() throws Exception {
            super.setUp();
            bk.createLedger(BookKeeper.DigestType.MAC, TestConfig.LEDGER_PASSWORD).close();
        }
        
        
        public BookkeeperCreateLedgerAdvTest(long ledgerId, int wQSize, int aQSize, BookKeeper.DigestType digestType,
                                          byte[] passwd, Map<String,byte[]> customMetadata, TestOutcome outcome) {
            this.ledgerId = ledgerId;
            this.wQSize = wQSize;
            this.aQSize = aQSize;
            this.digestType = digestType;
            this.passwd = passwd;
            this.customMetadata = customMetadata;
            this.outcome = outcome;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {

            return Arrays.asList(
                    new Object[][]{
                            {
                                    -1L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.BK_EXCEPTION
                            },
                            {
                                    0L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.BK_EXIST
                            },
                            {
                                    1L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    1L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    1L, ENS_SIZE - 1, ENS_SIZE - 1, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    1L, ENS_SIZE - 1, ENS_SIZE - 2, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            // here there is the violation of the quorum
                            {
                                    1L, ENS_SIZE - 1, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.INCORRECT_PARAMETER_EXCEPTION
                            },
                            {
                                    1L, ENS_SIZE, ENS_SIZE, null, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.NULL
                            },
                            {
                                    1L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.MAC, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    1L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32C, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    1L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.DUMMY, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    1L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, new byte[0],
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    1L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, null,
                                    Collections.emptyMap(), TestOutcome.NULL
                            },
                            {
                                    1L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    Collections.emptyMap(), TestOutcome.VALID
                            },
                            {
                                    1L, ENS_SIZE, ENS_SIZE, BookKeeper.DigestType.CRC32, "password".getBytes(),
                                    null, TestOutcome.VALID
                            }
                    }
            );
        }



        @Test
        public void testCreateLedger() {
            boolean testPassed = false;
            try {
                bk.createLedgerAdv(
                        ledgerId, ENS_SIZE, wQSize, aQSize,
                        digestType, passwd, customMetadata
                );
                testPassed = outcome.equals(TestOutcome.VALID);
            }catch (IllegalArgumentException e){
                testPassed = outcome.equals(TestOutcome.INCORRECT_PARAMETER_EXCEPTION);
            } catch (BKException e) {
                testPassed = (outcome.equals(TestOutcome.BK_EXIST) && e.getCode() ==
                        BKException.Code.LedgerExistException) ||
                        (outcome.equals(TestOutcome.BK_EXCEPTION) && e.getCode() ==
                                BKException.Code.UnexpectedConditionException);

            } catch (InterruptedException e) {
                fail();
            } catch (NullPointerException e) {
                testPassed = outcome.equals(TestOutcome.NULL);
            }
            assertTrue(testPassed);
        }
    }
    
    @RunWith(value = Parameterized.class)
    public static class BookkeeperOpenLedgerTest extends BookkeeperUnitTest {

        private long id;
        private final long testId;
        private final BookKeeper.DigestType digestType;
        private final byte[] passwd;
        private TestOutcome outcome;
        public BookkeeperOpenLedgerTest(long idToTest,BookKeeper.DigestType digestType, byte[] passwd,
                                        TestOutcome outcome) {
            this.testId = idToTest;
            this.digestType = digestType;
            this.passwd = passwd;
            this.outcome = outcome;
        }

        @Before
        public void setUp() throws Exception {
            super.setUp();
            LedgerHandle lh = bk.createLedger(BookKeeper.DigestType.MAC, TestConfig.LEDGER_PASSWORD);
            id = lh.getId();
            lh.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(
                    new Object[][]{
                            {-1, BookKeeper.DigestType.MAC, TestConfig.LEDGER_PASSWORD, TestOutcome.BK_EXCEPTION},
                            {0,BookKeeper.DigestType.MAC, TestConfig.LEDGER_PASSWORD, TestOutcome.VALID},
                            {0,BookKeeper.DigestType.CRC32, TestConfig.LEDGER_PASSWORD, TestOutcome.VALID},
                            // seems not problem with digestType
                            {0,null, TestConfig.LEDGER_PASSWORD, TestOutcome.VALID},
                            {0,BookKeeper.DigestType.DUMMY, "bad-password".getBytes(), TestOutcome.BK_EXCEPTION},
                            {0,BookKeeper.DigestType.DUMMY, null, TestOutcome.BK_EXCEPTION}
                    }
            );
        }
        @Test
        public void testOpenLedger() {
            boolean testPassed = false;
            try {
                LedgerHandle lh = bk.openLedger(testId, digestType, passwd);
                if (lh.getId() != id) {
                    throw new RuntimeException();
                }
                testPassed = outcome.equals(TestOutcome.VALID);
            } catch (BKException e) {
                testPassed = outcome.equals(TestOutcome.BK_EXCEPTION);
            } catch (InterruptedException e) {
                fail();
            }
            assertTrue(testPassed);
        }

    }

    @RunWith(value = Parameterized.class)
    public static class BookkeeperOpenNoRecLedgerTest extends BookkeeperUnitTest {

        private long id;
        private final long testId;
        private final BookKeeper.DigestType digestType;
        private final byte[] passwd;
        private TestOutcome outcome;
        public BookkeeperOpenNoRecLedgerTest(long idToTest,BookKeeper.DigestType digestType, byte[] passwd,
                                        TestOutcome outcome) {
            this.testId = idToTest;
            this.digestType = digestType;
            this.passwd = passwd;
            this.outcome = outcome;
        }

        @Before
        public void setUp() throws Exception {
            super.setUp();
            LedgerHandle lh = bk.createLedger(BookKeeper.DigestType.MAC, TestConfig.LEDGER_PASSWORD);
            id = lh.getId();
            lh.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(
                    new Object[][]{
                            {-1, BookKeeper.DigestType.MAC, TestConfig.LEDGER_PASSWORD, TestOutcome.BK_EXCEPTION},
                            {0,BookKeeper.DigestType.MAC, TestConfig.LEDGER_PASSWORD, TestOutcome.VALID},
                            {0,BookKeeper.DigestType.CRC32, TestConfig.LEDGER_PASSWORD, TestOutcome.VALID},
                            // seems not problem with digestType
                            {0,null, TestConfig.LEDGER_PASSWORD, TestOutcome.VALID},
                            {0,BookKeeper.DigestType.DUMMY, "bad-password".getBytes(), TestOutcome.BK_EXCEPTION},
                            {0,BookKeeper.DigestType.DUMMY, null, TestOutcome.BK_EXCEPTION}
                    }
            );
        }
        @Test
        public void testOpenLedger() {
            boolean testPassed = false;
            try {
                LedgerHandle lh = bk.openLedgerNoRecovery(testId, digestType, passwd);
                if (lh.getId() != id) {
                    throw new RuntimeException();
                }
                testPassed = outcome.equals(TestOutcome.VALID);
            } catch (BKException e) {
                testPassed = outcome.equals(TestOutcome.BK_EXCEPTION);
            } catch (InterruptedException e) {
                fail();
            }
            assertTrue(testPassed);
        }

    }

    @RunWith(value = Parameterized.class)
    public static class BookkeeperDeleteLedgerTest extends BookkeeperUnitTest {

        private long id;
        private final long testId;
        private TestOutcome outcome;
        public BookkeeperDeleteLedgerTest(long idToTest,
                                        TestOutcome outcome) {
            this.testId = idToTest;
            this.outcome = outcome;
        }

        @Before
        public void setUp() throws Exception {
            super.setUp();
            LedgerHandle lh = bk.createLedger(BookKeeper.DigestType.MAC, TestConfig.LEDGER_PASSWORD);
            id = lh.getId();
            lh.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(
                    new Object[][]{
                            {1, TestOutcome.BK_EXCEPTION},
                            {0, TestOutcome.VALID}
                    }
            );
        }
        @Test
        public void testOpenLedger() {
            boolean testPassed = false;
            try {
                bk.deleteLedger(testId);
                bk.openLedger(id, BookKeeper.DigestType.MAC, TestConfig.LEDGER_PASSWORD);
                testPassed = true;
            } catch (BKException e) {
                testPassed = outcome.equals(TestOutcome.VALID) && testId == id;
            } catch (InterruptedException e) {
                fail();
            }
            assertTrue(testPassed);
        }

    }
}

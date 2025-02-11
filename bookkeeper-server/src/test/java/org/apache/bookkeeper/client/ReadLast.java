package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.TestOutcome;

public interface ReadLast {

    boolean readTest(LedgerHandle lh);

    class ReadLastSimple implements ReadLast {

        private String dataExpected;
        private TestOutcome outcome;

        public ReadLastSimple(String dataExpected, TestOutcome outcome) {
            this.dataExpected = dataExpected;
            this.outcome = outcome;
        }

        @Override
        public boolean readTest(LedgerHandle lh) {
            boolean ret;
             try {
                 LedgerEntry entry =  lh.readLastEntry();
                 ret = new String(entry.getEntry()).equals(dataExpected) &&
                 this.outcome.equals(TestOutcome.VALID);
            } catch (InterruptedException | BKException e) {
                ret = false;
            }
             return ret;
        }


    }
    class ReadLastConfirmed implements ReadLast {

        private long idExpected;
        private TestOutcome outcome;

        public ReadLastConfirmed(long idExpected, TestOutcome outcome) {
            this.idExpected = idExpected;
            this.outcome = outcome;
        }

        @Override
        public boolean readTest(LedgerHandle lh) {
            boolean ret;
            try {
                long entry = lh.readLastConfirmed();
                ret = this.idExpected == entry &&
                        this.outcome.equals(TestOutcome.VALID);
            } catch (InterruptedException | BKException e) {
                ret = false;
            }
            return ret;
        }
    }
}

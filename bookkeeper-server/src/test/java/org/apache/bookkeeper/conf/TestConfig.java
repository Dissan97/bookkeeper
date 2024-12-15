package org.apache.bookkeeper.conf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.bookkeeper.client.AsyncCallback.AddCallback;

public class TestConfig {
        // timeout in seconds
        public static final int TEST_TIMEOUT = 120;
        // number of bookies server
        public static final int BOOKIES = 3;
        public static final Charset TEST_CHARSET = StandardCharsets.UTF_8;
        // default valid password
        public static final byte[] LEDGER_PASSWORD = "test-password".getBytes(TEST_CHARSET);
        // default value to pass for new ledger id
        public static final long DEFAULT_LEDGER_ID = -1L;
        // default add callback
        public static final int LEDGER_ENTRY_FOR_TEST = 5;


}

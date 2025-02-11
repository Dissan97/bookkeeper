package org.apache.bookkeeper.test;

import org.apache.bookkeeper.proto.DataFormats;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DefaultValues {
    public static final DataFormats.LedgerMetadataFormat.DigestType DIGEST_FOR_DM =
            DataFormats.LedgerMetadataFormat.DigestType.CRC32;
    public static final long LEDGER_ID = 0L;
    public static final byte[] MASTER_KEY = "master-key".getBytes();


    private DefaultValues() {}

    public static final byte[] PASSWORD = "password".getBytes();
    public static final int ENSEMBLE_SIZE = 3;
    public static final String ENTRY_PREAMBLE = "LedgerHandle$LedgerEntry#";
    public static List<byte[]> INIT_ENTRY = IntStream.range(0, 4).mapToObj(
            i -> (ENTRY_PREAMBLE +i).getBytes()
    ).collect(Collectors.toList());

    public static final int LEDGER_NETTY_FRAME_SIZE = DefaultValues.INIT_ENTRY.stream().mapToInt(
            b -> b.length
    ).sum();


}

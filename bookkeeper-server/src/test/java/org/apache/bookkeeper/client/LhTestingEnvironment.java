package org.apache.bookkeeper.client;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import lombok.Getter;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.test.DefaultValues;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.ByteStringUtil;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.versioning.Versioned;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mock;

import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

/**
 * Class Used for unit testing to isolate Ledger from outside to test ledger handle functionality
 * @see LedgerHandle
 */
public class LhTestingEnvironment {

    @Getter
    @Mock
    private ClientContext mockCtx;

    @Mock
    private ClientInternalConf mockInternalConf;
    @Mock
    private LedgerMetadata mockLedgerMetadata;
    @Mock
    private BookieWatcher mockBookieWatcher;
    @Mock
    EnsemblePlacementPolicy mockEnsemblePlacementPolicy;
    @Mock
    private BookieClient mockBookie;
    @Mock
    private BookieId mockBookieId1;
    @Mock
    private BookieId mockBookieId2;
    @Mock
    private BookieId mockBookieId3;
    @Mock
    private ByteBufAllocator mockByteBufAllocator;
    @Mock
    private OrderedScheduler mockOrderedScheduler;
    @Mock
    private OrderedExecutor mockOrderedExecutor;
    @Mock
    private BookKeeperClientStats mockBookKeeperClientStats;
    @Mock
    private Counter mockCounter;
    @Mock
    private OpStatsLogger mockOpStatsLogger;
    @Getter
    @Mock
    private Versioned<LedgerMetadata> mockVersioned;
    @Mock
    private LedgerManager mockLedgerManager;
    @Mock
    private List<BookieId> mockBookies;


    private final ClientConfiguration realConf = new ClientConfiguration();
    private LedgerMetadata realLedgerMetadata;
    private final Map<Long, ByteString> entryDigest = new TreeMap<>();
    @Getter
    private long entrySize = 0L;
    @Getter
    private final DigestType ledgerDigestType;
    final DigestManager digestManager;


    /**
     * Testing environment class to test LedgerHandle methods in isolation
     * @param digestType Which DigestType want to simulate during testing
     * @param ledgerEntries initial entries in the simulation environment
     * @throws GeneralSecurityException if some internal problems with DigestManager
     * @see DigestType
     * @see DigestManager#instantiate(
     * long, byte[], DataFormats.LedgerMetadataFormat.DigestType, ByteBufAllocator, boolean)
     */

    public LhTestingEnvironment(DigestType digestType, List<byte[]> ledgerEntries)
            throws GeneralSecurityException {
        this(digestType, ledgerEntries,false);
    }

    public LhTestingEnvironment(DigestType digestType, List<byte[]> ledgerEntries, boolean useV2WireProtocol)
            throws GeneralSecurityException {
        this.ledgerDigestType = digestType;
        this.digestManager = DigestManager.instantiate(
                DefaultValues.LEDGER_ID,
                DefaultValues.PASSWORD,
                DataFormats.LedgerMetadataFormat.DigestType.valueOf(this.ledgerDigestType.name()),
                Unpooled.buffer().alloc(),
                realConf.getUseV2WireProtocol()
        );
        if (ledgerEntries == null){
            ledgerEntries = Collections.emptyList();
        }
        this.initEnvData(ledgerEntries);
        this.mockCtx = mock(ClientContext.class);
        this.setRealConfBehavior(useV2WireProtocol);
        this.setupAllStubs();
        this.setupMockWhen();
    }

    /**
     * Testing environment class to test LedgerHandle methods in isolation
     * With default params
     * @apiNote  digestType=DigestType.CRC32, ledgerEntries.empty()
     * @see LhTestingEnvironment#LhTestingEnvironment(DigestType, List)
     */
    public LhTestingEnvironment() throws GeneralSecurityException {
        this(DigestType.CRC32, Collections.emptyList());
    }

    public LhTestingEnvironment(boolean useV2Protocol) throws GeneralSecurityException {
        this(DigestType.CRC32, Collections.emptyList(), useV2Protocol);
    }

    public LhTestingEnvironment(List<byte[]> data) throws GeneralSecurityException {
        this(DigestType.CRC32, data);
    }

    private void initEnvData(@NotNull List<byte[]> ledgerEntries) throws GeneralSecurityException {
        long id = 0;
        byte[] masterKey = DigestManager.generateMasterKey(DefaultValues.PASSWORD);
        for(byte[] entry: ledgerEntries){
            entrySize += entry.length;
            ByteBuf buf = Unpooled.wrappedBuffer(entry);
            entryDigest.put(id,
                    ByteStringUtil.byteBufListToByteString((ByteBufList)
                            digestManager.computeDigestAndPackageForSending(
                                    id,
                                    id,
                                    entry.length,
                                    buf,
                                    masterKey,
                                    0x0
                            ))
            );
            id++;
        }
    }

    private void setRealConfBehavior(boolean useV2WireProtocol) {
        this.realConf.setStickyReadsEnabled(false);
        this.realConf.setThrottleValue(0);
        this.realConf.setUseV2WireProtocol(useV2WireProtocol);
        this.realConf.setReorderReadSequenceEnabled(false);
        this.realConf.setFirstSpeculativeReadLACTimeout(0);
        this.realConf.setNettyMaxFrameSizeBytes(DefaultValues.LEDGER_NETTY_FRAME_SIZE);

    }

    private void setupAllStubs() {
        ClientInternalConf realInternalConf = ClientInternalConf.fromConfig(realConf);
        this.mockInternalConf = spy(realInternalConf);
        this.mockLedgerManager = mock(LedgerManager.class);
        this.mockBookieWatcher = mock(BookieWatcher.class);
        this.mockEnsemblePlacementPolicy = mock(EnsemblePlacementPolicy.class);
        this.setupBookies();
        ByteBufAllocator realByteBufAllocator = Unpooled.buffer().alloc();
        this.mockByteBufAllocator = spy(realByteBufAllocator);
        this.mockOrderedExecutor = mock(OrderedExecutor.class);
        this.mockOrderedScheduler = mock(OrderedScheduler.class);
        this.setMainWorkerPoolBehavior();
        this.mockBookKeeperClientStats = mock(BookKeeperClientStats.class);
        this.mockCounter = mock(Counter.class);
        this.mockOpStatsLogger = mock(OpStatsLogger.class);
        this.setClientStatsBehavior();
        this.setRealMetadataToSpy();
        this.mockLedgerMetadata = spy(this.realLedgerMetadata);
        this.mockVersioned = mock(Versioned.class);
        this.setVersionedBehavior();
    }

    private void setupMockWhen() {
        when(mockCtx.getConf()).thenReturn(mockInternalConf);
        when(mockCtx.getLedgerManager()).thenReturn(mockLedgerManager);
        when(mockLedgerManager.writeLedgerMetadata(anyLong(), any(), any())).thenReturn(
                CompletableFuture.completedFuture(mockVersioned)
        );
        when(mockCtx.getBookieWatcher()).thenReturn(this.mockBookieWatcher);
        when(mockCtx.getPlacementPolicy()).thenReturn(this.mockEnsemblePlacementPolicy);
        when(mockCtx.getBookieClient()).thenReturn(this.mockBookie);
        when(mockCtx.getByteBufAllocator()).thenReturn(this.mockByteBufAllocator);
        when(mockCtx.getMainWorkerPool()).thenReturn(this.mockOrderedExecutor);
        when(mockCtx.getScheduler()).thenReturn(this.mockOrderedScheduler);
        when(mockCtx.getClientStats()).thenReturn(this.mockBookKeeperClientStats);
        when(mockCtx.isClientClosed()).thenReturn(false);
    }

    private void setRealMetadataToSpy() {
        LedgerMetadata.State realState = LedgerMetadata.State.OPEN;
        Map<Long, List<BookieId>> ensembles = new HashMap<>();
        List<BookieId> array = new ArrayList<>();
        array.add(mockBookieId1);
        array.add(mockBookieId2);
        array.add(mockBookieId3);
        ensembles.put(0L, array);
        this.realLedgerMetadata = new LedgerMetadataImpl(
                this.entryDigest.size() - 1,
                0,
                DefaultValues.ENSEMBLE_SIZE,
                DefaultValues.ENSEMBLE_SIZE,
                DefaultValues.ENSEMBLE_SIZE,
                realState,
                Optional.empty(),
                Optional.empty(),
                ensembles,
                Optional.of(this.ledgerDigestType),
                Optional.of(DefaultValues.PASSWORD),
                0,
                false,
                0,
                Collections.emptyMap()
        );
    }

    private void setupBookies() {
        this.mockBookie = mock(BookieClient.class);
        this.mockBookieId1 = mock(BookieId.class);
        this.mockBookieId2 = mock(BookieId.class);
        this.mockBookieId3 = mock(BookieId.class);
        this.mockBookies = mock(List.class);
        when(mockBookies.get(0)).thenReturn(mockBookieId1);
        when(mockBookies.get(1)).thenReturn(mockBookieId2);
        when(mockBookies.get(2)).thenReturn(mockBookieId3);
        when(mockBookieId1.getId()).thenReturn("mockBookieId1");
        when(mockBookieId2.getId()).thenReturn("mockBookieId2");
        when(mockBookieId3.getId()).thenReturn("mockBookieId3");

        doAnswer(invocationOnMock -> {
            BookkeeperInternalCallbacks.WriteCallback cb = invocationOnMock.getArgument(5);
            long ledgerId = invocationOnMock.getArgument(1);
            long entryId = invocationOnMock.getArgument(3);
            BookieId addr = invocationOnMock.getArgument(0);
            ReferenceCounted toSend = invocationOnMock.getArgument(4);
            if (!this.realConf.getUseV2WireProtocol()){
                this.entryDigest.putIfAbsent(entryId,
                        ByteStringUtil.byteBufListToByteString((ByteBufList) toSend));
            }


            cb.writeComplete(BKException.Code.OK,
                    ledgerId,entryId, addr, invocationOnMock.getArgument(6));

            return null;
        }).when(mockBookie).addEntry(any(),
                anyLong(),
                any(),
                anyLong(),
                any(),
                any(),
                any(),
                anyInt(),
                anyBoolean(),
                any());
        doAnswer(invocationOnMock -> {

            long ledgerId = invocationOnMock.getArgument(1);
            long entryId = invocationOnMock.getArgument(2);
            BookkeeperInternalCallbacks.ReadEntryCallback cb = invocationOnMock.getArgument(3);
            Object ctx = invocationOnMock.getArgument(4);
            int code = BKException.Code.OK;
            if (entryId > this.entryDigest.size() + 1){
                code = BKException.Code.ReadException;
            }
            cb.readEntryComplete(code,
                    ledgerId,
                    entryId,
                    Unpooled.wrappedBuffer(this.entryDigest.get(
                            ( entryId) <= this.entryDigest.size() ?  entryId : 0L).toByteArray()),
                    ctx);
            return null;
        }).when(mockBookie).readEntry(
                any(),
                anyLong(),
                anyLong(),
                any(),
                any(),
                anyInt());

        doAnswer(
                invocationOnMock -> {
                    long ledgerId = invocationOnMock.getArgument(1);
                    long startEntry = invocationOnMock.getArgument(2);
                    int maxCount = invocationOnMock.getArgument(3);
                    long maxSize = invocationOnMock.getArgument(4);
                    int code = BKException.Code.ReadException;
                    ByteBufList bufList = null;
                    if (maxCount <= this.entryDigest.size() && maxSize <= this.getEntrySize()) {
                        code = BKException.Code.OK;
                        bufList = ByteBufList.get();
                        for (long i = 0; i < maxCount; i++) {
                            bufList.add(
                                    Unpooled.wrappedBuffer(this.entryDigest.get(i).toByteArray())
                            );
                        }
                    }




                    BookkeeperInternalCallbacks.BatchedReadEntryCallback cb = invocationOnMock.getArgument(5);
                    Object ctx = invocationOnMock.getArgument(6);
                    cb.readEntriesComplete(
                            code,
                            ledgerId,
                            startEntry,
                            bufList,
                            ctx
                    );
                    return null;
                }
        ).when(mockBookie).batchReadEntries(
                any(),
                anyLong(),
                anyLong(),
                anyInt(),
                anyLong(),
                any(),
                any(),
                anyInt()
        );

        doAnswer(
         invocationOnMock -> {
             BookkeeperInternalCallbacks.ReadLacCallback cb = invocationOnMock.getArgument(2);
             cb.readLacComplete( BKException.Code.OK, 0L,
                     null,
                     Unpooled.wrappedBuffer(this.entryDigest.get((long)(this.entryDigest.size() - 1)).toByteArray()),
                     0);
             return null;
         }
        ).when(mockBookie).readLac(any(), anyLong(), any(), any());

    }



    private void setMainWorkerPoolBehavior() {
        when(mockOrderedExecutor.chooseThread(anyLong())).thenReturn(this.mockOrderedExecutor);
        doAnswer(
                invocationOnMock -> {
                    Runnable op = invocationOnMock.getArgument(0);
                    op.run();
                    return null;
                }

        ).when(mockOrderedExecutor).execute(any());
    }

    private void setClientStatsBehavior() {
        when(mockBookKeeperClientStats.getEnsembleChangeCounter()).thenReturn(mockCounter);
        when(mockBookKeeperClientStats.getLacUpdateHitsCounter()).thenReturn(mockCounter);
        when(mockBookKeeperClientStats.getLacUpdateMissesCounter()).thenReturn(mockCounter);
        when(mockBookKeeperClientStats.getReadOpDmCounter()).thenReturn(mockCounter);
        when(mockBookKeeperClientStats.getClientChannelWriteWaitLogger()).thenReturn(mockOpStatsLogger);
        when(mockBookKeeperClientStats.getReadOpLogger()).thenReturn(mockOpStatsLogger);
        when(mockBookKeeperClientStats.getReadLacOpLogger()).thenReturn(mockOpStatsLogger);
        when(mockBookKeeperClientStats.getAddOpLogger()).thenReturn(mockOpStatsLogger);
        doNothing().when(mockCounter).inc();
        doNothing().when(mockOpStatsLogger).registerSuccessfulEvent(anyLong(), any());
        doNothing().when(mockOpStatsLogger).registerFailedEvent(anyLong(), any());
        doNothing().when(mockOpStatsLogger).registerSuccessfulValue(anyLong());
        doNothing().when(mockOpStatsLogger).registerFailedValue(anyLong());

    }

    private void setVersionedBehavior(){
        when(this.mockVersioned.getValue()).thenReturn(mockLedgerMetadata);
        when(mockLedgerMetadata.isClosed()).thenReturn(false);
        when(mockLedgerMetadata.getLastEntryId()).thenReturn((long) this.entryDigest.size() - 1);
        when(mockLedgerMetadata.getEnsembleSize()).thenReturn(DefaultValues.ENSEMBLE_SIZE);
        when(mockLedgerMetadata.getWriteQuorumSize()).thenReturn(DefaultValues.ENSEMBLE_SIZE);
        when(mockLedgerMetadata.getAckQuorumSize()).thenReturn(DefaultValues.ENSEMBLE_SIZE);
    }

    public long getLastAddEntryConfirmed(){
        return this.entryDigest.size() - 1;
    }

}

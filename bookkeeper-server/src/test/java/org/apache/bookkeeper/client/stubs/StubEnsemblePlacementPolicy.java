package org.apache.bookkeeper.client.stubs;

import io.netty.util.HashedWheelTimer;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookiesHealthInfo;
import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.StatsLogger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class StubEnsemblePlacementPolicy implements EnsemblePlacementPolicy {
    @Override
    public EnsemblePlacementPolicy initialize(ClientConfiguration conf, Optional<DNSToSwitchMapping> optionalDnsResolver, HashedWheelTimer hashedWheelTimer, FeatureProvider featureProvider, StatsLogger statsLogger, BookieAddressResolver bookieAddressResolver) {
        return null;
    }

    @Override
    public void uninitalize() {

    }

    @Override
    public Set<BookieId> onClusterChanged(Set<BookieId> writableBookies, Set<BookieId> readOnlyBookies) {
        return null;
    }

    @Override
    public PlacementResult<List<BookieId>> newEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies) throws BKException.BKNotEnoughBookiesException {
        return null;
    }

    @Override
    public PlacementResult<BookieId> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize, Map<String, byte[]> customMetadata, List<BookieId> currentEnsemble, BookieId bookieToReplace, Set<BookieId> excludeBookies) throws BKException.BKNotEnoughBookiesException {
        return null;
    }

    @Override
    public void registerSlowBookie(BookieId bookieSocketAddress, long entryId) {

    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(List<BookieId> ensemble, BookiesHealthInfo bookiesHealthInfo, DistributionSchedule.WriteSet writeSet) {
        return null;
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadLACSequence(List<BookieId> ensemble, BookiesHealthInfo bookiesHealthInfo, DistributionSchedule.WriteSet writeSet) {
        return null;
    }
}

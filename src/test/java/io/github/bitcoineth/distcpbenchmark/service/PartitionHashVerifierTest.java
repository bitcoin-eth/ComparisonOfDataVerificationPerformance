package io.github.bitcoineth.distcpbenchmark.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.github.bitcoineth.distcpbenchmark.model.FileDigest;
import io.github.bitcoineth.distcpbenchmark.model.PartitionDigestSummary;
import java.util.List;
import org.junit.jupiter.api.Test;

class PartitionHashVerifierTest {
    private final PartitionDigestAggregator aggregator = new PartitionDigestAggregator();

    @Test
    void aggregateIsStableWhenInputOrderChanges() {
        List<FileDigest> left = List.of(
                new FileDigest("part-00001.bin", 5L, "bbbb"),
                new FileDigest("part-00000.bin", 6L, "aaaa"));
        List<FileDigest> right = List.of(
                new FileDigest("part-00000.bin", 6L, "aaaa"),
                new FileDigest("part-00001.bin", 5L, "bbbb"));

        PartitionDigestSummary first = aggregator.aggregate("dt=2026-04-01/bucket=00000", left);
        PartitionDigestSummary second = aggregator.aggregate("dt=2026-04-01/bucket=00000", right);

        assertEquals(first.partitionSha256, second.partitionSha256);
        assertEquals(first.totalBytes, second.totalBytes);
        assertEquals(first.fileCount, second.fileCount);
    }

    @Test
    void aggregateChangesWhenRelativePathChanges() {
        PartitionDigestSummary original = aggregator.aggregate(
                "dt=2026-04-01/bucket=00000",
                List.of(new FileDigest("part-00000.bin", 5L, "samehash")));
        PartitionDigestSummary changed = aggregator.aggregate(
                "dt=2026-04-01/bucket=00000",
                List.of(new FileDigest("part-99999.bin", 5L, "samehash")));

        assertNotEquals(original.partitionSha256, changed.partitionSha256);
    }

    @Test
    void aggregateChangesWhenFileDigestChanges() {
        PartitionDigestSummary original = aggregator.aggregate(
                "dt=2026-04-01/bucket=00000",
                List.of(new FileDigest("part-00000.bin", 5L, "alpha")));
        PartitionDigestSummary changed = aggregator.aggregate(
                "dt=2026-04-01/bucket=00000",
                List.of(new FileDigest("part-00000.bin", 5L, "beta")));

        assertNotEquals(original.partitionSha256, changed.partitionSha256);
    }
}

package io.github.bitcoineth.distcpbenchmark.model;

import java.util.Collections;
import java.util.List;

public final class VerificationResult {
    public final DatasetStats sourceStats;
    public final List<PartitionComparison> partitionComparisons;
    public final List<FileDifference> fileDifferences;

    public VerificationResult(
            DatasetStats sourceStats,
            List<PartitionComparison> partitionComparisons,
            List<FileDifference> fileDifferences) {
        this.sourceStats = sourceStats;
        this.partitionComparisons = Collections.unmodifiableList(partitionComparisons);
        this.fileDifferences = Collections.unmodifiableList(fileDifferences);
    }

    public boolean matches() {
        return partitionComparisons.stream().allMatch(comparison -> "MATCH".equals(comparison.status));
    }
}

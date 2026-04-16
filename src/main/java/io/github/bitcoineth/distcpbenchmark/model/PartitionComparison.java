package io.github.bitcoineth.distcpbenchmark.model;

public final class PartitionComparison {
    public final String partitionPath;
    public final Long sourceFileCount;
    public final Long sourceTotalBytes;
    public final String sourcePartitionSha256;
    public final Long targetFileCount;
    public final Long targetTotalBytes;
    public final String targetPartitionSha256;
    public final String status;

    public PartitionComparison(
            String partitionPath,
            Long sourceFileCount,
            Long sourceTotalBytes,
            String sourcePartitionSha256,
            Long targetFileCount,
            Long targetTotalBytes,
            String targetPartitionSha256,
            String status) {
        this.partitionPath = partitionPath;
        this.sourceFileCount = sourceFileCount;
        this.sourceTotalBytes = sourceTotalBytes;
        this.sourcePartitionSha256 = sourcePartitionSha256;
        this.targetFileCount = targetFileCount;
        this.targetTotalBytes = targetTotalBytes;
        this.targetPartitionSha256 = targetPartitionSha256;
        this.status = status;
    }
}

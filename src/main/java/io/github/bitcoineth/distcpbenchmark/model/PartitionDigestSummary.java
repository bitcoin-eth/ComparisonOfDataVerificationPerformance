package io.github.bitcoineth.distcpbenchmark.model;

public final class PartitionDigestSummary {
    public final String partitionPath;
    public final long fileCount;
    public final long totalBytes;
    public final String partitionSha256;

    public PartitionDigestSummary(String partitionPath, long fileCount, long totalBytes, String partitionSha256) {
        this.partitionPath = partitionPath;
        this.fileCount = fileCount;
        this.totalBytes = totalBytes;
        this.partitionSha256 = partitionSha256;
    }
}

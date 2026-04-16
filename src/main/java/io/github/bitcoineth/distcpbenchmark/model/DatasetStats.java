package io.github.bitcoineth.distcpbenchmark.model;

public final class DatasetStats {
    public final int partitionCount;
    public final long fileCount;
    public final long totalBytes;

    public DatasetStats(int partitionCount, long fileCount, long totalBytes) {
        this.partitionCount = partitionCount;
        this.fileCount = fileCount;
        this.totalBytes = totalBytes;
    }
}

package io.github.bitcoineth.distcpbenchmark.model;

public final class BenchmarkRecord {
    public final String scenario;
    public final int runIndex;
    public final int partitionCount;
    public final long fileCount;
    public final long totalBytes;
    public final long distcpTimeMs;
    public final long verifyTimeMs;
    public final long totalTimeMs;
    public final double throughputMbPerS;
    public final String status;

    public BenchmarkRecord(
            String scenario,
            int runIndex,
            int partitionCount,
            long fileCount,
            long totalBytes,
            long distcpTimeMs,
            long verifyTimeMs,
            long totalTimeMs,
            double throughputMbPerS,
            String status) {
        this.scenario = scenario;
        this.runIndex = runIndex;
        this.partitionCount = partitionCount;
        this.fileCount = fileCount;
        this.totalBytes = totalBytes;
        this.distcpTimeMs = distcpTimeMs;
        this.verifyTimeMs = verifyTimeMs;
        this.totalTimeMs = totalTimeMs;
        this.throughputMbPerS = throughputMbPerS;
        this.status = status;
    }
}

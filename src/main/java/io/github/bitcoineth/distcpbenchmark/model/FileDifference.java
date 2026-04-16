package io.github.bitcoineth.distcpbenchmark.model;

public final class FileDifference {
    public final String partitionPath;
    public final String relativeFilePath;
    public final Long sourceSize;
    public final String sourceSha256;
    public final Long targetSize;
    public final String targetSha256;
    public final String status;

    public FileDifference(
            String partitionPath,
            String relativeFilePath,
            Long sourceSize,
            String sourceSha256,
            Long targetSize,
            String targetSha256,
            String status) {
        this.partitionPath = partitionPath;
        this.relativeFilePath = relativeFilePath;
        this.sourceSize = sourceSize;
        this.sourceSha256 = sourceSha256;
        this.targetSize = targetSize;
        this.targetSha256 = targetSha256;
        this.status = status;
    }
}

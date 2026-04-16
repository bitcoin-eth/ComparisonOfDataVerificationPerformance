package io.github.bitcoineth.distcpbenchmark.model;

public final class FileDigest {
    public final String relativePath;
    public final long size;
    public final String sha256;

    public FileDigest(String relativePath, long size, String sha256) {
        this.relativePath = relativePath;
        this.size = size;
        this.sha256 = sha256;
    }
}

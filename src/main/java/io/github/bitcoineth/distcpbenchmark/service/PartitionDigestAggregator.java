package io.github.bitcoineth.distcpbenchmark.service;

import io.github.bitcoineth.distcpbenchmark.model.FileDigest;
import io.github.bitcoineth.distcpbenchmark.model.PartitionDigestSummary;
import io.github.bitcoineth.distcpbenchmark.util.Digests;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public final class PartitionDigestAggregator {

    public PartitionDigestSummary aggregate(String partitionPath, Collection<FileDigest> fileDigests) {
        List<FileDigest> orderedFiles = new ArrayList<>(fileDigests);
        orderedFiles.sort(Comparator.comparing(fileDigest -> fileDigest.relativePath));

        MessageDigest aggregate = Digests.sha256();
        long totalBytes = 0L;
        for (FileDigest fileDigest : orderedFiles) {
            aggregate.update((fileDigest.relativePath + "\t" + fileDigest.size + "\t" + fileDigest.sha256 + "\n")
                    .getBytes(StandardCharsets.UTF_8));
            totalBytes += fileDigest.size;
        }
        return new PartitionDigestSummary(partitionPath, orderedFiles.size(), totalBytes, Digests.toHex(aggregate.digest()));
    }
}

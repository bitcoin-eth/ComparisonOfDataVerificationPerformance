package io.github.bitcoineth.distcpbenchmark.service;

import io.github.bitcoineth.distcpbenchmark.model.DatasetStats;
import io.github.bitcoineth.distcpbenchmark.model.FileDifference;
import io.github.bitcoineth.distcpbenchmark.model.FileDigest;
import io.github.bitcoineth.distcpbenchmark.model.PartitionComparison;
import io.github.bitcoineth.distcpbenchmark.model.PartitionDigestSummary;
import io.github.bitcoineth.distcpbenchmark.model.VerificationResult;
import io.github.bitcoineth.distcpbenchmark.util.Digests;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class PartitionHashVerifier {
    private static final int BUFFER_SIZE = 64 * 1024;
    private final PartitionDigestAggregator aggregator = new PartitionDigestAggregator();

    public DatasetStats summarizeDataset(FileSystem fileSystem, Path root) throws IOException {
        SortedMap<String, PartitionDigestSummary> digests = collectPartitionSummaries(fileSystem, root);
        return summarizeFrom(digests);
    }

    public VerificationResult verify(
            FileSystem sourceFileSystem,
            Path sourceRoot,
            FileSystem targetFileSystem,
            Path targetRoot) throws IOException {
        SortedMap<String, PartitionDigestSummary> sourceDigests = collectPartitionSummaries(sourceFileSystem, sourceRoot);
        SortedMap<String, PartitionDigestSummary> targetDigests = collectPartitionSummaries(targetFileSystem, targetRoot);
        DatasetStats sourceStats = summarizeFrom(sourceDigests);

        List<PartitionComparison> comparisons = new ArrayList<>();
        List<FileDifference> differences = new ArrayList<>();
        TreeSet<String> partitions = new TreeSet<>();
        partitions.addAll(sourceDigests.keySet());
        partitions.addAll(targetDigests.keySet());

        for (String partition : partitions) {
            PartitionDigestSummary source = sourceDigests.get(partition);
            PartitionDigestSummary target = targetDigests.get(partition);
            if (source == null) {
                comparisons.add(new PartitionComparison(
                        partition,
                        null,
                        null,
                        null,
                        target.fileCount,
                        target.totalBytes,
                        target.partitionSha256,
                        "EXTRA_IN_TARGET"));
                differences.addAll(renderExtraFiles(targetFileSystem, new Path(targetRoot, partition), partition));
                continue;
            }
            if (target == null) {
                comparisons.add(new PartitionComparison(
                        partition,
                        source.fileCount,
                        source.totalBytes,
                        source.partitionSha256,
                        null,
                        null,
                        null,
                        "MISSING_IN_TARGET"));
                differences.addAll(renderMissingFiles(sourceFileSystem, new Path(sourceRoot, partition), partition));
                continue;
            }

            boolean matches = source.fileCount == target.fileCount
                    && source.totalBytes == target.totalBytes
                    && source.partitionSha256.equals(target.partitionSha256);
            comparisons.add(new PartitionComparison(
                    partition,
                    source.fileCount,
                    source.totalBytes,
                    source.partitionSha256,
                    target.fileCount,
                    target.totalBytes,
                    target.partitionSha256,
                    matches ? "MATCH" : "MISMATCH"));
            if (!matches) {
                differences.addAll(compareFiles(
                        sourceFileSystem,
                        new Path(sourceRoot, partition),
                        targetFileSystem,
                        new Path(targetRoot, partition),
                        partition));
            }
        }
        return new VerificationResult(sourceStats, comparisons, differences);
    }

    private DatasetStats summarizeFrom(SortedMap<String, PartitionDigestSummary> digests) {
        long fileCount = 0L;
        long totalBytes = 0L;
        for (PartitionDigestSummary digest : digests.values()) {
            fileCount += digest.fileCount;
            totalBytes += digest.totalBytes;
        }
        return new DatasetStats(digests.size(), fileCount, totalBytes);
    }

    private List<FileDifference> renderExtraFiles(FileSystem targetFileSystem, Path partitionPath, String partition) throws IOException {
        List<FileDifference> differences = new ArrayList<>();
        for (FileDigest fileDigest : collectFileDigests(targetFileSystem, partitionPath).values()) {
            differences.add(new FileDifference(
                    partition,
                    fileDigest.relativePath,
                    null,
                    null,
                    fileDigest.size,
                    fileDigest.sha256,
                    "EXTRA_IN_TARGET"));
        }
        return differences;
    }

    private List<FileDifference> renderMissingFiles(FileSystem sourceFileSystem, Path partitionPath, String partition) throws IOException {
        List<FileDifference> differences = new ArrayList<>();
        for (FileDigest fileDigest : collectFileDigests(sourceFileSystem, partitionPath).values()) {
            differences.add(new FileDifference(
                    partition,
                    fileDigest.relativePath,
                    fileDigest.size,
                    fileDigest.sha256,
                    null,
                    null,
                    "MISSING_IN_TARGET"));
        }
        return differences;
    }

    private List<FileDifference> compareFiles(
            FileSystem sourceFileSystem,
            Path sourcePartition,
            FileSystem targetFileSystem,
            Path targetPartition,
            String partition) throws IOException {
        SortedMap<String, FileDigest> sourceFiles = collectFileDigests(sourceFileSystem, sourcePartition);
        SortedMap<String, FileDigest> targetFiles = collectFileDigests(targetFileSystem, targetPartition);
        TreeSet<String> allPaths = new TreeSet<>();
        allPaths.addAll(sourceFiles.keySet());
        allPaths.addAll(targetFiles.keySet());

        List<FileDifference> differences = new ArrayList<>();
        for (String relativePath : allPaths) {
            FileDigest source = sourceFiles.get(relativePath);
            FileDigest target = targetFiles.get(relativePath);
            if (source == null) {
                differences.add(new FileDifference(partition, relativePath, null, null, target.size, target.sha256, "EXTRA_IN_TARGET"));
                continue;
            }
            if (target == null) {
                differences.add(new FileDifference(partition, relativePath, source.size, source.sha256, null, null, "MISSING_IN_TARGET"));
                continue;
            }
            if (source.size != target.size) {
                differences.add(new FileDifference(partition, relativePath, source.size, source.sha256, target.size, target.sha256, "SIZE_MISMATCH"));
                continue;
            }
            if (!source.sha256.equals(target.sha256)) {
                differences.add(new FileDifference(partition, relativePath, source.size, source.sha256, target.size, target.sha256, "HASH_MISMATCH"));
            }
        }
        return differences;
    }

    private SortedMap<String, PartitionDigestSummary> collectPartitionSummaries(FileSystem fileSystem, Path root) throws IOException {
        if (!fileSystem.exists(root)) {
            throw new IllegalArgumentException("Path does not exist: " + root);
        }
        TreeMap<String, PartitionDigestSummary> digests = new TreeMap<>();
        walkForPartitions(fileSystem, root, root, digests);
        if (digests.isEmpty()) {
            throw new IllegalArgumentException("No Hive-style partitions found under " + root);
        }
        return digests;
    }

    private void walkForPartitions(
            FileSystem fileSystem,
            Path root,
            Path current,
            Map<String, PartitionDigestSummary> digests) throws IOException {
        FileStatus[] statuses = fileSystem.listStatus(current);
        boolean hasVisibleFiles = false;
        List<Path> subDirectories = new ArrayList<>();
        for (FileStatus status : statuses) {
            if (shouldIgnore(status.getPath().getName())) {
                continue;
            }
            if (status.isDirectory()) {
                subDirectories.add(status.getPath());
            } else {
                hasVisibleFiles = true;
            }
        }

        if (hasVisibleFiles) {
            String relativePartition = relativize(root, current);
            if (relativePartition.isEmpty() || !isHivePartitionPath(relativePartition)) {
                throw new IllegalArgumentException("Encountered non-Hive leaf directory with data files: " + current);
            }
            digests.put(relativePartition, computePartitionDigestSummary(fileSystem, current, relativePartition));
            return;
        }

        for (Path subDirectory : subDirectories) {
            walkForPartitions(fileSystem, root, subDirectory, digests);
        }
    }

    private PartitionDigestSummary computePartitionDigestSummary(FileSystem fileSystem, Path partitionPath, String relativePartition) throws IOException {
        SortedMap<String, FileDigest> fileDigests = collectFileDigests(fileSystem, partitionPath);
        return aggregator.aggregate(relativePartition, fileDigests.values());
    }

    private SortedMap<String, FileDigest> collectFileDigests(FileSystem fileSystem, Path partitionPath) throws IOException {
        TreeMap<String, FileDigest> files = new TreeMap<>();
        collectFilesRecursively(fileSystem, partitionPath, partitionPath, files);
        return files;
    }

    private void collectFilesRecursively(
            FileSystem fileSystem,
            Path partitionRoot,
            Path current,
            Map<String, FileDigest> files) throws IOException {
        for (FileStatus status : fileSystem.listStatus(current)) {
            if (shouldIgnore(status.getPath().getName())) {
                continue;
            }
            if (status.isDirectory()) {
                collectFilesRecursively(fileSystem, partitionRoot, status.getPath(), files);
            } else {
                String relativePath = relativize(partitionRoot, status.getPath());
                files.put(relativePath, new FileDigest(relativePath, status.getLen(), hashFile(fileSystem, status.getPath())));
            }
        }
    }

    private String hashFile(FileSystem fileSystem, Path path) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        MessageDigest digest = Digests.sha256();
        try (FSDataInputStream inputStream = fileSystem.open(path)) {
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
                if (read > 0) {
                    digest.update(buffer, 0, read);
                }
            }
        }
        return Digests.toHex(digest.digest());
    }

    private boolean shouldIgnore(String name) {
        return name.startsWith(".")
                || name.startsWith("_")
                || name.endsWith(".crc")
                || name.contains("._COPYING_");
    }

    private boolean isHivePartitionPath(String relativePartition) {
        String[] segments = relativePartition.split("/");
        for (String segment : segments) {
            int index = segment.indexOf('=');
            if (index <= 0 || index == segment.length() - 1) {
                return false;
            }
        }
        return true;
    }

    private String relativize(Path base, Path path) {
        String basePath = normalize(base.toUri().getPath());
        String childPath = normalize(path.toUri().getPath());
        if (basePath.equals(childPath)) {
            return "";
        }
        if (!childPath.startsWith(basePath + "/")) {
            throw new IllegalArgumentException("Path " + path + " is not under " + base);
        }
        return childPath.substring(basePath.length() + 1);
    }

    private String normalize(String value) {
        return value.endsWith("/") && value.length() > 1
                ? value.substring(0, value.length() - 1)
                : value;
    }
}

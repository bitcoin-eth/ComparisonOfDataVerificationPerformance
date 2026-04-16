package io.github.bitcoineth.distcpbenchmark.service;

import io.github.bitcoineth.distcpbenchmark.model.DatasetStats;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class DataGenerator {
    private static final int BUFFER_SIZE = 16 * 1024;

    public DatasetStats generate(
            FileSystem fileSystem,
            Path basePath,
            int partitions,
            int filesPerPartition,
            int minSizeBytes,
            int maxSizeBytes,
            long seed) throws IOException {
        if (fileSystem.exists(basePath)) {
            fileSystem.delete(basePath, true);
        }
        fileSystem.mkdirs(basePath);

        Random sizeRandom = new Random(seed);
        long totalBytes = 0L;
        for (int partitionIndex = 0; partitionIndex < partitions; partitionIndex++) {
            Path partitionPath = buildPartitionPath(basePath, partitionIndex);
            fileSystem.mkdirs(partitionPath);
            for (int fileIndex = 0; fileIndex < filesPerPartition; fileIndex++) {
                int fileSize = randomSize(sizeRandom, minSizeBytes, maxSizeBytes);
                writeFile(
                        fileSystem,
                        new Path(partitionPath, String.format("part-%05d.bin", fileIndex)),
                        fileSize,
                        seed ^ ((long) partitionIndex << 32) ^ fileIndex);
                totalBytes += fileSize;
            }
        }
        return new DatasetStats(partitions, (long) partitions * filesPerPartition, totalBytes);
    }

    private Path buildPartitionPath(Path basePath, int partitionIndex) {
        String dt = String.format("dt=2026-04-%02d", (partitionIndex % 28) + 1);
        String bucket = String.format("bucket=%05d", partitionIndex);
        return new Path(new Path(basePath, dt), bucket);
    }

    private int randomSize(Random random, int minSizeBytes, int maxSizeBytes) {
        if (minSizeBytes == maxSizeBytes) {
            return minSizeBytes;
        }
        return minSizeBytes + random.nextInt(maxSizeBytes - minSizeBytes + 1);
    }

    private void writeFile(FileSystem fileSystem, Path filePath, int size, long seed) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        Random contentRandom = new Random(seed);
        try (FSDataOutputStream outputStream = fileSystem.create(filePath, true)) {
            int remaining = size;
            while (remaining > 0) {
                int chunk = Math.min(remaining, buffer.length);
                contentRandom.nextBytes(buffer);
                outputStream.write(buffer, 0, chunk);
                remaining -= chunk;
            }
        }
    }
}

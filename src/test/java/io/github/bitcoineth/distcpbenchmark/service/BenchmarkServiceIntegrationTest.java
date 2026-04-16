package io.github.bitcoineth.distcpbenchmark.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bitcoineth.distcpbenchmark.model.BenchmarkRecord;
import io.github.bitcoineth.distcpbenchmark.model.DatasetStats;
import io.github.bitcoineth.distcpbenchmark.model.VerificationResult;
import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs(value = OS.WINDOWS, disabledReason = "MiniDFSCluster requires Hadoop native Windows setup (winutils/HADOOP_HOME).")
class BenchmarkServiceIntegrationTest {
    private static MiniDFSCluster cluster;
    private static Configuration configuration;
    private static java.nio.file.Path baseDir;

    @BeforeAll
    static void setUp() throws Exception {
        baseDir = Files.createTempDirectory("minidfs");
        HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
        hdfsConfiguration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());
        hdfsConfiguration.set("mapreduce.framework.name", "local");
        hdfsConfiguration.set("mapreduce.jobtracker.address", "local");
        hdfsConfiguration.set("fs.defaultFS", "hdfs://localhost:0");
        cluster = new MiniDFSCluster.Builder(hdfsConfiguration).numDataNodes(1).format(true).build();
        cluster.waitClusterUp();
        configuration = new Configuration(cluster.getConfiguration(0));
        configuration.set("fs.defaultFS", cluster.getFileSystem().getUri().toString());
        configuration.set("mapreduce.framework.name", "local");
        configuration.set("mapreduce.jobtracker.address", "local");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (cluster != null) {
            cluster.shutdown();
        }
        if (baseDir != null) {
            Files.walk(baseDir)
                    .sorted((left, right) -> right.compareTo(left))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception ignored) {
                        }
                    });
        }
    }

    @Test
    void generateDataCreatesExpectedLayout() throws Exception {
        try (FileSystem fileSystem = cluster.getFileSystem()) {
            DatasetStats stats = new DataGenerator().generate(fileSystem, new Path("/bench/source-a"), 3, 2, 64, 64, 42L);
            assertEquals(3, stats.partitionCount);
            assertEquals(6, stats.fileCount);
            assertTrue(fileSystem.exists(new Path("/bench/source-a/dt=2026-04-01/bucket=00000/part-00000.bin")));
        }
    }

    @Test
    void benchmarkProducesReports() throws Exception {
        try (FileSystem fileSystem = cluster.getFileSystem()) {
            Path source = new Path("/bench/source-b");
            Path target = new Path("/bench/target-b");
            new DataGenerator().generate(fileSystem, source, 2, 2, 64, 128, 99L);

            java.nio.file.Path reportDir = Files.createTempDirectory("benchmark-reports");
            List<BenchmarkRecord> records = new BenchmarkService(
                    configuration,
                    new DistCpService(),
                    new PartitionHashVerifier(),
                    new ReportWriter())
                    .runBenchmark(fileSystem, source, fileSystem, target, 1, reportDir);

            assertEquals(2, records.size());
            assertTrue(Files.exists(reportDir.resolve("benchmark-results.csv")));
            assertTrue(Files.exists(reportDir.resolve("benchmark-summary.md")));
            assertTrue(records.stream().allMatch(record -> "SUCCESS".equals(record.status)));
        }
    }

    @Test
    void verifyDetectsTargetTamperingAfterDistCp() throws Exception {
        try (FileSystem fileSystem = cluster.getFileSystem()) {
            Path source = new Path("/bench/source-c");
            Path target = new Path("/bench/target-c");
            new DataGenerator().generate(fileSystem, source, 1, 1, 64, 64, 7L);
            new DistCpService().sync(configuration, source, target);

            Path tamperedFile = new Path("/bench/target-c/dt=2026-04-01/bucket=00000/part-00000.bin");
            try (FSDataOutputStream outputStream = fileSystem.create(tamperedFile, true)) {
                outputStream.write("tampered".getBytes());
            }

            VerificationResult result = new PartitionHashVerifier().verify(fileSystem, source, fileSystem, target);
            assertFalse(result.matches());
            assertTrue(result.fileDifferences.stream().anyMatch(diff ->
                    "HASH_MISMATCH".equals(diff.status) || "SIZE_MISMATCH".equals(diff.status)));
        }
    }
}

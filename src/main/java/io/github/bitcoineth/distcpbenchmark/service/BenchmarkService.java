package io.github.bitcoineth.distcpbenchmark.service;

import io.github.bitcoineth.distcpbenchmark.model.BenchmarkRecord;
import io.github.bitcoineth.distcpbenchmark.model.DatasetStats;
import io.github.bitcoineth.distcpbenchmark.model.VerificationResult;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public final class BenchmarkService {
    private final Configuration configuration;
    private final DistCpService distCpService;
    private final PartitionHashVerifier verifier;
    private final ReportWriter reportWriter;

    public BenchmarkService(
            Configuration configuration,
            DistCpService distCpService,
            PartitionHashVerifier verifier,
            ReportWriter reportWriter) {
        this.configuration = configuration;
        this.distCpService = distCpService;
        this.verifier = verifier;
        this.reportWriter = reportWriter;
    }

    public List<BenchmarkRecord> runBenchmark(
            FileSystem sourceFileSystem,
            org.apache.hadoop.fs.Path source,
            FileSystem targetFileSystem,
            org.apache.hadoop.fs.Path target,
            int runs,
            Path reportDir) throws Exception {
        DatasetStats sourceStats = verifier.summarizeDataset(sourceFileSystem, source);
        List<BenchmarkRecord> records = new ArrayList<>();
        records.addAll(runScenario("distcp_only", sourceStats, sourceFileSystem, source, targetFileSystem, target, runs, reportDir));
        records.addAll(runScenario("distcp_plus_verify", sourceStats, sourceFileSystem, source, targetFileSystem, target, runs, reportDir));
        reportWriter.writeBenchmarkReports(records, reportDir);
        return records;
    }

    private List<BenchmarkRecord> runScenario(
            String scenario,
            DatasetStats sourceStats,
            FileSystem sourceFileSystem,
            org.apache.hadoop.fs.Path source,
            FileSystem targetFileSystem,
            org.apache.hadoop.fs.Path target,
            int runs,
            Path reportDir) {
        List<BenchmarkRecord> records = new ArrayList<>();
        for (int runIndex = 1; runIndex <= runs; runIndex++) {
            long distcpTimeMs = 0L;
            long verifyTimeMs = 0L;
            String status = "SUCCESS";
            try {
                resetTarget(targetFileSystem, target);
                long distcpStarted = System.nanoTime();
                distCpService.sync(configuration, source, target);
                distcpTimeMs = elapsedMillis(distcpStarted);

                if ("distcp_plus_verify".equals(scenario)) {
                    long verifyStarted = System.nanoTime();
                    VerificationResult verificationResult = verifier.verify(sourceFileSystem, source, targetFileSystem, target);
                    verifyTimeMs = elapsedMillis(verifyStarted);
                    if (!verificationResult.matches()) {
                        status = "VERIFY_MISMATCH";
                        reportWriter.writeVerificationReports(
                                verificationResult,
                                reportDir.resolve("verification").resolve("run-" + runIndex));
                    }
                }
            } catch (Exception e) {
                status = "FAILED";
                System.err.printf("Scenario %s run %d failed: %s%n", scenario, runIndex, e.getMessage());
            }

            long totalTimeMs = distcpTimeMs + verifyTimeMs;
            double throughputMbPerS = totalTimeMs == 0L
                    ? 0.0d
                    : (sourceStats.totalBytes / 1024.0d / 1024.0d) / (totalTimeMs / 1000.0d);
            records.add(new BenchmarkRecord(
                    scenario,
                    runIndex,
                    sourceStats.partitionCount,
                    sourceStats.fileCount,
                    sourceStats.totalBytes,
                    distcpTimeMs,
                    verifyTimeMs,
                    totalTimeMs,
                    throughputMbPerS,
                    status));
        }
        return records;
    }

    private void resetTarget(FileSystem fileSystem, org.apache.hadoop.fs.Path target) throws Exception {
        if (fileSystem.exists(target)) {
            fileSystem.delete(target, true);
        }
        fileSystem.mkdirs(target);
    }

    private long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}

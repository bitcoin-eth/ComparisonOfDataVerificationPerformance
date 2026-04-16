package io.github.bitcoineth.distcpbenchmark.service;

import io.github.bitcoineth.distcpbenchmark.model.BenchmarkRecord;
import io.github.bitcoineth.distcpbenchmark.model.FileDifference;
import io.github.bitcoineth.distcpbenchmark.model.PartitionComparison;
import io.github.bitcoineth.distcpbenchmark.model.VerificationResult;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public final class ReportWriter {

    public void writeBenchmarkReports(List<BenchmarkRecord> records, Path reportDir) throws IOException {
        Files.createDirectories(reportDir);
        writeBenchmarkCsv(records, reportDir.resolve("benchmark-results.csv"));
        writeBenchmarkMarkdown(records, reportDir.resolve("benchmark-summary.md"));
    }

    public void writeVerificationReports(VerificationResult result, Path reportDir) throws IOException {
        Files.createDirectories(reportDir);
        writePartitionCsv(result.partitionComparisons, reportDir.resolve("partition-report.csv"));
        writeFileDiffCsv(result.fileDifferences, reportDir.resolve("file-diff-report.csv"));
    }

    private void writeBenchmarkCsv(List<BenchmarkRecord> records, Path outputFile) throws IOException {
        try (Writer writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            writer.write("scenario,run_index,partition_count,file_count,total_bytes,distcp_time_ms,verify_time_ms,total_time_ms,throughput_mb_per_s,status\n");
            for (BenchmarkRecord record : records) {
                writer.write(csv(record.scenario) + ","
                        + record.runIndex + ","
                        + record.partitionCount + ","
                        + record.fileCount + ","
                        + record.totalBytes + ","
                        + record.distcpTimeMs + ","
                        + record.verifyTimeMs + ","
                        + record.totalTimeMs + ","
                        + String.format(Locale.ROOT, "%.4f", record.throughputMbPerS) + ","
                        + csv(record.status)
                        + "\n");
            }
        }
    }

    private void writeBenchmarkMarkdown(List<BenchmarkRecord> records, Path outputFile) throws IOException {
        Map<String, List<BenchmarkRecord>> byScenario = records.stream()
                .collect(Collectors.groupingBy(record -> record.scenario));
        List<BenchmarkRecord> distcpOnly = byScenario.getOrDefault("distcp_only", List.of());
        List<BenchmarkRecord> distcpPlusVerify = byScenario.getOrDefault("distcp_plus_verify", List.of());

        try (Writer writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            writer.write("# Benchmark Summary\n\n");
            if (!records.isEmpty()) {
                BenchmarkRecord sample = records.get(0);
                writer.write(String.format(Locale.ROOT,
                        "- Partitions: %,d%n- Files: %,d%n- Total bytes: %,d%n%n",
                        sample.partitionCount,
                        sample.fileCount,
                        sample.totalBytes));
            }
            writer.write("| Scenario | Avg total ms | Avg DistCp ms | Avg verify ms | Avg throughput MB/s |\n");
            writer.write("| --- | ---: | ---: | ---: | ---: |\n");
            writeScenarioRow(writer, "distcp_only", distcpOnly);
            writeScenarioRow(writer, "distcp_plus_verify", distcpPlusVerify);

            double baseline = average(recordsFor(distcpOnly).mapToLong(record -> record.totalTimeMs).asDoubleStream().summaryStatistics());
            double candidate = average(recordsFor(distcpPlusVerify).mapToLong(record -> record.totalTimeMs).asDoubleStream().summaryStatistics());
            double overheadRatio = baseline == 0.0d ? 0.0d : ((candidate - baseline) / baseline) * 100.0d;
            writer.write(String.format(Locale.ROOT, "%nVerification overhead ratio: %.2f%%%n%n", overheadRatio));
            writer.write("Conclusion: `distcp_plus_verify` measures the cost of running a full SHA-256 content verification after DistCp. It does not represent DistCp's built-in checksum overhead.\n");
        }
    }

    private java.util.stream.Stream<BenchmarkRecord> recordsFor(List<BenchmarkRecord> records) {
        return records.stream().filter(record -> "SUCCESS".equals(record.status));
    }

    private void writeScenarioRow(Writer writer, String scenario, List<BenchmarkRecord> records) throws IOException {
        DoubleSummaryStatistics total = recordsFor(records).mapToLong(record -> record.totalTimeMs).asDoubleStream().summaryStatistics();
        DoubleSummaryStatistics distcp = recordsFor(records).mapToLong(record -> record.distcpTimeMs).asDoubleStream().summaryStatistics();
        DoubleSummaryStatistics verify = recordsFor(records).mapToLong(record -> record.verifyTimeMs).asDoubleStream().summaryStatistics();
        DoubleSummaryStatistics throughput = recordsFor(records).mapToDouble(record -> record.throughputMbPerS).summaryStatistics();
        writer.write(String.format(Locale.ROOT,
                "| %s | %.2f | %.2f | %.2f | %.4f |%n",
                scenario,
                average(total),
                average(distcp),
                average(verify),
                average(throughput)));
    }

    private double average(DoubleSummaryStatistics statistics) {
        return statistics.getCount() == 0 ? 0.0d : statistics.getAverage();
    }

    private void writePartitionCsv(List<PartitionComparison> comparisons, Path outputFile) throws IOException {
        try (Writer writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            writer.write("partition_path,source_file_count,source_total_bytes,source_partition_sha256,target_file_count,target_total_bytes,target_partition_sha256,status\n");
            for (PartitionComparison comparison : comparisons) {
                writer.write(csv(comparison.partitionPath) + ","
                        + csv(number(comparison.sourceFileCount)) + ","
                        + csv(number(comparison.sourceTotalBytes)) + ","
                        + csv(comparison.sourcePartitionSha256) + ","
                        + csv(number(comparison.targetFileCount)) + ","
                        + csv(number(comparison.targetTotalBytes)) + ","
                        + csv(comparison.targetPartitionSha256) + ","
                        + csv(comparison.status)
                        + "\n");
            }
        }
    }

    private void writeFileDiffCsv(List<FileDifference> differences, Path outputFile) throws IOException {
        try (Writer writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            writer.write("partition_path,relative_file_path,source_size,source_sha256,target_size,target_sha256,status\n");
            for (FileDifference difference : differences) {
                writer.write(csv(difference.partitionPath) + ","
                        + csv(difference.relativeFilePath) + ","
                        + csv(number(difference.sourceSize)) + ","
                        + csv(difference.sourceSha256) + ","
                        + csv(number(difference.targetSize)) + ","
                        + csv(difference.targetSha256) + ","
                        + csv(difference.status)
                        + "\n");
            }
        }
    }

    private String number(Number value) {
        return value == null ? "" : value.toString();
    }

    private String csv(String value) {
        String sanitized = value == null ? "" : value;
        return "\"" + sanitized.replace("\"", "\"\"") + "\"";
    }
}

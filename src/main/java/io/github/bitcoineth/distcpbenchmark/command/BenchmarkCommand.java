package io.github.bitcoineth.distcpbenchmark.command;

import io.github.bitcoineth.distcpbenchmark.model.BenchmarkRecord;
import io.github.bitcoineth.distcpbenchmark.service.BenchmarkService;
import io.github.bitcoineth.distcpbenchmark.service.DistCpService;
import io.github.bitcoineth.distcpbenchmark.service.PartitionHashVerifier;
import io.github.bitcoineth.distcpbenchmark.service.ReportWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "benchmark", description = "Run DistCp-only and DistCp-plus-verify benchmark scenarios.")
public final class BenchmarkCommand extends AbstractHadoopCommand implements Callable<Integer> {

    @Option(names = "--source", required = true, description = "HDFS source path.")
    private String sourcePath;

    @Option(names = "--target", required = true, description = "HDFS target path.")
    private String targetPath;

    @Option(names = "--runs", defaultValue = "3", description = "Number of runs per scenario. Default: ${DEFAULT-VALUE}")
    private int runs;

    @Option(names = "--report-dir", defaultValue = "reports", description = "Local directory for benchmark reports. Default: ${DEFAULT-VALUE}")
    private String reportDir;

    @Override
    public Integer call() throws Exception {
        if (runs <= 0) {
            throw new IllegalArgumentException("--runs must be > 0");
        }
        Configuration configuration = createConfiguration();
        org.apache.hadoop.fs.Path source = new org.apache.hadoop.fs.Path(sourcePath);
        org.apache.hadoop.fs.Path target = new org.apache.hadoop.fs.Path(targetPath);
        Path localReportDir = toLocalPath(reportDir);

        try (FileSystem sourceFileSystem = source.getFileSystem(configuration);
             FileSystem targetFileSystem = target.getFileSystem(configuration)) {
            BenchmarkService benchmarkService = new BenchmarkService(
                    configuration,
                    new DistCpService(),
                    new PartitionHashVerifier(),
                    new ReportWriter());
            List<BenchmarkRecord> records = benchmarkService.runBenchmark(
                    sourceFileSystem,
                    source,
                    targetFileSystem,
                    target,
                    runs,
                    localReportDir);
            long failures = records.stream().filter(record -> !"SUCCESS".equals(record.status)).count();
            System.out.printf(
                    "Benchmark completed: %,d records written to %s (failures=%d)%n",
                    records.size(),
                    localReportDir,
                    failures);
            return failures == 0 ? 0 : 1;
        }
    }
}

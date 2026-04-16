package io.github.bitcoineth.distcpbenchmark.command;

import io.github.bitcoineth.distcpbenchmark.model.VerificationResult;
import io.github.bitcoineth.distcpbenchmark.service.PartitionHashVerifier;
import io.github.bitcoineth.distcpbenchmark.service.ReportWriter;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "verify-only", description = "Run partition-level SHA-256 verification only.")
public final class VerifyOnlyCommand extends AbstractHadoopCommand implements Callable<Integer> {

    @Option(names = "--source", required = true, description = "HDFS source path.")
    private String sourcePath;

    @Option(names = "--target", required = true, description = "HDFS target path.")
    private String targetPath;

    @Option(names = "--report-dir", defaultValue = "reports/verify", description = "Local directory for verification reports. Default: ${DEFAULT-VALUE}")
    private String reportDir;

    @Override
    public Integer call() throws Exception {
        Configuration configuration = createConfiguration();
        org.apache.hadoop.fs.Path source = new org.apache.hadoop.fs.Path(sourcePath);
        org.apache.hadoop.fs.Path target = new org.apache.hadoop.fs.Path(targetPath);
        Path localReportDir = toLocalPath(reportDir);

        try (FileSystem sourceFileSystem = source.getFileSystem(configuration);
             FileSystem targetFileSystem = target.getFileSystem(configuration)) {
            VerificationResult result = new PartitionHashVerifier().verify(sourceFileSystem, source, targetFileSystem, target);
            new ReportWriter().writeVerificationReports(result, localReportDir);
            System.out.printf(
                    "Verification completed: matches=%s, partition report=%s%n",
                    result.matches(),
                    localReportDir.resolve("partition-report.csv"));
            return result.matches() ? 0 : 1;
        }
    }
}

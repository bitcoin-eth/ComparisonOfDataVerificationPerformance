package io.github.bitcoineth.distcpbenchmark.command;

import io.github.bitcoineth.distcpbenchmark.model.DatasetStats;
import io.github.bitcoineth.distcpbenchmark.service.DataGenerator;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "generate-data", description = "Generate Hive-style benchmark data in HDFS.")
public final class GenerateDataCommand extends AbstractHadoopCommand implements Callable<Integer> {

    @Option(names = "--base-path", required = true, description = "HDFS base path to create benchmark data in.")
    private String basePath;

    @Option(names = "--partitions", required = true, description = "Number of Hive partitions to generate.")
    private int partitions;

    @Option(names = "--files-per-partition", required = true, description = "Number of files to generate in each partition.")
    private int filesPerPartition;

    @Option(names = "--min-size-bytes", required = true, description = "Minimum file size in bytes.")
    private int minSizeBytes;

    @Option(names = "--max-size-bytes", required = true, description = "Maximum file size in bytes.")
    private int maxSizeBytes;

    @Option(names = "--seed", required = true, description = "Random seed used to make generated data reproducible.")
    private long seed;

    @Override
    public Integer call() throws Exception {
        validate();
        Configuration configuration = createConfiguration();
        Path hdfsBasePath = new Path(basePath);
        try (FileSystem fileSystem = hdfsBasePath.getFileSystem(configuration)) {
            DatasetStats stats = new DataGenerator().generate(
                    fileSystem,
                    hdfsBasePath,
                    partitions,
                    filesPerPartition,
                    minSizeBytes,
                    maxSizeBytes,
                    seed);
            System.out.printf(
                    "Generated %,d partitions, %,d files, %,d total bytes at %s%n",
                    stats.partitionCount,
                    stats.fileCount,
                    stats.totalBytes,
                    hdfsBasePath);
        }
        return 0;
    }

    private void validate() {
        if (partitions <= 0) {
            throw new IllegalArgumentException("--partitions must be > 0");
        }
        if (filesPerPartition <= 0) {
            throw new IllegalArgumentException("--files-per-partition must be > 0");
        }
        if (minSizeBytes <= 0 || maxSizeBytes <= 0) {
            throw new IllegalArgumentException("File size bounds must be > 0");
        }
        if (minSizeBytes > maxSizeBytes) {
            throw new IllegalArgumentException("--min-size-bytes must be <= --max-size-bytes");
        }
    }
}

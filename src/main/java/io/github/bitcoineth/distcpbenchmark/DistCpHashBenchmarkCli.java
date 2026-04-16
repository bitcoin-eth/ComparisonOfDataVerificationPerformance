package io.github.bitcoineth.distcpbenchmark;

import io.github.bitcoineth.distcpbenchmark.command.BenchmarkCommand;
import io.github.bitcoineth.distcpbenchmark.command.GenerateDataCommand;
import io.github.bitcoineth.distcpbenchmark.command.VerifyOnlyCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
        name = "distcp-hash-benchmark",
        mixinStandardHelpOptions = true,
        description = "Benchmark DistCp versus DistCp plus partition-level SHA-256 verification.",
        subcommands = {
                GenerateDataCommand.class,
                BenchmarkCommand.class,
                VerifyOnlyCommand.class
        })
public final class DistCpHashBenchmarkCli implements Runnable {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new DistCpHashBenchmarkCli()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }
}

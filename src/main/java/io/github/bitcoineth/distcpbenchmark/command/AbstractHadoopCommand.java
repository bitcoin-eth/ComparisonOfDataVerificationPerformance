package io.github.bitcoineth.distcpbenchmark.command;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;

abstract class AbstractHadoopCommand {

    protected Configuration createConfiguration() {
        return new Configuration();
    }

    protected Path toLocalPath(String rawPath) {
        return Paths.get(rawPath).toAbsolutePath().normalize();
    }
}

package io.github.bitcoineth.distcpbenchmark.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

public final class DistCpService {

    public void sync(Configuration configuration, Path source, Path target) throws Exception {
        String hadoopBinary = resolveHadoopBinary();
        if (hadoopBinary != null) {
            runExternalDistCp(hadoopBinary, source, target);
            return;
        }

        DistCpOptions options = new DistCpOptions.Builder(Collections.singletonList(source), target)
                .withSyncFolder(true)
                .build();
        DistCp distCp = new DistCp(configuration, options);
        Job job = distCp.execute();
        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            throw new IllegalStateException("DistCp job failed");
        }
    }

    private void runExternalDistCp(String hadoopBinary, Path source, Path target) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(hadoopBinary);
        command.add("distcp");
        command.add("-update");
        command.add(source.toString());
        command.add(target.toString());

        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append(System.lineSeparator());
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("External distcp failed with exit code " + exitCode + ": " + output);
        }
    }

    private String resolveHadoopBinary() {
        String explicit = System.getenv("HADOOP_BIN");
        if (isUsable(explicit)) {
            return explicit;
        }

        String hadoopHome = System.getenv("HADOOP_HOME");
        if (isUsable(hadoopHome == null ? null : hadoopHome + "/bin/hadoop")) {
            return hadoopHome + "/bin/hadoop";
        }

        String[] candidates = {
                "/opt/module/hadoop-3.3.6/bin/hadoop",
                "/usr/local/hadoop/bin/hadoop",
                "/usr/bin/hadoop"
        };
        for (String candidate : candidates) {
            if (isUsable(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private boolean isUsable(String path) {
        return path != null && !path.isBlank() && new java.io.File(path).exists();
    }
}

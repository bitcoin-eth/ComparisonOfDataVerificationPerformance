package io.github.bitcoineth.distcpbenchmark.service;

import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

public final class DistCpService {

    public void sync(Configuration configuration, Path source, Path target) throws Exception {
        DistCpOptions options = new DistCpOptions.Builder(Collections.singletonList(source), target)
                .withSyncFolder(true)
                .build();
        DistCp distCp = new DistCp(configuration, options);
        Job job = distCp.execute();
        distCp.waitForJobCompletion(job);
        if (!job.isSuccessful()) {
            throw new IllegalStateException("DistCp job failed with state " + job.getJobState());
        }
    }
}

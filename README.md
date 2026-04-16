# ComparisonOfDataVerificationPerformance

`ComparisonOfDataVerificationPerformance` 是一个 Maven Java CLI 工具，用来量化以下两条流程的性能差异：

- `DistCp only`
- `DistCp + 分区级 SHA-256 核验`

重点场景是海量小文件。工具会输出原始 `CSV` 结果和可直接用于 GitHub/PR 的 `Markdown` 摘要。

## Environment

- JDK 11
- Maven 3.9+
- Hadoop 3.3.x 客户端配置
- `HADOOP_CONF_DIR` 或 `core-site.xml` / `hdfs-site.xml` 可被 JVM 读取

## Build

```bash
mvn clean package
```

打包完成后，主产物位于 `target/comparison-of-data-verification-performance-0.1.0-SNAPSHOT.jar`。

## Commands

### 1. Generate benchmark data

```bash
java -jar target/comparison-of-data-verification-performance-0.1.0-SNAPSHOT.jar \
  generate-data \
  --base-path hdfs:///tmp/distcp-benchmark/source \
  --partitions 32 \
  --files-per-partition 500 \
  --min-size-bytes 1024 \
  --max-size-bytes 8192 \
  --seed 20260416
```

### 2. Run benchmark

```bash
java -jar target/comparison-of-data-verification-performance-0.1.0-SNAPSHOT.jar \
  benchmark \
  --source hdfs:///tmp/distcp-benchmark/source \
  --target hdfs:///tmp/distcp-benchmark/target \
  --runs 3 \
  --report-dir reports
```

### 3. Verify only

```bash
java -jar target/comparison-of-data-verification-performance-0.1.0-SNAPSHOT.jar \
  verify-only \
  --source hdfs:///tmp/distcp-benchmark/source \
  --target hdfs:///tmp/distcp-benchmark/target \
  --report-dir reports/verify
```

## Partition Hash Definition

1. 以 Hive 风格叶子目录作为分区。
2. 对分区内文件按相对路径排序。
3. 流式计算每个文件的 `SHA-256`。
4. 生成规范化记录：`<relative_path>\t<size>\t<file_sha256>\n`。
5. 对所有规范化记录再次计算 `SHA-256`，得到该分区最终 hash。

## Outputs

- `benchmark-results.csv`
- `benchmark-summary.md`
- `partition-report.csv`
- `file-diff-report.csv`

## Interpretation Boundary

这个项目测量的是“DistCp 完成之后，再做完整内容 SHA-256 核验”的额外成本，不代表 DistCp 自带 checksum 行为的内部开销。

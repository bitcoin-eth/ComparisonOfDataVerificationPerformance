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
## 第一轮测试结论

第一轮测试在云服务器 `8.147.60.67` 上完成，测试目录为：

```bash
/root/project-tests/distcp-hash-benchmark-test
```

本轮对比的是两条流程的端到端耗时：

- 仅执行 `DistCp`
- 执行 `DistCp` 后，再对源端和目标端文件内容做分区级 `SHA-256` 核验

每组重复 3 次，测试结果如下：

| 用例 | 分区数 | 文件总数 | 数据量(MB) | DistCp平均耗时(ms) | DistCp+Hash平均耗时(ms) | Hash平均耗时(ms) | 额外开销占比 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| case01 | 4 | 480 | 0.70 | 21423.33 | 20572.33 | 986.67 | -3.97% |
| case02 | 8 | 1440 | 8.41 | 38090.00 | 34484.67 | 2212.00 | -9.47% |
| case03 | 12 | 3120 | 73.15 | 52815.33 | 58223.33 | 4445.00 | 10.24% |

各用例参数：

- `case01`：4 个分区，每个分区 120 个文件，单文件大小 1KB 到 2KB
- `case02`：8 个分区，每个分区 180 个文件，单文件大小 4KB 到 8KB
- `case03`：12 个分区，每个分区 260 个文件，单文件大小 16KB 到 32KB

结论：

- `case01` 和 `case02` 的额外开销占比为负值，说明在较小数据量下，单次测试波动已经接近 DistCp 与 Hash 的实际差异，结果更多反映环境抖动。
- `case03` 在数据量提升到 73.15 MB 后，Hash 核验的额外开销开始明显体现，平均多出 10.24% 的总耗时。
- 随着文件数量和文件内容变大，完整内容核验通常会继续增加额外时间，因为它需要再次完整读取源端和目标端文件内容。

原始结果文件位置：

- `/root/project-tests/distcp-hash-benchmark-test/summary.csv`
- `/root/project-tests/distcp-hash-benchmark-test/result-report-cn.md`
- `/root/project-tests/distcp-hash-benchmark-test/cases/case01/reports/benchmark-results.csv`
- `/root/project-tests/distcp-hash-benchmark-test/cases/case02/reports/benchmark-results.csv`
- `/root/project-tests/distcp-hash-benchmark-test/cases/case03/reports/benchmark-results.csv`

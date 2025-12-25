# Apache Celeborn Integration for Comet

本文档介绍了Apache Celeborn与Apache DataFusion Comet的集成实现。

## 概述

Apache Celeborn是一个分布式shuffle服务，可以替代Spark的本地磁盘shuffle。通过将Celeborn的Rust客户端集成到Comet中，可以实现：

- **零拷贝**：直接在Rust中处理Arrow格式数据
- **原生性能**：避免JNI调用开销
- **内存安全**：利用Rust的内存安全保证

## 架构设计

```
+---------------------------------------------------------------------+
|                        Spark Driver (JVM)                            |
|  +---------------------------------------------------------------+  |
|  |                   Java LifecycleManager                        |  |
|  |  +-------------+  +-------------+  +---------------------+    |  |
|  |  | RegisterShuffle |  | MapperEnd   |  | GetReducerFileGroup |    |  |
|  |  +-------------+  +-------------+  +---------------------+    |  |
|  +---------------------------------------------------------------+  |
|                              ^                                       |
|                              | RPC Endpoint (host:port)              |
+------------------------------+---------------------------------------+
                               |
                               | Netty RPC Protocol
                               |
+------------------------------+---------------------------------------+
|                              v                                       |
|  +---------------------------------------------------------------+  |
|  |              Rust ExecutorShuffleClient                        |  |
|  |  +---------------------------------------------------------+  |  |
|  |  | NettyLifecycleManagerClient                              |  |  |
|  |  |  - 连接到 Driver 的 LifecycleManager                     |  |  |
|  |  |  - 发送 RPC 请求 (RegisterShuffle, MapperEnd, etc.)      |  |  |
|  |  +---------------------------------------------------------+  |  |
|  |  +---------------------------------------------------------+  |  |
|  |  | DataPusher                                               |  |  |
|  |  |  - 直接推送数据到 Celeborn Worker                        |  |  |
|  |  |  - 支持 LZ4/ZSTD 压缩                                    |  |  |
|  |  +---------------------------------------------------------+  |  |
|  +---------------------------------------------------------------+  |
|                        Spark Executor (Comet/Rust)                   |
+---------------------------------------------------------------------+
                               |
                               | Push/Fetch Data
                               v
+---------------------------------------------------------------------+
|                       Celeborn Workers                               |
|  +-------------+  +-------------+  +-------------+                  |
|  |  Worker 1   |  |  Worker 2   |  |  Worker 3   |  ...             |
|  +-------------+  +-------------+  +-------------+                  |
+---------------------------------------------------------------------+
```

## 实现文件

### Rust端（native模块）

| 文件 | 描述 |
|------|------|
| `native/core/src/execution/shuffle/celeborn_writer.rs` | Celeborn shuffle writer执行计划 |
| `native/core/src/execution/shuffle/celeborn_reader.rs` | Celeborn shuffle reader执行计划 |
| `native/core/src/execution/shuffle/celeborn_jni.rs` | JNI接口函数 |
| `native/proto/src/proto/operator.proto` | CelebornShuffleWriter protobuf定义 |

### Scala端（spark模块）

| 文件 | 描述 |
|------|------|
| `spark/src/main/scala/org/apache/comet/Native.scala` | JNI方法声明 |
| `spark/src/main/scala/org/apache/comet/shuffle/CelebornShuffleManager.scala` | Celeborn shuffle管理器 |
| `common/src/main/scala/org/apache/comet/CometConf.scala` | Celeborn配置选项 |

## 核心组件

### CelebornShuffleWriterExec

Celeborn shuffle writer执行计划，负责：
- 接收输入数据并按分区进行Hash分区
- 使用murmur3哈希算法计算分区ID
- 将数据序列化为Arrow IPC格式
- 通过Celeborn Rust客户端推送数据到Workers

```rust
pub struct CelebornShuffleWriterExec {
    input: Arc<dyn ExecutionPlan>,
    partitioning: CometPartitioning,
    celeborn_config: CelebornShuffleConfig,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
    codec: CompressionCodec,
    tracing_enabled: bool,
}
```

### CelebornShuffleReaderExec

Celeborn shuffle reader执行计划，负责：
- 从Celeborn Workers读取shuffle数据
- 解码Arrow IPC格式数据
- 返回RecordBatch流

```rust
pub struct CelebornShuffleReaderExec {
    schema: SchemaRef,
    config: CelebornShuffleReaderConfig,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}
```

### CelebornShuffleManager (Scala)

Scala端的Celeborn shuffle管理器，提供：
- 创建和管理Celeborn客户端
- 推送数据到Celeborn
- 通知mapper结束
- 清理shuffle资源

```scala
class CelebornShuffleManager(conf: SparkConf) extends Logging {
  def createShuffleWriter(...): Long
  def pushData(handle: Long, partitionId: Int, data: Array[Byte]): Boolean
  def mapperEnd(handle: Long): Boolean
  def releaseClient(handle: Long): Unit
  def cleanupShuffle(handle: Long): Boolean
}
```

## JNI接口

| 方法 | 描述 |
|------|------|
| `createCelebornClient` | 创建Celeborn客户端并连接到LifecycleManager |
| `celebornPushData` | 推送数据到指定分区 |
| `celebornMapperEnd` | 通知mapper完成 |
| `releaseCelebornClient` | 释放客户端资源 |
| `celebornCleanupShuffle` | 清理shuffle数据 |
| `celebornGetPartitionLocation` | 获取分区位置信息 |
| `celebornClearClients` | 清除所有缓存的客户端 |

## 配置选项

| 配置项 | 默认值 | 描述 |
|--------|--------|------|
| `spark.comet.shuffle.celeborn.enabled` | `false` | 是否启用Celeborn shuffle |
| `spark.comet.shuffle.celeborn.master.endpoints` | `""` | Celeborn Master端点列表（逗号分隔） |
| `spark.comet.shuffle.celeborn.lifecycleManager.host` | `""` | LifecycleManager主机地址 |
| `spark.comet.shuffle.celeborn.lifecycleManager.port` | `9098` | LifecycleManager端口 |
| `spark.comet.shuffle.celeborn.push.bufferSize` | `4MB` | 推送缓冲区大小 |
| `spark.comet.shuffle.celeborn.push.maxReqsInFlight` | `32` | 最大并发推送请求数 |
| `spark.comet.shuffle.celeborn.fetch.maxReqsInFlight` | `3` | 最大并发获取请求数 |
| `spark.comet.shuffle.celeborn.compression.codec` | `lz4` | 压缩编解码器（lz4/zstd/none） |
| `spark.comet.shuffle.celeborn.rpc.timeout` | `30000` | RPC超时时间（毫秒） |

## 编译和使用

### 编译

1. 确保celeborn-client依赖已添加到`native/Cargo.toml`：

```toml
[workspace.dependencies]
celeborn-client = { path = "/path/to/incubator-celeborn/client-rust" }

[dependencies]
celeborn-client = { workspace = true, optional = true }

[features]
celeborn = ["celeborn-client"]
```

2. 编译时启用celeborn feature：

```bash
cd native && cargo build --features celeborn
```

3. 编译Scala模块：

```bash
./mvnw compile -DskipTests
```

### 使用

1. 配置Spark使用Celeborn shuffle：

```scala
val spark = SparkSession.builder()
  .config("spark.comet.shuffle.celeborn.enabled", "true")
  .config("spark.comet.shuffle.celeborn.master.endpoints", "celeborn-master:9097")
  .config("spark.comet.shuffle.celeborn.lifecycleManager.host", "spark-driver")
  .config("spark.comet.shuffle.celeborn.lifecycleManager.port", "9098")
  .getOrCreate()
```

2. 运行Spark作业，shuffle操作将自动使用Celeborn。

## Shuffle流程

### Write流程

1. **注册Shuffle**：在shuffle开始前，调用`register_shuffle`注册shuffle
2. **分区数据**：使用Hash分区将数据分配到不同分区
3. **推送数据**：调用`push_data`将数据推送到Celeborn Workers
4. **Mapper结束**：调用`mapper_end`通知mapper完成

### Read流程

1. **获取分区位置**：从LifecycleManager获取分区数据位置
2. **读取数据**：调用`read_partition`从Celeborn Workers读取数据
3. **解码数据**：将Arrow IPC格式数据解码为RecordBatch

## 性能优化建议

1. **批量操作**：尽量批量收集数据后再推送，减少RPC调用次数
2. **并发控制**：根据网络带宽调整`push.maxReqsInFlight`
3. **压缩选择**：
   - LZ4：压缩/解压速度快，适合CPU敏感场景
   - ZSTD：压缩率高，适合网络带宽受限场景
4. **内存管理**：使用Arrow的零拷贝特性，避免不必要的数据拷贝

## 错误处理

Celeborn客户端内置了重试机制：
- 连接错误：自动重试连接
- RPC错误：根据错误类型决定是否重试
- 分区未找到：触发Revive机制获取新的分区位置

## 后续工作

1. **QueryPlanSerde集成**：在Spark端添加CelebornShuffleWriter的序列化支持
2. **CelebornShuffleExchangeExec**：实现Spark端的shuffle exchange算子
3. **集成测试**：编写端到端的集成测试
4. **性能基准测试**：与本地磁盘shuffle进行性能对比

## 参考资料

- [Apache Celeborn官方文档](https://celeborn.apache.org/)
- [Apache Spark Comet](https://github.com/apache/datafusion-comet)
- [Celeborn Rust Client集成指南](/Users/fchen/Project/incubator-celeborn/client-rust/docs/COMET_INTEGRATION_GUIDE.md)

---

*文档更新时间: 2025-12-26*

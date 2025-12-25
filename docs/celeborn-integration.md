# Apache Celeborn Integration for Comet

本文档介绍了Apache Celeborn与Apache DataFusion Comet的集成实现。

## 概述

Apache Celeborn是一个分布式shuffle服务，可以替代Spark的本地磁盘shuffle。通过将Celeborn集成到Comet中，可以实现：

- **分布式Shuffle**：将shuffle数据存储在Celeborn Workers上，而不是本地磁盘
- **原生性能**：在Executor端使用Rust ExecutorShuffleClient直接与Celeborn Workers通信
- **内存安全**：利用Rust的内存安全保证

## 架构设计

```
+---------------------------------------------------------------------+
|                        Spark Driver (JVM)                            |
|  +---------------------------------------------------------------+  |
|  |              CometCelebornShuffleManager                       |  |
|  |  +----------------------------------------------------------+  |  |
|  |  |              LifecycleManager (Java)                      |  |  |
|  |  |  - 管理shuffle生命周期                                    |  |  |
|  |  |  - 提供RPC端点供Executor连接                              |  |  |
|  |  |  - 协调分区位置分配                                       |  |  |
|  |  +----------------------------------------------------------+  |  |
|  |                                                                |  |
|  |  CometCelebornShuffleHandle                                    |  |
|  |  - appUniqueId                                                 |  |
|  |  - lifecycleManagerHost                                        |  |
|  |  - lifecycleManagerPort                                        |  |
|  +---------------------------------------------------------------+  |
|                              ^                                       |
|                              | ShuffleHandle (包含LM地址)            |
+------------------------------+---------------------------------------+
                               |
                               | 传递给Executor
                               v
+---------------------------------------------------------------------+
|                     Spark Executor (Comet)                           |
|  +---------------------------------------------------------------+  |
|  |              CometCelebornShuffleWriter                        |  |
|  |  +----------------------------------------------------------+  |  |
|  |  |              Native (JNI)                                 |  |  |
|  |  |  - createCelebornClient()                                 |  |  |
|  |  |  - celebornPushData()                                     |  |  |
|  |  |  - celebornMapperEnd()                                    |  |  |
|  |  +----------------------------------------------------------+  |  |
|  |                              |                                 |  |
|  |                              v                                 |  |
|  |  +----------------------------------------------------------+  |  |
|  |  |              Rust ExecutorShuffleClient                   |  |  |
|  |  |  - setup_lifecycle_manager_ref(host, port)                |  |  |
|  |  |  - register_shuffle()                                     |  |  |
|  |  |  - push_data() -> 直接推送到Workers                       |  |  |
|  |  |  - mapper_end()                                           |  |  |
|  |  +----------------------------------------------------------+  |  |
|  +---------------------------------------------------------------+  |
|                              |                                       |
|                              | Netty RPC (连接到Driver LM)           |
|                              | Push/Fetch Data (直接到Workers)       |
+------------------------------+---------------------------------------+
                               |
                               v
+---------------------------------------------------------------------+
|                       Celeborn Workers                               |
|  +-------------+  +-------------+  +-------------+                  |
|  |  Worker 1   |  |  Worker 2   |  |  Worker 3   |  ...             |
|  +-------------+  +-------------+  +-------------+                  |
+---------------------------------------------------------------------+
```

## 关键设计决策

### Driver端：使用Java LifecycleManager

LifecycleManager是Celeborn的核心组件，负责：
- 管理shuffle的注册和注销
- 分配分区位置
- 协调mapper和reducer

由于LifecycleManager需要与Spark Driver紧密集成，我们使用Celeborn的Java客户端在Driver端创建LifecycleManager。

### Executor端：使用Rust ExecutorShuffleClient

在Executor端，我们使用Celeborn的Rust客户端（ExecutorShuffleClient）：
- 通过JNI从Scala调用
- 连接到Driver的LifecycleManager获取分区位置
- 直接与Celeborn Workers通信进行数据推送/获取

### ShuffleHandle传递LifecycleManager地址

`CometCelebornShuffleHandle`包含LifecycleManager的地址信息：
- `lifecycleManagerHost`：Driver主机地址
- `lifecycleManagerPort`：LifecycleManager端口

这些信息在Executor端用于连接到Driver的LifecycleManager。

## 实现文件

### Scala端（spark模块）

| 文件 | 描述 |
|------|------|
| `CometCelebornShuffleManager.scala` | 实现Spark ShuffleManager接口，在Driver端创建LifecycleManager |
| `CometCelebornShuffleHandle.scala` | 包含LifecycleManager地址的ShuffleHandle |
| `CometCelebornShuffleWriter.scala` | Shuffle Writer，通过JNI调用Rust客户端 |
| `CometCelebornShuffleReader.scala` | Shuffle Reader，从Celeborn读取数据 |
| `Native.scala` | JNI方法声明 |

### Rust端（native模块）

| 文件 | 描述 |
|------|------|
| `celeborn_writer.rs` | CelebornShuffleWriterExec执行计划 |
| `celeborn_reader.rs` | CelebornShuffleReaderExec执行计划 |
| `celeborn_jni.rs` | JNI接口函数 |

## 使用方法

### 1. 配置ShuffleManager

```scala
spark.conf.set("spark.shuffle.manager",
  "org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager")
```

### 2. 配置Celeborn

```scala
// Celeborn Master端点
spark.conf.set("spark.celeborn.master.endpoints", "celeborn-master:9097")

// 启用Comet Celeborn shuffle
spark.conf.set("spark.comet.shuffle.celeborn.enabled", "true")
```

### 3. 运行Spark作业

```scala
val df = spark.read.parquet("data.parquet")
val result = df.groupBy("key").count()
result.write.parquet("output")
```

## 配置选项

| 配置项 | 默认值 | 描述 |
|--------|--------|------|
| `spark.comet.shuffle.celeborn.enabled` | `false` | 是否启用Celeborn shuffle |
| `spark.celeborn.master.endpoints` | `""` | Celeborn Master端点列表（逗号分隔） |
| `spark.comet.shuffle.celeborn.push.bufferSize` | `4MB` | 推送缓冲区大小 |
| `spark.comet.shuffle.celeborn.push.maxReqsInFlight` | `32` | 最大并发推送请求数 |
| `spark.comet.shuffle.celeborn.fetch.maxReqsInFlight` | `3` | 最大并发获取请求数 |
| `spark.comet.shuffle.celeborn.compression.codec` | `lz4` | 压缩编解码器（lz4/zstd/none） |
| `spark.comet.shuffle.celeborn.rpc.timeout` | `30000` | RPC超时时间（毫秒） |

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

## Shuffle流程

### Write流程

1. **Driver注册Shuffle**：
   - `CometCelebornShuffleManager.registerShuffle()`被调用
   - 创建`LifecycleManager`（如果尚未创建）
   - 返回`CometCelebornShuffleHandle`，包含LifecycleManager地址

2. **Executor获取Writer**：
   - `CometCelebornShuffleManager.getWriter()`被调用
   - 从ShuffleHandle获取LifecycleManager地址
   - 创建`CometCelebornShuffleWriter`

3. **写入数据**：
   - 通过JNI调用`createCelebornClient()`创建Rust客户端
   - Rust客户端调用`setup_lifecycle_manager_ref()`连接到Driver
   - 调用`register_shuffle()`注册shuffle
   - 调用`push_data()`推送数据到Celeborn Workers
   - 调用`mapper_end()`通知mapper完成

### Read流程

1. **Executor获取Reader**：
   - `CometCelebornShuffleManager.getReader()`被调用
   - 创建`CometCelebornShuffleReader`

2. **读取数据**：
   - 连接到LifecycleManager获取分区位置
   - 从Celeborn Workers读取数据
   - 反序列化数据

## 编译

### 前提条件

1. Celeborn Rust客户端已编译
2. Celeborn Java客户端依赖已添加到pom.xml

### 编译步骤

```bash
# 编译Rust native模块（启用celeborn feature）
cd native && cargo build --features celeborn

# 编译Scala模块
./mvnw compile -DskipTests
```

## 依赖

在`pom.xml`中添加Celeborn依赖：

```xml
<dependency>
  <groupId>org.apache.celeborn</groupId>
  <artifactId>celeborn-client-spark-3-shaded_${scala.binary.version}</artifactId>
  <version>${celeborn.version}</version>
  <scope>provided</scope>
</dependency>
```

## 后续工作

1. **完整的Native执行路径**：在CelebornShuffleWriterExec中直接处理Arrow数据
2. **性能优化**：批量推送、异步IO
3. **集成测试**：端到端测试
4. **Fallback机制**：当Celeborn不可用时回退到本地shuffle

## 参考资料

- [Apache Celeborn官方文档](https://celeborn.apache.org/)
- [Apache Spark Comet](https://github.com/apache/datafusion-comet)
- [Celeborn Rust Client](https://github.com/apache/incubator-celeborn/tree/main/client-rust)

---

*文档更新时间: 2025-12-26*

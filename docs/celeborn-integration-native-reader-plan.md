# Comet Celeborn Integration: Native Reader Optimization Plan

## 目标

将目前的 Celeborn Shuffle Read 从 "Java Client + JNI Bridge" 模式升级为 "Rust Reader -> Native Operator" 模式。
这意味着 Comet 的 Native 执行计划将直接包含一个 `CelebornShuffleReader` 算子，该算子直接使用 Rust Celeborn Client 从 Worker 拉取数据，完全绕过 JVM。

## 架构变更

### 当前架构 (Reader)

```
Celeborn Worker -> Java ShuffleClient -> InputStream -> JNI -> Rust Native (via Scan)
```

### 目标架构 (Reader)

```
Celeborn Worker -> Rust ExecutorShuffleClient -> CelebornShuffleReaderExec (Rust) -> Comet Native Operators
```

## 实施步骤

### 1. Protobuf 定义

在 `native/proto/src/proto/operator.proto` 中新增 `CelebornShuffleReader` 消息定义，并在 `Operator` 消息中添加对应字段。

```protobuf
message CelebornShuffleReader {
  string app_id = 1;
  int32 shuffle_id = 2;
  int32 partition_id = 3;
  // Celeborn master endpoints
  repeated string master_endpoints = 4;
  // LifecycleManager host
  string lifecycle_manager_host = 5;
  // LifecycleManager port
  int32 lifecycle_manager_port = 6;
  // Attempt ID (optional, depends on implementation)
  int32 attempt_number = 7;
  // Range fetch support
  int32 start_map_index = 8;
  int32 end_map_index = 9;
}

message Operator {
  ...
  oneof op_struct {
    ...
    CelebornShuffleReader celeborn_shuffle_reader = 116;
  }
}
```

### 2. Scala 端改造

#### 2.1 新增 `CometCelebornExchangeSink`

创建 `spark/src/main/scala/org/apache/comet/serde/operator/CometCelebornExchangeSink.scala`。
该类负责将 Spark 的 `ShuffleQueryStageExec` (当其 Child 为 `CometShuffleExchangeExec` 且启用了 Celeborn 时) 转换为 Native 的 `CelebornShuffleReader` Operator。

需要从 `CometShuffleExchangeExec` 和 `SparkConf` 中提取必要信息：
- `appId`: 从 SparkContext 获取
- `shuffleId`: 从 ShuffleDependency 获取
- `masterEndpoints`: 从 SparkConf 获取
- `lifecycleManagerHost/Port`: 需要思考如何传递。Writer 是通过 Handle 传递的。Reader 此时可能只能从 Handle 获取？
  - *挑战*: `ShuffleExchangeExec` 不直接持有 Handle。
  - *解决方案*: `CometShuffleExchangeExec` 在 prepareDependency 时创建了 `CometShuffleDependency`。我们需要确保能获取到 LifecycleManager 的信息，或者像 Writer 一样，通过某种方式(如静态 map 或者从 Driver 广播)获取。
  - *简化方案*: 目前 `CometCelebornShuffleManager` 单例持有 `LifecycleManager` (Driver 端)。Executor 端 `CometCelebornShuffleReader` 持有 Handle。
  - 在生成 Plan 时（Driver 端），我们可以访问 `CometCelebornShuffleManager` 的 `lifecycleManager` 实例获取 Host/Port。

#### 2.2 修改 `CometExecRule`

在 `CometExecRule.scala` 中，识别 Celeborn Shuffle：

```scala
case s @ ShuffleQueryStageExec(_, e: CometShuffleExchangeExec, _) if isCeleborn(e) =>
  convertToComet(s, CometCelebornExchangeSink).getOrElse(s)
```

### 3. Rust 端改造

#### 3.1 完善 `CelebornShuffleReaderExec`

文件：`native/core/src/execution/shuffle/celeborn_reader.rs`
- 确保 `CelebornShuffleReaderExec` 实现了 `ExecutionPlan`。
- 确保它可以被正确初始化（从 Protobuf 配置）。

#### 3.2 物理计划构建

在 `native/core/src/execution/planner.rs` (或类似文件) 中，添加对 `CelebornShuffleReader` Proto 的处理逻辑，将其转换为 `CelebornShuffleReaderExec`。

### 4. 验证与测试

- 运行现有的集成测试 `CometCelebornIntergrationSuite`。
- 验证是否不再调用 Java 的 `readPartition`。
- 验证性能提升。

## 风险与注意事项

1. **LifecycleManager 地址获取**: 在 Driver 端生成 Plan 时需要获取 LifecycleManager 地址。
2. **连接复用**: Rust 端需要确保 `ExecutorShuffleClient` 的复用（已通过 `CelebornClientManager` 实现）。
3. **错误处理**: Rust 端直接读取时的网络错误处理需要健壮。

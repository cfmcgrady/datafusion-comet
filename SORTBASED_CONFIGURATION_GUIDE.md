# SortBased Shuffle Writer 配置指南

## 概述

SortBased Shuffle Writer 是 Apache Comet 中用于 Celeborn 集成的新功能，提供更好的内存效率和网络性能。本指南说明如何通过配置启用和调优 Sort 模式。

## 快速开始

### 默认配置（推荐）

Sort 模式已经是默认启用的，无需任何额外配置：

```bash
# 启动 Spark 应用，Sort 模式自动启用
spark-submit \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager \
  --conf spark.celeborn.master.endpoints=host1:9097,host2:9097 \
  --conf spark.comet.shuffle.celeborn.enabled=true \
  your-app.jar
```

## 配置项详解

### 1. 启用 Celeborn Shuffle

```properties
# 启用 Comet 的 Celeborn Shuffle 集成
spark.comet.shuffle.celeborn.enabled=true
```

### 2. Shuffle Writer 模式选择

```properties
# 选择 Shuffle Writer 模式
# 可选值: "sort" (默认), "hash"
spark.comet.shuffle.celeborn.writer.mode=sort
```

**模式对比**:

| 配置 | 说明 | 适用场景 |
|------|------|--------|
| `sort` | 累积记录，按分区排序，批量推送 | 默认，推荐用于大多数场景 |
| `hash` | 每分区立即推送 | 内存充足，需要低延迟的场景 |

### 3. Sort 模式内存配置

```properties
# Sort 模式内存阈值（字节）
# 当累积数据超过此阈值时，触发排序和推送
# 默认: 64MB
spark.comet.shuffle.celeborn.sort.memoryThreshold=67108864

# 也可以使用更易读的格式（需要 Spark 3.0+）
spark.comet.shuffle.celeborn.sort.memoryThreshold=64m
```

**调优建议**:

- **高内存环境** (>256GB): 增加到 256MB 或更多
  ```properties
  spark.comet.shuffle.celeborn.sort.memoryThreshold=268435456  # 256MB
  ```
  优点: 减少推送次数，提高网络效率
  缺点: 峰值内存占用更高

- **内存受限环境** (<64GB): 减少到 32MB 或更少
  ```properties
  spark.comet.shuffle.celeborn.sort.memoryThreshold=33554432  # 32MB
  ```
  优点: 降低峰值内存占用
  缺点: 更频繁的推送，网络开销增加

### 4. Sort 模式推送缓冲区配置

```properties
# 每次推送到 Celeborn 的最大缓冲区大小（字节）
# 默认: 4MB
spark.comet.shuffle.celeborn.sort.pushBufferSize=4194304

# 也可以使用更易读的格式
spark.comet.shuffle.celeborn.sort.pushBufferSize=4m
```

**调优建议**:

- **高延迟网络** (>100ms RTT): 增加到 8MB 或更多
  ```properties
  spark.comet.shuffle.celeborn.sort.pushBufferSize=8388608  # 8MB
  ```
  优点: 减少网络往返次数
  缺点: 单次推送数据量更大

- **低延迟网络** (<10ms RTT): 保持默认 4MB
  ```properties
  spark.comet.shuffle.celeborn.sort.pushBufferSize=4194304  # 4MB
  ```
  优点: 平衡网络效率和延迟

### 5. Celeborn 压缩配置

```properties
# 压缩编码（与 Sort 模式配合使用）
# 可选值: "lz4" (默认), "zstd", "none"
spark.comet.shuffle.celeborn.compression.codec=zstd
```

**推荐**:
- `zstd`: 更好的压缩率，适合带宽受限的环境
- `lz4`: 更快的压缩/解压，适合 CPU 受限的环境
- `none`: 不压缩，适合高速网络

## 完整配置示例

### 示例 1: 标准配置（推荐）

```bash
spark-submit \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager \
  --conf spark.celeborn.master.endpoints=celeborn-master1:9097,celeborn-master2:9097 \
  --conf spark.comet.shuffle.celeborn.enabled=true \
  --conf spark.comet.shuffle.celeborn.writer.mode=sort \
  --conf spark.comet.shuffle.celeborn.sort.memoryThreshold=64m \
  --conf spark.comet.shuffle.celeborn.sort.pushBufferSize=4m \
  --conf spark.comet.shuffle.celeborn.compression.codec=zstd \
  your-app.jar
```

### 示例 2: 高内存环境优化

```bash
spark-submit \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager \
  --conf spark.celeborn.master.endpoints=celeborn-master1:9097,celeborn-master2:9097 \
  --conf spark.comet.shuffle.celeborn.enabled=true \
  --conf spark.comet.shuffle.celeborn.writer.mode=sort \
  --conf spark.comet.shuffle.celeborn.sort.memoryThreshold=256m \
  --conf spark.comet.shuffle.celeborn.sort.pushBufferSize=8m \
  --conf spark.comet.shuffle.celeborn.compression.codec=lz4 \
  your-app.jar
```

### 示例 3: 内存受限环境优化

```bash
spark-submit \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager \
  --conf spark.celeborn.master.endpoints=celeborn-master1:9097,celeborn-master2:9097 \
  --conf spark.comet.shuffle.celeborn.enabled=true \
  --conf spark.comet.shuffle.celeborn.writer.mode=sort \
  --conf spark.comet.shuffle.celeborn.sort.memoryThreshold=32m \
  --conf spark.comet.shuffle.celeborn.sort.pushBufferSize=2m \
  --conf spark.comet.shuffle.celeborn.compression.codec=zstd \
  your-app.jar
```

### 示例 4: 使用 Hash 模式（原始行为）

```bash
spark-submit \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager \
  --conf spark.celeborn.master.endpoints=celeborn-master1:9097,celeborn-master2:9097 \
  --conf spark.comet.shuffle.celeborn.enabled=true \
  --conf spark.comet.shuffle.celeborn.writer.mode=hash \
  your-app.jar
```

## 配置项参考表

| 配置项 | 默认值 | 范围 | 说明 |
|--------|--------|------|------|
| `spark.comet.shuffle.celeborn.enabled` | `false` | `true`/`false` | 启用 Celeborn Shuffle |
| `spark.comet.shuffle.celeborn.writer.mode` | `sort` | `sort`/`hash` | Shuffle Writer 模式 |
| `spark.comet.shuffle.celeborn.sort.memoryThreshold` | `64m` | `1m` - `1g` | Sort 模式内存阈值 |
| `spark.comet.shuffle.celeborn.sort.pushBufferSize` | `4m` | `1m` - `64m` | Sort 模式推送缓冲区大小 |
| `spark.comet.shuffle.celeborn.compression.codec` | `lz4` | `lz4`/`zstd`/`none` | 压缩编码 |
| `spark.comet.shuffle.celeborn.master.endpoints` | 无 | 字符串 | Celeborn Master 端点 |
| `spark.comet.shuffle.celeborn.lifecycleManager.host` | 无 | 字符串 | LifecycleManager 主机 |
| `spark.comet.shuffle.celeborn.lifecycleManager.port` | `9098` | 整数 | LifecycleManager 端口 |

## 性能调优指南

### 1. 诊断当前配置

查看 Spark 日志中的 Celeborn 初始化信息：

```
[CELEBORN-JNI] Creating native Celeborn client for shuffle 0, 
  map 0, attemptId=0, compression=zstd, writerMode=sort, 
  sortMemoryThreshold=67108864MB
```

### 2. 监控内存使用

- 观察 Executor 的内存占用
- 如果频繁 GC，减少 `sort.memoryThreshold`
- 如果内存充足但推送频繁，增加 `sort.memoryThreshold`

### 3. 监控网络性能

- 观察推送数据的频率和大小
- 如果网络延迟高，增加 `sort.pushBufferSize`
- 如果网络带宽充足，可以增加 `sort.memoryThreshold`

### 4. 选择合适的压缩算法

```bash
# 测试不同压缩算法的性能
# 使用 zstd 获得最佳压缩率
spark.comet.shuffle.celeborn.compression.codec=zstd

# 使用 lz4 获得最快的压缩速度
spark.comet.shuffle.celeborn.compression.codec=lz4
```

## 常见问题

### Q: 如何在 Spark SQL 中启用 Sort 模式？

A: 在 Spark SQL 中，配置方式相同：

```sql
SET spark.comet.shuffle.celeborn.writer.mode=sort;
SET spark.comet.shuffle.celeborn.sort.memoryThreshold=64m;
```

### Q: Sort 模式和 Hash 模式有什么区别？

A: 
- **Sort 模式**: 累积数据到内存，按分区排序，批量推送。内存占用固定，网络效率高。
- **Hash 模式**: 每分区立即推送。内存占用与分区数成正比，网络调用频繁。

### Q: 如何从 Hash 模式切换到 Sort 模式？

A: 只需修改配置：

```properties
spark.comet.shuffle.celeborn.writer.mode=sort
```

无需重新编译或重启应用。

### Q: 内存阈值设置多少合适？

A: 根据以下因素选择：
- **分区数**: 分区数越多，可以设置更高的阈值
- **可用内存**: 通常设置为可用内存的 10-20%
- **网络延迟**: 延迟高时，可以设置更高的阈值

建议从 64MB 开始，根据实际情况调整。

## 相关文档

- [SortBased Shuffle Writer 实现文档](SORTBASED_SHUFFLE_IMPLEMENTATION.md)
- [Apache Celeborn 文档](https://celeborn.apache.org/)
- [Apache Comet 文档](https://datafusion.apache.org/comet/)

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.comet.execution.shuffle

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.comet.{CometExec, CometMetricNode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometConf
import org.apache.comet.serde.{OperatorOuterClass, PartitioningOuterClass, QueryPlanSerde}
import org.apache.comet.serde.OperatorOuterClass.{CompressionCodec, Operator}
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

/**
 * A [[ShuffleWriter]] that handles ColumnarBatch from Comet native execution
 * and writes to Celeborn using native Rust client.
 *
 * This writer:
 *   - Receives ColumnarBatch from Comet native execution
 *   - Uses native shuffle writer to partition and serialize data
 *   - Pushes serialized Arrow IPC data to Celeborn Workers via Rust client
 *
 * Data Flow:
 * {{{
 *   Comet Native Execution (ColumnarBatch)
 *       |
 *       v
 *   Native Shuffle Writer (partition + Arrow IPC serialize)
 *       |
 *       v
 *   Native Celeborn Client (push to workers)
 *       |
 *       v
 *   Celeborn Workers
 * }}}
 */
class CometNativeCelebornShuffleWriter[K, V](
    handle: CometCelebornNativeShuffleHandle[K, V, _],
    mapId: Long,
    context: TaskContext,
    celebornConf: CelebornConf,
    metricsReporter: ShuffleWriteMetricsReporter)
    extends ShuffleWriter[K, V]
    with Logging {

  private val shuffleId = handle.shuffleId
  private val numPartitions = handle.numPartitions
  private val numMappers = handle.numMappers
  private val outputPartitioning = handle.outputPartitioning
  private val outputAttributes = handle.outputAttributes

  // Use context.partitionId() as the actual mapId for Celeborn
  private val celebornMapId = context.partitionId()

  // Track if we've been stopped
  private var stopping = false
  private var mapStatus: MapStatus = null

  // Partition lengths for MapStatus
  private val partitionLengths = new Array[Long](numPartitions)

  override def write(inputs: Iterator[Product2[K, V]]): Unit = {
    val writeStartTime = System.nanoTime()

    logInfo(
      s"Starting native Celeborn shuffle write for shuffle $shuffleId, map $mapId " +
        s"(celebornMapId=$celebornMapId), numPartitions=$numPartitions, numMappers=$numMappers")

    try {
      // Build native shuffle plan for Celeborn
      val nativePlan = getNativePlan()

      val detailedMetrics = Seq(
        "elapsed_compute",
        "encode_time",
        "repart_time",
        "input_batches",
        "spill_count",
        "spilled_bytes")

      // Maps native metrics to SQL metrics
      val metricsOutputRows = new SQLMetric("outputRows")
      val metricsWriteTime = new SQLMetric("writeTime")
      val nativeSQLMetrics = Map(
        "output_rows" -> metricsOutputRows,
        "data_size" -> handle.shuffleWriteMetrics.getOrElse("dataSize", new SQLMetric("dataSize")),
        "write_time" -> metricsWriteTime) ++
        handle.shuffleWriteMetrics.filterKeys(detailedMetrics.contains)
      val nativeMetrics = CometMetricNode(nativeSQLMetrics)

      // Getting rid of the fake partitionId
      val newInputs = inputs.asInstanceOf[Iterator[_ <: Product2[Any, Any]]].map(_._2)

      val cometIter = CometExec.getCometIterator(
        Seq(newInputs.asInstanceOf[Iterator[ColumnarBatch]]),
        outputAttributes.length,
        nativePlan,
        nativeMetrics,
        handle.numParts,
        context.partitionId(),
        broadcastedHadoopConfForEncryption = None,
        encryptedFilePaths = Seq.empty)

      while (cometIter.hasNext) {
        cometIter.next()
      }
      cometIter.close()

      // For Celeborn shuffle, partition lengths are tracked by Celeborn service
      // We use the data_size metric as an approximation for total bytes written
      val dataSize = nativeSQLMetrics.get("data_size").map(_.value).getOrElse(0L)
      
      // Distribute bytes evenly across partitions for MapStatus
      // (Celeborn handles actual partition tracking)
      val bytesPerPartition = if (numPartitions > 0) dataSize / numPartitions else 0L
      for (i <- partitionLengths.indices) {
        partitionLengths(i) = bytesPerPartition
      }

      // Update metrics
      val writeTime = System.nanoTime() - writeStartTime
      metricsReporter.incWriteTime(writeTime)
      metricsReporter.incRecordsWritten(metricsOutputRows.value)
      metricsReporter.incBytesWritten(dataSize)

      logInfo(
        s"Shuffle $shuffleId map $mapId completed, " +
          s"total bytes: $dataSize, " +
          s"records: ${metricsOutputRows.value}")

    } catch {
      case e: Exception =>
        logError(s"Error writing shuffle data for shuffle $shuffleId, map $mapId", e)
        throw e
    }
  }

  private def getNativePlan(): Operator = {
    val scanBuilder = OperatorOuterClass.Scan.newBuilder()
    scanBuilder.setSource("CelebornShuffleInput")

    outputAttributes.foreach { attr =>
      val dataType = serializeDataType(attr.dataType)
      if (dataType.isEmpty) {
        throw new UnsupportedOperationException(
          s"Unsupported data type for Celeborn shuffle: ${attr.dataType}")
      }
      scanBuilder.addFields(dataType.get)
    }

    val scanOpBuilder = OperatorOuterClass.Operator.newBuilder()
    scanOpBuilder.setScan(scanBuilder.build())

    val shuffleWriterBuilder = OperatorOuterClass.CelebornShuffleWriter.newBuilder()

    // Set partitioning
    shuffleWriterBuilder.setPartitioning(serializePartitioning(outputPartitioning))

    // Set compression codec
    val codec = if (SparkEnv.get.conf.getBoolean("spark.shuffle.compress", true)) {
      CometConf.COMET_EXEC_SHUFFLE_COMPRESSION_CODEC.get() match {
        case "zstd" => CompressionCodec.Zstd
        case "lz4" => CompressionCodec.Lz4
        case "snappy" => CompressionCodec.Snappy
        case other => throw new UnsupportedOperationException(s"invalid codec: $other")
      }
    } else {
      CompressionCodec.None
    }
    shuffleWriterBuilder.setCodec(codec)
    shuffleWriterBuilder.setCompressionLevel(
      CometConf.COMET_EXEC_SHUFFLE_COMPRESSION_ZSTD_LEVEL.get)

    // Set Celeborn-specific fields
    val masterEndpoints = celebornConf.masterEndpoints.toSeq
    shuffleWriterBuilder.addAllMasterEndpoints(masterEndpoints.asJava)
    shuffleWriterBuilder.setAppId(handle.appUniqueId)
    shuffleWriterBuilder.setShuffleId(shuffleId)
    shuffleWriterBuilder.setMapId(celebornMapId)
    shuffleWriterBuilder.setAttemptId(context.attemptNumber())
    shuffleWriterBuilder.setNumMappers(numMappers)
    shuffleWriterBuilder.setNumPartitions(numPartitions)
    shuffleWriterBuilder.setLifecycleManagerHost(handle.lifecycleManagerHost)
    shuffleWriterBuilder.setLifecycleManagerPort(handle.lifecycleManagerPort)

    val shuffleWriterOpBuilder = OperatorOuterClass.Operator.newBuilder()
    shuffleWriterOpBuilder.setCelebornShuffleWriter(shuffleWriterBuilder.build())
    shuffleWriterOpBuilder.addChildren(scanOpBuilder.build())

    shuffleWriterOpBuilder.build()
  }

  private def serializePartitioning(partitioning: Partitioning): PartitioningOuterClass.Partitioning = {
    val partitioningBuilder = PartitioningOuterClass.Partitioning.newBuilder()

    partitioning match {
      case SinglePartition =>
        val singleBuilder = PartitioningOuterClass.SinglePartition.newBuilder()
        partitioningBuilder.setSinglePartition(singleBuilder.build())

      case hashPartitioning: HashPartitioning =>
        val hashBuilder = PartitioningOuterClass.HashPartition.newBuilder()
        hashBuilder.setNumPartitions(hashPartitioning.numPartitions)
        hashPartitioning.expressions.foreach { expr =>
          val exprProto = QueryPlanSerde.exprToProto(expr, outputAttributes)
          if (exprProto.isEmpty) {
            throw new UnsupportedOperationException(
              s"Unsupported hash partitioning expression: $expr")
          }
          hashBuilder.addHashExpression(exprProto.get)
        }
        partitioningBuilder.setHashPartition(hashBuilder.build())

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported partitioning type for Celeborn shuffle: ${partitioning.getClass.getName}")
    }

    partitioningBuilder.build()
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true

    try {
      if (success) {
        // Create MapStatus with partition lengths
        mapStatus = MapStatus(SparkEnv.get.blockManager.shuffleServerId, partitionLengths, mapId)

        logInfo(
          s"Shuffle $shuffleId map $mapId completed successfully, " +
            s"total bytes: ${partitionLengths.sum}")
        Some(mapStatus)
      } else {
        logInfo(s"Shuffle $shuffleId map $mapId aborted")
        None
      }
    } catch {
      case e: Exception =>
        logError(s"Error stopping shuffle writer for shuffle $shuffleId, map $mapId", e)
        throw e
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths
}

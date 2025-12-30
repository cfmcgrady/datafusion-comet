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

import java.io.ByteArrayOutputStream

import scala.reflect.ClassTag

import org.apache.celeborn.common.CelebornConf
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}

import org.apache.comet.Native

/**
 * Comet Celeborn Shuffle Writer that uses Rust ExecutorShuffleClient via JNI.
 *
 * This writer:
 *   - Uses Spark's serializer for data serialization (compatible with Reader)
 *   - Uses Rust ExecutorShuffleClient via JNI for pushing data to Celeborn Workers
 *   - Ensures data format compatibility between Writer and Reader
 *
 * Data Flow:
 * {{{
 *   Spark Records
 *       |
 *       v
 *   Spark Serializer (key-value pairs)
 *       |
 *       v
 *   Native.celebornPushData() (Rust ExecutorShuffleClient)
 *       |
 *       v
 *   Celeborn Workers
 * }}}
 */
class CometCelebornShuffleWriter[K, V](
    handle: CometCelebornShuffleHandle[K, V, _],
    mapId: Int,
    context: TaskContext,
    celebornConf: CelebornConf,
    metrics: ShuffleWriteMetricsReporter)
    extends ShuffleWriter[K, V]
    with Logging {

  private val shuffleId = handle.shuffleId
  private val numPartitions = handle.numPartitions
  private val numMappers = handle.numMappers
  private val dep = handle.dependency

  // Use context.partitionId() as the actual mapId for Celeborn
  // This is the partition index within the stage (0-based), not the global task ID
  // This matches what Celeborn's Java ShuffleWriter does
  private val celebornMapId = context.partitionId()

  // Serializer from the shuffle dependency - must match what Reader uses
  private val serializer: SerializerInstance = dep.serializer.newInstance()

  // Native instance for JNI calls
  private val native = new Native()

  // Native client handle (created lazily)
  private var nativeClientHandle: Long = 0L

  // Track if we've been stopped
  private var stopping = false
  private var mapStatus: MapStatus = null

  // Partition lengths for MapStatus
  private val partitionLengths = new Array[Long](numPartitions)

  // Per-partition buffers for batching data before pushing
  private val partitionBuffers = new Array[ByteArrayOutputStream](numPartitions)
  private val partitionSerializers = new Array[Any](numPartitions) // SerializationStream

  // Buffer size threshold for pushing to Celeborn (64KB)
  private val pushBufferSize = celebornConf.clientPushBufferMaxSize.toInt

  /**
   * Initialize the native Celeborn client.
   */
  private def initNativeClient(): Unit = {
    if (nativeClientHandle == 0L) {
      val masterEndpoints = celebornConf.masterEndpoints
      val masterEndpointsArray = masterEndpoints.toArray

      val attemptId = context.attemptNumber()
      logInfo(
        s"Creating native Celeborn client for shuffle $shuffleId, " +
          s"map $celebornMapId, attemptId=$attemptId, LM=${handle.lifecycleManagerHost}:${handle.lifecycleManagerPort}")

      nativeClientHandle = native.createCelebornClient(
        handle.appUniqueId,
        masterEndpointsArray,
        handle.lifecycleManagerHost,
        handle.lifecycleManagerPort,
        shuffleId,
        celebornMapId,
        attemptId,
        numMappers,
        numPartitions)

      logInfo(s"Native Celeborn client created with handle $nativeClientHandle")
    }
  }

  /**
   * Get or create a buffer for a partition.
   */
  private def getPartitionBuffer(partitionId: Int): (ByteArrayOutputStream, Any) = {
    if (partitionBuffers(partitionId) == null) {
      partitionBuffers(partitionId) = new ByteArrayOutputStream(pushBufferSize)
      partitionSerializers(partitionId) =
        serializer.serializeStream(partitionBuffers(partitionId))
    }
    val serStream = partitionSerializers(partitionId)
      .asInstanceOf[org.apache.spark.serializer.SerializationStream]
    (partitionBuffers(partitionId), serStream)
  }

  /**
   * Flush a partition buffer to Celeborn via native client.
   */
  private def flushPartition(partitionId: Int): Unit = {
    val buffer = partitionBuffers(partitionId)
    val serStream = partitionSerializers(partitionId)

    // First flush the serialization stream to ensure all data is written to buffer
    if (serStream != null) {
      serStream.asInstanceOf[org.apache.spark.serializer.SerializationStream].flush()
    }

    // Now check if buffer has data to push
    if (buffer != null && buffer.size() > 0) {
      val data = buffer.toByteArray
      val dataLength = data.length

      logInfo(s"Flushing partition $partitionId with $dataLength bytes to Celeborn")

      // Push data to Celeborn using native Rust client
      val pushStartTime = System.nanoTime()
      native.celebornPushData(nativeClientHandle, partitionId, data)
      val pushTime = System.nanoTime() - pushStartTime

      // Update metrics
      metrics.incBytesWritten(dataLength)
      metrics.incWriteTime(pushTime)
      partitionLengths(partitionId) += dataLength

      // Reset buffer for reuse
      buffer.reset()
    }
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val writeStartTime = System.nanoTime()
    var recordsWritten = 0L

    logInfo(
      s"Starting shuffle write for shuffle $shuffleId, map $mapId " +
        s"(celebornMapId=$celebornMapId), numPartitions=$numPartitions, numMappers=$numMappers")

    try {
      // Initialize native client
      initNativeClient()

      while (records.hasNext) {
        val record = records.next()
        val key = record._1
        val value = record._2

        // Determine partition for this record
        val partition = dep.partitioner.getPartition(key)

        // Get buffer and serializer for this partition
        val (buffer, serStream) = getPartitionBuffer(partition)
        val typedSerStream =
          serStream.asInstanceOf[org.apache.spark.serializer.SerializationStream]

        // Serialize key-value pair
        typedSerStream.writeKey(key)(ClassTag.Any.asInstanceOf[ClassTag[K]])
        typedSerStream.writeValue(value)(ClassTag.Any.asInstanceOf[ClassTag[V]])

        recordsWritten += 1

        logInfo(
          s"After writing record $recordsWritten to partition $partition, " +
            s"buffer size: ${buffer.size()}")

        // Flush if buffer is large enough
        if (buffer.size() >= pushBufferSize) {
          flushPartition(partition)
        }
      }

      // Flush serialization streams first to ensure all data is written to buffers
      for (partitionId <- 0 until numPartitions) {
        if (partitionSerializers(partitionId) != null) {
          val serStream = partitionSerializers(partitionId)
            .asInstanceOf[org.apache.spark.serializer.SerializationStream]
          serStream.flush()
          logInfo(
            s"After flushing serStream for partition $partitionId, " +
              s"buffer size: ${partitionBuffers(partitionId).size()}")
        }
      }

      // Flush all remaining data
      for (partitionId <- 0 until numPartitions) {
        flushPartition(partitionId)
      }

      // Update metrics
      val writeTime = System.nanoTime() - writeStartTime
      metrics.incRecordsWritten(recordsWritten)

      logInfo(
        s"Shuffle $shuffleId map $mapId wrote $recordsWritten records " +
          s"in ${writeTime / 1e6} ms")

    } catch {
      case e: Exception =>
        logError(s"Error writing shuffle data for shuffle $shuffleId, map $mapId", e)
        throw e
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true

    try {
      // Close all serialization streams
      for (partitionId <- 0 until numPartitions) {
        if (partitionSerializers(partitionId) != null) {
          partitionSerializers(partitionId)
            .asInstanceOf[org.apache.spark.serializer.SerializationStream]
            .close()
        }
      }

      if (success && nativeClientHandle != 0L) {
        // Signal mapper end to Celeborn via native client
        native.celebornMapperEnd(nativeClientHandle)

        // Create MapStatus with partition lengths
        mapStatus = MapStatus(SparkEnv.get.blockManager.shuffleServerId, partitionLengths, mapId)

        logInfo(
          s"Shuffle $shuffleId map $mapId completed successfully, " +
            s"total bytes: ${partitionLengths.sum}")
        Some(mapStatus)
      } else {
        // Abort - no need to do anything special, Celeborn will handle cleanup
        logInfo(s"Shuffle $shuffleId map $mapId aborted")
        None
      }
    } catch {
      case e: Exception =>
        logError(s"Error stopping shuffle writer for shuffle $shuffleId, map $mapId", e)
        throw e
    } finally {
      // Release native client
      if (nativeClientHandle != 0L) {
        try {
          native.releaseCelebornClient(nativeClientHandle)
        } catch {
          case e: Exception =>
            logWarning(s"Error releasing native Celeborn client", e)
        }
        nativeClientHandle = 0L
      }
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths
}

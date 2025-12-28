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

import java.io.{ByteArrayOutputStream, IOException}

import scala.reflect.ClassTag

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}

import org.apache.comet.Native

/**
 * Comet Celeborn Shuffle Writer that uses native Rust implementation for data serialization and
 * pushes data to Celeborn workers.
 *
 * This writer:
 *   - Uses Comet's native Rust code for efficient data serialization (Arrow format)
 *   - Connects to Celeborn LifecycleManager via the ShuffleClient
 *   - Pushes serialized data directly to Celeborn Workers
 *
 * Data Flow:
 * {{{
 *   Spark Records
 *       |
 *       v
 *   Native Rust Serialization (Arrow format)
 *       |
 *       v
 *   ShuffleClient.pushData()
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
    shuffleClient: ShuffleClient,
    metrics: ShuffleWriteMetricsReporter)
    extends ShuffleWriter[K, V]
    with Logging {

  private val shuffleId = handle.shuffleId
  private val numPartitions = handle.numPartitions
  private val numMappers = handle.numMappers
  private val dep = handle.dependency

  // Serializer from the shuffle dependency - must match what Reader uses
  private val serializer: SerializerInstance = dep.serializer.newInstance()

  // Buffer for serialization
  private val serBuffer = new ByteArrayOutputStream()
  private val serOutputStream = serializer.serializeStream(serBuffer)

  // Native instance for JNI calls
  private val native = new Native()

  // Native client handle for Rust-side operations
  private var nativeClientHandle: Long = 0L

  // Track if we've been stopped
  private var stopping = false
  private var mapStatus: MapStatus = null

  // Partition lengths for MapStatus
  private val partitionLengths = new Array[Long](numPartitions)

  /**
   * Initialize the native Celeborn client.
   */
  private def initNativeClient(): Unit = {
    if (nativeClientHandle == 0L) {
      logInfo(s"Initializing native Celeborn client for shuffle $shuffleId, map $mapId")

      // Get master endpoints from config
      val masterEndpoints = celebornConf.masterEndpoints

      nativeClientHandle = native.createCelebornClient(
        handle.appUniqueId,
        masterEndpoints,
        handle.lifecycleManagerHost,
        handle.lifecycleManagerPort,
        shuffleId,
        mapId,
        context.attemptNumber(),
        numMappers,
        numPartitions)

      if (nativeClientHandle == 0L) {
        throw new IOException("Failed to create native Celeborn client")
      }
    }
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // Initialize native client
    initNativeClient()

    val writeStartTime = System.nanoTime()
    var recordsWritten = 0L
    var bytesWritten = 0L

    try {
      // Use Spark's serializer to serialize key-value pairs
      // This ensures compatibility with the Reader which uses the same serializer

      while (records.hasNext) {
        val record = records.next()
        val key = record._1
        val value = record._2

        // Determine partition for this record
        val partition = dep.partitioner.getPartition(key)

        // Serialize key-value pair using Spark's serializer
        // Use ClassTag[Any] as the type tag for serialization
        serBuffer.reset()
        serOutputStream.writeKey(key)(ClassTag.Any.asInstanceOf[ClassTag[K]])
        serOutputStream.writeValue(value)(ClassTag.Any.asInstanceOf[ClassTag[V]])
        serOutputStream.flush()

        val serializedBytes = serBuffer.toByteArray

        // Push serialized data to Celeborn via native code
        val success = native.celebornPushData(nativeClientHandle, partition, serializedBytes)

        if (success) {
          bytesWritten += serializedBytes.length
          partitionLengths(partition) += serializedBytes.length
        }

        recordsWritten += 1
      }

      // Update metrics
      val writeTime = System.nanoTime() - writeStartTime
      metrics.incRecordsWritten(recordsWritten)
      metrics.incBytesWritten(bytesWritten)
      metrics.incWriteTime(writeTime)

      logInfo(s"Shuffle $shuffleId map $mapId wrote $recordsWritten records, $bytesWritten bytes")

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
      // Close serialization stream
      serOutputStream.close()

      if (success) {
        // Commit the map output
        if (nativeClientHandle != 0L) {
          native.celebornMapperEnd(nativeClientHandle)
        }

        // Create MapStatus with partition lengths
        mapStatus = MapStatus(SparkEnv.get.blockManager.shuffleServerId, partitionLengths, mapId)

        logInfo(s"Shuffle $shuffleId map $mapId completed successfully")
        Some(mapStatus)
      } else {
        // Abort the map output
        if (nativeClientHandle != 0L) {
          native.releaseCelebornClient(nativeClientHandle)
          nativeClientHandle = 0L
        }
        None
      }
    } catch {
      case e: Exception =>
        logError(s"Error stopping shuffle writer for shuffle $shuffleId, map $mapId", e)
        None
    } finally {
      // Clean up native resources
      if (nativeClientHandle != 0L) {
        native.releaseCelebornClient(nativeClientHandle)
        nativeClientHandle = 0L
      }
    }
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths
}

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

import java.io.IOException

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
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
      // For Comet, we expect records to be in a format that can be serialized natively
      // The actual serialization happens in the native Rust code

      while (records.hasNext) {
        val record = records.next()
        val key = record._1
        val value = record._2

        // Determine partition for this record
        val partition = handle.dependency.partitioner.getPartition(key)

        // Serialize and push data via native code
        // The native code handles batching and pushing to Celeborn workers
        val valueBytes = serializeValue(value)
        val success = native.celebornPushData(nativeClientHandle, partition, valueBytes)

        if (success) {
          bytesWritten += valueBytes.length
          partitionLengths(partition) += valueBytes.length
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

  /**
   * Serialize a value to bytes. For Comet, this typically handles Arrow-formatted data.
   */
  private def serializeValue(value: V): Array[Byte] = {
    value match {
      case bytes: Array[Byte] => bytes
      case _ =>
        // Use Java serialization for non-byte array values
        val stream = new java.io.ByteArrayOutputStream()
        val oos = new java.io.ObjectOutputStream(stream)
        oos.writeObject(value)
        oos.close()
        stream.toByteArray
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true

    try {
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

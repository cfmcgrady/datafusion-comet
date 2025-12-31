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

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.client.read.{CelebornInputStream, MetricsCallback}
import org.apache.celeborn.common.CelebornConf
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.util.CompletionIterator

import org.apache.comet.Native

/**
 * Comet Celeborn Shuffle Reader that reads shuffle data from Celeborn workers.
 *
 * This reader:
 *   - Connects to Celeborn workers via ShuffleClient
 *   - Reads shuffle data for specified partitions
 *   - Deserializes data using Comet's native Rust code (Arrow format)
 *
 * Data Flow:
 * {{{
 *   Celeborn Workers
 *       |
 *       v
 *   ShuffleClient.readPartition()
 *       |
 *       v
 *   CelebornInputStream
 *       |
 *       v
 *   Native Rust Deserialization (Arrow format)
 *       |
 *       v
 *   Spark Records
 * }}}
 */
class CometCelebornShuffleReader[K, C](
    handle: CometCelebornShuffleHandle[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    celebornConf: CelebornConf,
    shuffleClient: ShuffleClient,
    metrics: ShuffleReadMetricsReporter)
    extends ShuffleReader[K, C]
    with Logging {

  private val shuffleId = handle.shuffleId

  // Native instance for JNI calls
  private val native = new Native()

  // Native reader handle for Rust-side operations
  private var nativeReaderHandle: Long = 0L

  override def read(): Iterator[Product2[K, C]] = {
    logInfo(
      s"Reading shuffle $shuffleId, partitions [$startPartition, $endPartition), " +
        s"maps [$startMapIndex, $endMapIndex)")

    val readStartTime = System.nanoTime()

    // Create iterators for each partition
    val partitionIterators = (startPartition until endPartition).map { partitionId =>
      createPartitionIterator(partitionId)
    }

    // Combine all partition iterators
    val combinedIterator = partitionIterators.flatten.iterator

    // Wrap with completion callback to update metrics
    val completionIterator = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      combinedIterator,
      () => {
        val readTime = System.nanoTime() - readStartTime
        logInfo(s"Shuffle $shuffleId read completed in ${readTime / 1e6} ms")

        // Clean up native resources
        if (nativeReaderHandle != 0L) {
          native.releaseCelebornClient(nativeReaderHandle)
          nativeReaderHandle = 0L
        }
      })

    completionIterator
  }

  /**
   * Create an iterator for a single partition.
   */
  private def createPartitionIterator(partitionId: Int): Iterator[Product2[K, C]] = {
    try {
      // Read partition data from Celeborn using the Java client
      // The ShuffleClient handles the communication with Celeborn workers
      // Method signature: readPartition(shuffleId, partitionId, attemptNumber,
      //   startMapIndex, endMapIndex, metricsCallback)
      logDebug(
        s"Creating partition iterator for shuffle $shuffleId partition $partitionId, " +
          s"maps [$startMapIndex, $endMapIndex)")

      val metricsCallback = new MetricsCallback {
        override def incBytesRead(bytesRead: Long): Unit = {
          metrics.incRemoteBytesRead(bytesRead)
        }
        override def incReadTime(time: Long): Unit = {
          metrics.incFetchWaitTime(time)
        }
      }
      val inputStream = shuffleClient.readPartition(
        shuffleId,
        partitionId,
        context.attemptNumber().toInt,
        startMapIndex,
        endMapIndex,
        metricsCallback)

      if (inputStream == null) {
        logWarning(s"No data for shuffle $shuffleId partition $partitionId")
        return Iterator.empty
      }

      // Check if stream has data
      val available = inputStream.available()
      logInfo(
        s"Got input stream for shuffle $shuffleId partition $partitionId, " +
          s"available bytes: $available")

      // Create iterator from input stream
      new CelebornPartitionIterator[K, C](
        inputStream = inputStream,
        shuffleId = shuffleId,
        partitionId = partitionId,
        handle = handle,
        metrics = metrics)

    } catch {
      case e: Exception =>
        logError(s"Error reading shuffle $shuffleId partition $partitionId", e)
        throw e
    }
  }
}

/**
 * Iterator that reads records from a Celeborn partition input stream.
 */
private class CelebornPartitionIterator[K, C](
    inputStream: CelebornInputStream,
    shuffleId: Int,
    partitionId: Int,
    handle: CometCelebornShuffleHandle[K, _, C],
    metrics: ShuffleReadMetricsReporter)
    extends Iterator[Product2[K, C]]
    with Logging {

  private val deserializer = handle.dependency.serializer.newInstance()
  private val deserializeStream = deserializer.deserializeStream(inputStream)
  private val keyValueIterator = deserializeStream.asKeyValueIterator

  private var recordsRead = 0L
  private var bytesRead = 0L

  override def hasNext: Boolean = {
    val hasMore = keyValueIterator.hasNext
    if (!hasMore) {
      // Update metrics when done
      metrics.incRecordsRead(recordsRead)
      metrics.incRemoteBytesRead(bytesRead)

      // Close the stream
      try {
        deserializeStream.close()
        inputStream.close()
      } catch {
        case e: Exception =>
          logWarning(s"Error closing stream for shuffle $shuffleId partition $partitionId", e)
      }
    }
    hasMore
  }

  override def next(): Product2[K, C] = {
    val kv = keyValueIterator.next()
    recordsRead += 1
    (kv._1.asInstanceOf[K], kv._2.asInstanceOf[C])
  }
}

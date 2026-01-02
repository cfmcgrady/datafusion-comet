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
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.CompletionIterator

/**
 * Native Celeborn Shuffle Reader that reads Arrow-formatted shuffle data from Celeborn workers.
 *
 * This reader:
 *   - Connects to Celeborn workers via ShuffleClient
 *   - Reads shuffle data for specified partitions (Comet native format)
 *   - Uses NativeBatchDecoderIterator to decode Arrow IPC data
 *   - Returns ColumnarBatch directly without deserialization overhead
 *
 * Data Flow:
 * {{{
 *   Celeborn Workers
 *       |
 *       v
 *   ShuffleClient.readPartition()
 *       |
 *       v
 *   CelebornInputStream (Comet native shuffle format)
 *       |
 *       v
 *   NativeBatchDecoderIterator (native decoding)
 *       |
 *       v
 *   ColumnarBatch
 * }}}
 */
class CometNativeCelebornShuffleReader[K, C](
    handle: CometCelebornNativeShuffleHandle[K, _, C],
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
  private val decodeTime = new SQLMetric("decodeTime")

  override def read(): Iterator[Product2[K, C]] = {
    logInfo(
      s"Reading native shuffle $shuffleId, partitions [$startPartition, $endPartition), " +
        s"maps [$startMapIndex, $endMapIndex)")

    val readStartTime = System.nanoTime()

    // Create iterators for each partition
    val partitionIterators = (startPartition until endPartition).iterator.flatMap { partitionId =>
      createPartitionIterator(partitionId)
    }

    // Wrap ColumnarBatch as Product2
    val combinedIterator = partitionIterators.map { batch =>
      // Each ColumnarBatch is returned as a single Product2 with null key
      (null.asInstanceOf[K], batch.asInstanceOf[C])
    }

    // Wrap with completion callback to update metrics
    val completionIterator = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      combinedIterator,
      () => {
        val readTime = System.nanoTime() - readStartTime
        logInfo(s"Native shuffle $shuffleId read completed in ${readTime / 1e6} ms, " +
          s"decode time: ${decodeTime.value / 1e6} ms")
      })

    completionIterator
  }

  /**
   * Create an iterator for a single partition that reads and decodes Comet native shuffle data.
   */
  private def createPartitionIterator(partitionId: Int): Iterator[ColumnarBatch] = {
    try {
      logDebug(
        s"Creating native partition iterator for shuffle $shuffleId partition $partitionId, " +
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

      logInfo(
        s"Got input stream for native shuffle $shuffleId partition $partitionId")

      // Use NativeBatchDecoderIterator to decode the Comet native shuffle format
      // Note: Don't check available() as it may return 0 for network streams even when data exists
      NativeBatchDecoderIterator(inputStream, context, decodeTime)

    } catch {
      case e: Exception =>
        logError(s"Error reading native shuffle $shuffleId partition $partitionId", e)
        throw e
    }
  }
}

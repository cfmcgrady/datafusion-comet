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

import scala.collection.mutable

import org.apache.celeborn.client.{LifecycleManager, ShuffleClient}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager

import org.apache.comet.CometConf

/**
 * Utility object for Celeborn configuration conversion.
 */
private[shuffle] object CelebornSparkUtils {

  /**
   * Convert SparkConf to CelebornConf by extracting spark.celeborn.* properties.
   */
  def fromSparkConf(conf: SparkConf): CelebornConf = {
    val celebornConf = new CelebornConf()
    conf.getAll.foreach { case (key, value) =>
      if (key.startsWith("spark.celeborn.")) {
        celebornConf.set(key.substring("spark.".length), value)
      }
    }
    celebornConf
  }
}

/**
 * Comet Celeborn Shuffle Manager that integrates Apache Celeborn with Comet native execution.
 *
 * This ShuffleManager implementation:
 *   - Creates LifecycleManager on the Driver side (Java)
 *   - Uses Comet's native Rust ExecutorShuffleClient on the Executor side
 *   - Provides seamless integration between Spark shuffle and Celeborn remote shuffle service
 *
 * Architecture:
 * {{{
 *   Spark Driver (JVM)
 *   +-- CometCelebornShuffleManager
 *   |   +-- LifecycleManager (Celeborn Java client)
 *   |   +-- Creates CometCelebornShuffleHandle with LM address
 *
 *   Spark Executor (Comet/Rust)
 *   +-- CometCelebornShuffleWriter
 *   |   +-- Uses ExecutorShuffleClient (Rust) via JNI
 *   |   +-- Connects to LifecycleManager via RPC
 *   |   +-- Pushes data directly to Celeborn Workers
 *
 *   Celeborn Workers
 *   +-- Store and serve shuffle data
 * }}}
 *
 * Configuration:
 * {{{
 *   spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.\
 *     CometCelebornShuffleManager
 *   spark.celeborn.master.endpoints=host1:9097,host2:9097
 * }}}
 */
class CometCelebornShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends ShuffleManager
    with Logging {

  // Celeborn configuration
  private val celebornConf: CelebornConf = CelebornSparkUtils.fromSparkConf(conf)

  // LifecycleManager - only created on Driver
  @volatile private var lifecycleManager: LifecycleManager = _

  def getLifecycleManagerHost: String = {
    if (lifecycleManager != null) lifecycleManager.getHost else null
  }

  def getLifecycleManagerPort: Int = {
    if (lifecycleManager != null) lifecycleManager.getPort else 0
  }

  // Application unique ID
  @volatile private var appUniqueId: String = _

  // Fallback to SortShuffleManager for unsupported cases
  @volatile private var sortShuffleManager: SortShuffleManager = _

  // Track shuffles that fall back to sort shuffle
  private val sortShuffleIds: mutable.Set[Int] = mutable.Set.empty

  // Shuffle client for executor side
  @volatile private var shuffleClient: ShuffleClient = _

  /**
   * Initialize the LifecycleManager on the Driver side. This is called lazily when the first
   * shuffle is registered.
   */
  private def initializeLifecycleManager(): Unit = {
    if (isDriver && lifecycleManager == null) {
      synchronized {
        if (lifecycleManager == null) {
          logInfo(s"Creating Celeborn LifecycleManager for app: $appUniqueId")
          lifecycleManager = new LifecycleManager(appUniqueId, celebornConf)
          val host = lifecycleManager.getHost
          val port = lifecycleManager.getPort
          logInfo(s"LifecycleManager created at $host:$port")
        }
      }
    }
  }

  /**
   * Get or create the fallback SortShuffleManager.
   */
  private def getSortShuffleManager: SortShuffleManager = {
    if (sortShuffleManager == null) {
      synchronized {
        if (sortShuffleManager == null) {
          sortShuffleManager = new SortShuffleManager(conf)
        }
      }
    }
    sortShuffleManager
  }

  /**
   * Check if Comet Celeborn shuffle should be used for this shuffle.
   */
  private def shouldUseCelebornShuffle(numPartitions: Int): Boolean = {
    // Use the actual default values from CometConf
    val cometEnabled = conf.getBoolean(CometConf.COMET_EXEC_ENABLED.key, true)
    val celebornEnabled = conf.getBoolean(CometConf.COMET_SHUFFLE_CELEBORN_ENABLED.key, false)

    // Check if Celeborn master endpoints are configured
    val masterEndpoints = celebornConf.masterEndpoints
    val hasMasterEndpoints = masterEndpoints != null && masterEndpoints.nonEmpty

    logInfo(
      s"Celeborn shuffle check: cometEnabled=$cometEnabled, " +
        s"celebornEnabled=$celebornEnabled, hasMasterEndpoints=$hasMasterEndpoints")

    if (celebornEnabled && !hasMasterEndpoints) {
      logWarning(
        "Celeborn shuffle is enabled but no master endpoints are configured. " +
          "Set spark.celeborn.master.endpoints")
    }

    cometEnabled && celebornEnabled && hasMasterEndpoints
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {

    // Generate app unique ID from SparkContext
    if (appUniqueId == null) {
      appUniqueId = s"${dependency.rdd.context.applicationId}_${System.currentTimeMillis()}"
    }

    // Initialize LifecycleManager on Driver
    initializeLifecycleManager()

    val numPartitions = dependency.partitioner.numPartitions

    if (shouldUseCelebornShuffle(numPartitions)) {
      logInfo(s"Registering Celeborn shuffle $shuffleId with $numPartitions partitions")

      // Create CometCelebornShuffleHandle with LifecycleManager address
      new CometCelebornShuffleHandle[K, V, C](
        appUniqueId = appUniqueId,
        lifecycleManagerHost = lifecycleManager.getHost,
        lifecycleManagerPort = lifecycleManager.getPort,
        userIdentifier = lifecycleManager.getUserIdentifier,
        shuffleId = shuffleId,
        numMappers = dependency.rdd.getNumPartitions,
        dependency = dependency)
    } else {
      logInfo(s"Falling back to SortShuffleManager for shuffle $shuffleId")
      sortShuffleIds.add(shuffleId)
      getSortShuffleManager.registerShuffle(shuffleId, dependency)
    }
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {

    handle match {
      case h: CometCelebornShuffleHandle[K @unchecked, V @unchecked, _] =>
        // Create Comet Celeborn shuffle writer using native Rust client
        new CometCelebornShuffleWriter[K, V](
          handle = h,
          mapId = mapId.toInt,
          context = context,
          celebornConf = celebornConf,
          metrics = metrics)

      case _ =>
        getSortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    handle match {
      case h: CometCelebornShuffleHandle[K @unchecked, _, C @unchecked] =>
        // Get or create ShuffleClient for this executor
        if (shuffleClient == null) {
          synchronized {
            if (shuffleClient == null) {
              shuffleClient = ShuffleClient.get(
                h.appUniqueId,
                h.lifecycleManagerHost,
                h.lifecycleManagerPort,
                celebornConf,
                h.userIdentifier,
                null // extension
              )
            }
          }
        }

        new CometCelebornShuffleReader[K, C](
          handle = h,
          startMapIndex = startMapIndex,
          endMapIndex = endMapIndex,
          startPartition = startPartition,
          endPartition = endPartition,
          context = context,
          celebornConf = celebornConf,
          shuffleClient = shuffleClient,
          metrics = metrics)

      case _ =>
        getSortShuffleManager.getReader(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics)
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (sortShuffleIds.contains(shuffleId)) {
      sortShuffleIds.remove(shuffleId)
      getSortShuffleManager.unregisterShuffle(shuffleId)
    } else {
      // Unregister from Celeborn
      if (lifecycleManager != null) {
        lifecycleManager.unregisterAppShuffle(shuffleId, false)
      }
      if (shuffleClient != null) {
        shuffleClient.cleanupShuffle(shuffleId)
      }
      true
    }
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    getSortShuffleManager.shuffleBlockResolver
  }

  override def stop(): Unit = {
    logInfo("Stopping CometCelebornShuffleManager")

    if (shuffleClient != null) {
      shuffleClient.shutdown()
      ShuffleClient.reset()
      shuffleClient = null
    }

    if (lifecycleManager != null) {
      lifecycleManager.stop()
      lifecycleManager = null
    }

    if (sortShuffleManager != null) {
      sortShuffleManager.stop()
      sortShuffleManager = null
    }
  }
}

/**
 * Shuffle handle for Comet Celeborn shuffle. Contains LifecycleManager address for Executor to
 * connect.
 */
class CometCelebornShuffleHandle[K, V, C](
    val appUniqueId: String,
    val lifecycleManagerHost: String,
    val lifecycleManagerPort: Int,
    val userIdentifier: UserIdentifier,
    shuffleId: Int,
    val numMappers: Int,
    dependency: ShuffleDependency[K, V, C])
    extends BaseShuffleHandle[K, V, C](shuffleId, dependency) {

  def numPartitions: Int = dependency.partitioner.numPartitions
}

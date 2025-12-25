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

package org.apache.comet.shuffle

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import org.apache.comet.CometConf
import org.apache.comet.Native

/**
 * Celeborn shuffle manager for Comet.
 *
 * This class provides integration between Comet native execution engine and Apache Celeborn for
 * distributed shuffle operations. It uses Celeborn Rust client through JNI to push shuffle data
 * directly from the native side, avoiding JNI overhead for data transfer.
 *
 * Architecture:
 * {{{
 *   Spark Driver (JVM)
 *   +-- LifecycleManager (manages shuffle lifecycle)
 *   +-- Provides LM address to Executors
 *
 *   Spark Executor (Comet/Rust)
 *   +-- ExecutorShuffleClient (Rust)
 *   |   +-- Connects to LifecycleManager via RPC
 *   |   +-- Pushes data directly to Celeborn Workers
 *   |   +-- Reads data from Celeborn Workers
 *   +-- JNI Bridge (for initialization and control)
 *
 *   Celeborn Workers
 *   +-- Store and serve shuffle data
 * }}}
 *
 * Usage:
 * {{{
 *   spark.conf.set("spark.comet.shuffle.celeborn.enabled", "true")
 *   spark.conf.set("spark.comet.shuffle.celeborn.master.endpoints", "host1:9097,host2:9097")
 * }}}
 */
class CelebornShuffleManager(conf: SparkConf) extends Logging {

  private val native = new Native()

  // Celeborn configuration
  private val masterEndpoints: Array[String] = conf
    .get(CometConf.COMET_SHUFFLE_CELEBORN_MASTER_ENDPOINTS.key, "")
    .split(",")
    .map(_.trim)
    .filter(_.nonEmpty)

  private val lifecycleManagerHost: String = conf
    .get(CometConf.COMET_SHUFFLE_CELEBORN_LIFECYCLE_MANAGER_HOST.key, "")

  private val lifecycleManagerPort: Int = conf
    .getInt(CometConf.COMET_SHUFFLE_CELEBORN_LIFECYCLE_MANAGER_PORT.key, 9098)

  // Track active shuffle clients
  private val activeClients = new java.util.concurrent.ConcurrentHashMap[Long, Long]()

  /**
   * Check if Celeborn shuffle is enabled and properly configured.
   */
  def isEnabled: Boolean = {
    val enabled = conf.getBoolean(CometConf.COMET_SHUFFLE_CELEBORN_ENABLED.key, false)
    val hasEndpoints = masterEndpoints.nonEmpty
    val hasLifecycleManager = lifecycleManagerHost.nonEmpty

    if (enabled && !hasEndpoints) {
      logWarning(
        "Celeborn shuffle is enabled but no master endpoints are configured. " +
          s"Set ${CometConf.COMET_SHUFFLE_CELEBORN_MASTER_ENDPOINTS.key}")
    }

    if (enabled && !hasLifecycleManager) {
      logWarning(
        "Celeborn shuffle is enabled but LifecycleManager host is not configured. " +
          s"Set ${CometConf.COMET_SHUFFLE_CELEBORN_LIFECYCLE_MANAGER_HOST.key}")
    }

    enabled && hasEndpoints && hasLifecycleManager
  }

  /**
   * Create a Celeborn shuffle writer context for a map task.
   *
   * @param appId
   *   The application ID
   * @param shuffleId
   *   The shuffle ID
   * @param mapId
   *   The map task ID
   * @param attemptId
   *   The task attempt ID
   * @param numMappers
   *   Total number of mappers
   * @param numPartitions
   *   Total number of partitions
   * @return
   *   A handle to the native Celeborn client context
   */
  def createShuffleWriter(
      appId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      numPartitions: Int): Long = {

    require(isEnabled, "Celeborn shuffle is not enabled or not properly configured")

    logInfo(
      s"Creating Celeborn shuffle writer for shuffle $shuffleId, " +
        s"map $mapId, attempt $attemptId")

    val handle = native.createCelebornClient(
      appId,
      masterEndpoints,
      lifecycleManagerHost,
      lifecycleManagerPort,
      shuffleId,
      mapId,
      attemptId,
      numMappers,
      numPartitions)

    activeClients.put(handle, System.currentTimeMillis())
    handle
  }

  /**
   * Push data to Celeborn for a specific partition.
   *
   * @param handle
   *   The Celeborn client handle
   * @param partitionId
   *   The target partition ID
   * @param data
   *   The data to push (serialized Arrow IPC format)
   * @return
   *   true if successful
   */
  def pushData(handle: Long, partitionId: Int, data: Array[Byte]): Boolean = {
    native.celebornPushData(handle, partitionId, data)
  }

  /**
   * Signal that a mapper has finished writing all its data.
   *
   * @param handle
   *   The Celeborn client handle
   * @return
   *   true if the stage has ended (all mappers finished)
   */
  def mapperEnd(handle: Long): Boolean = {
    native.celebornMapperEnd(handle)
  }

  /**
   * Release a Celeborn client and clean up resources.
   *
   * @param handle
   *   The Celeborn client handle
   */
  def releaseClient(handle: Long): Unit = {
    if (handle != 0) {
      activeClients.remove(handle)
      native.releaseCelebornClient(handle)
    }
  }

  /**
   * Cleanup shuffle data from Celeborn.
   *
   * @param handle
   *   The Celeborn client handle
   * @return
   *   true if successful
   */
  def cleanupShuffle(handle: Long): Boolean = {
    native.celebornCleanupShuffle(handle)
  }

  /**
   * Get partition locations from Celeborn.
   *
   * @param handle
   *   The Celeborn client handle
   * @param partitionId
   *   The partition ID
   * @return
   *   Array of worker addresses
   */
  def getPartitionLocation(handle: Long, partitionId: Int): Array[String] = {
    native.celebornGetPartitionLocation(handle, partitionId)
  }

  /**
   * Stop the shuffle manager and clean up all resources.
   */
  def stop(): Unit = {
    logInfo("Stopping CelebornShuffleManager")

    // Release all active clients
    val handles = new java.util.ArrayList[Long]()
    activeClients.keySet().forEach(h => handles.add(h))
    handles.forEach(h => releaseClient(h))

    // Clear all cached clients
    native.celebornClearClients()
  }
}

/**
 * Companion object for CelebornShuffleManager.
 */
object CelebornShuffleManager extends Logging {

  @volatile private var instance: CelebornShuffleManager = _

  /**
   * Get or create the singleton CelebornShuffleManager instance.
   */
  def get(conf: SparkConf): CelebornShuffleManager = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = new CelebornShuffleManager(conf)
          logInfo("Created CelebornShuffleManager instance")
        }
      }
    }
    instance
  }

  /**
   * Get the existing CelebornShuffleManager instance.
   * @throws IllegalStateException
   *   if not initialized
   */
  def getInstance: CelebornShuffleManager = {
    if (instance == null) {
      throw new IllegalStateException(
        "CelebornShuffleManager not initialized. " +
          "Call get(conf) first.")
    }
    instance
  }

  /**
   * Check if CelebornShuffleManager is initialized.
   */
  def isInitialized: Boolean = instance != null

  /**
   * Stop and clear the singleton instance.
   */
  def stop(): Unit = {
    if (instance != null) {
      synchronized {
        if (instance != null) {
          instance.stop()
          instance = null
        }
      }
    }
  }
}

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

package org.apache.comet.serde.operator

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.comet.{CometNativeExec, CometSinkPlaceHolder}
import org.apache.spark.sql.comet.execution.shuffle.{CometCelebornShuffleManager, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass, QueryPlanSerde}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * A sink that generates a native Celeborn shuffle reader operator.
 */
object CometCelebornExchangeSink extends CometSink[SparkPlan] {

  override def isFfiSafe: Boolean = true

  override def convert(
      op: SparkPlan,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {

    val exchange = op match {
      case s: ShuffleQueryStageExec =>
        s.plan match {
          case e: CometShuffleExchangeExec => e
          case _ => return None
        }
      case e: CometShuffleExchangeExec => e
      case _ => return None
    }

    // Get Shuffle Manager
    val shuffleManager = SparkEnv.get.shuffleManager match {
      case m: CometCelebornShuffleManager => m
      case _ =>
        withInfo(op, "Current ShuffleManager is not CometCelebornShuffleManager")
        return None
    }

    val conf = SparkEnv.get.conf
    val appId = shuffleManager.getAppUniqueId
    // Note: shuffleId should be valid at this point (after registration)
    val shuffleId = exchange.shuffleDependency.shuffleId
    val masterEndpoints = conf.get("spark.celeborn.master.endpoints", "").split(",").toSeq

    if (masterEndpoints.isEmpty || (masterEndpoints.length == 1 && masterEndpoints.head.isEmpty)) {
      withInfo(op, "Celeborn master endpoints not configured")
      return None
    }

    val lmHost = shuffleManager.getLifecycleManagerHost
    val lmPort = shuffleManager.getLifecycleManagerPort

    if (lmHost == null || lmPort == 0) {
      withInfo(op, "LifecycleManager info not available")
      return None
    }

    val readerBuilder = OperatorOuterClass.CelebornShuffleReader
      .newBuilder()
      .setAppId(appId)
      .setShuffleId(shuffleId)
      .addAllMasterEndpoints(masterEndpoints.asJava)
      .setLifecycleManagerHost(lmHost)
      .setLifecycleManagerPort(lmPort)
      // Partition ID and Attempt Number will be set at runtime in Rust based on TaskContext
      .setPartitionId(-1)
      .setAttemptNumber(-1)
      .setStartMapIndex(0)
      .setEndMapIndex(exchange.numMappers)

    op.output.foreach { attr =>
      val fieldBuilder = OperatorOuterClass.SparkStructField
        .newBuilder()
        .setName(attr.name)
        .setNullable(attr.nullable)

      val dt = QueryPlanSerde.serializeDataType(attr.dataType)
      if (dt.isEmpty) {
        withInfo(op, s"Failed to serialize data type for attribute ${attr.name}")
        return None
      }
      fieldBuilder.setDataType(dt.get)
      readerBuilder.addSchema(fieldBuilder.build())
    }

    builder.setCelebornShuffleReader(readerBuilder)

    Some(builder.build())
  }

  override def createExec(nativeOp: Operator, op: SparkPlan): CometNativeExec =
    CometSinkPlaceHolder(nativeOp, op, op)
}

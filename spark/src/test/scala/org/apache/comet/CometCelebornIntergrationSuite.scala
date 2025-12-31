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

package org.apache.comet

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase

class CometCelebornIntergrationSuite extends CometTestBase {
  test("simple select") {
    val result = sql("select 1").count()
    println(s"Simple select count: $result")
    assert(result == 1, s"Expected count 1, got $result")
  }

  test("shuffle with group by") {
    // Create a test dataframe that will trigger shuffle
    val df = sql("""
      SELECT value, COUNT(*) as cnt
      FROM (
        SELECT 1 as value
        UNION ALL
        SELECT 2 as value
        UNION ALL
        SELECT 1 as value
      )
      GROUP BY value
    """)
    df.count

    // Collect the data and verify the results
    val data = df.collect()
    assert(data.length == 2, s"Expected 2 rows, got ${data.length}")

    // Verify the actual values
    val resultMap = data.map(row => (row.getInt(0), row.getLong(1))).toMap
    assert(resultMap(1) == 2, s"Expected count 2 for value 1, got ${resultMap(1)}")
    assert(resultMap(2) == 1, s"Expected count 1 for value 2, got ${resultMap(2)}")
  }

  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
    // Set the Celeborn shuffle manager
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager")

    // Celeborn master endpoints configuration
    conf.set("spark.celeborn.master.endpoints", "192.168.3.17:9097")
//    conf.set("spark.celeborn.master.endpoints", "10.27.36.96:9097")

    // Enable Comet Celeborn shuffle integration
    conf.set("spark.comet.shuffle.celeborn.enabled", "true")

    // Ensure Comet execution is enabled (should be true by default, but explicit is better)
    conf.set(CometConf.COMET_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")

    // Enable debug logging for troubleshooting
    conf.set("spark.sql.adaptive.enabled", "false") // Disable AQE for simpler debugging

    // Disable compression since Rust client doesn't support LZ4 compression yet
    conf.set("spark.celeborn.client.shuffle.compression.codec", "zstd")
    conf.set("spark.comet.shuffle.celeborn.writer.mode", "sort")

    conf
  }
}

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

    val result = df.count()
    println(s"Group by count: $result")
    assert(result == 2, s"Expected count 2, got $result")

    // Also check the actual data
    df.show()
  }

  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager")
    conf.set("spark.celeborn.master.endpoints", "192.168.3.17:9097")
    conf.set("spark.comet.shuffle.celeborn.enabled", "true")
    conf
  }
}

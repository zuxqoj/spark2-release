/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

/**
 * This test suite is designed to be used internally and to reduce the chance of conflicts.
 */
class HDPQuerySuite
  extends QueryTest
  with SQLTestUtils
  with TestHiveSingleton {

  for (isNewOrc <- Seq("false", "true"); isConverted <- Seq("false", "true");
      isColumnarBatch <- Seq("false", "true"); isVectorized <- Seq("false", "true")) {
    test("SPARK-18355 Read data from a ORC hive table with a new column " +
      s"(isNewOrc=$isNewOrc, isColumnar=$isColumnarBatch, " +
      s"isVectorized=$isVectorized, isConverted=$isConverted)") {
      withSQLConf(
        SQLConf.ORC_ENABLED.key -> isNewOrc,
        SQLConf.ORC_COLUMNAR_BATCH_READER_ENABLED.key -> isColumnarBatch,
        SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> isVectorized,
        HiveUtils.CONVERT_METASTORE_ORC.key -> isConverted) {
        withTempDatabase { db =>
          val client = spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
          client.runSqlHive(
            s"""
               |CREATE TABLE $db.t(
               |  click_id string,
               |  search_id string,
               |  uid bigint)
               |PARTITIONED BY (
               |  ts string,
               |  hour string)
               |STORED AS ORC
             """.stripMargin)

          client.runSqlHive(
            s"""
               |INSERT INTO TABLE $db.t
               |PARTITION (ts = '98765', hour = '01')
               |VALUES (12, 2, 12345)
             """.stripMargin
          )

          checkAnswer(
            sql(s"SELECT * FROM $db.t"),
            Row("12", "2", 12345, "98765", "01"))

          client.runSqlHive(s"ALTER TABLE $db.t ADD COLUMNS (dummy string)")

          checkAnswer(
            sql(s"SELECT click_id, search_id FROM $db.t"),
            Row("12", "2"))

          checkAnswer(
            sql(s"SELECT search_id, click_id FROM $db.t"),
            Row("2", "12"))

          checkAnswer(
            sql(s"SELECT search_id FROM $db.t"),
            Row("2"))

          if (isNewOrc == "true") {
            checkAnswer(
              sql(s"SELECT * FROM $db.t"),
              Row("12", "2", 12345, null, "98765", "01"))

            checkAnswer(
              sql(s"SELECT dummy, click_id FROM $db.t"),
              Row(null, "12"))
          }
        }
      }
    }
  }
}

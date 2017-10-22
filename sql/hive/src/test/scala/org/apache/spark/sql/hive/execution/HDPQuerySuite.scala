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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.orc.OrcFileOperator
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

/**
 * This test suite is designed to be used internally and to reduce the chance of conflicts.
 */
class HDPQuerySuite
  extends QueryTest
  with SQLTestUtils
  with TestHiveSingleton {

  private val client = spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client

  // To control explicitly, this test uses atomic types. ORC_COLUMNAR_BATCH_READER_ENABLED and
  // ORC_VECTORIZED_READER_ENABLED are automatically disabled in complex types.
  private def testAtomicTypes(result: Row): Unit = {
    val location = Utils.createTempDir()
    val uri = location.toURI
    try {
      client.runSqlHive("USE default")
      client.runSqlHive(
        """
          |CREATE EXTERNAL TABLE hive_orc(
          |  a STRING,
          |  b CHAR(10),
          |  c VARCHAR(10))
          |STORED AS orc""".
          stripMargin)
      // Hive throws an exception if I assign the location in the create table statement.
      client.runSqlHive(s"ALTER TABLE hive_orc SET LOCATION '$uri'")
      client.runSqlHive(
        """
          |INSERT INTO TABLE hive_orc
          |SELECT 'a', 'b', 'c'
          |FROM (SELECT 1) t""".stripMargin)
      // We create a different table in Spark using the same schema which points to
      // the same location.
      spark.sql(
        s"""
           |CREATE EXTERNAL TABLE spark_orc(
           |  a STRING,
           |  b CHAR(10),
           |  c VARCHAR(10))
           |STORED AS orc
           |LOCATION '$uri'""".stripMargin)
      checkAnswer(spark.table("hive_orc"), result)
      checkAnswer(spark.table("spark_orc"), result)
    } finally {
      client.runSqlHive("DROP TABLE IF EXISTS hive_orc")
      client.runSqlHive("DROP TABLE IF EXISTS spark_orc")
      Utils.deleteRecursively(location)
    }
  }

  private def testComplexTypes(result: Row): Unit = {
    val location = Utils.createTempDir()
    val uri = location.toURI
    try {
      client.runSqlHive("USE default")
      client.runSqlHive(
        """
          |CREATE EXTERNAL TABLE hive_orc(
          |  a STRING,
          |  b CHAR(8),
          |  c VARCHAR(10),
          |  d STRUCT<x:CHAR(4), y:CHAR(3)>,
          |  e ARRAY<CHAR(3)>,
          |  f MAP<CHAR(3),CHAR(4)>)
          |STORED AS orc""".stripMargin)
      // Hive throws an exception if I assign the location in the create table statement.
      client.runSqlHive(s"ALTER TABLE hive_orc SET LOCATION '$uri'")
      client.runSqlHive(
        """
          |INSERT INTO TABLE hive_orc
          |SELECT
          |  'a',
          |  'b',
          |  'c',
          |  NAMED_STRUCT('x', CAST('s' AS CHAR(4)), 'y', CAST('t' AS CHAR(3))),
          |  ARRAY(CAST('a' AS CHAR(3))),
          |  MAP(CAST('k1' AS CHAR(3)), CAST('v1' AS CHAR(4)))
          |FROM (SELECT 1) t""".stripMargin)
      // We create a different table in Spark using the same schema which points to
      // the same location.
      spark.sql(
        s"""
           |CREATE EXTERNAL TABLE spark_orc(
           |  a STRING,
           |  b CHAR(10),
           |  c VARCHAR(10),
           |  d STRUCT<x:CHAR(4), y:CHAR(3)>,
           |  e ARRAY<CHAR(3)>,
           |  f MAP<CHAR(3),CHAR(4)>)
           |STORED AS orc
           |LOCATION '$uri'""".stripMargin)
      checkAnswer(spark.table("hive_orc"), result)
      checkAnswer(spark.table("spark_orc"), result)
    } finally {
      client.runSqlHive("DROP TABLE IF EXISTS hive_orc")
      client.runSqlHive("DROP TABLE IF EXISTS spark_orc")
      Utils.deleteRecursively(location)
    }
  }

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

  for (isNewOrc <- Seq("false", "true"); isConverted <- Seq("false", "true");
      isColumnarBatch <- Seq("false", "true"); isVectorized <- Seq("false", "true")) {
    test("converted ORC table supports resolving mixed case field" +
      s"(isNewOrc=$isNewOrc, isColumnar=$isColumnarBatch, " +
      s"isVectorized=$isVectorized, isConverted=$isConverted)") {
      withSQLConf(
        SQLConf.ORC_ENABLED.key -> isNewOrc,
        SQLConf.ORC_COLUMNAR_BATCH_READER_ENABLED.key -> isColumnarBatch,
        SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> isVectorized,
        HiveUtils.CONVERT_METASTORE_ORC.key -> isConverted) {
        withTable("dummy_orc") {
          withTempPath { dir =>
            val df = spark.range(5).selectExpr("id", "id as valueField", "id as partitionValue")
            df.write
              .partitionBy("partitionValue")
              .mode("overwrite")
              .orc(dir.getAbsolutePath)

            spark.sql(
              s"""
                 |CREATE EXTERNAL TABLE dummy_orc (id LONG, valueField LONG)
                 |PARTITIONED BY (partitionValue INT)
                 |STORED AS ORC
                 |LOCATION "${dir.toURI}"""".stripMargin)
            spark.sql(s"MSCK REPAIR TABLE dummy_orc")
            checkAnswer(spark.sql("SELECT * FROM dummy_orc"), df)
          }
        }
      }
    }
  }

  for (isNewOrc <- Seq("false"); isConverted <- Seq("false", "true")) {
    test(s"Read char/varchar column written by Hive " +
      s"(isNewOrc=$isNewOrc, isConverted=$isConverted)") {
      withSQLConf(
        SQLConf.ORC_ENABLED.key -> isNewOrc,
        HiveUtils.CONVERT_METASTORE_ORC.key -> isConverted) {
        testAtomicTypes(Row("a", "b         ", "c"))
        testComplexTypes(
          Row("a", "b       ", "c", Row("s   ", "t  "), Seq("a  "), Map("k1 " -> "v1  ")))
      }
    }
  }

  for (isNewOrc <- Seq("true");
      isChar <- Seq("false", "true");
      isConverted <- Seq("false", "true");
      isColumnarBatch <- Seq("false", "true");
      isVectorized <- Seq("false", "true")) {
    test("Read char/varchar column written by Hive " +
      s"(isNewOrc=$isNewOrc, isChar=$isChar, isColumnar=$isColumnarBatch, " +
      s"isVectorized=$isVectorized, isConverted=$isConverted)") {
      withSQLConf(
        SQLConf.ORC_ENABLED.key -> isNewOrc,
        SQLConf.ORC_CHAR_ENABLED.key -> isChar,
        SQLConf.ORC_COLUMNAR_BATCH_READER_ENABLED.key -> isColumnarBatch,
        SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> isVectorized,
        HiveUtils.CONVERT_METASTORE_ORC.key -> isConverted) {
        if (isConverted == "false" || isChar == "true") {
          testAtomicTypes(Row("a", "b         ", "c"))
        } else {
          testAtomicTypes(Row("a", "b", "c"))
        }
      }
    }
  }

  for (isNewOrc <- Seq("true");
      isChar <- Seq("false", "true");
      isConverted <- Seq("false", "true")) {
    test(s"Read char in complex types (isNewOrc=$isNewOrc, isChar=$isChar, " +
      s"isConverted=$isConverted)") {
      withSQLConf(
        SQLConf.ORC_ENABLED.key -> isNewOrc,
        SQLConf.ORC_CHAR_ENABLED.key -> isChar,
        HiveUtils.CONVERT_METASTORE_ORC.key -> isConverted) {
        if (isConverted == "false" || isChar == "true") {
          testComplexTypes(
            Row("a", "b       ", "c", Row("s   ", "t  "), Seq("a  "), Map("k1 " -> "v1  ")))
        } else {
          testComplexTypes(Row("a", "b", "c", Row("s", "t"), Seq("a"), Map("k1" -> "v1")))
        }
      }
    }
  }

  for (isNewOrc <- Seq("false", "true");
      isConverted <- Seq("false", "true")) {
    test(s"create hive serde table with new syntax(isNewOrc=$isNewOrc, isConverted=$isConverted)") {
      withSQLConf(
        SQLConf.ORC_ENABLED.key -> isNewOrc,
        HiveUtils.CONVERT_METASTORE_ORC.key -> isConverted) {
        withTable("t", "t2", "t3") {
          withTempPath { path =>
            sql(
              s"""
                 |CREATE TABLE t(id int) USING hive
                 |OPTIONS(fileFormat 'orc', compression 'Zlib')
                 |LOCATION '${path.toURI}'
              """.stripMargin)
            val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
            assert(DDLUtils.isHiveTable(table))
            assert(table.storage.serde == Some("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))
            assert(table.storage.properties.get("compression") == Some("Zlib"))
            assert(spark.table("t").collect().isEmpty)

            sql("INSERT INTO t SELECT 1")
            checkAnswer(spark.table("t"), Row(1))
            // Check if this is compressed as ZLIB.
            val maybeOrcFile = path.listFiles().find(_.getName.startsWith("part"))
            assert(maybeOrcFile.isDefined)
            val orcFilePath = maybeOrcFile.get.toPath.toString
            val expectedCompressionKind =
              OrcFileOperator.getFileReader(orcFilePath).get.getCompression
            assert("ZLIB" === expectedCompressionKind.name())

            sql("CREATE TABLE t2 USING HIVE AS SELECT 1 AS c1, 'a' AS c2")
            val table2 = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t2"))
            assert(DDLUtils.isHiveTable(table2))
            assert(
              table2.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
            checkAnswer(spark.table("t2"), Row(1, "a"))

            sql("CREATE TABLE t3(a int, p int) USING hive PARTITIONED BY (p)")
            sql("INSERT INTO t3 PARTITION(p=1) SELECT 0")
            checkAnswer(spark.table("t3"), Row(0, 1))
          }
        }
      }
    }
  }

  for (isNewOrc <- Seq("true", "false"); isConverted <- Seq("true", "false")) {
    test("alter datasource table add columns - orc " +
      s"(isNewOrc=$isNewOrc, isConverted=$isConverted)") {
      withSQLConf(
        SQLConf.ORC_ENABLED.key -> isNewOrc,
        HiveUtils.CONVERT_METASTORE_ORC.key -> isConverted) {
        withTable("t1") {
          sql("CREATE TABLE t1 (c1 int) USING ORC")
          sql("INSERT INTO t1 VALUES (1)")
          sql("ALTER TABLE t1 ADD COLUMNS (c2 int)")
          checkAnswer(
            spark.table("t1"),
            Seq(Row(1, null))
          )
          checkAnswer(
            sql("SELECT * FROM t1 WHERE c2 is null"),
            Seq(Row(1, null))
          )

          sql("INSERT INTO t1 VALUES (3, 2)")
          checkAnswer(
            sql("SELECT * FROM t1 WHERE c2 = 2"),
            Seq(Row(3, 2))
          )
        }
      }
    }

    test("alter datasource table add columns - partitioned - orc" +
      s"(isNewOrc=$isNewOrc, isConverted=$isConverted)") {
      withSQLConf(
        SQLConf.ORC_ENABLED.key -> isNewOrc,
        HiveUtils.CONVERT_METASTORE_ORC.key -> isConverted) {
        withTable("t1") {
          sql("CREATE TABLE t1 (c1 int, c2 int) USING ORC PARTITIONED BY (c2)")
          sql("INSERT INTO t1 PARTITION(c2 = 2) VALUES (1)")
          sql("ALTER TABLE t1 ADD COLUMNS (c3 int)")
          checkAnswer(
            spark.table("t1"),
            Seq(Row(1, null, 2))
          )
          checkAnswer(
            sql("SELECT * FROM t1 WHERE c3 is null"),
            Seq(Row(1, null, 2))
          )
          sql("INSERT INTO t1 PARTITION(c2 =1) VALUES (2, 3)")
          checkAnswer(
            sql("SELECT * FROM t1 WHERE c3 = 3"),
            Seq(Row(2, 3, 1))
          )
          checkAnswer(
            sql("SELECT * FROM t1 WHERE c2 = 1"),
            Seq(Row(2, 3, 1))
          )
        }
      }
    }
  }
}

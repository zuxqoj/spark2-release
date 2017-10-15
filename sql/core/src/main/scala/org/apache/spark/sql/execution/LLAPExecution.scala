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

package org.apache.spark.sql.execution

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

/**
 * LLAP execution helper objects. If LlapRelation does not exists, it does nothing silently.
 */
object LLAPExecution extends Logging {

  private val LLAP_RELATION_CLASS_NAME = "com.hortonworks.spark.sql.hive.llap.LlapRelation"

  // Load LlapRelation class
  lazy val maybeClazz: Option[Class[_]] = {
    try {
      Some(Utils.classForName(LLAP_RELATION_CLASS_NAME))
    } catch {
      case _: ClassNotFoundException => None
    }
  }

  /**
   * Closes all LlapRelations in the given executionId to notify Hive to clean up.
   * If `spark.sql.hive.llap` is not true or `LlapRelation` is not found, it dose no-op
   * silently.
   */
  def closeLlapRelation(session: SparkSession, executionId: Long): Unit = {
    if (session.conf.get("spark.sql.hive.llap", "false") == "true") {
      maybeClazz match { case Some(clazz) =>
        val queryExecution = SQLExecution.getQueryExecution(executionId)
        assert(queryExecution != null)
        val relations = queryExecution.sparkPlan.collect {
          case p: LeafExecNode => p
        }
        relations.foreach {
          r => r match {
            case r: RowDataSourceScanExec
              if (r.relation.getClass.getCanonicalName.endsWith(LLAP_RELATION_CLASS_NAME)) =>
                clazz.getMethod("close").invoke(r.relation)
                logDebug("close hive connection via " + r.relation.getClass.getCanonicalName)
            }
         }
      }
    }
  }
}

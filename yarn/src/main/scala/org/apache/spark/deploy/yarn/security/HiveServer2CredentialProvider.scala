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

package org.apache.spark.deploy.yarn.security

import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction
import java.sql.{Connection, DriverManager}

import scala.reflect.runtime.universe
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[security] class HiveServer2CredentialProvider extends ServiceCredentialProvider
    with Logging {

  override def serviceName: String = "hiveserver2"

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {

    var con: Connection = null
    try {
      Utils.classForName("org.apache.hive.jdbc.HiveDriver")

      val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
      val hiveConfClass = mirror.classLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")
      val ctor = hiveConfClass.getDeclaredConstructor(classOf[Configuration],
        classOf[Object].getClass)
      val hiveConf = ctor.newInstance(hadoopConf, hiveConfClass).asInstanceOf[Configuration]

      val hs2HostKey = "hive.server2.thrift.bind.host"
      val hs2PortKey = "hive.server2.thrift.port"
      val hs2PrincKey = "hive.server2.authentication.kerberos.principal"

      require(hiveConf.get(hs2HostKey) != null, s"$hs2HostKey is not configured")
      require(hiveConf.get(hs2PortKey) != null, s"$hs2PortKey is not configured")
      require(hiveConf.get(hs2PrincKey) != null, s"$hs2PrincKey is not configured")

      val jdbcUrl = s"jdbc:hive2://${hiveConf.get(hs2HostKey)}:${hiveConf.get(hs2PortKey)}/;" +
        s"principal=${hiveConf.get(hs2PrincKey)}"

      doAsRealUser {
        con = DriverManager.getConnection(jdbcUrl)
        val method = con.getClass.getMethod("getDelegationToken", classOf[String], classOf[String])
        val currentUser = UserGroupInformation.getCurrentUser()
        val realUser = Option(currentUser.getRealUser()).getOrElse(currentUser)
        val tokenStr = method.invoke(con, realUser.getUserName, hiveConf.get(hs2PrincKey))
          .asInstanceOf[String]
        val token = new Token[DelegationTokenIdentifier]()
        token.decodeFromUrlString(tokenStr)
        creds.addToken(new Text("hive.jdbc.delegation.token"), token)
        logInfo(s"Add HiveServer2 token $token to credentials")
      }
    } catch {
      case NonFatal(e) => logWarning(s"Failed to get HiveServer2 delegation token", e)
    } finally {
      if (con != null) {
        con.close()
        con = null
      }
    }

    None
  }

  /**
   * Run some code as the real logged in user (which may differ from the current user, for
   * example, when using proxying).
   */
  private def doAsRealUser[T](fn: => T): T = {
    val currentUser = UserGroupInformation.getCurrentUser()
    val realUser = Option(currentUser.getRealUser()).getOrElse(currentUser)

    // For some reason the Scala-generated anonymous class ends up causing an
    // UndeclaredThrowableException, even if you annotate the method with @throws.
    try {
      realUser.doAs(new PrivilegedExceptionAction[T]() {
        override def run(): T = fn
      })
    } catch {
      case e: UndeclaredThrowableException => throw Option(e.getCause()).getOrElse(e)
    }
  }
}

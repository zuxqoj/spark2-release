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

package org.apache.spark.api.python

import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils


private[spark] class VirtualEnvFactory(pythonExec: String, conf: SparkConf, isDriver: Boolean)
  extends Logging {

  private val virtualEnvType = conf.get("spark.pyspark.virtualenv.type", "native")
  private val virtualEnvPath = conf.get("spark.pyspark.virtualenv.bin.path", "")
  private var virtualEnvName: String = _
  private var virtualPythonExec: String = _
  private val VIRTUALENV_ID = new AtomicInteger()

  /*
   * Create virtualenv using native virtualenv or conda
   *
   * Native Virtualenv:
   *   -  Execute command: virtualenv -p pythonExec --no-site-packages virtualenvName
   *   -  Execute command: python -m pip --cache-dir cache-dir install -r requirement_file
   *
   * Conda
   *   -  Execute command: conda create --prefix prefix --file requirement_file -y
   *
   */
  def setupVirtualEnv(): String = {
    logDebug("Start to setup virtualenv...")
    logDebug("user.dir=" + System.getProperty("user.dir"))
    logDebug("user.home=" + System.getProperty("user.home"))

    require(virtualEnvType == "native" || virtualEnvType == "conda",
      s"VirtualEnvType: ${virtualEnvType} is not supported" )
    if (isDriver) {
      val virtualenv_basedir = Files.createTempDir()
      virtualenv_basedir.deleteOnExit()
      virtualEnvName = virtualenv_basedir.getAbsolutePath
    } else {
      virtualEnvName = "virtualenv_" + conf.getAppId + "_" + VIRTUALENV_ID.getAndIncrement()
    }

    // use the absolute path when it is local mode otherwise just use filename as it would be
    // fetched from FileServer
    val pyspark_requirements =
      if (Utils.isLocalMaster(conf)
        || (isDriver && conf.get("spark.submit.deployMode") != "cluster")) {
        conf.get("spark.pyspark.virtualenv.requirements")
      } else {
        conf.get("spark.pyspark.virtualenv.requirements").split("/").last
      }

    val createEnvCommand =
      if (virtualEnvType == "native") {
        Arrays.asList(virtualEnvPath,
          "-p", pythonExec,
          "--no-site-packages", virtualEnvName)
      } else {
        Arrays.asList(virtualEnvPath,
          "create", "--prefix", virtualEnvName,
          "--file", pyspark_requirements, "-y")
      }
    execCommand(createEnvCommand)
    // virtualenv will be created in the working directory of Executor.
    virtualPythonExec = virtualEnvName + "/bin/python"
    if (virtualEnvType == "native") {
      execCommand(Arrays.asList(virtualPythonExec, "-m", "pip",
        "--cache-dir", System.getProperty("user.home"),
        "install", "-r", pyspark_requirements))
    }
    virtualPythonExec
  }

  def execCommand(commands: java.util.List[String]): Unit = {
    logDebug("Running command:" + commands.asScala.mkString(" "))
    val pb = new ProcessBuilder(commands).inheritIO()
    // pip internally use environment variable `HOME`
    pb.environment().put("HOME", System.getProperty("user.home"))
    val proc = pb.start()
    val exitCode = proc.waitFor()
    if (exitCode != 0) {
      throw new RuntimeException("Fail to run command: " + commands.asScala.mkString(" "))
    }
  }
}

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.julianjaffe.spark_druid_connector.model

import com.julianjaffe.spark_druid_connector.configuration.{Configuration, DruidConfigurationKeys}

import scala.collection.mutable

class HdfsDeepStorageConfig extends DeepStorageConfig(DruidConfigurationKeys.hdfsDeepStorageTypeKey) {
  private val optionsMap: mutable.Map[String, String] = mutable.Map[String, String](
    DruidConfigurationKeys.deepStorageTypeKey -> deepStorageType
  )

  def storageDirectory(storageDirectory: String): HdfsDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.hdfsDeepStorageTypeKey,
      DruidConfigurationKeys.storageDirectoryKey)
    optionsMap.put(key, storageDirectory)
    this
  }

  def hadoopConf(conf: String): HdfsDeepStorageConfig = {
    val key = Configuration.toKey(DruidConfigurationKeys.hdfsDeepStorageTypeKey,
      DruidConfigurationKeys.hdfsHadoopConfKey)
    optionsMap.put(key, conf)
    this
  }

  override def toOptions: Map[String, String] = optionsMap.toMap
}

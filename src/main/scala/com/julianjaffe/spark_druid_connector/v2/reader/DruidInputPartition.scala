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

package com.julianjaffe.spark_druid_connector.v2.reader

import com.julianjaffe.spark_druid_connector.MAPPER
import com.julianjaffe.spark_druid_connector.configuration.{Configuration, SerializableHadoopConfiguration}
import org.apache.druid.common.config.NullHandling
import org.apache.druid.query.filter.DimFilter
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

/**
  * Defines a single partition in the dataframe's underlying RDD. This object is generated in the driver and then
  * serialized to the executors where it is responsible for creating the actual ([[InputPartitionReader]]) which
  * does the actual reading.
  */
class DruidInputPartition(
                           segment: DataSegment,
                           schema: StructType,
                           filter: Option[DimFilter],
                           columnTypes: Option[Set[String]],
                           conf: Configuration,
                           useSparkConfForDeepStorage: Boolean,
                           useCompactSketches: Boolean,
                           useDefaultNullHandling: Boolean
                         ) extends InputPartition[InternalRow] {
  // There's probably a better way to do this
  private val session = SparkSession.getActiveSession.get // We're running on the driver, it exists
  private val broadcastConf =
    session.sparkContext.broadcast(
      new SerializableHadoopConfiguration(session.sparkContext.hadoopConfiguration)
    )
  private val serializedSegment: String = MAPPER.writeValueAsString(segment)
  private val serializedFilter: String =  MAPPER.writeValueAsString(filter.getOrElse(Option.empty))

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new DruidInputPartitionReader(
      serializedSegment,
      schema,
      Some(serializedFilter),
      columnTypes,
      broadcastConf,
      conf,
      useSparkConfForDeepStorage,
      useCompactSketches,
      useDefaultNullHandling
    )
  }
}

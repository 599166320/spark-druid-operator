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

import com.fasterxml.jackson.core.`type`.TypeReference
import com.julianjaffe.spark_druid_connector.MAPPER
import com.julianjaffe.spark_druid_connector.configuration.{Configuration, DruidConfigurationKeys,
  SerializableHadoopConfiguration}
import com.julianjaffe.spark_druid_connector.mixins.Logging
import com.julianjaffe.spark_druid_connector.registries.{ComplexMetricRegistry, SegmentReaderRegistry}
import com.julianjaffe.spark_druid_connector.utils.NullHandlingUtils
import com.julianjaffe.spark_druid_connector.v2.INDEX_IO
import org.apache.druid.java.util.common.{FileUtils, ISE, StringUtils}
import org.apache.druid.segment.QueryableIndex
import org.apache.druid.timeline.DataSegment
import org.apache.spark.broadcast.Broadcast

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

class DruidBaseInputPartitionReader(
                                     segmentStr: String,
                                     columnTypes: Option[Set[String]],
                                     broadcastedHadoopConf: Broadcast[SerializableHadoopConfiguration],
                                     conf: Configuration,
                                     useSparkConfForDeepStorage: Boolean,
                                     useCompactSketches: Boolean,
                                     useDefaultNullHandling: Boolean
                               ) extends Logging {
  // Need to initialize Druid's internal null handling as well for filters etc.
  NullHandlingUtils.initializeDruidNullHandling(useDefaultNullHandling)

  if (columnTypes.isDefined) {
    // Callers will need to explicitly register any complex metrics not known to ComplexMetricRegistry by default
    columnTypes.get.foreach {
      ComplexMetricRegistry.registerByName(_, useCompactSketches)
    }
  } else {
    ComplexMetricRegistry.initializeDefaults()
  }
  ComplexMetricRegistry.registerSerdes()

  // If there are mixed deep storage types, callers will need to handle initialization themselves.
  if (!useSparkConfForDeepStorage && DruidBaseInputPartitionReader.initialized.compareAndSet(false, true)) {
    val deepStorageType = conf.get(DruidConfigurationKeys.deepStorageTypeDefaultKey)
    SegmentReaderRegistry.registerInitializerByType(deepStorageType)
    SegmentReaderRegistry.initialize(deepStorageType, conf.dive(deepStorageType))
  }

  private[reader] val segment =
    MAPPER.readValue[DataSegment](segmentStr, new TypeReference[DataSegment] {})
  private[reader] val queryableIndex: QueryableIndex = loadSegment(segment)
  private lazy val hadoopConf = broadcastedHadoopConf.value.value
  private[reader] lazy val tmpDir: File = FileUtils.createTempDir


  private[reader] def loadSegment(segment: DataSegment): QueryableIndex = {
    val segmentDir = new File(tmpDir, segment.getId.toString)
    if (!segmentDir.exists) {
      logInfo(
        StringUtils.format(
          "Fetching segment [%s] to [%s].", segment.getId, segmentDir
        )
      )
      if (!segmentDir.mkdir) throw new ISE("Failed to make directory[%s]", segmentDir)
       SegmentReaderRegistry.load(segment.getLoadSpec, segmentDir, hadoopConf)
    }
    val index = INDEX_IO.loadIndex(segmentDir)
    logInfo(s"Loaded segment [${segment.getId}].")
    index
  }
}

private[reader] object DruidBaseInputPartitionReader {
  private val initialized = new AtomicBoolean(false)
}

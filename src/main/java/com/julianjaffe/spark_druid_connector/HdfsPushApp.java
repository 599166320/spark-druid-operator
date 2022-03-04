package com.julianjaffe.spark_druid_connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.julianjaffe.spark_druid_connector.configuration.Configuration;
import com.julianjaffe.spark_druid_connector.configuration.DruidConfigurationKeys;
import com.julianjaffe.spark_druid_connector.configuration.DruidDataWriterConfig;
import com.julianjaffe.spark_druid_connector.registries.ComplexMetricRegistry;
import com.julianjaffe.spark_druid_connector.registries.SegmentWriterRegistry;
import com.julianjaffe.spark_druid_connector.registries.ShardSpecRegistry;
import com.julianjaffe.spark_druid_connector.utils.NullHandlingUtils;
import com.julianjaffe.spark_druid_connector.v2.writer.DruidDataWriter;
import com.julianjaffe.spark_druid_connector.v2.writer.DruidWriterCommitMessage;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.guice.LifecycleScope;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.parsers.TimestampParser;
import org.apache.druid.segment.*;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import scala.Immutable;
import scala.Int;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HdfsPushApp {
    static DataSchema dataSchema = null;
    static DruidDataWriterConfig config = null;
    private final static File tmpPersistDir = FileUtils.createTempDir("persist");
    private final static File tmpMergeDir = FileUtils.createTempDir("merge");
    private final static Closer closer;
    private final static ObjectMapper MAPPER = new DefaultObjectMapper();
    private final static IndexIO INDEX_IO;
    private final static IndexMergerV9 INDEX_MERGER_V9;
    private final static Configuration indexSpecConf;
    private  final static IndexSpec indexSpec;
    private  final static DataSegmentPusher pusher;
    private final static Map<Long,Tuple2<List<IndexableAdapter>, List<IncrementalIndex>>> bucketToIndexMap = new HashMap<>();

    static {

        scala.collection.immutable.Map<String,String> properties = new scala.collection.immutable.HashMap<>();
        properties.$plus(new Tuple2<>("",""));
        scala.collection.JavaConverters.mapAsScalaMap(ImmutableMap.of());

        Configuration conf = new Configuration(properties);
        int partitionId =  0;
        StructType schema = null;
        DruidDataWriterConfig config = null;
        Configuration writerConf = conf
                .dive(DruidConfigurationKeys.writerPrefix())
                .merge(Configuration.fromKeyValue(DruidConfigurationKeys.tableKey(),
                        conf.getString(DruidConfigurationKeys.tableKey())));


        Option<scala.collection.immutable.Map<Object, scala.collection.immutable.Map<String, String>>> partitionIdToDruidPartitionsMap = writerConf
                .get(DruidConfigurationKeys.partitionMapKey())
                .map(serializedMap -> {
                    try {
                        return MAPPER.readValue(serializedMap, new TypeReference<scala.collection.immutable.Map<Object, scala.collection.immutable.Map<String, String>>>(){});
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
      );
        String version = "";
        try {
            config = new DruidDataWriterConfig(
                    conf.getString(DruidConfigurationKeys.tableKey()),
                    partitionId,
                    schema,
                    MAPPER.writeValueAsString(dataSchema),
                    writerConf.get(DruidConfigurationKeys.shardSpecTypeDefaultKey()),
                    writerConf.getInt(DruidConfigurationKeys.rowsPerPersistDefaultKey()),
                    writerConf.get(DruidConfigurationKeys.deepStorageTypeDefaultKey()),
                    conf,
                    version,
                    partitionIdToDruidPartitionsMap,
                    writerConf.getBoolean(DruidConfigurationKeys.useCompactSketchesDefaultKey()),
                    writerConf.getBoolean(DruidConfigurationKeys.useDefaultValueForNullDefaultKey())
            );
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }


        NullHandlingUtils.initializeDruidNullHandling(config.useDefaultNullHandling());
        String dataSchemaStr = "{\"dataSource\":\"bb04\",\"timestampSpec\":{\"column\":\"__time\",\"format\":\"auto\",\"missingValue\":null},\"dimensionsSpec\":{\"dimensions\":[{\"type\":\"string\",\"name\":\"isRobot\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"channel\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"cityName\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"isUnpatrolled\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"page\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"countryName\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"regionIsoCode\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"long\",\"name\":\"added\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":false},{\"type\":\"string\",\"name\":\"metroCode\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"comment\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"isNew\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"isMinor\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"long\",\"name\":\"delta\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":false},{\"type\":\"string\",\"name\":\"countryIsoCode\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"isAnonymous\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"user\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"string\",\"name\":\"regionName\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true},{\"type\":\"long\",\"name\":\"deleted\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":false},{\"type\":\"string\",\"name\":\"namespace\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true}],\"dimensionExclusions\":[\"__time\"]},\"metricsSpec\":[],\"granularitySpec\":{\"type\":\"uniform\",\"segmentGranularity\":{\"type\":\"all\"},\"queryGranularity\":{\"type\":\"none\"},\"rollup\":true,\"intervals\":[]},\"transformSpec\":{\"filter\":null,\"transforms\":[]}}";
        try {
            dataSchema = MAPPER.readValue(dataSchemaStr,DataSchema.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        closer = Closer.create();
        closer.register(new Closeable(){
            public void close(){
                try {
                    FileUtils.deleteDirectory(tmpMergeDir);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    FileUtils.deleteDirectory(tmpPersistDir);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        INDEX_IO = new IndexIO(MAPPER,() -> 1000000);
        INDEX_MERGER_V9 = new IndexMergerV9(MAPPER, INDEX_IO, OnHeapMemorySegmentWriteOutMediumFactory.instance());
        indexSpecConf = config.properties().dive(DruidConfigurationKeys.indexSpecPrefix());
        indexSpec = new IndexSpec(
                DruidDataWriter.getBitmapSerde(
                        "",//indexSpecConf.get(DruidConfigurationKeys.bitmapTypeDefaultKey()),
                        indexSpecConf.getBoolean(
                                DruidConfigurationKeys.compressRunOnSerializationKey(),
                                RoaringBitmapSerdeFactory.DEFAULT_COMPRESS_RUN_ON_SERIALIZATION)
                ),
                DruidDataWriter.getCompressionStrategy(
                        indexSpecConf.get(new Tuple2<String,String>(DruidConfigurationKeys.dimensionCompressionKey(),
                                        CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY.toString())
                                )
                ),
                DruidDataWriter.getCompressionStrategy(
                        indexSpecConf.get(
                                new  Tuple2<String,String>(
                                DruidConfigurationKeys.metricCompressionKey(),
                                CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY.toString()))
                ),
                DruidDataWriter.getLongEncodingStrategy(
                        indexSpecConf.get(
                                new  Tuple2<String,String>(
                                DruidConfigurationKeys.longEncodingKey(),
                                CompressionFactory.DEFAULT_LONG_ENCODING_STRATEGY.toString())
                        )
                )
        );

        List<String> complexColumnTypes = Arrays.stream(dataSchema.getAggregators()).filter(a->a.getType() == ValueType.COMPLEX).map(a->a.getComplexTypeName()).collect(Collectors.toList());

        DruidDataWriterConfig finalConfig = config;
        complexColumnTypes.forEach(o->{
            ComplexMetricRegistry.registerByName(o, finalConfig.writeCompactSketches());
        });
        ComplexMetricRegistry.registerSerdes();


        pusher = SegmentWriterRegistry.getSegmentPusher(config.deepStorageType(), config.properties());
    }



    public static void main(String[] args) throws IndexSizeExceededException {
        Long timestamp = 0L;
        dataSchema.getGranularitySpec().getSegmentGranularity().bucketStart(DateTimes.utc(timestamp)).getMillis();
        IncrementalIndex index = null;
        List<String> dimensions = dataSchema.getDimensionsSpec().getDimensions().stream().map(o->o.getName()).collect(Collectors.toList());
        String tsColumn = dataSchema.getTimestampSpec().getTimestampColumn();
        int tsColumnIndex = config.schema().fieldIndex(tsColumn);
        Function<Object, DateTime>  timestampParser = TimestampParser.createObjectTimestampParser(dataSchema.getTimestampSpec().getTimestampFormat());
        Map<String,Integer> partitionMap = config.partitionMap().map(o-> ImmutableMap.of(config.partitionId()+"",config.partitionId())).getOrElse(()->ImmutableMap.of("partitionId",config.partitionId()));
        Map<String, Object> event = null;
        index.add(index.formatRow(new MapBasedInputRow(timestamp,dimensions,event)));

    }

    private static IndexableAdapter flushIndex(IncrementalIndex index) throws IOException {
        return new QueryableIndexIndexableAdapter(
                closer.register(
                        INDEX_IO.loadIndex(
                                INDEX_MERGER_V9
                                        .persist(
                                                index,
                                                index.getInterval(),
                                                tmpPersistDir,
                                                indexSpec,
                                                OnHeapMemorySegmentWriteOutMediumFactory.instance()
                                        )
                        )
                )
        );
    }

    private static IncrementalIndex createInterval(Long startMillis) {
        // Using OnHeapIncrementalIndex to minimize changes when migrating from IncrementalIndex. In the future, this should
        // be optimized further. See https://github.com/apache/druid/issues/10321 for more information.
        return new OnheapIncrementalIndex.Builder()
                .setIndexSchema(
                        new IncrementalIndexSchema.Builder()
                                .withDimensionsSpec(dataSchema.getDimensionsSpec())
                                .withQueryGranularity(
                                        dataSchema.getGranularitySpec().getQueryGranularity()
                                )
                                .withMetrics(dataSchema.getAggregators())
          .withTimestampSpec(dataSchema.getTimestampSpec())
                .withMinTimestamp(startMillis)
                .withRollup(dataSchema.getGranularitySpec().isRollup())
                .build()
      )
      .setMaxRowCount(config.rowsPerPersist())
                .build();
    }



    static WriterCommitMessage commit() throws IOException {
        // Return segment locations on deep storage
        List<String> specs = bucketToIndexMap.values().stream().map(t->{
            List<IndexableAdapter> adapters = t._1;
            t._2.forEach(index-> {
                try {
                    adapters.add(flushIndex(index));
                    index.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            if(!adapters.isEmpty()){
                // TODO: Merge adapters up to a total number of rows, and then split into new segments.
                //  The tricky piece will be determining the partition number for multiple segments (interpolate 0?)
                IndexMergerV9 finalStaticIndexer = INDEX_MERGER_V9;
                File file = null;
                try {
                    file = finalStaticIndexer.merge(
                            adapters,
                            true,
                            dataSchema.getAggregators(),
                            tmpMergeDir,
                            indexSpec,
                            -1 // TODO: Make maxColumnsToMerge configurable
                    );
                } catch (IOException e) {
                    e.printStackTrace();
                }

                List<String> allDimensions = adapters.stream().map(a->a.getDimensionNames()).flatMap(ds->Stream.of(ds.toArray(new String[ds.size()]))).collect(Collectors.toList());
                Interval interval = adapters.stream().map(a->a.getDataInterval()).reduce((l, r)-> Intervals.utc(Math.min(l.getStartMillis(), r.getStartMillis()),Math.max(l.getEndMillis(), r.getEndMillis()))).get();
                scala.collection.immutable.Map<String, String> partitionMap = null;
                ShardSpec shardSpec = ShardSpecRegistry.createShardSpec(config.shardSpec(), partitionMap);
                DataSegment dataSegmentTemplate = new DataSegment(
                        config.dataSource(),
                        dataSchema.getGranularitySpec().getSegmentGranularity().bucket(DateTimes.utc(interval.getStartMillis())),
                        config.version(),
                        null, // scalastyle:ignore null
                        allDimensions,
                        Arrays.stream(dataSchema.getAggregators()).map(a->a.getName()).collect(Collectors.toList()),
                        shardSpec,
                        -1,
                        0L
                );
                DataSegment finalDataSegment = null;
                try {
                    finalDataSegment = pusher.push(file, dataSegmentTemplate, true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    return MAPPER.writeValueAsString(finalDataSegment);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
           return null;
        }).filter(o->o != null).collect(Collectors.toList());
        closer.close();

        ArrayBuffer arrayBuffer = new ArrayBuffer<>();
        for(String s:specs){
            arrayBuffer.$plus$eq(s);
        }
        return DruidWriterCommitMessage.apply(arrayBuffer);
    }
}

package com.julianjaffe.spark_druid_connector.v2.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.julianjaffe.spark_druid_connector.configuration.Configuration;
import com.julianjaffe.spark_druid_connector.configuration.SerializableHadoopConfiguration;
import com.julianjaffe.spark_druid_connector.mixins.Logging;
import com.julianjaffe.spark_druid_connector.utils.SchemaUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.realtime.firehose.IngestSegmentFirehose;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
public class DruidInputPartitionReader extends DruidBaseInputPartitionReader implements InputPartitionReader<InternalRow>, Logging {

    private final static ObjectMapper MAPPER = new DefaultObjectMapper();
    private IngestSegmentFirehose firehose;
    private StructType schema;
    private boolean useDefaultNullHandling;


    public DruidInputPartitionReader(String segmentStr, StructType schema, Option<String> filter, Option<scala.collection.immutable.Set<String>>columnTypes, Broadcast<SerializableHadoopConfiguration> broadcastedHadoopConf, Configuration conf,boolean useSparkConfForDeepStorage,  boolean useCompactSketches,  boolean useDefaultNullHandling) {
        super(segmentStr, columnTypes, broadcastedHadoopConf, conf, useSparkConfForDeepStorage, useCompactSketches, useDefaultNullHandling);
        DimFilter f = null;
        try {
            if(filter.nonEmpty()){
                String  fstr = filter.get();
                if(fstr.length() > 3){
                    f = MAPPER.readValue(fstr,DimFilter.class);
                }
            }
        }catch (Exception e){
        }
        firehose = DruidInputPartitionReader.makeFirehose(
                new WindowedStorageAdapter(
                        new QueryableIndexStorageAdapter(queryableIndex()), segment().getInterval()
                ),
                f,//filter.orNull(null),
                Arrays.asList(schema.fieldNames())
        );
        this.schema = schema;
        this.useDefaultNullHandling = useDefaultNullHandling;
    }



    @Override
    public boolean next() throws IOException {
        return  firehose.hasMore();
    }

    @Override
    public InternalRow get() {
        try {
            return SchemaUtils.convertInputRowToSparkRow(firehose.nextRow(), schema, useDefaultNullHandling);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (Option.apply(firehose).nonEmpty()) {
                firehose.close();
            }
            if (Option.apply(queryableIndex()).nonEmpty()) {
                queryableIndex().close();
            }
            if (Option.apply(tmpDir()).nonEmpty()) {
                FileUtils.deleteDirectory(tmpDir());
            }
        }catch (Exception e){
            // Since we're just going to rethrow e and tearing down the JVM will clean up the firehose and queryable index
            // even if we can't, the only leak we have to worry about is the temp file. Spark should clean up temp files as
            // well, but rather than rely on that we'll try to take care of it ourselves.
            log().warn("Encountered exception attempting to close a DruidInputPartitionReader!");
            if (Option.apply(tmpDir()).nonEmpty() && tmpDir().exists()) {
                FileUtils.deleteDirectory(tmpDir());
            }
            throw e;
        }
    }


    public static IngestSegmentFirehose makeFirehose(
            WindowedStorageAdapter adapter,
            DimFilter filter,
            List<String> columns){
        // This could be in-lined into the return, but this is more legible
        Indexed<String> availableDimensionsIndexed = adapter.getAdapter().getAvailableDimensions();

        Set<String> availableDimensions = new HashSet<String>();
        Iterator<String> it = availableDimensionsIndexed.iterator();
        while(it.hasNext()){
            availableDimensions.add(it.next());
        }

        it = adapter.getAdapter().getAvailableMetrics().iterator();

        Set<String> availableMetrics = new HashSet<>();
        while(it.hasNext()){
            availableMetrics.add(it.next());
        }

        List<String> dimensions = columns.stream().filter(c->availableDimensions.contains(c)).collect(Collectors.toList());

        List<String> metrics = columns.stream().filter(availableMetrics::contains).collect(Collectors.toList());

        List<WindowedStorageAdapter> adapters = new ArrayList<>();
        adapters.add(adapter);
        return new IngestSegmentFirehose(adapters, TransformSpec.NONE, dimensions, metrics, filter);
    }
}

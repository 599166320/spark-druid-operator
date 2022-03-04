package com.julianjaffe.spark_druid_connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.julianjaffe.spark_druid_connector.configuration.Configuration;
import com.julianjaffe.spark_druid_connector.utils.DeepStorageConstructorHelpers;
import com.julianjaffe.spark_druid_connector.v2.reader.DruidInputPartitionReader;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.*;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.firehose.IngestSegmentFirehose;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPuller;
import org.apache.druid.storage.hdfs.HdfsLoadSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class ReadHdfsApp {
    private final static ObjectMapper MAPPER = new DefaultObjectMapper();
    public static void main(String[] args) throws IOException, SegmentLoadingException {

        InjectableValues.Std  baseInjectableValues  =
                new InjectableValues.Std()
                        .addValue(ExprMacroTable.class, new ExprMacroTable(Arrays.asList(
                                new LikeExprMacro(),
                                new RegexpExtractExprMacro(),
                                new TimestampCeilExprMacro(),
                                new TimestampExtractExprMacro(),
                                new TimestampFormatExprMacro(),
                                new TimestampParseExprMacro(),
                                new TimestampShiftExprMacro(),
                                new TimestampFloorExprMacro(),
                                new TrimExprMacro.BothTrimExprMacro(),
                                new TrimExprMacro.LeftTrimExprMacro(),
                                new TrimExprMacro.RightTrimExprMacro())))
                        .addValue(ObjectMapper.class, MAPPER)
                        .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT);

        MAPPER.setInjectableValues(baseInjectableValues);

        org.apache.hadoop.conf.Configuration hadoopConf = new HdfsConfiguration();
        hadoopConf.set("fs.default.name", "hdfs://druid-hadoop-demo:9000");
        hadoopConf.set("fs.defaultFS", "hdfs://druid-hadoop-demo:9000");
        hadoopConf.set("dfs.datanode.address", "druid-hadoop-demo");
        hadoopConf.set("dfs.client.use.datanode.hostname", "true");

        //org.apache.hadoop.conf.Configuration hadoopConfiguration = DeepStorageConstructorHelpers.createHadoopConfiguration(hadoopConf);
        HdfsDataSegmentPuller puller = new HdfsDataSegmentPuller(hadoopConf);
        InjectableValues.Std injectableValues = baseInjectableValues.addValue(HdfsDataSegmentPuller.class, puller);
        MAPPER.setInjectableValues(injectableValues);
        MAPPER.registerSubtypes(new NamedType(HdfsLoadSpec.class, "hdfs"));
        String  json = "{\"type\":\"hdfs\",\"path\":\"hdfs://druid-hadoop-demo:9000/druid/segments/bb03/-1461365430908T082332.096Z_1461404820424T153627.903Z/1/1_5d1c720e-7704-4af0-9f15-252c83f5935d_index.zip\"}";
        LoadSpec loadSpec = MAPPER.readValue(json, LoadSpec.class);

        String segmentStr = "{\"dataSource\":\"bb03\",\"interval\":\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\",\"version\":\"1\",\"loadSpec\":{\"type\":\"hdfs\",\"path\":\"hdfs://druid-hadoop-demo:9000/druid/segments/bb03/-1461365430908T082332.096Z_1461404820424T153627.903Z/1/2_8aecbbb2-80c3-4af2-88b2-f6274c188003_index.zip\"},\"dimensions\":\"isRobot,channel,cityName,isUnpatrolled,page,countryName,regionIsoCode,added,metroCode,comment,isNew,isMinor,delta,countryIsoCode,isAnonymous,user,regionName,deleted,namespace\",\"metrics\":\"\",\"shardSpec\":{\"type\":\"numbered\",\"partitionNum\":2,\"partitions\":3},\"binaryVersion\":9,\"size\":3728195,\"identifier\":\"bb03_-146136543-09-08T08:23:32.096Z_146140482-04-24T15:36:27.903Z_1_2\"}";
        DataSegment segment = MAPPER.readValue(segmentStr, DataSegment.class);
        NullHandling.initializeForTests();
        File  tmpDir = FileUtils.createTempDir();
        File segmentDir = new File(tmpDir,segment.getId().toString());
        loadSpec.loadSegment(segmentDir);
        IndexIO INDEX_IO = new IndexIO(MAPPER,() -> 1000000);
        QueryableIndex queryableIndex = INDEX_IO.loadIndex(segmentDir);
        DimFilter f = null;
        StructType schema = null;
        String field = "page";

        IngestSegmentFirehose firehose = DruidInputPartitionReader.makeFirehose(
                new WindowedStorageAdapter(
                        new QueryableIndexStorageAdapter(queryableIndex), segment.getInterval()
                ),
                f,//filter.orNull(null),
                Arrays.asList(field)//Arrays.asList(schema.fieldNames())
        );
        while(firehose.hasMore()){
            InputRow inputRow = firehose.nextRow();
            Object col = inputRow.getRaw(field);
            System.out.println(UTF8String.fromString(col.toString()).toString());;
        }


        try {
            if (Option.apply(firehose).nonEmpty()) {
                firehose.close();
            }
            if (Option.apply(queryableIndex).nonEmpty()) {
                queryableIndex.close();
            }
            if (Option.apply(tmpDir).nonEmpty()) {
                FileUtils.deleteDirectory(tmpDir);
            }
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}

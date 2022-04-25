package com.julianjaffe.spark_druid_connector;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
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

public class ReadHdfsApp1 {
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
        //String  json = "{\"type\":\"hdfs\",\"path\":\"hdfs://druid-hadoop-demo:9000/druid/segments/bb03/-1461365430908T082332.096Z_1461404820424T153627.903Z/1/1_5d1c720e-7704-4af0-9f15-252c83f5935d_index.zip\"}";
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

    public void loadSegment(){
        //http://10.110.223.231:8888/druid/coordinator/v1/metadata/datasources/metrics_agg1m/segments?full
        //["2022-03-03T00:00:00.000/2022-03-03T01:00:00.000", "2022-03-03T02:00:00.000/2022-03-03T04:00:00.000"]
    }
    public static void main1(String[] args) throws IOException, SegmentLoadingException {

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
        hadoopConf.set("fs.default.name", "hdfs://10.110.223.231:9000");
        hadoopConf.set("fs.defaultFS", "hdfs://10.110.223.231:9000");
        hadoopConf.set("dfs.datanode.address", "10.110.223.231");
        hadoopConf.set("dfs.client.use.datanode.hostname", "true");

        //org.apache.hadoop.conf.Configuration hadoopConfiguration = DeepStorageConstructorHelpers.createHadoopConfiguration(hadoopConf);
        HdfsDataSegmentPuller puller = new HdfsDataSegmentPuller(hadoopConf);
        InjectableValues.Std injectableValues = baseInjectableValues.addValue(HdfsDataSegmentPuller.class, puller);
        MAPPER.setInjectableValues(injectableValues);
        MAPPER.registerSubtypes(new NamedType(HdfsLoadSpec.class, "hdfs"));
        //String  json = "{\"type\":\"hdfs\",\"path\":\"hdfs://druid-hadoop-demo:9000/druid/segments/bb03/-1461365430908T082332.096Z_1461404820424T153627.903Z/1/1_5d1c720e-7704-4af0-9f15-252c83f5935d_index.zip\"}";
        String  json = "{\"type\":\"hdfs\",\"path\":\"hdfs://10.110.223.231:9000/druid/segments/metrics_agg1m/20220303T030000.000Z_20220303T040000.000Z/2022-03-03T03_42_14.521Z/0_dfdc807c-b009-4b86-86d0-0099cf509080_index.zip\"}";
        LoadSpec loadSpec = MAPPER.readValue(json, LoadSpec.class);

        String segmentStr = "{\"dataSource\":\"metrics_agg1m\",\"interval\":\"2022-03-03T03:00:00.000Z/2022-03-03T04:00:00.000Z\",\"version\":\"2022-03-03T03:42:14.521Z\",\"loadSpec\":{\"type\":\"hdfs\",\"path\":\"hdfs://10.110.223.231:9000/druid/segments/metrics_agg1m/20220303T030000.000Z_20220303T040000.000Z/2022-03-03T03_42_14.521Z/0_dfdc807c-b009-4b86-86d0-0099cf509080_index.zip\"},\"dimensions\":\"le,quantile,env,keys,labels,app,idc,ip,name,version,port\",\"metrics\":\"ct,v,v_max,v_min,increase,increase_max,increase_min,rate,rate_max,rate_min\",\"shardSpec\":{\"type\":\"numbered\",\"partitionNum\":0,\"partitions\":0},\"binaryVersion\":9,\"size\":776263,\"identifier\":\"metrics_agg1m_2022-03-03T03:00:00.000Z_2022-03-03T04:00:00.000Z_2022-03-03T03:42:14.521Z\"}";
        DataSegment segment = MAPPER.readValue(segmentStr, DataSegment.class);
        NullHandling.initializeForTests();
        File  tmpDir = FileUtils.createTempDir();
        File segmentDir = new File(tmpDir,segment.getId().toString());
        loadSpec.loadSegment(segmentDir);
        IndexIO INDEX_IO = new IndexIO(MAPPER,() -> 1000000);
        QueryableIndex queryableIndex = INDEX_IO.loadIndex(segmentDir);
        DimFilter f = null;
        StructType schema = null;
        String field = "name";

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

package com.julianjaffe.spark_druid_connector;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DruidWriterApp {

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("druidApp");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        String ip = "192.168.3.52";
        //String ip = "192.168.231.233";
        Map<String,String> properties = new HashMap<>();
        properties.put("metadata.dbType" , "mysql");
        properties.put( "metadata.connectUri" , "jdbc:mysql://"+ip+":3306/druid?allowPublicKeyRetrieval=true");
        properties.put("metadata.user" , "druid");
        //properties.put("metadata.password" , "8u7666gjlHyya765");
        properties.put("metadata.password" , "{\"type\": \"mapString\",\"config\":{\"password\": \"8u7666gjlHyya765\"}}");


        properties.put("broker.host" , ip);
        properties.put("broker.port" ,"8082");
        //properties.put( "table" , "metrics_agg1m");
        properties.put( "table" , "bb");
        //properties.put("reader.deepStorageType" , "hdfs");

        HdfsConfiguration hadoopConf = new HdfsConfiguration();
        hadoopConf.set("fs.default.name", "hdfs://druid-hadoop-demo:9000");
        hadoopConf.set("fs.defaultFS", "hdfs://druid-hadoop-demo:9000");
        hadoopConf.set("dfs.datanode.address", "druid-hadoop-demo");
        hadoopConf.set("dfs.client.use.datanode.hostname", "true");

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(bout);
        hadoopConf.write(dataOutputStream);
        properties.put("deepStorageType" , "hdfs");
        properties.put("hdfs.hadoopConf" , StringUtils.encodeBase64String(bout.toByteArray()));
        bout.close();
        properties.put("hdfs.storageDirectory" , "hdfs://druid-hadoop-demo:9000/druid/segments/");

        //properties.put("reader.deepStorageType" , "local");
        //properties.put("local.storageDirectory" , "/Users/gz00018ml/soft/apache-druid-0.22.0-SNAPSHOT/var/druid/segments/");

        Dataset dataset = sparkSession.read().format("druid").options(properties).load();


        dataset.show();
        System.out.println(dataset.count());

        Map<String,String> writerConfigs = new HashMap<>();
        writerConfigs.put("table","bb05");
        writerConfigs.put("writer.timestampColumn","__time");
        writerConfigs.put("writer.version","1");
        writerConfigs.put("writer.deepStorageType","hdfs");
        writerConfigs.put("writer.storageDirectory","hdfs://druid-hadoop-demo:9000/druid/segments/");
        properties.remove("table");
        writerConfigs.putAll(properties);



        dataset
                .write()
                .format("druid")
                .mode(SaveMode.Overwrite)
                .options(writerConfigs)
        .save();

        sparkSession.stop();
    }
}

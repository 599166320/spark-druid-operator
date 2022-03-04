package com.julianjaffe.spark_druid_connector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.julianjaffe.spark_druid_connector.registries.DynamicConfigProviderRegistry;
import com.julianjaffe.spark_druid_connector.registries.SegmentReaderRegistry;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.runtime.BoxedUnit;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DruidApp {



    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("druidApp");
                //.set("spark.kryo.referenceTracking", "false")
                //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.set("spark.kryo.registrator", " org.apache.druid.query.filter.SelectorDimFilter")
                //.registerKryoClasses(new Class[]{org.apache.druid.query.filter.SelectorDimFilter.class});

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
        properties.put( "table" , "bb03");
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

        Dataset<Row> dataset = sparkSession.read().format("druid").options(properties).load();
        dataset.show();
        dataset.printSchema();
        //dataset.show();


        //dataset.select("page","added").filter("added=36").show();

        dataset.createOrReplaceTempView("use_data");

        dataset = sparkSession.sql("SELECT * FROM use_data where added=18");
        dataset.show();
        System.out.println("added=18 行数:"+dataset.count());
        dataset.show();
        System.out.println("added=18 行数:"+dataset.count());

        //sparkSession.sql("SELECT * FROM use_data where added=18").write().format("json").save("file:///Users/gz00018ml/IdeaProjects/spark-druid-connector-1-main/log1.txt");

        //dataset.write().format("json").save("file:///Users/gz00018ml/IdeaProjects/spark-druid-connector-1-main/log.txt");
        //dataset.show();
        sparkSession.stop();
    }
}

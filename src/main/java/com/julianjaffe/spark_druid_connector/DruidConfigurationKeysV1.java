package com.julianjaffe.spark_druid_connector;

import scala.Tuple2;

public class DruidConfigurationKeysV1 {

    static String shardSpecTypeKey = "shardSpecType";
    static String rowsPerPersistKey = "rowsPerPersist";
    static String deepStorageTypeKey = "deepStorageType";
    static String useCompactSketchesKey = "useCompactSketches";
    static String useDefaultValueForNull = "useDefaultValueForNull";
    static String versionKey = "version";
    static String bitmapTypeKey = "bitmap";

    public static  Tuple2<String,String> shardSpecTypeDefaultKey(){
        return new Tuple2<String,String>(shardSpecTypeKey, "numbered");
    }

    public static Tuple2<String,Object> rowsPerPersistDefaultKey(){
        return new Tuple2<String,Object>(rowsPerPersistKey, 2000000);
    }

    public static Tuple2<String,String> deepStorageTypeDefaultKey(){
        return new Tuple2<>(deepStorageTypeKey, "local");
    }

    public static Tuple2<String,Object> useCompactSketchesDefaultKey(){
        return new Tuple2<String,Object>(useCompactSketchesKey, false);
    }

    public static Tuple2<String,Object> useDefaultValueForNullDefaultKey(){
        return new Tuple2<String,Object>(useDefaultValueForNull, true);
    }

    public static Tuple2<String,String> versonDefaultKey(){
        return new Tuple2<String,String>(versionKey, System.currentTimeMillis()+"");
    }

    public static Tuple2 bitmapTypeDefaultKey(){
        return new Tuple2(bitmapTypeKey, "roaring");
    }

}

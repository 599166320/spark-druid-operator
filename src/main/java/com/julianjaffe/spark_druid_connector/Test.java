package com.julianjaffe.spark_druid_connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.filter.DimFilter;
import scala.Option;

public class Test {
    private final static ObjectMapper MAPPER = new DefaultObjectMapper();
    public static void main(String[] args) throws JsonProcessingException {
        Option<DimFilter> filter = Option.empty();
        String serializedFilter =  MAPPER.writeValueAsString(filter);
        System.out.println(serializedFilter);
    }
}

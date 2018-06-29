package com.cisco.gungnir.udf;

import org.apache.spark.sql.SparkSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class Udfs {
    private SparkSession spark;

    public Udfs(SparkSession spark) throws Exception{
        this.spark = spark;
    }

    public void registerFunctions() {
        spark.udf().register("getTimestampField", new RawTimestampField(), DataTypes.StringType);
        spark.udf().register("preprocess", new Preprocess(), DataTypes.StringType);
    }

    private static class RawTimestampField implements UDF1<String, String> {
        private transient ObjectMapper objectMapper;

        public String call(String value) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            try {
                ObjectNode objectNode = (ObjectNode) objectMapper.readTree(value);
                if(objectNode.has("timeRcvd")){
                    return objectNode.get("timeRcvd").asText();
                }
                if(objectNode.has("@timestamp")){
                    return objectNode.get("@timestamp").asText();
                }
                return "";
            } catch (Exception e) {
                return "";
            }
        }
    }

    private static class Preprocess implements UDF1<String, String> {
        private transient ObjectMapper objectMapper;

        public String call(String value) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            try {
                ObjectNode objectNode = (ObjectNode) objectMapper.readTree(value);
                if (objectNode.get("appname") != null) {
                    return objectNode.toString();
                } else {
                    String message = objectNode.get("@message").asText();
                    String[] jsonMessages = message.split(":", 2);
                    if (jsonMessages.length == 2) {
                        JsonNode metric = objectMapper.readTree(jsonMessages[1].trim());
                        objectNode.set("SM", metric);
                        objectNode.remove("@message");
                        return objectNode.toString();
                    } else {
                        return value;
                    }
                }
            } catch (Exception e) {
                return value;
            }
        }
    }

}

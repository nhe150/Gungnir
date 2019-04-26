package com.cisco.gungnir.config;

import util.DatasetFunctions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.StringJoiner;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.input_file_name;

public class ConfigProvider implements Serializable {
    private SparkSession spark;
    private JsonNode appConfig;

    public ConfigProvider(SparkSession spark, String appConfigPath) throws Exception {
        this.spark = spark;
        this.appConfig = LoadConfig(appConfigPath);
    }

    public ConfigProvider(SparkSession spark, JsonNode appConfig) {
        this.spark = spark;
        this.appConfig = appConfig;
    }

    public JsonNode getAppConfig(){
        return appConfig;
    }

    public JsonNode LoadConfig(String configPath) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Dataset config = spark.read().option("multiline", true).json(configPath);
        if(DatasetFunctions.hasColumn(config, "_corrupt_record")){
            throw new IllegalArgumentException("the provided config in "+ configPath + " is not a valid json");
        }
        return objectMapper.readTree(config.toJSON().first().toString());
    }

    public JsonNode readJobConfig(String configName) throws Exception {
        return LoadConfig(retrieveConfigValue(appConfig, "jobLocation") + configName + ".json");
    }

    public StructType readSchema(String schemaName) throws Exception {
        Dataset schema = spark.read().option("multiline", true).json(retrieveConfigValue(appConfig, "schemaLocation") + schemaName + ".json");
        if(DatasetFunctions.hasColumn(schema, "_corrupt_record")){
            throw new IllegalArgumentException("the provided schema "+ schemaName + " is not a valid json");
        }
        schema.printSchema();
        return schema.schema();
    }

    public String readSql(String sqlName) throws Exception {
        Dataset queries = spark.read().textFile(retrieveConfigValue(appConfig, "queryLocation"))
                .withColumn("filename", input_file_name()).selectExpr("get_only_file_name(filename) as name", "value")
                .groupBy("name").agg(collect_list("value").as("value")).selectExpr("name", "concat_ws(' ', value) as query");
        Dataset query = queries.select( "query").where("name='"+ sqlName + "'");
        if(query.rdd().isEmpty()) throw new IllegalArgumentException("Can find query with query name "+ sqlName);
        return query.as(Encoders.STRING()).collectAsList().get(0).toString();
    }

    public static JsonNode merge(JsonNode mainNode, JsonNode updateNode) {
        if(updateNode == null && mainNode==null) return null;
        if(mainNode == null && updateNode!=null) return updateNode.deepCopy();
        if(updateNode == null && mainNode!=null) return mainNode.deepCopy();
        Iterator<String> fieldNames = updateNode.fieldNames();
        while (fieldNames.hasNext()) {

            String fieldName = fieldNames.next();
            JsonNode jsonNode = mainNode.get(fieldName);
            // if field exists and is an embedded object
            if (jsonNode != null && jsonNode.isObject()) {
                merge(jsonNode, updateNode.get(fieldName));
            }
            else {
                if (mainNode instanceof ObjectNode) {
                    // Overwrite field
                    JsonNode value = updateNode.get(fieldName);
                    ((ObjectNode) mainNode).put(fieldName, value);
                }
            }

        }
        return mainNode;
    }

    public static boolean hasConfigValue(JsonNode node, String field) {
        JsonNode jsonNode = node;
        for(String s: field.split("\\.")){
            if(jsonNode == null || jsonNode.get(s) == null)
                return false;
            jsonNode = jsonNode.get(s);
        }
        return true;
    }

    public static String retrieveConfigValue(JsonNode node, String field) throws Exception {
        JsonNode jsonNode = node;
        StringJoiner joiner = new StringJoiner(".");
        for(String s: field.split("\\.")){
            joiner.add(s);
            if(jsonNode == null || jsonNode.get(s) == null)
                throw new IllegalArgumentException("Can not find configuration for field: " + joiner.toString());
            jsonNode = jsonNode.get(s);
        }
        return jsonNode.asText();
    }

    public String retrieveAppConfigValue(String field) throws Exception {
        return retrieveConfigValue(appConfig, field);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        out.writeUTF(mapper.writeValueAsString(appConfig));
    }

    private void readObject(ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        ObjectMapper mapper = new ObjectMapper();

        JsonNode node = mapper.readTree(in.readUTF());
        if (!node.isObject()) {
            throw new IOException("malformed name field detected");
        }

        appConfig = node;
    }

}

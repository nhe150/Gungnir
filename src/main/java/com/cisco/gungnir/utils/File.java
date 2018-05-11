package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;

import static com.cisco.gungnir.utils.CommonFunctions.aggregateDates;
import static com.cisco.gungnir.utils.CommonFunctions.getPeriodStartDate;
import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class File implements Serializable {
    private ConfigProvider configProvider;
    private SparkSession spark;

    public File(SparkSession spark, ConfigProvider configProvider) throws Exception {
        this.spark = spark;
        this.configProvider = configProvider;
    }

    private JsonNode getFileConfig(JsonNode providedConfig) throws Exception {
        JsonNode merged = ConfigProvider.merge(configProvider.getAppConfig().deepCopy(), providedConfig);

        return merged;
    }

    public Dataset readFromFile(String processType, JsonNode providedConfig) throws Exception {
        JsonNode fileConfig = getFileConfig(providedConfig);

        switch (processType) {
            case "batch":
                if(ConfigProvider.hasConfigValue(fileConfig, "date")){
                    return readDataByDate(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "input"),
                            configProvider.readSchema(ConfigProvider.retrieveConfigValue(fileConfig, "schemaName")),
                            ConfigProvider.retrieveConfigValue(fileConfig, "date"),
                            ConfigProvider.hasConfigValue(fileConfig, "period") ? ConfigProvider.retrieveConfigValue(fileConfig, "period"): null,
                            ConfigProvider.retrieveConfigValue(fileConfig, "partitionKey"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "format"));
                }
                if(!ConfigProvider.hasConfigValue(fileConfig, "schemaName")){
                    return readFileBatch(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "input"), ConfigProvider.retrieveConfigValue(fileConfig, "format"), "*");
                }
                return readFileBatchWithSchema(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                        ConfigProvider.retrieveConfigValue(fileConfig, "input"),
                        configProvider.readSchema(ConfigProvider.retrieveConfigValue(fileConfig, "schemaName")), ConfigProvider.retrieveConfigValue(fileConfig, "format"), "*");
            case "stream":
                if(!ConfigProvider.hasConfigValue(fileConfig, "schemaName")){
                    return readFileStream(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "input"), ConfigProvider.retrieveConfigValue(fileConfig, "format"));
                }
                return readFileStreamWithSchema(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                        ConfigProvider.retrieveConfigValue(fileConfig, "input"),
                        configProvider.readSchema(ConfigProvider.retrieveConfigValue(fileConfig, "schemaName")), ConfigProvider.retrieveConfigValue(fileConfig, "format"));
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for readFromFile");
        }
    }

    public void writeToFile(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        JsonNode fileConfig = getFileConfig(providedConfig);
        String outputPath = ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation") + ConfigProvider.retrieveConfigValue(fileConfig, "output");

        switch (processType) {
            case "batch":
                if(ConfigProvider.hasConfigValue(fileConfig, "partitionKey")){
                    batchToFileByKey(dataset, outputPath,
                            ConfigProvider.retrieveConfigValue(fileConfig, "format"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "saveMode"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "partitionKey"));
                    return;
                }
                batchToFileByKey(dataset, outputPath, ConfigProvider.retrieveConfigValue(fileConfig, "format"), ConfigProvider.retrieveConfigValue(fileConfig, "saveMode"), null);
                break;
            case "stream":
                if(ConfigProvider.hasConfigValue(fileConfig, "partitionKey")){
                    streamToFileByKey(dataset, outputPath,
                            ConfigProvider.retrieveConfigValue(fileConfig, "format"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "saveMode"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "partitionKey"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "output"));
                    return;
                }
                streamToFileByKey(dataset, outputPath,
                        ConfigProvider.retrieveConfigValue(fileConfig, "format"),
                        ConfigProvider.retrieveConfigValue(fileConfig, "saveMode"),
                        null,
                        ConfigProvider.retrieveConfigValue(fileConfig, "output"));
                break;
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for writeToFile");
        }
    }

    public StreamingQuery streamToFileByKey(Dataset<Row> dataset, String outputPath, String format, String saveMode, String partitionKey, String queryName) throws Exception{
        if(partitionKey != null){
            return dataset
                    .writeStream()
                    .queryName("sinkToFile_" + queryName)
                    .partitionBy(partitionKey)
                    .outputMode(saveMode)
                    .trigger(ProcessingTime(configProvider.retrieveAppConfigValue("spark.streamngTriggerWindow")))
                    .format(format)
                    .option("path", outputPath).start();
        } else{
            return dataset
                    .writeStream()
                    .queryName("sinkToFile_" + queryName)
                    .outputMode(saveMode)
                    .trigger(ProcessingTime(configProvider.retrieveAppConfigValue("spark.streamngTriggerWindow")))
                    .format(format)
                    .option("path", outputPath).start();
        }
    }

    public Dataset readFileStream(String dataLocation, String input, String format){
        input = input.replaceAll("\\s","");
        String[] inputs = input.split(",");
        Dataset dataset= spark
                .readStream()
                .format(format)
                .load(dataLocation + inputs[0]);
        for(int i=1; i<inputs.length; i++){
            dataset = dataset.union(spark
                    .readStream()
                    .format(format)
                    .load(dataLocation + inputs[i]));
        }
        return dataset.selectExpr("CAST(value AS STRING)");
    }

    public Dataset readFileStreamWithSchema(String dataLocation, String input, StructType schema, String format){
        input = input.replaceAll("\\s","");
        String[] inputs = input.split(",");
        Dataset dataset= spark
                .readStream()
                .format(format)
                .option("multiline", true).option("mode", "PERMISSIVE")
                .schema(schema)
                .load(dataLocation + inputs[0]);
        for(int i=1; i<inputs.length; i++){
            dataset = dataset.union(spark
                    .readStream()
                    .format(format)
                    .option("multiline", true).option("mode", "PERMISSIVE")
                    .schema(schema)
                    .load(dataLocation + inputs[i]));
        }
        return dataset;
    }

    public void batchToFileByKey(Dataset<Row> dataset, String outputPath, String format, String saveMode, String partitionKey) throws Exception{
        if(partitionKey != null){
            dataset.write()
                    .mode(getSaveMode(saveMode))
                    .partitionBy(partitionKey)
                    .format(format)
                    .save(outputPath + "/" + partitionKey);
        } else {
            dataset.write()
                    .mode(getSaveMode(saveMode))
                    .format(format)
                    .save(outputPath);
        }

    }

    public Dataset readFileBatch(String dataLocation, String input, String format, String regex){
        input = input.replaceAll("\\s","");
        String[] inputs = input.split(",");
        Dataset dataset = spark.read().format(format).load(dataLocation + inputs[0] + "/" + regex );
        for(int i=1; i<inputs.length; i++){
            dataset = dataset.union(spark
                    .read()
                    .format(format)
                    .load(dataLocation + inputs[i] + "/" + regex ));
        }
        return dataset.selectExpr("CAST(value AS STRING)");
    }

    public Dataset readFileBatchWithSchema(String dataLocation, String input, StructType schema, String format, String regex){
        input = input.replaceAll("\\s","");
        String[] inputs = input.split(",");
        Dataset dataset = spark.createDataFrame(new ArrayList<>(), schema);
        for(int i=0; i<inputs.length; i++){
            dataset = dataset.union(spark
                    .read()
                    .format(format)
                    .option("multiline", true)
                    .schema(schema)
                    .load(dataLocation + inputs[i] + "/" + regex ));
        }
        return dataset;
    }

    public Dataset<Row> readDataByDate(String dataLocation, String input, StructType schema, String date, String period, String partitionKey, String format) throws Exception {
        Dataset<Row> dataset = spark.createDataFrame(new ArrayList<>(), schema);

        for(String d: aggregateDates(getPeriodStartDate(date, period), period)){
            dataset = dataset.union(readFileBatchWithSchema(dataLocation,input, schema, format, partitionKey + "=" + d));
        }
        return dataset;
    }

    public static SaveMode getSaveMode(String mode){
        switch (mode.toLowerCase()) {
            case "append":
                return SaveMode.Append;
            case "update":
                return SaveMode.Append;
            case "complete":
                return SaveMode.Overwrite;
            case "overwrite":
                return SaveMode.Overwrite;
            case "errorifexists":
                return SaveMode.ErrorIfExists;
            case "ignore":
                return SaveMode.Ignore;
            default:
                throw new IllegalArgumentException("Invalid saveMode: " + mode);
        }
    }

}
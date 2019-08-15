package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import util.DatasetFunctions;

import java.io.Serializable;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
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
                    return readDataByDateBatch(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "input"),
                            ConfigProvider.hasConfigValue(fileConfig, "schemaName") ? configProvider.readSchema(ConfigProvider.retrieveConfigValue(fileConfig, "schemaName")): null,
                            DateUtil.getDate(ConfigProvider.retrieveConfigValue(fileConfig, "date")),
                            ConfigProvider.hasConfigValue(fileConfig, "period") ? ConfigProvider.retrieveConfigValue(fileConfig, "period"): null,
                            ConfigProvider.hasConfigValue(fileConfig, "partitionKey") ? ConfigProvider.retrieveConfigValue(fileConfig, "partitionKey"): null,
                            ConfigProvider.retrieveConfigValue(fileConfig, "format"),
                            ConfigProvider.hasConfigValue(fileConfig, "multiline") && fileConfig.get("multiline").asBoolean());
                }
                Dataset datasetBatch = readFileBatch(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                        ConfigProvider.retrieveConfigValue(fileConfig, "input"),
                        ConfigProvider.retrieveConfigValue(fileConfig, "format"),
                        ConfigProvider.hasConfigValue(fileConfig, "multiline") && fileConfig.get("multiline").asBoolean(), "*");

                if(ConfigProvider.hasConfigValue(fileConfig, "schemaName")){
                    return withSchema(datasetBatch, configProvider.readSchema(ConfigProvider.retrieveConfigValue(fileConfig, "schemaName")));
                }
                return datasetBatch;
            case "stream":
                if(ConfigProvider.hasConfigValue(fileConfig, "date")){
                    return readDataByDateStream(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "input"),
                            ConfigProvider.hasConfigValue(fileConfig, "schemaName") ? configProvider.readSchema(ConfigProvider.retrieveConfigValue(fileConfig, "schemaName")): null,
                            DateUtil.getDate(ConfigProvider.retrieveConfigValue(fileConfig, "date")),
                            ConfigProvider.hasConfigValue(fileConfig, "period") ? ConfigProvider.retrieveConfigValue(fileConfig, "period"): null,
                            ConfigProvider.hasConfigValue(fileConfig, "partitionKey") ? ConfigProvider.retrieveConfigValue(fileConfig, "partitionKey"): null,
                            ConfigProvider.retrieveConfigValue(fileConfig, "format"),
                            ConfigProvider.hasConfigValue(fileConfig, "multiline") && fileConfig.get("multiline").asBoolean());
                }

                Dataset datasetStream = readFileStream(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                        ConfigProvider.retrieveConfigValue(fileConfig, "input"),
                        ConfigProvider.retrieveConfigValue(fileConfig, "format"),
                        ConfigProvider.hasConfigValue(fileConfig, "multiline") && fileConfig.get("multiline").asBoolean(), "*");

                if(ConfigProvider.hasConfigValue(fileConfig, "schemaName")){
                    return withSchema(datasetStream, configProvider.readSchema(ConfigProvider.retrieveConfigValue(fileConfig, "schemaName")));
                }
                return datasetStream;
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for readFromFile");
        }
    }

    public void writeToFile(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        if(dataset==null) throw new IllegalArgumentException("can't write to file: the input dataset is NULL, please check previous query");

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

    public StreamingQuery streamToFileByKey(Dataset dataset, String outputPath, String format, String saveMode, String partitionKey, String queryName) throws Exception{
        if(partitionKey != null){

            return
                    dataset
                   // .withWatermark("pdate", "25 hours")
                    .writeStream()
                    .queryName("sinkToFile_" + queryName)
                    .partitionBy(partitionKey)
                    .outputMode(saveMode)
                    .trigger(ProcessingTime(configProvider.retrieveAppConfigValue("spark.streamngTriggerWindow")))
                    .format(format)
                    .option("header", "true")
                    .option("path", outputPath).start();
        } else{
            return dataset
                    .writeStream()
                    .queryName("sinkToFile_" + queryName)
                    .outputMode(saveMode)
                    .trigger(ProcessingTime(configProvider.retrieveAppConfigValue("spark.streamngTriggerWindow")))
                    .format(format)
                    .option("header", "true")
                    .option("path", outputPath).start();
        }
    }

    public void batchToFileByKey(Dataset dataset, String outputPath, String format, String saveMode, String partitionKey) throws Exception{
        if(partitionKey != null){
            dataset.write()
                    .mode(getSaveMode(saveMode))
                    .partitionBy(partitionKey)
                    .format(format)
                    .option("header", "true")
                    .save(outputPath);
        } else {
            dataset.write()
                    .mode(getSaveMode(saveMode))
                    .format(format)
                    .option("header", "true")
                    .save(outputPath);
        }

    }

    public Dataset readFileStream(String dataLocation, String input, String format, boolean multiline, String regex){
        input = input.replaceAll("\\s","");
        String[] inputs = input.split(",");
        Dataset ds = readFileBatch(dataLocation, inputs[0], format, multiline, regex);
        String loadPath = dataLocation + inputs[0] + "/" + regex;
        if(inputs[0].split("\\.").length>1){
            loadPath = dataLocation + inputs[0];
        }
        Dataset dataset = spark.readStream().format(format).schema(ds.schema()).option("multiline", multiline).option("header", "true").load(loadPath);
        for(int i=1; i<inputs.length; i++){
            loadPath = dataLocation + inputs[i] + "/" + regex;
            if(inputs[i].split("\\.").length>1){
                loadPath = dataLocation + inputs[i];
            }
            dataset = dataset.union(spark
                    .readStream()
                    .format(format)
                    .schema(ds.schema())
                    .option("multiline", multiline)
                    .option("header", "true")
                    .load(loadPath));
        }
        return dataset;
    }

    public Dataset readFileBatch(String dataLocation, String input, String format, boolean multiline, String regex){
        input = input.replaceAll("\\s","");
        String[] inputs = input.split(",");
        String loadPath = dataLocation + inputs[0] + "/" + regex;
        if(inputs[0].split("\\.").length>1){
            loadPath = dataLocation + inputs[0];
        }
        Dataset dataset = spark.read().format(format).option("multiline", multiline).option("header", "true").load(loadPath );
        for(int i=1; i<inputs.length; i++){
            loadPath = dataLocation + inputs[i] + "/" + regex;
            if(inputs[i].split("\\.").length>1){
                loadPath = dataLocation + inputs[i];
            }
            dataset = dataset.union(spark
                    .read()
                    .format(format)
                    .option("multiline", multiline)
                    .option("header", "true")
                    .load(loadPath));
        }
        return dataset;
    }

    public Dataset withSchema(Dataset dataset, StructType schema){
        if(!DatasetFunctions.hasColumn(dataset, "value")){
            if(DatasetFunctions.hasColumn(dataset, "raw")) {
                schema = schema.add("raw", DataTypes.StringType);
            }
            dataset = dataset.selectExpr("to_json(struct(*)) as value")
                    .select(from_json(col("value"), schema).as("data")).select("data.*");
        }else {
            dataset = dataset.select(from_json(col("value"), schema).as("data"), col("value").as("raw")).select("data.*", "raw");
        }
        return dataset;
    }


    public Dataset readDataByDateStream(String dataLocation, String input, StructType schema, String date, String period, String partitionKey, String format, boolean multiline) throws Exception {
        List<String> dateList = Aggregation.aggregateDates(Aggregation.getPeriodStartDate(date, period), period);

        String regex = partitionKey!=null? partitionKey + "=" + dateList.get(0): dateList.get(0);
        Dataset dataset = schema!=null? withSchema(readFileStream(dataLocation,input, format, multiline, regex), schema): readFileStream(dataLocation,input, format, multiline, regex);

        for(int i=1; i< dateList.size(); i++){
            regex = partitionKey!=null? partitionKey + "=" + dateList.get(i): dateList.get(i);
            dataset = dataset.union(schema!=null? withSchema(readFileStream(dataLocation,input, format, multiline, regex), schema): readFileStream(dataLocation,input, format, multiline, regex));
        }
        return dataset;
    }

    public Dataset readDataByDateBatch(String dataLocation, String input, StructType schema, String date, String period, String partitionKey, String format, boolean multiline) throws Exception {
        List<String> dateList = Aggregation.aggregateDates(Aggregation.getPeriodStartDate(date, period), period);

        String regex = partitionKey!=null? partitionKey + "=" + dateList.get(0): dateList.get(0);
        Dataset dataset = schema!=null? withSchema(readFileBatch(dataLocation,input, format, multiline, regex), schema): readFileBatch(dataLocation,input, format, multiline, regex);

        for(int i=1; i< dateList.size(); i++){
            regex = partitionKey!=null? partitionKey + "=" + dateList.get(i): dateList.get(i);
            dataset = dataset.union(schema!=null? withSchema(readFileBatch(dataLocation,input, format, multiline, regex), schema): readFileBatch(dataLocation,input, format, multiline, regex));
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
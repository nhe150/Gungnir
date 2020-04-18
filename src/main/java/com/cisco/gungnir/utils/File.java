package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import util.DatasetFunctions;

import java.io.Serializable;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class File implements Serializable {
    private ConfigProvider configProvider;
    private SparkSession spark;
    private transient  FileSystem fs;
    private boolean localTest;

    public File(SparkSession spark, ConfigProvider configProvider) throws Exception {
        this.spark = spark;
        this.configProvider = configProvider;
        fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        localTest = ConfigProvider.hasConfigValue(configProvider.getAppConfig(), "local");
    }

    private JsonNode getFileConfig(JsonNode providedConfig) throws Exception {
        JsonNode merged = ConfigProvider.merge(configProvider.getAppConfig().deepCopy(), providedConfig);
        return merged;
    }

    public Dataset readFromFile(String processType, JsonNode providedConfig) throws Exception {
        JsonNode fileConfig = getFileConfig(providedConfig);

        switch (processType) {
            case "batch":
                if (ConfigProvider.hasConfigValue(fileConfig, "date")) {
                    String format = ConfigProvider.retrieveConfigValue(fileConfig, "format");
                    return readDataByDateBatch(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                            ConfigProvider.retrieveConfigValue(fileConfig, "input"),
                            ConfigProvider.hasConfigValue(fileConfig, "schemaName") ? ("csv".equals(format) ?
                                    configProvider.readSchema(ConfigProvider.retrieveConfigValue(fileConfig, "schemaName"), true)
                                    : configProvider.readSchema(ConfigProvider.retrieveConfigValue(fileConfig, "schemaName"))) : null,
                            DateUtil.getDate(ConfigProvider.retrieveConfigValue(fileConfig, "date")),
                            ConfigProvider.hasConfigValue(fileConfig, "period") ? ConfigProvider.retrieveConfigValue(fileConfig, "period") : null,
                            ConfigProvider.hasConfigValue(fileConfig, "partitionKey") ? ConfigProvider.retrieveConfigValue(fileConfig, "partitionKey") : null,
                            ConfigProvider.hasConfigValue(fileConfig, "enableBasePath") ? fileConfig.get("enableBasePath").asBoolean() : false,
                            ConfigProvider.hasConfigValue(fileConfig, "alias") ? ConfigProvider.retrieveConfigValue(fileConfig, "alias") : null,
                            ConfigProvider.hasConfigValue(fileConfig, "tabDelimited") ? fileConfig.get("tabDelimited").asBoolean() : false,
                            format,
                            ConfigProvider.hasConfigValue(fileConfig, "multiline") && fileConfig.get("multiline").asBoolean(),
                            ConfigProvider.hasConfigValue(fileConfig, "aliasColumn") ? ConfigProvider.retrieveConfigValue(fileConfig, "aliasColumn") : null);
                }
                Dataset datasetBatch = readFileBatch(ConfigProvider.retrieveConfigValue(fileConfig, "dataLocation"),
                        ConfigProvider.retrieveConfigValue(fileConfig, "input"),
                        ConfigProvider.retrieveConfigValue(fileConfig, "format"),
                        ConfigProvider.hasConfigValue(fileConfig, "multiline") && fileConfig.get("multiline").asBoolean(),
                        "*", false, false,
                        ConfigProvider.hasConfigValue(fileConfig, "schemaName") ? configProvider.readSchema(ConfigProvider.retrieveConfigValue(fileConfig, "schemaName")): null);

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
                            // don't support alias and tabDelimited for streaming
                            false, null, false,
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
        Dataset ds = readFileBatch(dataLocation, inputs[0], format, multiline, regex, false, false, null);
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

    /**
     * no header=true support yet, before it is wrong to have header = true since our wap daa having no header; spark default header false
     */
    public Dataset read(String format, boolean multiline, String path, String basePath, boolean enableBasePath, boolean tabDelimited, StructType schema) {
        boolean exists = false;
        try {
            exists = fs.exists(new Path(path));
        } catch (Exception e) {
            System.out.println("cannot find file:" + path);
        }

        DataFrameReader reader = null;
        if (exists || localTest) {
            reader = spark.read().format(format).option("multiline", multiline);
            if (enableBasePath) {
                reader.option("basePath", basePath);
            }
            if (tabDelimited) {
                reader.option("delimiter", "\t");
            }
            if (schema != null && "csv".equals(format)) {
                System.out.println("schema is:" +schema.prettyJson());
                reader.schema(schema);
            }

            return reader.load(path);
        }

        return null;
    }

    public Dataset readFileBatch(String dataLocation, String input, String format, boolean multiline,
                                 String regex, boolean enableBasePath, boolean tabDelimited, StructType schema){
        input = input.replaceAll("\\s","");
        String[] inputs = input.split(",");
        String loadPath = dataLocation + inputs[0] + "/" + regex;
        String basePath = dataLocation + inputs[0] + "/";
        if(inputs[0].split("\\.").length>1){
            loadPath = dataLocation + inputs[0];
            System.out.println(loadPath);
        }


        Dataset dataset = read(format, multiline, loadPath, basePath, enableBasePath, tabDelimited, schema );
        for(int i=1; i<inputs.length; i++){
            loadPath = dataLocation + inputs[i] + "/" + regex;
            basePath = dataLocation + inputs[i] + "/";
            if(inputs[i].split("\\.").length>1){
                loadPath = dataLocation + inputs[i];
                System.out.println(" readFile" + i + loadPath);
            }

            Dataset ds= read(format, multiline, loadPath, basePath, enableBasePath, tabDelimited, schema);
            if( ds != null ) {
                dataset = dataset.union(ds);

            }else{
                System.out.println("error empty dataset");
            }

        }
        return dataset;
    }

    public Dataset withSchema(Dataset dataset, StructType schema){
        if( dataset == null ){
            return null;
        }

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


    public Dataset readDataByDateStream(String dataLocation, String input, StructType schema, String date, String period, String partitionKey,
                                        boolean enableBasePath, String alias, boolean tabDelimited,
                                        String format, boolean multiline) throws Exception {
        List<String> dateList = Aggregation.aggregateDates(Aggregation.getPeriodStartDate(date, period), period);

        String regex = getPartitionString(alias, partitionKey, dateList.get(0));
        Dataset dataset = filterBySchema(schema, format) ? withSchema(readFileStream(dataLocation,input, format, multiline, regex), schema): readFileStream(dataLocation,input, format, multiline, regex);

        for(int i=1; i< dateList.size(); i++){
            regex = getPartitionString(alias, partitionKey,  dateList.get(i));
            Dataset ds = filterBySchema(schema, format)? withSchema(readFileStream(dataLocation,input, format, multiline, regex), schema): readFileStream(dataLocation,input, format, multiline, regex);
            if( ds != null ) {
                dataset = dataset.union(ds);
            }
        }
        return dataset;
    }

    private boolean filterBySchema(StructType schema, String format) {
        return schema != null && !"csv".equals(format);
    }

    public Dataset readDataByDateBatch(String dataLocation, String input, StructType schema, String date, String period, String partitionKey,
                                       boolean enableBasePath, String alias, boolean tabDelimited,
                                       String format, boolean multiline, String aliasColumn) throws Exception {
        List<String> dateList = Aggregation.aggregateDates(Aggregation.getPeriodStartDate(date, period), period);
        String regex = getPartitionString(alias, partitionKey, dateList.get(0));

        Dataset dataset = filterBySchema(schema, format) ? withSchema(readFileBatch(dataLocation,input, format, multiline, regex, enableBasePath, tabDelimited, schema), schema):
                readFileBatch(dataLocation,input, format, multiline, regex, enableBasePath, tabDelimited, schema);

        for(int i=1; i< dateList.size(); i++){
            regex = getPartitionString(alias, partitionKey ,  dateList.get(i));
            Dataset ds = filterBySchema(schema, format) ? withSchema(readFileBatch(dataLocation,input, format, multiline, regex, enableBasePath, tabDelimited, schema), schema):
                    readFileBatch(dataLocation,input, format, multiline, regex, enableBasePath, tabDelimited, schema);
            if( ds != null ) {
                dataset = dataset.union(ds);
            }
        }

        if( !enableBasePath && aliasColumn != null ) {
            dataset = dataset.withColumn(aliasColumn,  lit(date));
        }

        return dataset;
    }

    private static String getPartitionString(String alias, String partitionKey, String partitionValue) {
        String result = partitionKey != null ? partitionKey + "=" + partitionValue : partitionValue;
        //alias overwrite regex for non conforming partitionkeys format like webex meeting /pda/chartLibrary data
        if (alias != null) {
            System.out.println("alias=" + alias);
            result = String.format(alias, partitionValue);
        }
        System.out.println("partitionString=" + result);
        return result;
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

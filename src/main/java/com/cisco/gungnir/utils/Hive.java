package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructField;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

/**
 * Hive provide ability to create hive table and insert into hive table using writeToHive function.
 * Need to config hive.schema and hive.columns so that hive table schema is mainteined due to insertInto is position based
 */
public class Hive implements Serializable {
    private ConfigProvider configProvider;
    private SparkSession spark;
    private JsonNode hiveConfig;

    public Hive(SparkSession spark, ConfigProvider configProvider) throws Exception {
        this.spark = spark;
        this.configProvider = configProvider;
    }

    private JsonNode getHiveConfig(JsonNode providedConfig) throws Exception {
        JsonNode merged = ConfigProvider.merge(configProvider.getAppConfig().deepCopy(), providedConfig);
        return merged;
    }


    public void writeToHive(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        if (dataset == null)
            throw new IllegalArgumentException("can't write to Hive: the input dataset is NULL, please check previous query");

        hiveConfig = getHiveConfig(providedConfig);

        switch (processType) {
            case "batch":
                writeToHiveBatch(dataset, ConfigProvider.retrieveConfigValue(hiveConfig, "hive.table"));
                break;
            case "stream":
                throw new IllegalArgumentException("streaming processs writeToHive is not supported yet");
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for writeToHive");
        }
    }

    public StreamingQuery streamToHiveByKey(Dataset dataset, String outputPath, String format, String saveMode, String partitionKey, String queryName) throws Exception {
        return

                dataset
                        .writeStream()
                        .queryName("sinkToHive_" + queryName)
                        .partitionBy(partitionKey)
                        .outputMode(saveMode)
                        .trigger(ProcessingTime(configProvider.retrieveAppConfigValue("spark.streamngTriggerWindow")))
                        .format(format)
                        .option("header", "true")
                        .option("path", outputPath).start();

    }

    public void writeToHiveBatch(Dataset dataset, String table) throws Exception {
        createHiveTable(dataset, table);

        String columnNames = ConfigProvider.retrieveConfigValue(hiveConfig, "hive.columns");
        String[] cols = configProvider.readSql(columnNames).split(",");
        for (int index = 0; index < cols.length; index++) {
            cols[index] = cols[index].trim();
        }

        List<String> columns = Arrays.asList(cols);
        dataset.selectExpr(Util.convertListToSeq(columns))
                .write()
                .format("orc")
                .option("orc.compress", "zlib")
                .mode(SaveMode.Overwrite)
                .insertInto(table);

    }

    public void createHiveTable(Dataset dataset, String table) throws Exception {
        StringBuilder fBuilder = new StringBuilder();
        for (StructField f : dataset.schema().fields()) {
            fBuilder.append(f.name() + " " + f.dataType().typeName().replace("long", "bigint").replace("integer", "int").replace("map", "map<string,string>") + ",");
        }
        String schema = fBuilder.toString();
        String hive_create_sql = "create table if not exists " + table + "(" + schema.substring(0, schema.length() - 1)
                + ") STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB')";
        System.out.println("hive new possible schema:" + hive_create_sql);

        //always using existing schema; possible schema for reference only --- 12/16/2019 Norman He cisco
        String schemaName = ConfigProvider.retrieveConfigValue(hiveConfig, "hive.schema");
        hive_create_sql = configProvider.readSql(schemaName);
        if (!hive_create_sql.contains(table)) {
            throw new IllegalArgumentException(table + " is not inside create_hive_sql:" + hive_create_sql);
        }
        System.out.println("existing hive schema:" + hive_create_sql);
        spark.sql(hive_create_sql);
    }

}
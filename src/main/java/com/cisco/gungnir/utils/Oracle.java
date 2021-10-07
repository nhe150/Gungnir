package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.streaming.Trigger.ProcessingTime;

public class Oracle implements Serializable {
    private ConfigProvider configProvider;
    private SparkSession spark;
    private Map<String, String> oracleConfig;

    public Oracle(SparkSession spark, ConfigProvider configProvider) throws Exception {
        this.spark = spark;
        this.configProvider = configProvider;
    }

    private JsonNode getOracleConfig(JsonNode providedConfig) throws Exception {
        JsonNode merged = ConfigProvider.merge(configProvider.getAppConfig().deepCopy(), providedConfig);

        Map<String, String> oracleConfigMap = new HashMap<>();
        oracleConfigMap.put("url", ConfigProvider.retrieveConfigValue(merged, "oracle.url"));
        oracleConfigMap.put("user", ConfigProvider.retrieveConfigValue(merged, "oracle.username"));
        oracleConfigMap.put("password", ConfigProvider.retrieveConfigValue(merged, "oracle.password"));
        oracleConfigMap.put("dbtable", ConfigProvider.retrieveConfigValue(merged, "oracle.table"));
        if( ConfigProvider.hasConfigValue(merged, "oracle.driver") ) {
            oracleConfigMap.put("driver", ConfigProvider.retrieveConfigValue(merged, "oracle.driver"));
        }else {
            oracleConfigMap.put("driver", "oracle.jdbc.driver.OracleDriver");
        }

        oracleConfigMap.put("oracle.jdbc.timezoneAsRegion", "false");

        this.oracleConfig = oracleConfigMap;
        return merged;
    }


    public void writeToOracle(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        if (dataset == null)
            throw new IllegalArgumentException("can't write to Jdbc database: the input dataSet is NULL, please check" +
                    " previous query");
        dataset = dataset.drop("raw");
        JsonNode mergedConfig = getOracleConfig(providedConfig);

        switch (processType) {
            case "batch":
                if(ConfigProvider.hasConfigValue(mergedConfig,"oracle.pk")){
                    batchUpSertToOracle(dataset,
                            ConfigProvider.retrieveConfigValue(mergedConfig, "oracle.driver"),
                            ConfigProvider.retrieveConfigValue(mergedConfig, "oracle.saveMode"), ConfigProvider.retrieveConfigValue(mergedConfig, "oracle.pk"));
                }else{
                    batchToOracle(dataset, ConfigProvider.retrieveConfigValue(mergedConfig, "oracle.saveMode"),
                            ConfigProvider.retrieveConfigValue(mergedConfig, "oracle.driver"));
                }
                break;
            case "stream":

                if(!ConfigProvider.hasConfigValue(mergedConfig,"oracle.pk")){
                    throw new IllegalArgumentException("Need primary key for stream writeToOracle");
                }
                streamUpSertToOracle(dataset,ConfigProvider.retrieveConfigValue(mergedConfig, "output"), ConfigProvider.retrieveConfigValue(mergedConfig, "oracle.saveMode"), ConfigProvider.retrieveConfigValue(mergedConfig, "oracle.pk"));

                break;
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for writeToOracle");
        }
    }


    public void batchToOracle(Dataset dataset, String saveMode, String driver) {
        columnNameToLowerCase(dataset)
                .write()
                .mode(File.getSaveMode(saveMode))
                .format("jdbc")
                .option("driver", driver)
                .options(oracleConfig)
                .save();
    }

    public void batchUpSertToOracle(Dataset dataset, String driver, String saveMode, String pk) {
        columnNameToLowerCase(dataset).coalesce(1).foreachPartition( new OracleUpsertWriter(oracleConfig, driver,
                dataset.schema(),pk) );

    }

    public StreamingQuery streamUpSertToOracle(Dataset<Row> dataset, String queryName, String saveMode, String pk) throws Exception {
        return dataset
                .coalesce(100)
                .writeStream()
                .option("oracle.jdbc.timezoneAsRegion", "false")
                .outputMode(saveMode)
                .foreach((ForeachWriter) new OracleStreamWriter(oracleConfig,oracleConfig.get("dbtable"), dataset.schema(), pk))
                .trigger(ProcessingTime(configProvider.retrieveAppConfigValue("spark.streamngTriggerWindow")))
                .queryName("sinkToJDBC_" + queryName)
                .start();
    }

    private Dataset<Row> columnNameToLowerCase(Dataset dataset) {
        for (String col : dataset.columns()) {
            dataset = dataset.withColumnRenamed(col, col.toLowerCase());
        }
        return dataset;
    }
}
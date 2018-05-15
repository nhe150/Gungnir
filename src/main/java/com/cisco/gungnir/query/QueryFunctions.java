package com.cisco.gungnir.query;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.utils.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;


public class QueryFunctions implements Serializable {
    private SparkSession spark;
    private ConfigProvider configProvider;
    public final Kafka kafka;
    public final Cassandra cassandra;
    public final File file;

    public QueryFunctions(SparkSession spark, ConfigProvider configProvider) throws Exception{
        this.spark = spark;
        this.configProvider = configProvider;
        this.kafka = new Kafka(spark, configProvider);
        this.cassandra = new Cassandra(spark, configProvider);
        this.file = new File(spark, configProvider);
        setSparkConfig();
        SqlFunctions sqlFunctions = new SqlFunctions(spark, configProvider);
        sqlFunctions.registerFunctions();
    }

    public Dataset splitData(Dataset dataset, JsonNode providedConfig) throws Exception{
        spark.udf().register("getTimestampField", new SqlFunctions.RawTimestampField(), DataTypes.StringType);
        spark.udf().register("preprocess", new SqlFunctions.Preprocess(), DataTypes.StringType);
        spark.udf().register("appFilter", new SqlFunctions.AppFilter(ConfigProvider.retrieveConfigValue(providedConfig, "appName")), DataTypes.BooleanType);

        return dataset.selectExpr("appFilter(value) as included", "value").where("included=true").selectExpr("convertTimeString(getTimestampField(value)) as pdate", "preprocess(value) as value");
    }

    private void setSparkConfig() throws Exception{
        spark.sqlContext().setConf("spark.sql.streaming.checkpointLocation", configProvider.retrieveAppConfigValue("spark.streamingCheckpointLocation"));
        spark.sqlContext().setConf("spark.streaming.stopGracefullyOnShutdown", configProvider.retrieveAppConfigValue("spark.streamingStopGracefullyOnShutdown"));
        spark.sqlContext().setConf("spark.streaming.backpressure.enabled", configProvider.retrieveAppConfigValue("spark.streamingBackpressureEnabled"));

        spark.sparkContext().setLogLevel(configProvider.retrieveAppConfigValue("spark.logLevel"));
    }

}

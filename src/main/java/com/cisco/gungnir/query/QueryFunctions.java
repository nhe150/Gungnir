package com.cisco.gungnir.query;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.utils.Cassandra;
import com.cisco.gungnir.utils.Kafka;
import com.cisco.gungnir.utils.File;
import com.cisco.gungnir.utils.SqlFunctions;
import org.apache.spark.sql.*;

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
        SqlFunctions.registerFunctions(spark);
    }

    private void setSparkConfig() throws Exception{
        spark.sqlContext().setConf("spark.sql.streaming.checkpointLocation", configProvider.retrieveAppConfigValue("spark.streamingCheckpointLocation"));
        spark.sqlContext().setConf("spark.streaming.stopGracefullyOnShutdown", configProvider.retrieveAppConfigValue("spark.streamingStopGracefullyOnShutdown"));
        spark.sqlContext().setConf("spark.streaming.backpressure.enabled", configProvider.retrieveAppConfigValue("spark.streamingBackpressureEnabled"));

        spark.sparkContext().setLogLevel(configProvider.retrieveAppConfigValue("spark.logLevel"));
    }

}

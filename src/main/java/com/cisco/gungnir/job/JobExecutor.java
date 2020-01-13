package com.cisco.gungnir.job;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.query.QueryExecutor;
import com.webex.dap.spark.listener.DapSparkStreamingListenerFactory;
import org.apache.spark.sql.SparkSession;


import java.io.Serializable;

public class JobExecutor implements Serializable {
    private ConfigProvider configProvider;
    private QueryExecutor queryExecutor;
    private SparkSession spark;

    public JobExecutor(SparkSession spark, ConfigProvider configProvider) throws Exception{
        this.configProvider = configProvider;
        this.queryExecutor = new QueryExecutor(spark, configProvider);
        this.spark = spark;
        setSparkConfig();
    }

    public void execute(String jobName, String jobType) throws Exception {
        if("stream".equals(jobType)){
            spark.streams().addListener(DapSparkStreamingListenerFactory.buildDapStreamingQueryListener(configProvider.retrieveAppConfigValue("kafka.broker")));
        }

        queryExecutor.execute(configProvider.readJobConfig(jobName).get("queryPlan"), jobType);
    }

    private void setSparkConfig() throws Exception{
        spark.sqlContext().setConf("spark.sql.streaming.checkpointLocation", configProvider.retrieveAppConfigValue("spark.streamingCheckpointLocation"));
        spark.sqlContext().setConf("spark.streaming.stopGracefullyOnShutdown", configProvider.retrieveAppConfigValue("spark.streamingStopGracefullyOnShutdown"));
        spark.sqlContext().setConf("spark.streaming.backpressure.enabled", configProvider.retrieveAppConfigValue("spark.streamingBackpressureEnabled"));
        spark.sqlContext().setConf("spark.network.timeout", configProvider.retrieveAppConfigValue("spark.networkTimeout"));
        spark.sqlContext().setConf("spark.sql.session.timeZone", "GMT");

        //enable orc Hive support using native in spark 2.3
        spark.sqlContext().setConf("spark.sql.orc.impl","native");
        spark.sqlContext().setConf("spark.sql.orc.enableVectorizedReader","true");
        spark.sqlContext().setConf("spark.sql.orc.filterPushdown", "true");
        spark.sqlContext().setConf("spark.sql.orc.enabled", "true");
        spark.sqlContext().setConf("spark.sql.hive.convertMetastoreOrc", "true");
        spark.sqlContext().setConf("spark.sql.orc.char.enabled", "true");

        //make hive partition dynamic
        spark.sqlContext().setConf("hive.exec.dynamic.partition","true");
        spark.sqlContext().setConf("hive.exec.dynamic.partition.mode", "nonstrict");

        //set orc compression to zlib
        spark.sqlContext().setConf("spark.sql.orc.compression.codec", "zlib");
        spark.sqlContext().setConf("spark.sql.parquet.compression.codec","none");

        //enable hive bucketing
        spark.sqlContext().setConf("hive.enforce.bucketing", "false");
        spark.sqlContext().setConf("hive.enforce.sorting", "false");



        spark.sparkContext().setLogLevel(configProvider.retrieveAppConfigValue("spark.logLevel"));
    }
}

package com.cisco.gungnir.job;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.query.QueryExecutor;
import com.cisco.gungnir.utils.StreamingMetrics;
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
    }

    public void execute(String jobName, String jobType) throws Exception {
        if("stream".equals(jobType)){
            spark.streams().addListener(new StreamingMetrics(configProvider.retrieveAppConfigValue("kafka.broker"), configProvider.retrieveAppConfigValue("kafka.streamingMetricsTopic")));
        }
        queryExecutor.execute(configProvider.readJobConfig(jobName).get("queryPlan"), jobType);
    }
}

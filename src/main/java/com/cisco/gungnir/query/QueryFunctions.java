package com.cisco.gungnir.query;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.utils.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.commons.lang.StringUtils;

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

    public Dataset executeSqlQueries(Dataset ds, String queryName, JsonNode parameters) throws Exception {
        if(ds==null) throw new IllegalArgumentException("can't execute sql query " + queryName + ": the input dataset is NULL, please check previous query");

        String[] queryList = configProvider.readSql(queryName).split(";");
        String view = "SOURCE_VIEW";
        for(int i=0; i<queryList.length; i++){
            setWatermark(ds, parameters).createOrReplaceTempView(view);
            String query = queryList[i].trim();
            System.out.println("executing spark sql query: " + query);
            view = StringUtils.substringBetween(query, "TEMP_VIEW", "AS");
            if(query.contains("TEMP_VIEW")){
                if (query.contains("DropDuplicates")){
                    ds = dropDuplicates(ds, query);
                } else {
                    query = query.replaceAll("select", "SELECT");
                    int index = Math.max(query.indexOf("SELECT"), 0);
                    ds = spark.sql(query.substring(index));
                }
            }else {
                ds = spark.sql(query);
            }
        }

        return ds;
    }

    private Dataset setWatermark(Dataset ds, JsonNode parameters){
        if(parameters != null && parameters.has("aggregatePeriod")){
            SqlFunctions.AggregationUtil aggregationUtil = new SqlFunctions.AggregationUtil(parameters.get("aggregatePeriod").asText());
            aggregationUtil.registerAggregationFunctions(spark);
            String timestampField = parameters.has("timeStampField") ? parameters.get("timeStampField").asText(): "time_stamp";
            ds = ds.withWatermark(timestampField, aggregationUtil.getWatermarkDelayThreshold());
        }
        return ds;
    }

    private Dataset dropDuplicates(Dataset ds, String query){
        String f = query.split("DropDuplicates")[1].replaceAll("\\s+","");
        String[] fields = f.split(",");
        return ds.dropDuplicates(fields);
    }

    public Dataset splitData(Dataset dataset, JsonNode providedConfig) throws Exception{
        if(dataset==null) throw new IllegalArgumentException("can't execute splitData query: the input dataset is NULL, please check previous query");

        spark.udf().register("getTimestampField", new SqlFunctions.RawTimestampField(), DataTypes.StringType);
        spark.udf().register("preprocess", new SqlFunctions.Preprocess(), DataTypes.StringType);
        spark.udf().register("appFilter", new SqlFunctions.AppFilter(ConfigProvider.retrieveConfigValue(providedConfig, "appName")), DataTypes.BooleanType);

        return dataset.selectExpr("appFilter(value) as included", "value").where("included=true").selectExpr("convertTimeString(getTimestampField(value)) as pdate", "preprocess(value) as value");
    }

    private void setSparkConfig() throws Exception{
        spark.sqlContext().setConf("spark.sql.streaming.checkpointLocation", configProvider.retrieveAppConfigValue("spark.streamingCheckpointLocation"));
        spark.sqlContext().setConf("spark.streaming.stopGracefullyOnShutdown", configProvider.retrieveAppConfigValue("spark.streamingStopGracefullyOnShutdown"));
        spark.sqlContext().setConf("spark.streaming.backpressure.enabled", configProvider.retrieveAppConfigValue("spark.streamingBackpressureEnabled"));
        spark.sqlContext().setConf("spark.sql.session.timeZone", "GMT");
        spark.sparkContext().setLogLevel(configProvider.retrieveAppConfigValue("spark.logLevel"));

    }

}

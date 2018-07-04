package com.cisco.gungnir.query;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.udf.UdfFunctions;
import com.cisco.gungnir.utils.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;

import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.col;


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
        UdfFunctions udfFunctions = new UdfFunctions(spark, configProvider);
        udfFunctions.registerFunctions();
    }

    public Dataset executeSqlQueries(Dataset ds, String queryName, JsonNode parameters) throws Exception {
        if(ds==null) throw new IllegalArgumentException("can't execute sql query " + queryName + ": the input dataset is NULL, please check previous query");

        registerFunctions(queryName, parameters);

        String[] queryList = configProvider.readSql(queryName).split(";");
        String view = "SOURCE_VIEW";
        for(int i=0; i<queryList.length; i++){
            setWatermark(ds, parameters).createOrReplaceTempView(view);
            String query = queryList[i].trim();
            System.out.println("executing spark sql query: " + query);
            if(query.contains("TEMP_VIEW")){
                String tempView = StringUtils.substringBetween(query, "TEMP_VIEW", "AS");
                view = tempView==null ? view: tempView;
                if (query.contains("DropDuplicates")){
                    ds = dropDuplicates(ds, query);
                } else {
                    query = query.replaceAll("select", "SELECT");
                    int index = Math.max(query.indexOf("SELECT"), 0);
                    ds = spark.sql(query.substring(index));
                }
            }else if(query.contains("SELECT")) {
                ds = spark.sql(query);
            }else{
                spark.sql(query);
            }
        }

        return applySchema(ds, parameters);
    }

    private Dataset applySchema(Dataset ds, JsonNode parameters) throws Exception {
        if(parameters!= null && parameters.has("schemaName") && ds != null && ds.columns().length!=0) {
            return ds.select(from_json(col("value"), configProvider.readSchema(parameters.get("schemaName").asText())).as("data"), col("value").as("raw")).select("data.*", "raw");
        }
        return ds;
    }

    private Dataset setWatermark(Dataset ds, JsonNode parameters){
        if(parameters != null && parameters.has("aggregatePeriod")){
            Aggregation aggregationUtil = new Aggregation(parameters.get("aggregatePeriod").asText());
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

    private void registerFunctions(String queryName, JsonNode parameters) throws Exception{
        if("splitData".equals(queryName)){
            spark.udf().register("appFilter", new UdfFunctions.AppFilter("appname\":" + '"' + ConfigProvider.retrieveConfigValue(parameters, "appName") + '"'), DataTypes.BooleanType);
        }
    }

}

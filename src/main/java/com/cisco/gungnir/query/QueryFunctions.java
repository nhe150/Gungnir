package com.cisco.gungnir.query;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.udf.UdfFunctions;
import com.cisco.gungnir.utils.*;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.net.URL;
import java.net.URLConnection;

import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.col;


public class QueryFunctions implements Serializable {
    private SparkSession spark;
    private ConfigProvider configProvider;
    public final Kafka kafka;
    public final Cassandra cassandra;
    public final File file;
    public final Hive hive;
    public final Oracle oracle;
    public final HttpRequest httpRequest;

    public QueryFunctions(SparkSession spark, ConfigProvider configProvider) throws Exception{
        this.spark = spark;
        this.configProvider = configProvider;
        this.kafka = new Kafka(spark, configProvider);
        this.cassandra = new Cassandra(spark, configProvider);
        this.file = new File(spark, configProvider);
        this.hive = new Hive(spark, configProvider);
        this.oracle = new Oracle(spark, configProvider);
        this.httpRequest = new HttpRequest(spark, configProvider);
        UdfFunctions udfFunctions = new UdfFunctions(spark, configProvider);
        udfFunctions.registerFunctions();
    }

    public Dataset executeSqlQueries(Dataset ds, String queryName, JsonNode parameters) throws Exception {

        registerFunctions(queryName, parameters);

        String[] queryList = configProvider.readSql(queryName).split(";");
        String view = "SOURCE_VIEW";
        if(ds!=null) {
            System.out.println("setWaterMark for view  of " + queryName);
            ds = setWatermark(ds, parameters);
            ds.createOrReplaceTempView(view);
        }

        for(int i=0; i<queryList.length; i++){
            String query = queryList[i].trim();


            if (query.contains("DropDuplicates")){
                System.out.println("dropducliates query:" + query);
                if(ds==null) throw new IllegalArgumentException("can't execute sql query " + queryName + ": the input dataset is NULL, please check previous query");
                ds = dropDuplicates(ds, query, parameters);
            }
            else if(query.startsWith("SELECT") || query.startsWith("select")) {
                System.out.println("select query:" + query);

                ds = spark.sql(query);
               // ds = setWatermark(ds, parameters);

            }else{
                System.out.println("execute a query " + query);
                ds = spark.sql(query);


               // ds = setWatermark(ds, parameters);

            }

        }

        ds = applySchema(ds, parameters);
        return ds;
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
            System.out.println( "setwatermark " + timestampField + ": " + aggregationUtil.getWatermarkDelayThreshold());
            ds = ds.withWatermark(timestampField, aggregationUtil.getWatermarkDelayThreshold());

        }
        return ds;
    }

    private Dataset dropDuplicates(Dataset ds, String query, JsonNode params){
        String f = query.split("DropDuplicates")[1].replaceAll("\\s+","");
        String[] fields = f.split(",");
        Dataset deduplicatedDs = ds.dropDuplicates(fields);
        String q = query.replaceAll("as", "AS").replaceAll("view", "VIEW");
        String view = StringUtils.substringBetween(q, "VIEW", "AS");
        deduplicatedDs.createOrReplaceTempView(view);
       // deduplicatedDs = setWatermark(deduplicatedDs, params);
        return deduplicatedDs;
    }

    private void registerFunctions(String queryName, JsonNode parameters) throws Exception{
        if("splitData".equals(queryName)){
            if(ConfigProvider.hasConfigValue(parameters, "featureName")) {
                spark.udf().register("appFilter", new UdfFunctions.AppFilter("featureName\":" + '"' + ConfigProvider.retrieveConfigValue(parameters, "featureName") + '"'), DataTypes.BooleanType);
            } else {
                spark.udf().register("appFilter", new UdfFunctions.AppFilter("appname\":" + '"' + ConfigProvider.retrieveConfigValue(parameters, "appName") + '"'), DataTypes.BooleanType);
            }
        }  
        
    }

}

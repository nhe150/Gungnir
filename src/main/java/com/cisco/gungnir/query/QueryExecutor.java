package com.cisco.gungnir.query;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.utils.Aggregation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import util.DatasetFunctions;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class QueryExecutor implements Serializable {
    private SparkSession spark;
    private ConfigProvider configProvider;
    private QueryFunctions queryFunctions;

    public QueryExecutor(SparkSession spark, ConfigProvider configProvider) throws Exception{
        this.spark = spark;
        this.configProvider = configProvider;
        this.queryFunctions = new QueryFunctions(spark, configProvider);
    }

    public QueryResult executeQuery(QueryResult previousResult, JsonNode query, String queryType) throws Exception{
        String queryName = ConfigProvider.retrieveConfigValue(query, "queryName");
        Dataset previous = previousResult.getQueryResult();
        Dataset result = null;
        updateParameters(query, previousResult.getQuery());
        JsonNode parameters = query.get("parameters");
        System.out.println("executing query: " + query);

        switch (queryName) {
            case "readFromKafka":
                result = queryFunctions.kafka.readFromKafka(queryType, parameters);
                break;
            case "writeToKafka":
                queryFunctions.kafka.writeToKafka(previous, queryType, parameters);
                break;
            case "readFromCassandra":
                result = queryFunctions.cassandra.readFromCassandra(queryType, parameters);
                break;
            case "writeToCassandra":
                queryFunctions.cassandra.writeToCassandra(previous, queryType, parameters);
                break;
            case "deleteFromCassandra":
                queryFunctions.cassandra.deleteFromCassandra(previous, queryType, parameters);
                break;
            case "writeToHive":
                queryFunctions.hive.writeToHive(previous, queryType, parameters);
                break;
            case "readFromFile":
                result = queryFunctions.file.readFromFile(queryType, parameters);
                break;
            case "writeToFile":
                queryFunctions.file.writeToFile(previous, queryType, parameters);
                break;
            case "writeToOracle":
                queryFunctions.oracle.writeToOracle(previous, queryType, parameters);
                break;
            case "readHttpRequest":
                result = queryFunctions.httpRequest.readFromHttpRequest(previous, queryType, parameters);
                break;
            default:
                result = queryFunctions.executeSqlQueries(previous, queryName, parameters);
        }

        if(parameters!= null && parameters.has("timeStampField") && result != null && result.columns().length!=0) {
            //if( !parameters.has("aggregatePeriod")) {
                System.out.println("no aggregatePeroid, set timestamp field");
                result = setTimestampField(result, parameters.get("timeStampField").asText());
            //}
        }

        if(result!=null){
            if("batch".equals(queryType)) result.show(false);
//            if("stream".equals(queryType)) result.writeStream().format("console").start();
        }
        return new QueryResult(query, result);
    }

    private void updateParameters(JsonNode currentQuery, JsonNode priviousQuery) throws Exception{
        if(!currentQuery.has("parameters")) ((ObjectNode) currentQuery).put("parameters", new ObjectMapper().createObjectNode());
        if(priviousQuery!=null && getQueryOutputLocation(priviousQuery.get("parameters"))!=null){
            ((ObjectNode) currentQuery.get("parameters")).put("output", getQueryOutputLocation(priviousQuery.get("parameters")));
        }
    }

    private String getQueryOutputLocation(JsonNode parameters) throws Exception{
        String output = "";
        if(ConfigProvider.hasConfigValue(parameters, "output")){
            output = output + ConfigProvider.retrieveConfigValue(parameters, "output");
            if(ConfigProvider.hasConfigValue(parameters, "aggregatePeriod")){
                output = output + "_" + ConfigProvider.retrieveConfigValue(parameters, "aggregatePeriod");
            }
        } else {
            return null;
        }
        return output;
    }

    public class QueryResult{
        private JsonNode query;
        private Dataset result;
        public QueryResult(JsonNode query, Dataset result){
            this.query = query;
            this.result = result;
        }
        public JsonNode getQuery(){
            return query;
        }
        public Dataset getQueryResult(){
            return result;
        }
    }

    public void execute(JsonNode queryPlan, String queryType) throws Exception{
        Queue<QueryResult> processQueue = new LinkedList<>();
        for(JsonNode queries: queryPlan){
            if(processQueue.isEmpty()){
                for(JsonNode query: queries){
                    if(query.get("disable")!=null && query.get("disable").asBoolean()) continue;
                    if(isQueryExpandable(query)){
                        for(JsonNode q: expandQuery(query)){
                            QueryResult next = executeQuery(new QueryResult(null, null), q, queryType);
                            processQueue.add(next);
                        }
                    } else {
                        QueryResult next = executeQuery(new QueryResult(null, null), query, queryType);
                        processQueue.add(next);
                    }
                }
            } else {
                int numberOfDatasetToProcess = processQueue.size();
                for(int i=0; i< numberOfDatasetToProcess; i++){
                    QueryResult current = processQueue.remove();
                    for(JsonNode query: queries){
                        if(query.get("disable")!=null && query.get("disable").asBoolean()) continue;
                        if(isQueryExpandable(query)){
                            for(JsonNode q: expandQuery(query)){
                                QueryResult next = executeQuery(current, q, queryType);
                                if(next.getQueryResult() != null) {
                                    processQueue.add(next);
                                }
                            }
                        } else {
                            QueryResult next = executeQuery(current, query, queryType);
                            if(next.getQueryResult() != null) {
                                processQueue.add(next);
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean isQueryExpandable(JsonNode query) throws Exception {
        if("readFromFile".equals(ConfigProvider.retrieveConfigValue(query, "queryName")) && ConfigProvider.hasConfigValue(query, "dateRange")) {
            return true;
        }
        return false;
    }

    private List<JsonNode> expandQuery(JsonNode query) throws Exception {
        List<JsonNode> expanded = new ArrayList<>();
        String startDate = ConfigProvider.retrieveConfigValue(query, "dateRange.startDate");
        String endDate = ConfigProvider.retrieveConfigValue(query, "dateRange.endDate");
        String period =  ConfigProvider.retrieveConfigValue(query, "parameters.period");

        for(String periodStartDate: Aggregation.getPeriodStartDateList(startDate, endDate, period)){
            JsonNode parameter = query.get("parameters");
            JsonNode newParameter = parameter.deepCopy();
            ((ObjectNode) newParameter).put("date", periodStartDate);

            JsonNode newQuery = query.deepCopy();
            ((ObjectNode) newQuery).remove("parameters");
            ((ObjectNode) newQuery).replace("parameters", newParameter);

            expanded.add(newQuery);
        }

        return expanded;
    }

    //prepare for watermark
    private Dataset setTimestampField(Dataset ds, String timestampField) {
        if(!DatasetFunctions.hasColumn(ds, timestampField)) throw new IllegalArgumentException("Could not find timestamp field name: " + timestampField);
        StructField field = (StructField) ds.select(timestampField).schema().head();
        if(field.dataType().sameType(DataTypes.StringType)){
            System.out.println("setTimeStampField calling toTimestamp UDF");
            return ds.withColumn(timestampField, callUDF("toTimestamp", col(timestampField)));
        }
        return ds;
    }
}
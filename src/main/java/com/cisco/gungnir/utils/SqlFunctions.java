package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;
import util.UDFUtil;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

import static com.cisco.gungnir.utils.CommonFunctions.TimeConverter;
import static com.cisco.gungnir.utils.CommonFunctions.getFirstDayOfWeek;

public class SqlFunctions implements Serializable {
    private SparkSession spark;
    private ConfigProvider configProvider;

    public SqlFunctions(SparkSession spark, ConfigProvider configProvider) throws Exception{
        this.spark = spark;
        this.configProvider = configProvider;
    }
    public void registerFunctions() throws Exception{
        spark.udf().register("validOrg", new ValidOrgLookup(configProvider.retrieveAppConfigValue("configLocation") + "officialOrgList.csv"), DataTypes.StringType);
        spark.udf().register("testUser", new TestUser(configProvider.retrieveAppConfigValue("configLocation") + "testUserList.json"), DataTypes.IntegerType);
        spark.udf().register("convertTime", new ConvertTime("yyyy-MM-dd"), DataTypes.StringType);
        spark.udf().register("calcAvgFromHistMin", new calcAvgFromHistMin(), DataTypes.FloatType);
        spark.udf().register("endOfDay", new EndOfDay(), DataTypes.TimestampType);
        spark.udf().register("uuid", new Uuid(), DataTypes.StringType);
        spark.udf().register("shortUuid", new shortUuid(), DataTypes.StringType);
        spark.udf().register("convertTimeString", new ConvertTimeString("yyyy-MM-dd"), DataTypes.StringType);

        spark.udf()
                .register("get_only_file_name", (String fullPath) -> {
                    int lastIndex = fullPath.lastIndexOf("/") + 1;
                    return fullPath.substring(lastIndex, fullPath.length());
                }, DataTypes.StringType);
        spark.udf().register("toTimestamp", new ToTimestamp(), DataTypes.TimestampType);

        UDFUtil.register(spark.sqlContext());
    }

    public static class AggregationUtil{
        private String watermarkDelayThreshold = "1800 minutes";
        private String aggregationPeriod = "daily";

        public AggregationUtil(String period){
            setAggregatePeriod(period);
        }

        public void setAggregatePeriod(String period){
            if(period == null) return;
            switch (period){
                case "daily":
                    setWatermarkDelayThreshold("1800 minutes");
                    break;
                case "weekly":
                    setWatermarkDelayThreshold("8 days");
                    break;
                case "monthly":
                    setWatermarkDelayThreshold("32 days");
                    break;
                default:
                    throw new IllegalArgumentException("Invalid input for aggregation period");
            }
            aggregationPeriod = period;
        }

        public String getAggregatePeriod(){
            return this.aggregationPeriod;
        }

        public void registerAggregationFunctions(SparkSession spark){
            spark.udf().register("periodTag", new PeriodTag(aggregationPeriod), DataTypes.StringType);
            spark.udf().register("aggregateStartDate", new AggregateStartDate(aggregationPeriod), DataTypes.TimestampType);
        }

        public void setWatermarkDelayThreshold(String threshold){
            this.watermarkDelayThreshold = threshold;
        }

        public String getWatermarkDelayThreshold(){
            return this.watermarkDelayThreshold;
        }

        private static class AggregateStartDate implements UDF1<Timestamp, Timestamp> {
            private String period;
            public AggregateStartDate(String period){
                this.period = period;
            }
            public Timestamp call(Timestamp timeStamp) throws Exception {
                DateTime dateTime = new DateTime(timeStamp);
                switch (period){
                    case "weekly":
                        return new Timestamp(getFirstDayOfWeek(dateTime).withMillisOfDay(0).getMillis());
                    case "monthly":
                        return new Timestamp(dateTime.withDayOfMonth(1).withMillisOfDay(0).getMillis());
                    default:
                        return new Timestamp(dateTime.withMillisOfDay(0).getMillis());
                }
            }
        }

        private static class PeriodTag implements UDF1<String, String> {
            private String period;
            public PeriodTag(String period){
                this.period = period;
            }
            public String call(String dummy) throws Exception {
                return period;
            }
        }
    }

    private static class ToTimestamp implements UDF1<String, Timestamp> {
        private CommonFunctions.TimeConverter timeConverter;
        public ToTimestamp(){
            this.timeConverter = new CommonFunctions.TimeConverter();
        }

        public Timestamp call(String timeStamp) throws Exception {
            return timeConverter.toTimestamp(timeStamp);
        }
    }

    private static class calcAvgFromHistMin implements UDF1<WrappedArray<GenericRowWithSchema>, Float> {
        public Float call(WrappedArray<GenericRowWithSchema> array) throws Exception {
            if(array == null || array.length() == 0) return 0 * 1.0f;
            List<GenericRowWithSchema> list = JavaConversions.seqAsJavaList(array);
            Long durationSum = list.stream()
                    .map(x -> x.getLong(x.fieldIndex("duration")))
                    .reduce(0L, (x, y) -> x + y);
            Long totalSum = list.stream()
                    .map(x -> x.getLong(x.fieldIndex("duration")) * (x.getLong(x.fieldIndex("min")) + 1))
                    .reduce(0L, (x, y) -> x + y);
            if(durationSum == 0) return 0 * 1.0f;
            return totalSum * 1.0f / durationSum;
        }
    }

    private static class Uuid implements UDF1<String, String> {
        public String call(String dummy) throws Exception {
            return UUID.randomUUID().toString();
        }
    }

    private static class shortUuid implements UDF1<String, String> {
        public String call(String dummy) throws Exception {
            return UUID.nameUUIDFromBytes(dummy.getBytes()).toString();
        }
    }

    private static class EndOfDay implements UDF1<Timestamp, Timestamp> {
        public Timestamp call(Timestamp timeStamp) throws Exception {
            DateTime dateTime = new DateTime(timeStamp);
            return new Timestamp(dateTime.plusDays(1).withTimeAtStartOfDay().minusSeconds(1).getMillis());
        }
    }

    private static class ConvertTime implements UDF1<Timestamp, String> {
        private TimeConverter timeConverter;
        public ConvertTime(String toPattern){
            this.timeConverter = new TimeConverter(toPattern);
        }

        public String call(Timestamp timeStamp) throws Exception {
            return timeConverter.convert(timeStamp);
        }
    }

    private static class ConvertTimeString implements UDF1<String, String> {
        private TimeConverter timeConverter;
        public ConvertTimeString(String toPattern){
            this.timeConverter = new TimeConverter(toPattern);
        }

        public String call(String timeStampString) throws Exception {
            return timeConverter.convert(timeStampString);
        }
    }

    private class TestUser implements UDF1<String, Integer> {
        private List<String> testUserList;
        public TestUser(String configPath){
            Dataset testUser = spark.read().option("multiline", true).json(configPath).selectExpr("explode(testUsers) as testUser");
            testUserList = testUser.map((MapFunction<Row, String>) row -> row.<String>getString(0), Encoders.STRING()).collectAsList();
        }
        public Integer call(String userId) throws Exception {
            return testUserList.contains(userId)? 1:0;
        }
    }

    private class ValidOrgLookup implements UDF1<String, String> {
        private List<String> excludedOrgList;

        public ValidOrgLookup(String configPath){
            Dataset orgList = spark.read().format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("delimiter", ",")
                    .load(configPath);
            Dataset excludedOrg = orgList.where("exclude=1").selectExpr("org_id");
            excludedOrgList = excludedOrg.map((MapFunction<Row, String>) row -> row.<String>getString(0), Encoders.STRING()).collectAsList();
        }

        public String call(String orgId) throws Exception {
            return excludedOrgList.contains(orgId) ? "1" : "0";
        }
    }

    public static class AppFilter implements UDF1<String, Boolean> {
        private String tag;

        public AppFilter(String appName) {
            this.tag = "appname\":" + '"' + appName + '"';
        }

        public Boolean call(String line) {
            return line.contains(tag);
        }
    }


    public static class RawTimestampField implements UDF1<String, String> {
        private transient ObjectMapper objectMapper;

        public String call(String value) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            try {
                ObjectNode objectNode = (ObjectNode) objectMapper.readTree(value);
                if(objectNode.has("timeRcvd")){
                    return objectNode.get("timeRcvd").asText();
                }
                if(objectNode.has("@timestamp")){
                    return objectNode.get("@timestamp").asText();
                }
                return "";
            } catch (Exception e) {
                return "";
            }
        }
    }

    public static class Preprocess implements UDF1<String, String> {
        private transient ObjectMapper objectMapper;

        public String call(String value) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            try {
                ObjectNode objectNode = (ObjectNode) objectMapper.readTree(value);
                if (objectNode.get("appname") != null) {
                    return objectNode.toString();
                } else {
                    String message = objectNode.get("@message").asText();
                    String[] jsonMessages = message.split(":", 2);
                    if (jsonMessages.length == 2) {
                        JsonNode metric = objectMapper.readTree(jsonMessages[1].trim());
                        objectNode.set("SM", metric);
                        objectNode.remove("@message");
                        return objectNode.toString();
                    } else {
                        return value;
                    }
                }
            } catch (Exception e) {
                return value;
            }
        }
    }
}

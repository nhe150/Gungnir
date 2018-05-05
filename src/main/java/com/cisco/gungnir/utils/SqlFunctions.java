package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;
import util.UDFUtil;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.cisco.gungnir.utils.CommonFunctions.TimeConverter;
import static com.cisco.gungnir.utils.CommonFunctions.getFirstDayOfWeek;

public class SqlFunctions implements Serializable {
    public static void registerFunctions(SparkSession spark){
        spark.udf().register("validOrg", new ValidOrgLookup("/officialOrgList.csv"), DataTypes.StringType);
        spark.udf().register("convertTime", new ConvertTime("yyyy-MM-dd"), DataTypes.StringType);
        spark.udf().register("calcAvgFromHistMin", new calcAvgFromHistMin(), DataTypes.FloatType);
        spark.udf().register("endOfDay", new EndOfDay(), DataTypes.TimestampType);
        spark.udf().register("uuid", new Uuid(), DataTypes.StringType);
        spark.udf().register("shortUuid", new shortUuid(), DataTypes.StringType);

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

    private static class ValidOrgLookup implements UDF1<String, String> {
        private Map<String, String> orgExcludeMap;

        public ValidOrgLookup(String fileName){
            this.orgExcludeMap = parseUsingOpenCSV(fileName);
        }

        public Map<String, String> parseUsingOpenCSV(String fileName) {
            Map<String, String> orgExcludeMap = new HashMap();
            InputStream inputStream = getClass().getResourceAsStream(fileName);

            try{
                BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                for(String line = br.readLine(); line != null; line = br.readLine()){
                    String[] record = line.split(",");
                    String orgId = record[0].replaceAll("\"", "");
                    orgExcludeMap.put(orgId, record[7]);
                }
                br.close();
            } catch (Exception e){
                e.printStackTrace();
            }

            return orgExcludeMap;
        }

        public String call(String orgId) throws Exception {
            return orgExcludeMap.get(orgId) == null ? "0" : orgExcludeMap.get(orgId);
        }
    }
}

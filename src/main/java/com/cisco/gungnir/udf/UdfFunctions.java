package com.cisco.gungnir.udf;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.utils.TimeConverter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class UdfFunctions implements Serializable {
    private SparkSession spark;
    private ConfigProvider configProvider;

    public UdfFunctions(SparkSession spark, ConfigProvider configProvider) throws Exception{
        this.spark = spark;
        this.configProvider = configProvider;
    }

    public void registerFunctions() throws Exception{
        spark.udf().register("validOrg", new ValidOrgLookup(configProvider.retrieveAppConfigValue("configLocation") + "officialOrgList.csv"), DataTypes.StringType);
        spark.udf().register("testUser", new TestUser(configProvider.retrieveAppConfigValue("configLocation") + "testUserList.json"), DataTypes.IntegerType);
        spark.udf().register("ep1", new DeviceMapping(configProvider.retrieveAppConfigValue("configLocation") + "deviceMapping.csv"), DataTypes.StringType);
        spark.udf().register("uaCategory", new UaMapping(configProvider.retrieveAppConfigValue("configLocation") + "uaMapping.csv"), DataTypes.StringType);
        spark.udf().register("convertTime", new ConvertTime("yyyy-MM-dd"), DataTypes.StringType);
        spark.udf().register("endOfDay", new EndOfDay(), DataTypes.TimestampType);
        spark.udf().register("uuid", new Uuid(), DataTypes.StringType);
        spark.udf().register("shortUuid", new shortUuid(), DataTypes.StringType);
        spark.udf().register("convertTimeString", new ConvertTimeString("yyyy-MM-dd"), DataTypes.StringType);
        spark.udf().register("toTimestamp", new ToTimestamp(), DataTypes.TimestampType);
        spark.udf().register("calcAvgFromHistMin", new calcAvgFromHistMin(), DataTypes.FloatType);

        spark.udf()
                .register("get_only_file_name", (String fullPath) -> {
                    int lastIndex = fullPath.lastIndexOf("/") + 1;
                    return fullPath.substring(lastIndex, fullPath.length());
                }, DataTypes.StringType);
    }

    private static class ToTimestamp implements UDF1<String, Timestamp> {
        private TimeConverter timeConverter;
        public ToTimestamp(){
            this.timeConverter = new TimeConverter();
        }

        public Timestamp call(String timeStamp) throws Exception {
            return timeConverter.toTimestamp(timeStamp);
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

    private class UaMapping implements UDF1<String, String> {
        private Map<String, String> deviceTypeMap;

        public UaMapping(String configPath){
            Dataset deviceUaType = spark.read().format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("delimiter", ",")
                    .load(configPath);
            JavaPairRDD<String, String> javaPairRDD = deviceUaType.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
                public Tuple2<String, String> call(Row row) throws Exception {
                    return new Tuple2<String, String>((String) row.get(0), (String) row.get(1));
                }
            });

            deviceTypeMap = new HashMap<>(javaPairRDD.collectAsMap());
        }

        public String call(String ua) throws Exception {
            String key = (ua==null ? "": ua);
            return deviceTypeMap.containsKey(key)?  deviceTypeMap.get(key): "OTHER";
        }
    }

    private class DeviceMapping implements UDF2<String, String, String> {
        private Map<String, String> deviceTypeMap;

        public DeviceMapping(String configPath){
            Dataset deviceUaType = spark.read().format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("delimiter", ",")
                    .load(configPath);
            JavaPairRDD<String, String> javaPairRDD = deviceUaType.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
                public Tuple2<String, String> call(Row row) throws Exception {
                    return new Tuple2<String, String>((String) row.get(0), (String) row.get(1));
                }
            });

            deviceTypeMap = new HashMap<>(javaPairRDD.collectAsMap());
        }

        public String call(String device, String ua) throws Exception {
            String key = (device==null ? "": device) + "|" + (ua==null ? "": ua);
            return deviceTypeMap.containsKey(key)?  deviceTypeMap.get(key): "Other";
        }
    }

    public static class AppFilter implements UDF1<String, Boolean> {

        private String[] tags;

        public AppFilter(String tag) {

            String[] terms;
            if(tag.contains(",")) {
                terms = tag.split(":");
                tags = terms[1].split(",");
                tags[0]=tags[0] + "\"";
                int pos = 1;
                while(pos<tags.length-1) {
                    tags[pos] = "\"" + tags[pos] + "\"";
                    pos++;
                }
                tags[pos] = "\"" + tags[pos];
                for(int index=0; index<tags.length; index++) {
                    tags[index] = terms[0] + ":" + tags[index];
                }
            }
            else {
                tags = new String[1];
                tags[0] = tag;
            }
        }

        public Boolean call(String line) {

            for(String tag:tags){


                if(line.contains(tag)) return true;
            }

            return false;
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
}

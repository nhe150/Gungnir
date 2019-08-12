package com.cisco.gungnir.job;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.query.QueryFunctions;
import com.cisco.gungnir.utils.DateUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;


import org.joda.time.DateTime;


public abstract class DataMonitor implements Serializable {
    SparkSession spark;
    ConfigProvider configProvider;
    QueryFunctions queryFunctions;

    public DataMonitor() {

    }

    public void set(SparkSession spark, ConfigProvider appConfigProvider) throws Exception {
        ConfigProvider gungnirConfigProvider = new ConfigProvider(spark, appConfigProvider.retrieveAppConfigValue("gungnirConfigFile"));
        ConfigProvider mergedConfigProvider = new ConfigProvider(spark, ConfigProvider.merge(gungnirConfigProvider.getAppConfig().deepCopy(), appConfigProvider.getAppConfig().deepCopy()));

        this.spark = spark;
        this.configProvider = mergedConfigProvider;
        this.queryFunctions = new QueryFunctions(spark, mergedConfigProvider);

        registerUDF();
    }

    public void registerUDF() {
        spark.udf().register("isBusinessDay", new BusinessDay(), DataTypes.BooleanType);
        spark.udf().register("convertTime", new TimeConverter(), DataTypes.StringType);
    }

    public Dataset readFromCass()  throws Exception {
        return queryFunctions.cassandra.readFromCassandra("batch", configProvider.getAppConfig());

    }

    //used for scala code. incase of type eraser, using extra parameter to distinguish --- 2019-07-23 Norman He@cisco
    public void writeToKafka(Dataset<Row> result, boolean diff) throws Exception {
        queryFunctions.kafka.writeToKafka(result, "batch", configProvider.getAppConfig(), diff);
    }

    public void writeToKafka(Dataset result) throws Exception {
        queryFunctions.kafka.writeToKafka(result, "batch", configProvider.getAppConfig());
    }

    public abstract void run(String currentDate, String threshold, String orgId) throws Exception;


    public class TimeConverter implements UDF1<Long, String> {
        public String call(Long unixtimeStamp) throws Exception {
            Date date = new Date(unixtimeStamp * 1000L);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
            return sdf.format(date);
        }
    }

    public class BusinessDay implements UDF1<String, Boolean> {
        public Boolean call(String startDate) throws Exception {
            return DateUtil.isBusinessDay(startDate);
        }
    }

    /**
     * @param addedDay numbers days added from today
     * @return Date in yyyy-MM-dd format
     */
    public String getDate(int addedDay) {
        return new DateTime(DateTimeZone.UTC).plusDays(addedDay).toString("yyyy-MM-dd");
    }




    List<String> getOrgList(JsonNode node) {
        ArrayList<String> orgList = new ArrayList<>();
        if (node.get("orgids").isArray()) {
            for (final JsonNode objNode : node.get("orgids")) {
                orgList.add(objNode.asText());
            }
        }
        return orgList;
    }

    String whereOrgId(List<String> orgList) {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < orgList.size(); i++) {
            sb.append("'").append(orgList.get(i)).append("',");
        }
        sb.deleteCharAt(sb.length() - 1);  // remove last "'"
        return sb.toString();
    }


}

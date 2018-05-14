package com.cisco.gungnir.job;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.query.QueryFunctions;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class SparkDataMonitor implements Serializable {
    private SparkSession spark;
    private ConfigProvider configProvider;
    private QueryFunctions queryFunctions;

    public SparkDataMonitor(SparkSession spark, ConfigProvider appConfigProvider) throws Exception{
        ConfigProvider gungnirConfigProvider = new ConfigProvider(spark, appConfigProvider.retrieveAppConfigValue("gungnirConfigFile"));
        ConfigProvider mergedConfigProvider =  new ConfigProvider(spark, ConfigProvider.merge(gungnirConfigProvider.getAppConfig().deepCopy(), appConfigProvider.getAppConfig().deepCopy()));

        this.spark = spark;
        this.configProvider = mergedConfigProvider;
        this.queryFunctions = new QueryFunctions(spark, mergedConfigProvider);
    }

    public void run(String currentDate, String threshold, String orgId) throws Exception {
        Dataset aggregates = queryFunctions.cassandra.readFromCassandra("batch", configProvider.getAppConfig());

        Dataset data = allCounts(aggregates, orgId);

        Dataset dataWithFlag = dataWithFlag(currentDate, data, threshold);

        Dataset messages = createMessages(dataWithFlag);

        queryFunctions.kafka.writeToKafka(messages, "batch", configProvider.getAppConfig());
    }

    private Dataset allCounts(Dataset dataset, String orgId) throws Exception{
        spark.udf().register("convertTime", new TimeConverter(), DataTypes.StringType);
        Dataset data;
        if("*".equals(orgId)){
            data = dataset;
        } else {
            data = dataset.where("orgid = " + orgId);
        }

        Dataset orgIdList = spark.createDataset(getOrgList(configProvider.getAppConfig()), Encoders.STRING()).selectExpr("value as orgid");

        orgIdList.show(false);
        data.show(false);
        data = orgIdList.join(data.alias("data"), orgIdList.col("orgid").equalTo(data.col("orgid")))
                .selectExpr("data.*");
        data.show(false);

        data.show(false);
        Dataset allCounts = getOrgCount(dataset).union(getCountPerOrg(data));
        allCounts.show(false);
        return allCounts.selectExpr("orgid", "relation_name", "convertTime(unix_timestamp(time_stamp)) as pdate", "count");
    }

    private Dataset dataWithFlag(String currentDate, Dataset data, String threshold) throws Exception {
        spark.udf().register("isBusinessDay", new BusinessDay(), DataTypes.BooleanType);

        Dataset history = data.where("to_date(pdate) < to_date('" + currentDate + "')");
        Dataset currentData = data.where("pdate = '" + currentDate + "'");

        history.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .save(configProvider.retrieveAppConfigValue("dataLocation") + "history_aggregates_summary");

        Dataset average;
        Dataset historyForBusinessDays = history.filter("isBusinessDay(pdate)");

        if(isBusinessDay(currentDate)){
            average = getOrgAverage(historyForBusinessDays);
        } else {
            Dataset historyForNonBusinessDays = history.except(historyForBusinessDays);
            average = getOrgAverage(historyForNonBusinessDays);
        }

        Dataset dataWithFlag = currentData.alias("currentData").join(average, currentData.col("orgid").equalTo(average.col("orgid")).and(currentData.col("relation_name").equalTo(average.col("relation_name"))))
                .selectExpr("currentData.orgid", "currentData.relation_name", "pdate", "count", "avg", "CASE WHEN (count IS NULL OR count/avg < " + threshold + " OR count/avg > " + 2/Double.parseDouble(threshold) +") THEN 'failure' ELSE 'success' END as status");

        dataWithFlag.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .save(configProvider.retrieveAppConfigValue("dataLocation") + "aggregates_data_monitor_summary");

        return dataWithFlag;
    }

    private Dataset getOrgAverage(Dataset dataset) {
        return dataset.where("count IS NOT NULL").groupBy("orgid", "relation_name").avg("count").selectExpr("orgid", "relation_name", "`avg(count)` as avg");
    }

    private Dataset getOrgCount(Dataset aggregates){
        Dataset orgCount = aggregates.selectExpr("CONCAT(relation_name, '^', period) as relation_name", "time_stamp")
                .groupBy("relation_name", "time_stamp")
                .count()
                .selectExpr("'orgCount' as orgid", "relation_name", "time_stamp", "count");
        orgCount.show(false);
        return orgCount;
    }

    private Dataset getCountPerOrg(Dataset aggregates){
        aggregates.cache();
        Dataset countPerOrg = aggregates
                .where("relation_name = 'fileUsed'")
                .selectExpr("orgid", "CONCAT('files^', period) as relation_name", "time_stamp", "files as count");

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'fileUsed'")
                .selectExpr("orgid", "CONCAT('filesize^', period) as relation_name", "time_stamp", "filesize as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'activeUser'")
                .selectExpr("orgid", "CONCAT('onetoonecount^', period) as relation_name", "time_stamp", "onetoonecount as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'activeUser'")
                .selectExpr("orgid", "CONCAT('spacecount^', period) as relation_name", "time_stamp", "spacecount as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'activeUser'")
                .selectExpr("orgid", "CONCAT('usercountbyorg^', period) as relation_name", "time_stamp", "usercountbyorg as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'callDuration'")
                .where("ep1 = 'Desktop client'")
                .selectExpr("orgid", "CONCAT('number_of_minutes^', period) as relation_name", "time_stamp", "number_of_minutes as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'callDuration'")
                .where("ep1 = 'Desktop client'")
                .selectExpr("orgid", "CONCAT('number_of_total_calls^', period) as relation_name", "time_stamp", "number_of_successful_calls as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'callQuality'")
                .selectExpr("orgid", "CONCAT('number_of_good_calls^', period) as relation_name", "time_stamp", "number_of_total_calls-number_of_bad_calls as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'callQuality'")
                .selectExpr("orgid", "CONCAT('number_of_bad_calls^', period) as relation_name", "time_stamp", "number_of_bad_calls as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'registeredEndpoint'")
                .where("model = 'SPARK-BOARD55'")
                .selectExpr("orgid", "CONCAT('registeredEndpointCount^', period) as relation_name", "time_stamp", "registeredEndpointCount as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'conv'")
                .selectExpr("orgid", "CONCAT('convCount^', period) as relation_name", "time_stamp", "userCountByOrg as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'locus'")
                .selectExpr("orgid", "CONCAT('locusCount^', period) as relation_name", "time_stamp", "userCountByOrg as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'metrics'")
                .selectExpr("orgid", "CONCAT('metricsCount^', period) as relation_name", "time_stamp", "userCountByOrg as count"));
        countPerOrg.show(false);
        return countPerOrg;
    }

    private Dataset createMessages(Dataset dataset) throws Exception {
        String host = "Cassandra Ips:" + configProvider.retrieveAppConfigValue("cassandra.host");
        Dataset message = dataset.where("status = 'failure'").selectExpr(
                "'Spark' as pipeLine",
                "'Data Process' as phase",
                "'Apache Spark' as component",
                "CONCAT(pdate, ' 00:00:00') as sendTime",
                "struct('" + host + "' as host, CONCAT(orgid, '^', relation_name) as name, CONCAT(pdate, ' 00:00:00') as createTime, CONCAT(pdate, ' 00:00:00') as lastModifiedTime, count as size, 'records' as unit, 'Integer' as category, status as stageStatus) as data");
        return message;
    }

    public class TimeConverter implements UDF1<Long, String> {
        public String call(Long unixtimeStamp) throws Exception {
            Date date = new Date(unixtimeStamp*1000L);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
            return sdf.format(date);
        }
    }

    public class BusinessDay implements UDF1<String, Boolean> {
        public Boolean call(String startDate) throws Exception {
            return isBusinessDay(startDate);
        }
    }

    public boolean isBusinessDay(String startDate) throws Exception {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        final Date date = format.parse(startDate);

        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        // check if weekend
        if(cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
            return false;
        }

        // check if New Year's Day
        if (cal.get(Calendar.MONTH) == Calendar.JANUARY
                && cal.get(Calendar.DAY_OF_MONTH) == 1) {
            return false;
        }

        // check if Christmas Eve
        if (cal.get(Calendar.MONTH) == Calendar.DECEMBER
                && cal.get(Calendar.DAY_OF_MONTH) == 24) {
            return false;
        }

        // check if Christmas
        if (cal.get(Calendar.MONTH) == Calendar.DECEMBER
                && cal.get(Calendar.DAY_OF_MONTH) == 25) {
            return false;
        }

        // check if 4th of July
        if (cal.get(Calendar.MONTH) == Calendar.JULY
                && cal.get(Calendar.DAY_OF_MONTH) == 4) {
            return false;
        }

        // check Thanksgiving (4th Thursday of November)
        if (cal.get(Calendar.MONTH) == Calendar.NOVEMBER
                && cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) == 4
                && cal.get(Calendar.DAY_OF_WEEK) == Calendar.THURSDAY) {
            return false;
        }

        // check BlackFriday (4th Friday of November)
        if (cal.get(Calendar.MONTH) == Calendar.NOVEMBER
                && cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) == 4
                && cal.get(Calendar.DAY_OF_WEEK) == Calendar.FRIDAY) {
            return false;
        }

        // check Memorial Day (last Monday of May)
        if (cal.get(Calendar.MONTH) == Calendar.MAY
                && cal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY
                && cal.get(Calendar.DAY_OF_MONTH) > (31 - 7) ) {
            return false;
        }

        // check Labor Day (1st Monday of September)
        if (cal.get(Calendar.MONTH) == Calendar.SEPTEMBER
                && cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) == 1
                && cal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY) {
            return false;
        }

        // check President's Day (3rd Monday of February)
        if (cal.get(Calendar.MONTH) == Calendar.FEBRUARY
                && cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) == 3
                && cal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY) {
            return true;
        }

        // check Veterans Day (November 11)
        if (cal.get(Calendar.MONTH) == Calendar.NOVEMBER
                && cal.get(Calendar.DAY_OF_MONTH) == 11) {
            return true;
        }

        // check MLK Day (3rd Monday of January)
        if (cal.get(Calendar.MONTH) == Calendar.JANUARY
                && cal.get(Calendar.DAY_OF_WEEK_IN_MONTH) == 3
                && cal.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY) {
            return true;
        }

        // IF NOTHING ELSE, IT'S A BUSINESS DAY
        return true;
    }

    private List getOrgList(JsonNode node){
        ArrayList<String> orgList=new ArrayList<>();
        if (node.get("orgids").isArray()) {
            for (final JsonNode objNode : node.get("orgids")) {
                orgList.add(objNode.asText());
            }
        }
        return orgList;
    }
}

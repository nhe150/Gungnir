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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

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
        if(currentDate==null) currentDate = new DateTime(DateTimeZone.UTC).plusDays(-1).toString("yyyy-MM-dd");
        if(!isBusinessDay(currentDate)) return;

        Dataset aggregates = queryFunctions.cassandra.readFromCassandra("batch", configProvider.getAppConfig());

        Dataset data = allCounts(aggregates, orgId, currentDate);

        Dataset dataWithFlag = dataWithFlag(currentDate, data, threshold);

        Dataset messages = createMessages(dataWithFlag);

        queryFunctions.kafka.writeToKafka(messages, "batch", configProvider.getAppConfig());
    }

    private Dataset allCounts(Dataset dataset, String orgId, String currentDate) throws Exception{
        spark.udf().register("convertTime", new TimeConverter(), DataTypes.StringType);
        dataset.cache();
        dataset  = dataset.where("period = 'daily'");
        dataset =  dataset.withColumn("pdate", callUDF("unix_timestamp", col("time_stamp"))).withColumn("pdate", callUDF("convertTime", col("pdate")));

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

        Dataset allCounts = getOrgCount(dataset, currentDate).union(getCountPerOrg(data));
        allCounts.show(false);
        return allCounts.selectExpr("orgid", "relation_name", "pdate", "count");
    }

    private Dataset dataWithFlag(String currentDate, Dataset data, String threshold) throws Exception {
        spark.udf().register("isBusinessDay", new BusinessDay(), DataTypes.BooleanType);

        String startDate = new DateTime(DateTimeZone.UTC).plusDays(-30).toString("yyyy-MM-dd");

        Dataset history = data.where( "to_date('" + startDate + "') < to_date(pdate)" + " AND " + "to_date(pdate) < to_date('" + currentDate + "')");
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
                .selectExpr("currentData.orgid", "currentData.relation_name", "pdate", "count", "avg", "CAST((count/avg) * 100 AS INT) as percentage", "CASE WHEN (((avg>800 AND currentData.relation_name='activeUser') OR (avg>100 AND currentData.relation_name<>'activeUser')) AND ((currentData.relation_name='fileUsed' AND count=0) OR (currentData.relation_name='number_of_good_calls' AND count=0) OR (currentData.relation_name<>'fileUsed' AND currentData.relation_name<>'number_of_good_calls' AND (count IS NULL OR count/avg < " + threshold + " OR count/avg > " + 2/Double.parseDouble(threshold) +")))) THEN 'failure' ELSE 'success' END as status");        dataWithFlag.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .save(configProvider.retrieveAppConfigValue("dataLocation") + "aggregates_data_monitor_summary");

        return dataWithFlag;
    }

    private Dataset getOrgAverage(Dataset dataset) {
        return dataset.where("count IS NOT NULL").groupBy("orgid", "relation_name").avg("count").selectExpr("orgid", "relation_name", "`avg(count)` as avg");
    }

    private Dataset getOrgCount(Dataset aggregates, String currentDate){
        List<String> data = new ArrayList<>();
        data.add("fileUsed," + currentDate);
        data.add("activeUser," + currentDate);
        data.add("registeredEndpoint," + currentDate);
        data.add("callDuration," + currentDate);
        data.add("callQuality," + currentDate);

        Dataset df = spark.createDataset(data, Encoders.STRING()).toDF();

        Dataset df1 = df.selectExpr("split(value, ',')[0] as relation_name", "split(value, ',')[1] as pdate");

        Dataset orgCount = aggregates.selectExpr("relation_name", "pdate").union(df1)
                .groupBy("relation_name", "pdate")
                .count()
                .selectExpr("'orgCount' as orgid", "relation_name", "pdate", "count");
        orgCount.show(false);
        return orgCount;
    }

    private Dataset getCountPerOrg(Dataset aggregates){
        aggregates.cache();
        Dataset countPerOrg = aggregates
                .where("relation_name = 'fileUsed'")
                .selectExpr("orgid", "'fileUsed' as relation_name", "pdate", "files as count");
//
//        countPerOrg= countPerOrg.union(aggregates
//                .where("relation_name = 'fileUsed'")
//                .selectExpr("orgid", "CONCAT('filesize^', period) as relation_name", "pdate", "filesize as count"));

//        Dataset countPerOrg= aggregates
//                .where("relation_name = 'activeUser'")
//                .selectExpr("orgid", "CONCAT('onetoonecount^', period) as relation_name", "pdate", "onetoonecount as count"));
//
//        countPerOrg= countPerOrg.union(aggregates
//                .where("relation_name = 'activeUser'")
//                .selectExpr("orgid", "CONCAT('spacecount^', period) as relation_name", "pdate", "spacecount as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'activeUser'")
                .selectExpr("orgid", "'activeUser' as relation_name", "pdate", "usercountbyorg as count"));

//        countPerOrg= countPerOrg.union(aggregates
//                .where("relation_name = 'callDuration'")
//                .where("ep1 = 'Desktop client'")
//                .selectExpr("orgid", "CONCAT('number_of_minutes^', period) as relation_name", "pdate", "number_of_minutes as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'callDuration'")
                .where("ep1 = 'Desktop client'")
                .selectExpr("orgid", "'number_of_total_calls' as relation_name", "pdate", "number_of_successful_calls as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'callQuality'")
                .selectExpr("orgid", "'number_of_good_calls' as relation_name", "pdate", "number_of_total_calls-number_of_bad_calls as count"));

//        countPerOrg= countPerOrg.union(aggregates
//                .where("relation_name = 'callQuality'")
//                .selectExpr("orgid", "CONCAT('number_of_bad_calls^', period) as relation_name", "pdate", "number_of_bad_calls as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'registeredEndpoint'")
                .where("model = 'SPARK-BOARD55'")
                .selectExpr("orgid", "'roomDevice' as relation_name", "pdate", "registeredEndpointCount as count"));

//        countPerOrg= countPerOrg.union(aggregates
//                .where("relation_name = 'conv'")
//                .selectExpr("orgid", "CONCAT('convCount^', period) as relation_name", "pdate", "userCountByOrg as count"));
//
//        countPerOrg= countPerOrg.union(aggregates
//                .where("relation_name = 'locus'")
//                .selectExpr("orgid", "CONCAT('locusCount^', period) as relation_name", "pdate", "userCountByOrg as count"));
//
//        countPerOrg= countPerOrg.union(aggregates
//                .where("relation_name = 'metrics'")
//                .selectExpr("orgid", "CONCAT('metricsCount^', period) as relation_name", "pdate", "userCountByOrg as count"));
        countPerOrg.show(false);
        return countPerOrg;
    }


    private Dataset createMessages(Dataset dataset) throws Exception {
        Dataset message = dataset.where("status = 'failure'").selectExpr(
                "'crs' as component",
                "'metrics' as eventtype",
                "struct('Spark' as pipeLine, 'DataProcess' as phase, CONCAT(pdate, 'T00:00:00Z') as sendTime, struct(CONCAT(orgid, '_', relation_name) as name, pdate as reportDate, count as volume, avg as historicalAverageVolume, percentage, status) as data) as metrics");
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

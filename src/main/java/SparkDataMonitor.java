import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import util.Cassandra;
import util.Constants;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

public class SparkDataMonitor implements Serializable {
    private SparkSession spark;
    private Constants constants;

    public SparkDataMonitor(String appName, Constants constants){
        this.constants = constants;
        this.spark = createSparkSession(appName, constants);
    }

    private SparkSession createSparkSession(String appName, Constants constants) {
        SparkSession spark = SparkSession.builder()
                .config("spark.cassandra.connection.host", constants.CassandraHosts())
                .config("spark.cassandra.auth.username", constants.CassandraUsername())
                .config("spark.cassandra.auth.password", constants.CassandraPassword())
                .appName(appName).getOrCreate();

        spark.sparkContext().setLogLevel(constants.logLevel());
        return spark;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option config = new Option("c", "config", true, "config file");
        config.setRequired(true);
        options.addOption(config);

        Option date = new Option("d", "date", true, "date of data to be monitored");
        date.setRequired(true);
        options.addOption(date);

        Option thresholdOpt = new Option("t", "threshold", true, "threshold of data volume off to be consider abnormal");
        thresholdOpt.setRequired(false);
        options.addOption(thresholdOpt);

        Option orgIdOpt = new Option("o", "orgId", true, "orgId of data to be monitored");
        orgIdOpt.setRequired(false);
        options.addOption(orgIdOpt);

        Option orgIdListOpt = new Option("ol", "orgIdList", true, "file contains a list of orgIds to be monitored");
        orgIdListOpt.setRequired(false);
        options.addOption(orgIdListOpt);

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("sparkDataMonitor", options);

            System.exit(1);
            return;
        }

        String configFile = cmd.getOptionValue("config");
        String currentDate  = cmd.getOptionValue("date");
        String threshold = cmd.getOptionValue("threshold")== null ? "0.3" : cmd.getOptionValue("threshold");
        String orgId = cmd.getOptionValue("orgId")== null ? "*" : "'" + cmd.getOptionValue("orgId") + "'";
        String orgIdListFile = cmd.getOptionValue("orgIdList");

        SparkDataMonitor app = new SparkDataMonitor("sparkDataMonitor", new Constants(configFile));

        Dataset aggregates = Cassandra.readRecord(app.spark, app.constants.CassandraKeySpace(), app.constants.CassandraTableAgg());

        Dataset data = app.allCounts(aggregates, orgId, orgIdListFile);

        Dataset dataWithFlag = app.dataWithFlag(currentDate, data, threshold);

        Dataset messages = app.createMessages(dataWithFlag);

        app.sinkToKafka(messages, app.constants.dataMonitorBroker(), app.constants.dataMonitorTopic());

    }

    private Dataset allCounts(Dataset dataset, String orgId, String orgIdListFile) throws Exception{
        spark.udf().register("convertTime", new TimeConverter(), DataTypes.StringType);
        Dataset data;
        if("*".equals(orgId)){
            data = dataset;
        } else {
            data = dataset.where("orgid = " + orgId);
        }

        if(orgIdListFile != null){
            Dataset orgIdList = spark.createDataset(new ArrayList<>(), Encoders.STRING());
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            if(fs.exists(new org.apache.hadoop.fs.Path(orgIdListFile))){
                orgIdList = spark.read().textFile(orgIdListFile).toDF("orgId");
            } else if(Files.exists(Paths.get(orgIdListFile))){
                File orgList = new File(orgIdListFile);
                InputStream targetStream = new FileInputStream(orgList);
                String orgs = IOUtils.toString(targetStream);
                orgIdList = spark.createDataset(Arrays.asList(orgs), Encoders.STRING()).toDF("orgId");;
            } else {
                throw new IllegalArgumentException("Couldn't find the file " + orgIdListFile + " that contains the list of orgs for monitoring");
            }
            data = orgIdList.join(data.alias("data"), orgIdList.col("orgId").equalTo(data.col("orgId")))
                    .selectExpr("data.*");
        }

        Dataset allCounts = getOrgCount(dataset).union(getCountPerOrg(data));
        return allCounts.selectExpr("orgId", "relation_name", "convertTime(unix_timestamp(time_stamp)) as pdate", "count");
    }

    private Dataset dataWithFlag(String currentDate, Dataset data, String threshold) throws Exception {
        spark.udf().register("isBusinessDay", new BusinessDay(), DataTypes.BooleanType);

        Dataset history = data.where("to_date(pdate) < to_date('" + currentDate + "')");
        Dataset currentData = data.where("pdate = '" + currentDate + "'");

        history.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .save(constants.outputLocation() + "history_aggregates_summary");

        Dataset average;
        Dataset historyForBusinessDays = history.filter("isBusinessDay(pdate)");

        if(isBusinessDay(currentDate)){
            average = getOrgAverage(historyForBusinessDays);
        } else {
            Dataset historyForNonBusinessDays = history.except(historyForBusinessDays);
            average = getOrgAverage(historyForNonBusinessDays);
        }

        Dataset dataWithFlag = currentData.alias("currentData").join(average, currentData.col("orgId").equalTo(average.col("orgId")).and(currentData.col("relation_name").equalTo(average.col("relation_name"))))
                .selectExpr("currentData.orgId", "currentData.relation_name", "pdate", "count", "avg", "CASE WHEN (count IS NULL OR count/avg < " + threshold + " OR count/avg > " + 2/Double.parseDouble(threshold) +") THEN 'failure' ELSE 'success' END as status");

        dataWithFlag.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .save(constants.outputLocation() + "aggregates_data_monitor_summary");

        return dataWithFlag;
    }

    private Dataset getOrgAverage(Dataset dataset) {
        return dataset.where("count IS NOT NULL").groupBy("orgid", "relation_name").avg("count").selectExpr("orgid", "relation_name", "`avg(count)` as avg");
    }

    private Dataset getOrgCount(Dataset aggregates){
        Dataset orgCount = aggregates.selectExpr("CONCAT(relation_name, '^', period) as relation_name", "time_stamp")
                .groupBy("relation_name", "time_stamp")
                .count()
                .selectExpr("'orgCount' as orgId", "relation_name", "time_stamp", "count");
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
                .selectExpr("orgid", "CONCAT('number_of_minutes^', period) as relation_name", "time_stamp", "number_of_minutes as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'callDuration'")
                .selectExpr("orgid", "CONCAT('number_of_total_calls^', period) as relation_name", "time_stamp", "number_of_successful_calls as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'callQuality'")
                .selectExpr("orgid", "CONCAT('number_of_good_calls^', period) as relation_name", "time_stamp", "number_of_total_calls-number_of_bad_calls as count"));

        countPerOrg= countPerOrg.union(aggregates
                .where("relation_name = 'callQuality'")
                .selectExpr("orgid", "CONCAT('number_of_good_bad_calls^', period) as relation_name", "time_stamp", "number_of_total_calls as count"));

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
        return countPerOrg;
    }

    private Dataset createMessages(Dataset dataset){
        String host = "Cassandra Ips:" + constants.CassandraHosts();
        Dataset message = dataset.where("status = 'failure'").selectExpr(
                "'Spark' as pipeLine",
                "'Data Process' as phase",
                "'Apache Spark' as component",
                "CONCAT(pdate, ' 00:00:00') as sendTime",
                "struct('" + host + "' as host, CONCAT(orgid, '^', relation_name) as name, CONCAT(pdate, ' 00:00:00') as createTime, CONCAT(pdate, ' 00:00:00') as lastModifiedTime, count as size, 'records' as unit, 'Integer' as category, status as stageStatus) as data");
        return message;
    }

    private void sinkToKafka(Dataset<Row> dataset, String broker, String topic){
        dataset
                .selectExpr("to_json(struct(*)) AS value")
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", broker)
                .option("topic", topic)
                .option("kafka.retries", constants.kafkaProducerRetries())
                .option("kafka.retry.backoff.ms", constants.kafkaRetryBackoffMs())
                .save();
    }

    public static class TimeConverter implements UDF1<Long, String> {
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
}
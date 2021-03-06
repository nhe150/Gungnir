package com.cisco.gungnir.job;

import com.cisco.gungnir.utils.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Logger;

import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static org.apache.spark.sql.functions.*;

// CallAnalyzer
public class QoSDataMonitor implements Serializable {

    private SparkSession spark;
    private static final Logger LOGGER = Logger.getLogger(QoSDataMonitor.class.getName());


    public QoSDataMonitor(SparkSession spark) throws Exception{
        this.spark = spark;
    }

    public void run(int orgNum, String threshold, boolean ifInitialize, int historyDuration, boolean isTest) throws Exception {

        // ELK index default values
        String dataIndex = "call_analyzer";
        String avgIndex = "call_analyzer_anomalydetection";
        String alertIndex = "call_analyzer_alert";
        String currentDate = new DateTime(DateTimeZone.UTC).toString("yyyy-MM-dd");

        if(isTest){
            dataIndex = "call_analyzer_unit_test";
            avgIndex = "call_analyzer_model_test";
            alertIndex = "call_analyzer_alert_test";
            currentDate = "2019-01-17";
            //threshold = "0.8";

            /* Unit Test Data Info: 100 documents in ELK, with old and new schema mixed.

               Lower alert bound: threshold => 0.8
               Upper alert bound: 2/threshold => 2.5

               Org  1/14 1/15 1/16 AVG   Yesterday  %            Alert
               A    0    14   2    8     2          0.25 < 0.8   true
               B    2    2    70   24.6  70         2.8  > 2.5   true
               C    0    2    4    3     4          1.3          false
               D    0    2    2    2     2          1            false
            */
        }

        spark.udf().register("convertTime", new TimeConverter(), DataTypes.StringType);
        spark.udf().register("isBusinessDay", new BusinessDay(), DataTypes.BooleanType);

        // Read from ELK
        Dataset callAnalyzerData = readFromELK(dataIndex);

        // Generate the avg call counts per org data model to ELK
        LOGGER.info("Model generate start: " + new DateTime(DateTimeZone.UTC).toString());
        if(ifInitialize){
            GenerateOrgAveModel(callAnalyzerData, currentDate, avgIndex, historyDuration, orgNum);
        }
        LOGGER.info("Model generate end: " + new DateTime(DateTimeZone.UTC).toString());

        // Do the alerting
        String  yesterday = AddDay(currentDate,-1);
        Dataset dataWithFlag = GenerateAlert(callAnalyzerData, yesterday, threshold, avgIndex, orgNum);

        // Generate alert message
        Dataset messages = createMessages(dataWithFlag);

        // Write alert message to ELK
        writeTOELK(messages, alertIndex, true);

    }

    private Dataset GenerateOrgAveModel(Dataset dataset, String endDate, String avgIndex, int duration, int orgNum) throws Exception {
        if(dataset  == null || dataset.count() == 0){ LOGGER.info("No data to calculate model"); return null; }

        // Avg call counts per top n org
        String historyStartDate = AddDay(endDate,-duration-1);
        Dataset avgModel = avgPerOrg(dataset, historyStartDate, endDate, true)
            .orderBy(desc("avg"))
            .limit(orgNum);

        if(avgModel!=null)avgModel.show(false);

        // Write Data Model to ELK, override.
        writeTOELK(avgModel, avgIndex,false);

        return avgModel;
    }

    private Dataset GenerateAlert(Dataset dataset, String targetDate, String threshold, String avgIndex, int orgNum) throws Exception{
        if(dataset  == null || dataset.count() == 0){ LOGGER.info("No ELK data for alert"); return null; }

        // Read avg call counts per org data model from ELK
        Dataset historyAvgData = readFromELKAvg(avgIndex)
            .withColumn("historyAvg", col("avg"));
        if(historyAvgData  == null || historyAvgData.count() == 0){ LOGGER.info("No data model"); return null; }
        historyAvgData.show(false);

        Dataset historyAvgDataSum = historyAvgData.agg(sum("historyAvg")).withColumn("historyAvgDataSum",col("sum(historyAvg)"));
        historyAvgDataSum.show(false);

        // Data of top n org that matters
        Dataset orgIdList = historyAvgData.select("org_id");
        orgIdList.show(false);

        Dataset orgData = orgIdList.join(dataset.alias("data"), orgIdList.col("org_id").equalTo(dataset.col("org_id")))
            .selectExpr("data.*");

        // Get Avg call count on target date
        Dataset currentAvgData = avgPerOrg(orgData, AddDay(targetDate,-1), AddDay(targetDate, 1), false)
            .withColumn("currentAvg", col("avg"));

        // TimeStamp for the alert generate time
        String eventTimestamp = new DateTime(DateTimeZone.UTC).toString();

        // No Data from all top n orgs
        if(currentAvgData == null || currentAvgData.count() == 0){

            Dataset dataWithFlagNoData = historyAvgDataSum
                .selectExpr(
                    "'All Top " + orgNum + " Orgs Have No Data' as org_id",
                    "0 as currentAvg",
                    " historyAvgDataSum as historyAvg",
                    "0 as percentage",
                    "'" + eventTimestamp + "' as eventTimestamp",
                    "'" + targetDate + "' as eventTime",
                    "'" + threshold + "' as threshold",
                    "'true' as status"
                );

            dataWithFlagNoData.show(false);
            return dataWithFlagNoData;
        }

        // Return after no data check on non-business day
        if(!DateUtil.isBusinessDay(targetDate)){ LOGGER.info("No alert for non-business day");return null;}

        // Missing specific top n org's data
        if(currentAvgData.count() < orgNum ){
            Dataset tempCurrentAvgData = orgIdList
                .alias("orgIdList")
                .join(
                    currentAvgData,
                    orgIdList.col("org_id").equalTo(currentAvgData.col("org_id")),
                    "leftanti"
                )
                .selectExpr(
                    "org_id",
                    "0 as avg",
                    "0 as currentAvg"
                );

            // Adding the missing data's current avg back
            currentAvgData = currentAvgData.union(tempCurrentAvgData);
        }

        Dataset dataWithFlag = currentAvgData
            .alias("currentAvgData")
            .join(
                historyAvgData,
                currentAvgData.col("org_id")
                .equalTo(historyAvgData.col("org_id"))
            )
            .selectExpr(
                "currentAvgData.org_id",
                "currentAvg",
                "historyAvg",
                "CAST((currentAvg/historyAvg) * 100 AS INT) as percentage",
                "'" + eventTimestamp + "' as eventTimestamp",
                "'" + targetDate + "' as eventTime",
                "'" + threshold + "' as threshold",
                "CASE WHEN (" +
                    " currentAvg IS NULL" +
                    " OR currentAvg/historyAvg < " + threshold +
                    " OR currentAvg/historyAvg > " + 2/Double.parseDouble(threshold) +
                    ") " +
                "THEN 'true' " +   // isAlert: true
                "ELSE 'false' " +
                "END as status"
            );

        dataWithFlag.show(false);

        return dataWithFlag;
    }

    private Dataset createMessages(Dataset dataset) throws Exception {
        if(dataset == null || dataset.count() == 0){ LOGGER.info("No alert");return null;}

        Dataset message = dataset
            .selectExpr(
                "'CRS' as component",
                "'Teams' as product",
                "'Reporting' as service",
                "'CallAnalyzer' as type",
                "eventTimestamp as eventTime",
                "'processing' as stage",
                "struct( " +
                    "eventTime as time, " +
                    "'orgID' as name, " +
                    "org_id as id, " +
                    "struct(" +
                    "currentAvg as volume, " +
                    "historyAvg as historicalAverageVolume, " +
                    "percentage, " +
                    "threshold as threshold " +
                    ") as value, " +
                    "status as isAlert" +
                    ") as metrics"
            );

        message.show(false);

        return message;
    }

    private String AddDay(String dt, int n)throws Exception{

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar c = Calendar.getInstance();

        c.setTime(sdf.parse(dt));
        c.add(Calendar.DATE, n);
        dt = sdf.format(c.getTime());

        return dt;
    }

    private String convertToStamp(String DateString) throws Exception{

        SimpleDateFormat sdff = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdft = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        sdff.setTimeZone(TimeZone.getTimeZone("GMT"));
        sdft.setTimeZone(TimeZone.getTimeZone("GMT"));

        Date startDate = sdff.parse(DateString);
        String startTimeStampString = sdft.format(startDate);

        return startTimeStampString;

    }

    private Dataset avgPerOrg(Dataset dataset, String startDate, String endDate, boolean isModelUpdate){
        if(dataset  == null || dataset.count() == 0){ LOGGER.info("No data to calculate avg"); return null; }

        // Change the time from timestamp to date string for aggregation
        Dataset result = dataset
            .cache()
            .withColumn("pdate", callUDF("convertTime", col("locus_start_time")));

        // Business day data within last 30 days
        Dataset historyData = result
            .filter(col("pdate").notEqual(""))
            .where( "to_date('" + startDate + "') < to_date(pdate)" + " AND " + "to_date(pdate) < to_date('" + endDate + "')");

        if(isModelUpdate){
            historyData = historyData.filter("isBusinessDay(pdate)");
        }

        // Calculate the avg per org
        Dataset avgModel = historyData
            .groupBy("org_id", "pdate")
            .agg(
                count(lit(1)).alias("numRec")
            );

        avgModel.show();

        avgModel = avgModel
            .groupBy("org_id")
            .agg(
                avg("numRec").alias("avg")
            );

        LOGGER.info("Avg start date: " + startDate);
        LOGGER.info("Avg end date: " + endDate);
        LOGGER.info("Avg input count: " + result.count());
        LOGGER.info("Avg valid history count: " + historyData.count());
        LOGGER.info("Avg output count: " + avgModel.count());

        return avgModel;
    }

    // ELK
    private Dataset readFromELK(String index) throws Exception {

        return spark.read()
            .format("org.elasticsearch.spark.sql")
            .option("es.net.http.auth.user", "waprestapi.gen")
            .option("es.net.http.auth.pass", "C1sco123!!")
            .option("es.nodes", "https://clpsj-bts-call.webex.com")
            //.option("es.nodes", "https://clpsj-call.webex.com")
            .option("es.port", "443")
            .option("es.nodes.path.prefix", "esapi")
            .option("es.nodes.wan.only", "true")
            .option("es.net.ssl", "true")
            .option("es.net.ssl.cert.allow.self.signed", "true")
            .option("es.mapping.date.rich", "false")
            .schema(ESSchema())
            .option(
                "es.read.field.as.array.include", // avoid nested array error outs
                "crid_media_audio_metrics.rx_media_e2e_lost_percent:2," +
                "crid_media_audio_metrics.rx_media_hop_lost:2," +
                "crid_media_audio_metrics.rx_media_session_jitter:2," +
                "crid_media_audio_metrics.rx_rtp_pkts:2," +
                "crid_media_audio_metrics.tx_rtt:2," +
                "crid_media_share_metrics.rx_media_e2e_lost_percent:2," +
                "crid_media_share_metrics.rx_media_hop_lost:2," +
                "crid_media_share_metrics.rx_media_session_jitter:2," +
                "crid_media_share_metrics.rx_rtp_pkts:2," +
                "crid_media_share_metrics.tx_rtt:2," +
                "crid_media_video_metrics.rx_media_e2e_lost_percent:2," +
                "crid_media_video_metrics.rx_media_hop_lost:2," +
                "crid_media_video_metrics.rx_media_session_jitter:2," +
                "crid_media_video_metrics.rx_rtp_pkts:2," +
                "crid_media_video_metrics.tx_rtt:2," +
                "rx_media_e2e_lost_percent.audio:2," +
                "rx_media_e2e_lost_percent.share:2," +
                "rx_media_e2e_lost_percent.video:2," +
                "rx_media_hop_lost.audio:2," +
                "rx_media_hop_lost.share:2," +
                "rx_media_hop_lost.video:2," +
                "rx_media_session_jitter.audio:2," +
                "rx_media_session_jitter.share:2," +
                "rx_media_session_jitter.video:2," +
                "rx_rtp_bitrate.audio:2," +
                "rx_rtp_bitrate.share:2," +
                "rx_rtp_bitrate.video:2," +
                "rx_rtp_pkts.audio:2," +
                "rx_rtp_pkts.share:2," +
                "rx_rtp_pkts.video:2," +
                "tx_avail_bitrate.audio:2," +
                "tx_avail_bitrate.share:2," +
                "tx_avail_bitrate.video:2," +
                "tx_queue_delay.audio:2," +
                "tx_queue_delay.share:2," +
                "tx_queue_delay.video:2," +
                "tx_rtp_bitrate.audio:2," +
                "tx_rtp_bitrate.share:2," +
                "tx_rtp_bitrate.video:2," +
                "tx_rtp_pkts.audio:2," +
                "tx_rtp_pkts.share:2," +
                "tx_rtp_pkts.video:2," +
                "tx_rtt.audio:2," +
                "tx_rtt.share:2," +
                "tx_rtt.video:2,"
            )
            .load(index + "/quality");

    }

    private Dataset readFromELKAvg(String index) throws Exception {

        return spark.read()
            .format("org.elasticsearch.spark.sql")
            .option("es.net.http.auth.user", "waprestapi.gen")
            .option("es.net.http.auth.pass", "C1sco123!!")
            .option("es.nodes", "https://clpsj-bts-call.webex.com")
            //.option("es.nodes", "https://clpsj-call.webex.com")
            .option("es.port", "443")
            .option("es.nodes.path.prefix", "esapi")
            .option("es.nodes.wan.only", "true")
            .option("es.net.ssl", "true")
            .option("es.net.ssl.cert.allow.self.signed", "true")
            .load(index + "/quality");

    }

    private void writeTOELK(Dataset dataset, String index, boolean isAppend) throws Exception{
        if(dataset == null || dataset.count()==0) {LOGGER.info("No alert to Write to ELK");return;}

        DataFrameWriter dfw = dataset.write()
            .format("org.elasticsearch.spark.sql")
            .option("es.net.http.auth.user", "waprestapi.gen")
            .option("es.net.http.auth.pass", "C1sco123!!")
            .option("es.nodes", "https://clpsj-bts-call.webex.com")
            //.option("es.nodes", "https://clpsj-call.webex.com")
            .option("es.port", "443")
            .option("es.nodes.path.prefix", "esapi")
            .option("es.nodes.wan.only", "true")
            .option("es.net.ssl", "true")
            .option("es.net.ssl.cert.allow.self.signed", "true");

        if(isAppend){
            dfw
                .mode("Append")
                //.mode("Overwrite")// Can use this to clean index
                .save(index + "/quality");
        }else{
            dfw
                .mode("Overwrite")
                .save(index + "/quality");
        }

    }

    private StructType ESSchema() {

        StructType metrics_schema = new StructType(
            new StructField[]{
                DataTypes.createStructField("rx_media_hop_lost", DataTypes.createArrayType(DataTypes.LongType), false),
                DataTypes.createStructField("rx_rtp_pkts", DataTypes.createArrayType(DataTypes.LongType), false),
                DataTypes.createStructField("tx_rtt", DataTypes.createArrayType(DataTypes.LongType), false),
                DataTypes.createStructField("rx_media_e2e_lost_percent", DataTypes.createArrayType(DataTypes.LongType), false),
                DataTypes.createStructField("rx_media_session_jitter", DataTypes.createArrayType(DataTypes.LongType), false)
            }
    );

        StructType detail_schema = new StructType(
            new StructField[]{
                DataTypes.createStructField("audio", DataTypes.createArrayType(DataTypes.LongType), false),
                DataTypes.createStructField("video", DataTypes.createArrayType(DataTypes.LongType), false),
                DataTypes.createStructField("share", DataTypes.createArrayType(DataTypes.LongType), false)
                }
        );

        StructType schema = new StructType(
            new StructField[]{
                DataTypes.createStructField("measurement", DataTypes.StringType, true),
                DataTypes.createStructField("mq_metric_type", DataTypes.StringType, true),
                DataTypes.createStructField("crid_media_type", DataTypes.StringType, false),
                DataTypes.createStructField("correlation_id", DataTypes.StringType, false),
                DataTypes.createStructField("time", DataTypes.StringType, false),
                DataTypes.createStructField("device_type", DataTypes.StringType, false),
                DataTypes.createStructField("session_type", DataTypes.StringType, false),
                DataTypes.createStructField("network_type", DataTypes.StringType, false),
                DataTypes.createStructField("media_agent_type", DataTypes.StringType, false),
                DataTypes.createStructField("crid_media_score", DataTypes.StringType, false),
                DataTypes.createStructField("crid_media_audio_score", DataTypes.StringType, false),
                DataTypes.createStructField("crid_media_video_score", DataTypes.StringType, false),
                DataTypes.createStructField("crid_media_share_score", DataTypes.StringType, false),
                DataTypes.createStructField("crid_media_reason", DataTypes.StringType, false),
                DataTypes.createStructField("server_group", DataTypes.StringType, true),
                DataTypes.createStructField("server_org", DataTypes.StringType, true),
                DataTypes.createStructField("is_cascade", DataTypes.StringType, false),
                DataTypes.createStructField("remote_server_group", DataTypes.StringType, true),
                DataTypes.createStructField("remote_server_org", DataTypes.StringType, true),
                DataTypes.createStructField("client_region", DataTypes.StringType, true),
                DataTypes.createStructField("count", DataTypes.StringType, true),
                DataTypes.createStructField("locus_session_id", DataTypes.StringType, false),
                DataTypes.createStructField("org_id", DataTypes.StringType, false),
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("tracking_id", DataTypes.StringType, false),
                DataTypes.createStructField("locus_id", DataTypes.StringType, false),
                DataTypes.createStructField("locus_start_time", DataTypes.StringType, true),
                DataTypes.createStructField("client_media_engine_software_version", DataTypes.StringType, false),
                DataTypes.createStructField("device_version", DataTypes.StringType, false),
                DataTypes.createStructField("server_alias", DataTypes.StringType, true),
                DataTypes.createStructField("remote_server_alias", DataTypes.StringType, true),
                DataTypes.createStructField("labels", DataTypes.StringType, false),
                DataTypes.createStructField("ip_reflexive_addr", DataTypes.StringType, false),
                DataTypes.createStructField("start_time", DataTypes.StringType, false),
                DataTypes.createStructField("is_webex_backed", DataTypes.BooleanType, true),
                DataTypes.createStructField("is_test", DataTypes.BooleanType, true),

                DataTypes.createStructField("crid_media_audio_metrics", metrics_schema, false),
                DataTypes.createStructField("crid_media_video_metrics", metrics_schema, false),
                DataTypes.createStructField("crid_media_share_metrics", metrics_schema, false),

                DataTypes.createStructField("tx_rtp_pkts", detail_schema, false),
                DataTypes.createStructField("tx_avail_bitrate", detail_schema, false),
                DataTypes.createStructField("tx_rtp_bitrate", detail_schema, false),
                DataTypes.createStructField("tx_queue_delay", detail_schema, false),
                DataTypes.createStructField("tx_rtt", detail_schema, false),
                DataTypes.createStructField("rx_rtp_pkts", detail_schema, false),
                DataTypes.createStructField("rx_media_hop_lost", detail_schema, false),
                DataTypes.createStructField("rx_rtp_bitrate", detail_schema, false),
                DataTypes.createStructField("rx_media_e2e_lost_percent", detail_schema, false),
                DataTypes.createStructField("rx_media_session_jitter", detail_schema, false),

                DataTypes.createStructField("kafka_timestamp", DataTypes.StringType, true),
                DataTypes.createStructField("spark_process_time", DataTypes.StringType, true)
            }
        );

        return schema;
    }

    // UDF
    public class TimeConverter implements UDF1<String, String> {
        public String call(String startTimeStampString) throws Exception {
            // Convert String "2019-01-16T08:01:28.121Z" to String "2019-01-16"
            SimpleDateFormat sdft = new SimpleDateFormat("yyyy-MM-dd");
            sdft.setTimeZone(TimeZone.getTimeZone("GMT"));
            String startDateString = "";
            try {
                Date startDate = sdft.parse(startTimeStampString);
                startDateString = sdft.format(startDate);
            }catch(Exception e){
                return startDateString; // This will be filtered out later
            }
            return startDateString;
        }
    }

    public class BusinessDay implements UDF1<String, Boolean> {
        public Boolean call(String startDate) throws Exception {
            return DateUtil.isBusinessDay(startDate);
        }
    }



}

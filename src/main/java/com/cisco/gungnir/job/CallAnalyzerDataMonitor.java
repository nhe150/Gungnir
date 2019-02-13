package com.cisco.gungnir.job;

// import com.cisco.gungnir.job.SparkDataMonitor.TimeConverter; // Do not work, copied locally
// import com.cisco.gungnir.job.SparkDataMonitor.BusinessDay;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static org.apache.spark.sql.functions.*;


public class CallAnalyzerDataMonitor implements Serializable {

    private SparkSession spark;

    public CallAnalyzerDataMonitor(SparkSession spark) throws Exception{
        this.spark = spark;
    }

    public void run() throws Exception {
        // Read from ELK
        Dataset callAnalyzerData = readFromELK("call_analyzer_test");
        callAnalyzerData.printSchema();

        // Get the temp model
        callAnalyzerData = GenerateOrgAveModel(callAnalyzerData, "2019-01-17");

        //

        // Write Data Model to ELK
        writeTOELK(callAnalyzerData, "call_analyzer_temp");
    }


    private Dataset readFromELK(String index) {

        return spark.read()
            .format("org.elasticsearch.spark.sql")
            .option("es.net.http.auth.user", "waprestapi.gen")
            .option("es.net.http.auth.pass", "C1sco123!")
            .option("es.nodes", "https://clpsj-bts-call.webex.com")
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
            .load(index + "/quality")
            .limit(100);

    }

    private void writeTOELK(Dataset dataset, String index){
        if(dataset == null || dataset.count()==0) {
            System.out.println("Nothing to Write"); return;
        }

        String resourcesPath = System.getProperty("user.dir") + "/src/test/resources/";

//      dataset.filter("CAST(start_time AS Date) = DATE_SUB(CURRENT_DATE(), 1)");

        dataset.write()
                .format("json")
                .mode("Overwrite")
                .json(resourcesPath + "temp/output3/");

        dataset.write()
                .format("org.elasticsearch.spark.sql")
                .option("es.net.http.auth.user", "waprestapi.gen")
                .option("es.net.http.auth.pass", "C1sco123!")
                .option("es.nodes", "https://clpsj-bts-call.webex.com")
                .option("es.port", "443")
                .option("es.nodes.path.prefix", "esapi")
                .option("es.nodes.wan.only", "true")
                .option("es.net.ssl", "true")
                .option("es.net.ssl.cert.allow.self.signed", "true")
                .mode("Overwrite")
                .save(index + "/quality");
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
                DataTypes.createStructField("rx_media_session_jitter", detail_schema, false)

             }
        );

        return schema;
    }

    private Dataset GenerateOrgAveModel(Dataset dataset, String targetDate) throws Exception {
        // Change the time from timestamp to date string for aggregation
        spark.udf().register("convertTime", new TimeConverter(), DataTypes.StringType);
        spark.udf().register("isBusinessDay", new BusinessDay(), DataTypes.BooleanType);
        Dataset result = dataset
            .cache()
            .withColumn("pdate", callUDF("convertTime", col("start_time")));

        // Business day data within last 30 days
        String historyStartDate = new DateTime(DateTimeZone.UTC).plusDays(-30).toString("yyyy-MM-dd");
        Dataset historyData = result
            .where( "to_date('" + historyStartDate + "') < to_date(pdate)" + " AND " + "to_date(pdate) < to_date('" + targetDate + "')")
            .filter("isBusinessDay(pdate)");
        //.limit(1);

        // Create the avg per top 50 org
        Dataset avgModel = historyData
                .groupBy("org_id", "pdate")
                .agg(
                    count(lit(1)).alias("numRec")
                )
                .orderBy(desc("numRec"))
                ;

        avgModel.show();

        avgModel = avgModel
            .groupBy("org_id")
            .agg(
                avg("numRec").alias("avg")
            )
            .orderBy(desc("avg"))
            .limit(50)
            ;
                //.selectExpr("org_id", "pdate", "count");
                //.groupBy("org_id", "pdate")
                //.avg("count");

        avgModel.show();

        // Test
        long input =  result.count();
        long history =  historyData.count();
        long output =  avgModel.count();
        System.out.println("historyStartDate: " + historyStartDate);
        System.out.println("targetDate: " + targetDate);
        System.out.println("input: " + input);
        System.out.println("history: " + history);
        System.out.println("output: " + output);

        return avgModel;
    }

    // UDF
    public class TimeConverter implements UDF1<String, String> {
        public String call(String startTimeStampString) throws Exception { // String "2019-01-16T08:01:28.121Z" to String "2019-01-16"
            SimpleDateFormat sdff = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            SimpleDateFormat sdft = new SimpleDateFormat("yyyy-MM-dd");
            sdff.setTimeZone(TimeZone.getTimeZone("GMT"));
            sdft.setTimeZone(TimeZone.getTimeZone("GMT"));

            Date  startDate = sdft.parse(startTimeStampString);

            String startDateString = sdft.format(startDate);

            return startDateString;
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

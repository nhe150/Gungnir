package com.cisco.gungnir.pipelines;

//import com.cisco.gungnir.config.ConfigProvider;
//import com.cisco.gungnir.job.SparkDataMonitor;
//import org.apache.spark.SparkConf;
//import org.apache.spark.sql.Dataset;
import com.cisco.gungnir.pipelines.Schema;
import org.apache.spark.sql.*;
import java.io.Serializable;
//import org.joda.time.DateTime;
//import org.joda.time.DateTimeZone;
//import org.apache.spark.SparkConf;
import org.apache.spark.sql.types.*;


public class CallAnalyzerMonitor implements Serializable {

    public static void main(String[] args) throws Exception {

        System.out.println("Hello");

        /*
        String configFile = cmd.getOptionValue("config");
        String currentDate  = cmd.getOptionValue("date");
        String threshold = cmd.getOptionValue("threshold")== null ? "0.3" : cmd.getOptionValue("threshold");
        String orgId = cmd.getOptionValue("orgId")== null ? "*" : "'" + cmd.getOptionValue("orgId") + "'";


        String configFile = "";
        String currentDate  = "";
        String threshold = "";
        String orgId = "";
        */

        SparkSession spark = SparkSession.builder()
                .master("local[4]")
                .appName("CallAnalyzerMonitor")
                .getOrCreate();


        System.out.println("Hello");

        Dataset CallAnalyzerData = spark.read()
            .format("org.elasticsearch.spark.sql")
            .option("es.net.http.auth.user", "waprestapi.gen")
            .option("es.net.http.auth.pass", "C1sco123!")
            .option("es.nodes", "https://clpsj-bts-call.webex.com")
            .option("es.port", "443")
            .option("es.nodes.path.prefix", "esapi")
            .option("es.nodes.wan.only", "true")
            .option("es.net.ssl", "true")
            .option("es.net.ssl.cert.allow.self.signed", "true")
            .option("es.mapping.date.rich","false")
            //.schema(Schema.schema())
            .schema(ESSchema())
            .option("es.read.field.as.array.include", //es.read.field.as.array.include
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
            .load("call_analyzer/quality").limit(1);

        CallAnalyzerData.printSchema();

        //CallAnalyzerData.filter("CAST(start_time AS Date) = DATE_SUB(CURRENT_DATE(), 1)");
                       // .withColumn("crid_media_audio_metrics.rx_media_e2e_lost_percent", CallAnalyzerData(""));

        //CallAnalyzerData.show(1);

/*
        CallAnalyzerData.write()
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
                .save("call_analyzer_test/quality");

*/
        String resourcesPath = System.getProperty("user.dir") + "/src/test/resources/";

        CallAnalyzerData.write()
                .format("json")
                .mode("Overwrite")
                .json(resourcesPath + "temp/output1/");



        System.out.println("Hello");
        //System.out.println(CallAnalyzerData.count());

        /*
        ConfigProvider appConfigProvider = new ConfigProvider(spark, configFile);

        SparkDataMonitor app = new SparkDataMonitor(spark, appConfigProvider){

            @Override
            public void run(String currentDate, String threshold, String orgId) throws Exception {

                if(currentDate==null) currentDate = new DateTime(DateTimeZone.UTC).plusDays(-1).toString("yyyy-MM-dd");
                if(!isBusinessDay(currentDate)) return;

                Dataset aggregates = queryFunctions.cassandra.readFromCassandra("batch", configProvider.getAppConfig());

                Dataset data = allCounts(aggregates, orgId, currentDate);

                Dataset dataWithFlag = dataWithFlag(currentDate, data, threshold);

                Dataset messages = createMessages(dataWithFlag);

                queryFunctions.kafka.writeToKafka(messages, "batch", configProvider.getAppConfig());

            }

            @Override
            protected Dataset createMessages(Dataset dataset) throws Exception {
                Dataset message = dataset.where("status = 'failure'").selectExpr(
                        "'crs' as component",
                        "'metrics' as eventtype",
                        "struct('Spark' as pipeLine, 'DataProcess' as phase, CONCAT(pdate, 'T00:00:00Z') as sendTime, struct(CONCAT(orgid, '_', relation_name) as name, pdate as reportDate, count as volume, avg as historicalAverageVolume, percentage, status) as data) as metrics");
                return message;
            }
        };

        app.run(currentDate, threshold, orgId);
        */

    }


     static public StructType ESSchema() {


         StructType metrics_schema = new StructType(
             new StructField[]{
                 DataTypes.createStructField("rx_media_hop_lost",DataTypes.createArrayType(DataTypes.LongType),false),
                 DataTypes.createStructField("rx_rtp_pkts",DataTypes.createArrayType(DataTypes.LongType),false),
                 DataTypes.createStructField("tx_rtt",DataTypes.createArrayType(DataTypes.LongType),false),
                 DataTypes.createStructField("rx_media_e2e_lost_percent",DataTypes.createArrayType(DataTypes.LongType),false),
                 DataTypes.createStructField("rx_media_session_jitter",DataTypes.createArrayType(DataTypes.LongType),false)
             }
         );

         StructType detail_schema = new StructType(
             new StructField[]{
                 DataTypes.createStructField("audio",DataTypes.createArrayType(DataTypes.LongType),false),
                 DataTypes.createStructField("video",DataTypes.createArrayType(DataTypes.LongType),false),
                 DataTypes.createStructField("share",DataTypes.createArrayType(DataTypes.LongType),false)
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

                DataTypes.createStructField("crid_media_audio_metrics", metrics_schema,false),
                DataTypes.createStructField("crid_media_video_metrics", metrics_schema,false),
                DataTypes.createStructField("crid_media_share_metrics", metrics_schema,false),

                DataTypes.createStructField("tx_rtp_pkts", detail_schema,false),
                DataTypes.createStructField("tx_avail_bitrate", detail_schema,false),
                DataTypes.createStructField("tx_rtp_bitrate", detail_schema,false),
                DataTypes.createStructField("tx_queue_delay", detail_schema,false),
                DataTypes.createStructField("tx_rtt", detail_schema,false),
                DataTypes.createStructField("rx_rtp_pkts", detail_schema,false),
                DataTypes.createStructField("rx_media_hop_lost", detail_schema,false),
                DataTypes.createStructField("rx_rtp_bitrate", detail_schema,false),
                DataTypes.createStructField("rx_media_e2e_lost_percent", detail_schema,false),
                DataTypes.createStructField("rx_media_session_jitter", detail_schema,false)

            }
         );

        return schema;
    }

}


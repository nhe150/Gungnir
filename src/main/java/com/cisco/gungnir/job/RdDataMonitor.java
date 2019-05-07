package com.cisco.gungnir.job;

import com.cisco.gungnir.config.ConfigProvider;
import com.cisco.gungnir.query.QueryFunctions;
import com.cisco.gungnir.job.SparkDataMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import java.util.logging.Logger;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class RdDataMonitor implements Serializable {
    private SparkSession spark;
    private ConfigProvider configProvider;
    private QueryFunctions queryFunctions;

    private static final Logger LOGGER = Logger.getLogger(RdDataMonitor.class.getName());

    public RdDataMonitor(SparkSession spark, ConfigProvider appConfigProvider) throws Exception{
        ConfigProvider gungnirConfigProvider = new ConfigProvider(spark, appConfigProvider.retrieveAppConfigValue("gungnirConfigFile"));
        ConfigProvider mergedConfigProvider =  new ConfigProvider(spark, ConfigProvider.merge(gungnirConfigProvider.getAppConfig().deepCopy(), appConfigProvider.getAppConfig().deepCopy()));

        this.spark = spark;
        this.configProvider = mergedConfigProvider;
        this.queryFunctions = new QueryFunctions(spark, mergedConfigProvider);
    }

    private Dataset getAvgPerOrg(Dataset ds, String currentDate) {

        spark.udf().register("isBusinessDay", new BusinessDay(), DataTypes.BooleanType);

        String startDate = new DateTime(DateTimeZone.UTC).plusDays(-30).toString("yyyy-MM-dd");

        Dataset history = ds.where( "to_date('" + startDate + "') < to_date(pdate)" + " AND " + "to_date(pdate) < to_date('" + currentDate + "')")
                .filter("isBusinessDay(pdate)");  // only count biz days within last 30 days.


        Dataset ds2 = history.groupBy("orgid","pdate").agg(
                sum("incall").as("incall"),
                sum("local_share_cable").as("cable"),
                sum("local_share_wireless").as("wireless"),
                sum("white_boarding").as("whiteboard"),
                countDistinct("deviceid").as("deviceCount")  // note: the # of DISTINCT deviceID could change everyday.
        );

        Dataset ds3 = ds2.withColumn("incall", col("incall").divide(col("deviceCount")))
                .withColumn("cable", col("cable").divide(col("deviceCount")))
                .withColumn("wireless", col("wireless").divide(col("deviceCount")))
                .withColumn("whiteboard", col("whiteboard").divide(col("deviceCount")));

        Dataset avgPerOrg = ds3.groupBy("orgid").agg(
                avg("incall").as("incallAvg"),
                avg(col("cable")).as("cableAvg"),
                avg("wireless").as("wirelessAvg"),
                avg("whiteboard").as("whiteboardAvg")
        );


        System.out.println("@@@@@@@@@@@@@ getAvgPerOrg: @@@@@@@@@@@@@");

        avgPerOrg.show();

        return avgPerOrg;
    }

    private Dataset getCurrentDateData(Dataset ds, String currentDate) {


        LOGGER.info("@@@@@@@@@@@@@ Below is current date data @@@@@@@@@@@@\n");
        LOGGER.info("currentDate is: " +  currentDate);

        Dataset curr = ds.where("pdate='" + currentDate + "'");

        Dataset c1 = curr.groupBy("orgid", "pdate").agg(
                // keep pdate here because of there's a JOIN on table next, we want to keep current date.
                sum("incall").as("incall"),
                sum("local_share_cable").as("cable"),
                sum("local_share_wireless").as("wireless"),
                sum("white_boarding").as("whiteboard"),
                countDistinct("deviceid").as("deviceCount")
        );

        Dataset c2 = c1.withColumn("incall", col("incall").divide(col("deviceCount")))
                .withColumn("cable", col("cable").divide(col("deviceCount")))
                .withColumn("wireless", col("wireless").divide(col("deviceCount")))
                .withColumn("whiteboard", col("whiteboard").divide(col("deviceCount")));

        LOGGER.info("@@@@@@@@@@@@ current date data @@@@@@@@@@@@\n");
        c2.show();

        return c2;
    }

    private Dataset getAnomaly(Dataset avgPerOrg, Dataset c2, double threshold) {

        Dataset joined = avgPerOrg.join(c2.alias("c2"), c2.col("orgid").equalTo(avgPerOrg.col("orgid")));

        System.out.println("@@@@@@@@@@@@ (below) after joined @@@@@@@@@@@@\n");

        joined.show();

        Dataset joinedWithPercentage = joined
                .withColumn("incallDiffPctg", (col("incall").minus(col("incallAvg"))).divide(col("incallAvg"))
                        .multiply(100).cast("int"))
                .withColumn("cableDiffPctg", (col("cable").minus(col("cableAvg"))).divide(col("cableAvg"))
                        .multiply(100).cast("int"))
                .withColumn("wirelessDiffPctg", (col("wireless").minus(col("wirelessAvg"))).divide(col("wirelessAvg"))
                        .multiply(100).cast("int"))
                .withColumn("whiteboardDiffPctg", (col("whiteboard").minus(col("whiteboardAvg"))).divide(col("whiteboardAvg"))
                        .multiply(100).cast("int"));

        LOGGER.info("@@@@@@@@@@@@@ Threshold is: " + threshold);

        Dataset normal = joinedWithPercentage.filter(col("incall").geq(col("incallAvg").multiply(1 - threshold)))

                .filter(col("incall").leq(col("incallAvg").multiply(1 + threshold)))
                .filter(col("cable").geq(col("cableAvg").multiply(1 - threshold)))
                .filter(col("cable").leq(col("cableAvg").multiply(1 + threshold)))
                .filter(col("wireless").geq(col("wirelessAvg").multiply(1 - threshold)))
                .filter(col("wireless").leq(col("wirelessAvg").multiply(1 + threshold)))
                .filter(col("whiteboard").geq(col("whiteboardAvg").multiply(1 - threshold)))
                .filter(col("whiteboard").leq(col("whiteboardAvg").multiply(1 + threshold)))
                ;


        Dataset anomaly = joinedWithPercentage.except(normal);

        System.out.println("anomaly below");

        anomaly.show();

        return anomaly;
    }

    public void run(String currentDate, String threshold, String orgId) throws Exception {
        if(currentDate==null) currentDate = new DateTime(DateTimeZone.UTC).plusDays(-1).toString("yyyy-MM-dd");
        LOGGER.info("Entering program. currentDate: " + currentDate);

        if(!isBusinessDay(currentDate)) return;

        Dataset input = queryFunctions.cassandra.readFromCassandra("batch", configProvider.getAppConfig());


        List<String> orgList = getOrgList(configProvider.getAppConfig());

        System.out.println("orgList:");

        for (String org : orgList) {
            System.out.println(org);
        }
        String whereOrgIdClause = whereOrgId(orgList);

        Dataset dsFilteredByOrgs = input.where("orgid in (" + whereOrgIdClause + ")");

        System.out.println("dsFilteredByOrgs:\n");
        dsFilteredByOrgs.show();


        Dataset avgPerOrg = getAvgPerOrg(dsFilteredByOrgs, currentDate);

        Dataset c2 = getCurrentDateData(dsFilteredByOrgs, currentDate);  // c means currentDate

        Dataset anomaly = getAnomaly(avgPerOrg, c2, Double.parseDouble(threshold));


        Dataset result = createMessages(anomaly);

       queryFunctions.kafka.writeToKafka(result, "batch", configProvider.getAppConfig());
    }


    private Dataset createMessages(Dataset dataset) throws Exception {
        Dataset message = dataset.selectExpr(
                "'crs' as component",
                "'metrics' as eventtype",
                "struct('RoomDeviceUsg' as pipeLine, " +
                        "'anomalyDetection' as phase, " +
                        "CONCAT(pdate, 'T00:00:00Z') as sendTime, " +
                        "struct(" +
                            "c2.orgid, " +
                            "incall, " +
                            "cable, " +
                            "wireless," +
                            "whiteboard, " +
                            "incallAvg, " +
                            "cableAvg, " +
                            "wirelessAvg, " +
                            "whiteboardAvg, " +
                            "incallDiffPctg," +
                            "cableDiffPctg," +
                            "wirelessDiffPctg," +
                            "whiteboardDiffPctg," +
                            "deviceCount, " +
                            "pdate as reportDate )" +
                        " as data ) " +
                 "as metrics");

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

    private String whereOrgId(List<String> orgList) {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < orgList.size(); i++) {
            sb.append("'").append(orgList.get(i)).append("',");
        }
        sb.deleteCharAt(sb.length() - 1);  // remove last "'"
        return sb.toString();
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

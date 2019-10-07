package com.cisco.gungnir.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.logging.Logger;

import static com.cisco.gungnir.utils.DateUtil.isBusinessDay;
import static org.apache.spark.sql.functions.*;

public class RdDataMonitor extends DataMonitor {

    private static final Logger LOGGER = Logger.getLogger(RdDataMonitor.class.getName());

    public RdDataMonitor(){
        super();
    }

    private Dataset getAvgPerOrg(Dataset ds, String currentDate) {

        String startDate = getDate(-30);

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
        String whereOrgIdClause = whereOrgId(orgList);
        Dataset dsFilteredByOrgs = input.where("orgid in (" + whereOrgIdClause + ")");

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








}

package com.cisco.gungnir.job

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.DataTypes
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util
import java.util.logging.Logger

import org.apache.spark.sql.functions._

object RdDataMonitor {
  val LOGGER = Logger.getLogger(classOf[RdDataMonitor].getName)
}

class RdDataMonitor() extends DataMonitor {
  private def getAvgPerOrg(ds: Dataset[_], currentDate: String) = {
    val startDate = getDate(-30)
    val history = ds.where("to_date('" + startDate + "') < to_date(pdate)" + " AND " + "to_date(pdate) < to_date('" + currentDate + "')") // only count biz days within last 30 days.
    val ds2 = history.groupBy("orgid", "pdate").agg(sum("incall").as("incall"),
      sum("local_share_cable").as("cable"),
      sum("local_share_wireless").as("wireless"),
      sum("white_boarding").as("whiteboard"),
      countDistinct("deviceid").as("deviceCount"))
    // note: the # of DISTINCT deviceID could change everyday.)

    val ds3 = ds2.withColumn("incall", col("incall").divide(col("deviceCount")))
      .withColumn("cable",  col("cable").divide(col("deviceCount")))
      .withColumn("wireless", col("wireless").divide(col("deviceCount")))
      .withColumn("whiteboard", col("whiteboard").divide(col("deviceCount")))
    val avgPerOrg = ds3.groupBy("orgid").agg(avg("incall").cast("int").as("incallAvg"), avg(col("cable")).cast("int").as("cableAvg"), avg("wireless").cast("int").as("wirelessAvg"), avg("whiteboard").cast("int").as("whiteboardAvg"))
    System.out.println("@@@@@@@@ getAvgPerOrg: @@@@@@@@@@@@")
    avgPerOrg.show()
    avgPerOrg
  }

  private def getCurrentDateData(ds: Dataset[_], currentDate: String) = {
    RdDataMonitor.LOGGER.info("currentDate is: " + currentDate)
    val curr = ds.where("pdate='" + currentDate + "'")
    val c1 = curr.groupBy("orgid", "pdate").agg( // keep pdate here because of there's a JOIN on table next, we want to keep current date.
      sum("incall").as("incall"), sum("local_share_cable").as("cable"), sum("local_share_wireless").as("wireless"), sum("white_boarding").as("whiteboard"), countDistinct("deviceid").as("deviceCount"))
    val c2 = c1.withColumn("incall", col("incall").divide(col("deviceCount"))).withColumn("cable", col("cable").divide(col("deviceCount"))).withColumn("wireless", col("wireless").divide(col("deviceCount"))).withColumn("whiteboard", col("whiteboard").divide(col("deviceCount")))
    c2.show()
    c2
  }

  /**
    *
    * @param avgPerOrg
    * @param c2
    * @param threshold
    * @return anomaly and normal dataset as tuple
    */
  private def getAnomaly(avgPerOrg: Dataset[_], c2: Dataset[_], threshold: Double)= {
    val joined = avgPerOrg.join(c2.alias("c2"), c2.col("orgid").equalTo(avgPerOrg.col("orgid")))
    System.out.println("@@@@@@@@@@@@ (below) after joined @@@@@@@@@@@@\n")
    joined.show()
    val joinedWithPercentage = joined.withColumn("incallDiffPctg", col("incall").minus(col("incallAvg")).divide(col("incallAvg")).multiply(100).cast("int")).withColumn("cableDiffPctg", col("cable").minus(col("cableAvg")).divide(col("cableAvg")).multiply(100).cast("int")).withColumn("wirelessDiffPctg", col("wireless").minus(col("wirelessAvg")).divide(col("wirelessAvg")).multiply(100).cast("int")).withColumn("whiteboardDiffPctg", col("whiteboard").minus(col("whiteboardAvg")).divide(col("whiteboardAvg")).multiply(100).cast("int"))
    RdDataMonitor.LOGGER.info("@@@@@@@@@@@@@ Threshold is: " + threshold)
    val normal = joinedWithPercentage.filter(col("incall").geq(col("incallAvg").multiply(1 - threshold))).filter(col("incall").leq(col("incallAvg").multiply(1 + threshold))).filter(col("cable").geq(col("cableAvg").multiply(1 - threshold))).filter(col("cable").leq(col("cableAvg").multiply(1 + threshold))).filter(col("wireless").geq(col("wirelessAvg").multiply(1 - threshold))).filter(col("wireless").leq(col("wirelessAvg").multiply(1 + threshold))).filter(col("whiteboard").geq(col("whiteboardAvg").multiply(1 - threshold))).filter(col("whiteboard").leq(col("whiteboardAvg").multiply(1 + threshold)))
    val anomaly: Dataset[Row] = joinedWithPercentage.except(normal)
    System.out.println("anomaly below")
    anomaly.show()
    (anomaly,  normal)

  }

  @throws[Exception]
  override def run(xcurrentDate: String, threshold: String, orgId: String): Unit = {

    val myDate = if (xcurrentDate == null)
      new DateTime(DateTimeZone.UTC).plusDays(-1).toString("yyyy-MM-dd")
    else
      xcurrentDate


    RdDataMonitor.LOGGER.info("Entering program. currentDate: " + myDate)
    

    //need to scrutinize
    val input = readFromCass().toDF()
    val orgList = getOrgList(configProvider.getAppConfig)
    val whereOrgIdClause = whereOrgId(orgList)

    val dsFilteredByOrgs = input.where("orgid in (" + whereOrgIdClause + ")")


    val avgPerOrg = getAvgPerOrg(dsFilteredByOrgs, myDate)
    val c2 = getCurrentDateData(dsFilteredByOrgs, myDate) // c means currentDate
    val (anomaly, normal)  = getAnomaly(avgPerOrg, c2, threshold.toDouble)
    val anomalyMessages = createMessages(anomaly, true)
    writeToKafka(anomalyMessages, false)

    val normalMessages = createMessages(normal, false)
    writeToKafka(normalMessages, false)


  }

  @throws[Exception]
  private def createMessages(dataset: Dataset[Row], alert: Boolean) = {
    val message = dataset.selectExpr("'crs' as component",
      "'metrics' as eventtype",
      "struct('RoomDeviceUsg' as pipeLine, " + "'anomalyDetection' as phase, " + "CONCAT(pdate, 'T00:00:00Z') as sendTime, " +
        "struct(" + "c2.orgid, " + "incall, " + "cable, " + "wireless," + "whiteboard, " + "incallAvg, " +
        "cableAvg, " + "wirelessAvg, " + "whiteboardAvg, " +
        "incallDiffPctg," + "cableDiffPctg," + "wirelessDiffPctg," +
        "whiteboardDiffPctg," + "deviceCount, "  +
        " '" + alert  + "' as isAlert, " +
        "pdate as reportDate )" +
        " as data ) " +
        "as metrics")
    message
  }
}
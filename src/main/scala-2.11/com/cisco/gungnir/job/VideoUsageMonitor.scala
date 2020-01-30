package com.cisco.gungnir.job

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.DataTypes
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util
import java.util.logging.Logger

import com.cisco.gungnir.utils.DateUtil.isBusinessDay
import org.apache.spark.sql.functions._

object VideoUsageMonitor {
  val LOGGER = Logger.getLogger(classOf[VideoUsageMonitor].getName)
}

class VideoUsageMonitor() extends DataMonitor {

  private def getSumPerOrg(ds: Dataset[_], currentDate: String) = {
    val sumPerOrg = ds.groupBy("orgid", "pdate").agg(
      sum("videotime").as("video time "))
    // note: the # of DISTINCT deviceID could change everyday.)

    System.out.println("@@@@@@@@ getSumPerOrg: @@@@@@@@@@@@")
    sumPerOrg.show()
    sumPerOrg
  }

  @throws[Exception]
  override def run(xcurrentDate: String, threshold: String, orgId: String): Unit = {

    val myDate = if (xcurrentDate == null)
      new DateTime(DateTimeZone.UTC).plusDays(-1).toString("yyyy-MM-dd")
    else
      xcurrentDate


    VideoUsageMonitor.LOGGER.info("Entering program. currentDate: " + myDate)
    if (!isBusinessDay(myDate)) return

    //need to scrutinize
    val input = readFromCass().toDF()
    val orgList = getOrgList(configProvider.getAppConfig)
    val whereOrgIdClause = whereOrgId(orgList)

    val dsFilteredByOrgs = input.where("orgid in (" + whereOrgIdClause + ") and relation_name='videoUsage' and pdate = '" + myDate + "'")

    val sumPerOrg = getSumPerOrg(dsFilteredByOrgs, myDate)
    val videoUsageMsges = createMessages(sumPerOrg, true)
    writeToKafka(videoUsageMsges, false)
  }

  @throws[Exception]
  private def createMessages(dataset: Dataset[Row], alert: Boolean) = {
    val message = dataset.selectExpr("'crs' as component",
      "'metrics' as eventtype",
      "struct('videoUsage' as pipeLine, " +
        "struct(" + "orgid, " + "videotime, " +
        "pdate as reportDate )" +
        " as data ) " +
        "as metrics")
    message
  }
}
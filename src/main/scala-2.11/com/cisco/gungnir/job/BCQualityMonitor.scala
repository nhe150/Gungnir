package com.cisco.gungnir.job

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.DataTypes
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util
import java.util.logging.Logger


import org.apache.spark.sql.functions._

object BCQualityMonitor {
  val LOGGER = Logger.getLogger(classOf[BCQualityMonitor].getName)
}

class BCQualityMonitor() extends DataMonitor {

  private def getSumPerOrg(ds: Dataset[_], currentDate: String) = {
    val sumPerOrg = ds.groupBy("orgid", "pdate").agg(
      sum("duration").as("duration"),
      sum("audio_is_good").as("audio_is_good"))
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


    BCQualityMonitor.LOGGER.info("Entering program. currentDate: " + myDate)
    

    //need to scrutinize
    val input = readFromCass().toDF()
    val orgList = getOrgList(configProvider.getAppConfig)
    val whereOrgIdClause = whereOrgId(orgList)

    val dsFilteredByOrgs = input.where("orgid in (" + whereOrgIdClause + ") and relation_name='callQuality' and pdate = '" + myDate + "' ")

    val sumPerOrg = getSumPerOrg(dsFilteredByOrgs, myDate)
    val BCQualityMsges = createMessages(sumPerOrg, true)
    writeToKafka(BCQualityMsges, false)
  }

  @throws[Exception]
  private def createMessages(dataset: Dataset[Row], alert: Boolean) = {
    val message = dataset.selectExpr("'crs' as component",
      "'metrics' as eventtype",
      "struct('BCQuality' as pipeLine, " +
        "struct(" + "orgid, " + "duration, " + "audio_is_good, " +
        "pdate as reportDate )" +
        " as data ) " +
        "as metrics")
    message
  }
}
package com.cisco.gungnir.job

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.DataTypes
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util
import java.util.logging.Logger


import org.apache.spark.sql.functions._

object SparkCallQualityMonitor {
  val LOGGER = Logger.getLogger(classOf[SparkCallQualityMonitor].getName)
}

class SparkCallQualityMonitor() extends DataMonitor {

  private def getSumPerOrg(ds: Dataset[_], currentDate: String) = {
    val sumPerOrg = ds.groupBy("orgid", "time_stamp").agg(
      sum("usercountbyorg").as("usercountbyorg"),
      sum("number_of_bad_calls").as("bad_calls"),
      sum("number_of_total_calls").as("total_calls")
      )
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

    SparkCallQualityMonitor.LOGGER.info("Entering program. currentDate: " + myDate)

    val month = myDate.toString().substring(0,7)
    
    //need to scrutinize
    val input = readFromCass().toDF()
    val orgList = getOrgList(configProvider.getAppConfig)
    val whereOrgIdClause = whereOrgId(orgList)

    val dsFilteredByOrgs = input.where("orgid in (" + whereOrgIdClause + ") and relation_name='callQuality' and month = '" + month + "' and period = 'daily' and time_stamp = '" + myDate + "' ")

    val sumPerOrg = getSumPerOrg(dsFilteredByOrgs, myDate)
    val sparkcallQualityMsges = createMessages(sumPerOrg, true)
    writeToKafka(sparkcallQualityMsges, false)
  }

  @throws[Exception]
  private def createMessages(dataset: Dataset[Row], alert: Boolean) = {
    val message = dataset.selectExpr("'crs' as component",
      "'metrics' as eventtype",
      "struct('Spark' as pipeLine, " +
        "struct(" + "orgid, " + "bad_calls, " + "total_calls, " + 
        "time_stamp as reportDate, " + "usercountbyorg as volume )" +
        " as data ) " +
        "as metrics")
    message
  }
}
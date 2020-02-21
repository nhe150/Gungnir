package com.cisco.gungnir.job

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.DataTypes
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util
import java.util.logging.Logger


import org.apache.spark.sql.functions._

object SparkTopUserMonitor {
  val LOGGER = Logger.getLogger(classOf[SparkTopUserMonitor].getName)
}

class SparkTopUserMonitor() extends DataMonitor {

  private def getSumPerOrg(ds: Dataset[_], currentDate: String) = {
    val sumPerOrg = ds.groupBy("orgid", "time_stamp").agg(
      count("userid").as("number_of_userid"),
      sum("usercountbyorg").as("usercountbyorg"),
      sum("messages").as("messages"),
      sum("calls").as("calls")
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

    SparkTopUserMonitor.LOGGER.info("Entering program. currentDate: " + myDate)

    val month = myDate.toString().substring(0,7)
    
    //need to scrutinize
    val input = readFromCass().toDF()
    val orgList = getOrgList(configProvider.getAppConfig)
    val whereOrgIdClause = whereOrgId(orgList)

    val dsFilteredByOrgs = input.where("orgid in (" + whereOrgIdClause + ") and relation_name='topUser' and month = '" + month + "' and period = 'daily' and time_stamp = '" + myDate + "' ")

    val sumPerOrg = getSumPerOrg(dsFilteredByOrgs, myDate)
    val sparktopUserMsges = createMessages(sumPerOrg, true)
    writeToKafka(sparktopUserMsges, false)
  }

  @throws[Exception]
  private def createMessages(dataset: Dataset[Row], alert: Boolean) = {
    val message = dataset.selectExpr("'crs' as component",
      "'metrics' as eventtype",
      "struct('Spark' as pipeLine, " +
        "struct(" + "orgid, " + "messages, " + "calls, " + "number_of_userid, " +
        "time_stamp as reportDate, " + "usercountbyorg as volume )" +
        " as data ) " +
        "as metrics")
    message
  }
}
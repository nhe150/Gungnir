package com.cisco.gungnir.job

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util
import java.util.logging.Logger


import org.apache.spark.sql.functions._

object SparkAggTableMonitor {
  val LOGGER = Logger.getLogger(classOf[SparkAggTableMonitor].getName)
}

class SparkAggTableMonitor() extends DataMonitor {

  private def getSumPerOrg(ds: Dataset[_], currentDate: String, region: String) = {
    val sumPerOrg = ds.groupBy("relation_name", "orgid", "time_stamp").agg(
      sum("usercountbyorg").as("usercountbyorg"),
      sum("onetoonecount").as("onetoonecount"),
      sum("spacecount").as("spacecount"),
      sum("number_of_successful_calls").as("successful_calls"),
      sum("number_of_minutes").as("minutes"),
      sum("number_of_bad_calls").as("bad_calls"),
      sum("number_of_total_calls").as("total_calls"),
      sum("files").as("files"),
      sum("filesize").as("filesize"),
      count("ua_category").as("number_of_ua_category"),
      sum("messages").as("message"),
      count("userid").as("number_of_userid"),
      sum("messages").as("messages"),
      sum("calls").as("calls")
      )
    // note: the # of DISTINCT deviceID could change everyday.)

    val output = sumPerOrg.withColumn("region", functions.lit(region))
    System.out.println("@@@@@@@@ getSumPerOrg: @@@@@@@@@@@@")
    output.show()
    output
  }

  @throws[Exception]
  override def run(xcurrentDate: String, threshold: String, orgId: String): Unit = {
    run(xcurrentDate, threshold, orgId, null, null)
  }

  @throws[Exception]
  override def run(xcurrentDate: String, threshold: String, orgId: String, region: String, relationNames: String): Unit = {

    val myDate = if (xcurrentDate == null)
      new DateTime(DateTimeZone.UTC).plusDays(-1).toString("yyyy-MM-dd")
    else
      xcurrentDate

    val relationNameInClause = if (relationNames == null) 
      getInClauseFromList(getRelationNamesSparkAgg())
    else 
      getInClauseFromString(relationNames)
    
    SparkAggTableMonitor.LOGGER.info("Entering program. currentDate: " + myDate)
    

    //need to scrutinize
    val input = readFromCass().toDF()
    val orgList = getOrgList(configProvider.getAppConfig)
    val whereOrgIdClause = whereOrgId(orgList)
    
    val dsFilteredByOrgs = input.where("orgid in (" + whereOrgIdClause + ") and relation_name in ("+ relationNameInClause +") and period = 'daily' and time_stamp = '" + myDate + "' ")
    

    val sumPerOrg = getSumPerOrg(dsFilteredByOrgs, myDate, region)
    val activeUserMsges = createMessages(sumPerOrg, true)
    writeToKafka(activeUserMsges, false)
  }

  @throws[Exception]
  private def createMessages(dataset: Dataset[Row], alert: Boolean) = {
    val message = dataset.selectExpr("'crs' as component",
      "'metrics' as eventtype",
      "struct(" + "relation_name"+" as pipeLine, " +
        "struct(" + "orgid, " + "usercountbyorg, " + "onetoonecount, " + "spacecount, " + "successful_calls, " +
         "minutes, " + "bad_calls, " + "total_calls, " + "files, " + "filesize, " + "number_of_ua_category, " + "message, " + "number_of_userid, " + "messages, " + "calls, " + "region, " +
        "time_stamp as reportDate )" +
        " as data ) " +
        "as metrics")
    message
  }
}
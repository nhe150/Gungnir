package com.cisco.gungnir.job

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util
import java.util.logging.Logger


import org.apache.spark.sql.functions._

object SparkDataTableMonitor {
  val LOGGER = Logger.getLogger(classOf[SparkDataTableMonitor].getName)
}

class SparkDataTableMonitor() extends DataMonitor {

  private def getSumPerOrg(ds: Dataset[_], currentDate: String, region: String) = {
    val sumPerOrg = ds.groupBy("relation_name", "orgid", "pdate").agg(
      sum("iscall").as("call"),
      sum("ismessage").as("message"),
      sum("legduration").as("legduration"),
      sum("duration").as("duration"),
      sum("audio_is_good").as("audio_is_good"),
      sum("contentsize").as("contentsize"),
      sum("isfile").as("isfile"),
      count("group").as("number_of_groups"),
      sum("videotime").as("videotime"))
    // note: the # of DISTINCT deviceID could change everyday.)

    val output = sumPerOrg.withColumn("region", functions.lit(region))
    //sumPerOrg.withColumn("region", region)
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
      getInClauseFromList(getRelationNamesSparkData())
    else 
      getInClauseFromString(relationNames)
    
    SparkDataTableMonitor.LOGGER.info("Entering program. currentDate: " + myDate)
    

    //need to scrutinize
    val input = readFromCass().toDF()
    val orgList = getOrgList(configProvider.getAppConfig)
    val whereOrgIdClause = whereOrgId(orgList)
    
    val dsFilteredByOrgs = input.where("orgid in (" + whereOrgIdClause + ") and relation_name in ("+ relationNameInClause +") and pdate = '" + myDate + "'")

    val sumPerOrg = getSumPerOrg(dsFilteredByOrgs, myDate, region)
    val sparkdatatableMsges = createMessages(sumPerOrg, true)
    writeToKafka(sparkdatatableMsges, false)
  }

  @throws[Exception]
  private def createMessages(dataset: Dataset[Row], alert: Boolean) = {
    val message = dataset.selectExpr("'crs' as component", 
      "'metrics' as eventtype",
      "struct(" + "relation_name"+" as pipeLine, " + 
        "struct(" + "orgid, " + "call, " + "message, " + "legduration, " + "duration, " + "audio_is_good, " + "contentsize, " + "isfile, " + "number_of_groups, " + "videotime, " + "region," +
        "pdate as reportDate )" +
        " as data ) " +
        "as metrics")
    message
  }
}
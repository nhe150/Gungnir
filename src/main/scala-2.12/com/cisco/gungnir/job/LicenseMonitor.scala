package com.cisco.gungnir.job


import java.util.logging.Logger

import com.cisco.gungnir.utils.DateUtil
import org.apache.spark.sql.functions.{avg, countDistinct, lit}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object LicenseMonitor {
  private val LOGGER = Logger.getLogger(classOf[LicenseMonitor].getName)
}

class LicenseMonitor() extends DataMonitor {
  /**
    *
    * @param ds
    * @param currentDate
    * @return last 33 business day all success unique userid(assigned license) dividend by business days
    */
  private def getAvg(ds: DataFrame, currentDate: String) = {
      val startDate = getDate(-33)

      val history = ds.where("to_date('" + startDate + "') < to_date(pdate)" + " AND " + "to_date(pdate) < to_date('" + currentDate + "')")
        .filter("isBusinessDay(pdate)")
        .filter(" status = 'SUCCESS' ")

      val byDates =  history.groupBy("orgid", "pdate").agg(countDistinct("userid").as("licenseCount"))

      val avgByDates = byDates.groupBy("orgid").agg(avg("licenseCount").cast("int").as("avg"))
      avgByDates.show(false)
      avgByDates
  }

  private def getCurrentDateData(ds: DataFrame, currentDate: String) = { //only track success license assignment
    val curr = ds.where("pdate='" + currentDate + "' and status='SUCCESS' ")
    val c1: DataFrame = curr.groupBy("orgid", "pdate").agg(countDistinct("userid").as("licenseCount"))
    val c2: DataFrame = c1.withColumn("isBusinessDay", lit(DateUtil.isBusinessDay(currentDate)))
    c2.show(false)
    c2
  }



  @throws[Exception]
  override def run(currentDate: String, threshold: String, orgId: String): Unit = {
    val nowDate =  if (currentDate == null )  getDate(-1) else currentDate
    println(nowDate)

    val input = readFromCass().toDF()
    val orgList = getOrgList(configProvider.getAppConfig)
    val whereOrgIdClause = whereOrgId(orgList)


    val ds = input.where("orgid in (" + whereOrgIdClause + ") and relation_name='license' ")

    val current = getCurrentDateData(ds, nowDate)

    val avg = getAvg(ds, nowDate)

    val joined = current.join(avg, "orgid")
    joined.show(false)

    writeToKafka(createMessages(joined.alias("data")), false)

  }


  private def createMessages(dataset : Dataset[Row]) = {
    val message = dataset.selectExpr(
      "'crs' as component",
         "'metrics' as eventtype",
        "struct('license' as pipeLine, "
          + "'anomalyDetection' as phase, "
          + "CONCAT(pdate, 'T00:00:00Z') as sendTime, "
          + "struct("
          + "pdate as date, licenseCount, isBusinessDay, orgid, avg ) as data )" +
          " as metrics ")
    message.show(false)

    message.alias("dt")
  }
}
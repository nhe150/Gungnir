package com.cisco.gungnir.job

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.DataTypes
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util
import java.util.logging.Logger

import com.cisco.gungnir.utils.DateUtil.isBusinessDay
import org.apache.spark.sql.functions._

object CallDurationMonitor {
  val LOGGER = Logger.getLogger(classOf[CallDurationMonitor].getName)
}

class CallDurationMonitor() extends DataMonitor {

  private def getSumPerOrg(ds: Dataset[_], currentDate: String) = {
    val sumPerOrg = ds.groupBy("orgid", "pdate").agg(
      sum("legduration").as("legduration"))
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


    CallDurationMonitor.LOGGER.info("Entering program. currentDate: " + myDate)
    if (!isBusinessDay(myDate)) return

    //need to scrutinize
    val input = readFromCass().toDF()
    val orgList = getOrgList(configProvider.getAppConfig)
    val whereOrgIdClause = whereOrgId(orgList)

    val dsFilteredByOrgs = input.where("orgid in (" + whereOrgIdClause + ") and relation_name='callDuration' ")

    val sumPerOrg = getSumPerOrg(dsFilteredByOrgs, myDate)
    val activeUserMsges = createMessages(sumPerOrg, true)
    writeToKafka(activeUserMsges, false)
  }

  @throws[Exception]
  private def createMessages(dataset: Dataset[Row], alert: Boolean) = {
    val message = dataset.selectExpr("'crs' as component",
      "'metrics' as eventtype",
      "struct('activeUser' as pipeLine, " +
        "struct(" + "orgid, " + "legduration, " +
        "pdate as reportDate )" +
        " as data ) " +
        "as metrics")
    message
  }
}
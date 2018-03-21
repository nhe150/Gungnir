package util

import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import org.apache.spark.sql.functions._

object UDFUtil {
  lazy val testUsers = loadTestUser

  def loadTestUser = {
    var testUsers = mutable.HashSet[String]()
    val stream = getClass.getResourceAsStream("/testuser.txt")
    for (line <- scala.io.Source.fromInputStream(stream).getLines)
      testUsers.add(line.trim())
    testUsers
  }


  def testUser_(uid: String) = {
    if (testUsers.contains(uid)) 1 else 0
  }

  def testUser = udf[Int, String](testUser_)

  val ep1 = (device: String, ua: String) => {
    (Option(device).getOrElse("") + "|" + Option(ua).getOrElse("UA")) match {

      case "ANDROID|SparkBoard" => "Room device"
      case "ANDROID|wx2-android" => "Mobile app"
      case "DESKTOP|sparkmac" => "Desktop client"
      case "DESKTOP|sparkwindows" => "Desktop client"
      case "IPAD|WX2_iOS" => "Mobile app"
      case "IPHONE|spark_ios_sdk" => "Mobile app"
      case "IPHONE|WX2_iOS" => "Mobile app"
      case "MAC|sparkmac" => "Desktop client"
      case "SPARK|SHARE-Novum" => "Room device"
      case "SPARK|VOICE-Novum" => "Room device"
      case "TP_ENDPOINT|ce" => "Room device"
      case "WEB|Chrome" => "Browser"
      case "WEB|Firefox" => "Browser"
      case "WEBEX|wbxcb" => "WebEx"
      case "WEBEX|WbxTPAgent" => "WebEx"
      case "WINDOWS|sparkwindows" => "Desktop client"
      case "|sparkmac" => "Desktop client"
      case "|sparkwindows" => "Desktop client"
      case _ => "Other"
    }
  }

  def register(sqlContext: SQLContext) = {
    sqlContext.udf.register("ep1", ep1)
    sqlContext.udf.register("testUser", testUser)
  }
}


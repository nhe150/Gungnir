package util

import org.apache.spark.sql.SQLContext

object UDFUtil {
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
  }
}


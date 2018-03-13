package util

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf

object UDFUtil {

  val ep1 = (device: String, ua: String)  => {
    (Option(device).getOrElse("") + "|" + Option(ua).getOrElse("UA")) match {

      case "ANDROID|SparkBoard"=>"Roomdevice"
      case "ANDROID|wx2-android"=>"Mobileapp"
      case "DESKTOP|sparkmac"=>"Desktopclient"
      case "DESKTOP|sparkwindows"=>"Desktopclient"
      case "IPAD|WX2_iOS"=>"Mobileapp"
      case "IPHONE|spark_ios_sdk"=>"Mobileapp"
      case "IPHONE|WX2_iOS"=>"Mobileapp"
      case "MAC|sparkmac"=>"Desktopclient"
      case "SPARK|SHARE-Novum"=>"Roomdevice"
      case "SPARK|VOICE-Novum"=>"Roomdevice"
      case "TP_ENDPOINT|ce"=>"Roomdevice"
      case "WEB|Chrome"=>"Browser"
      case "WEB|Firefox"=>"Browser"
      case "WEBEX|wbxcb"=>"WebEx"
      case "WEBEX|WbxTPAgent"=>"WebEx"
      case "WINDOWS|sparkwindows"=>"Desktopclient"
      case "|sparkmac"=>"Desktopclient"
      case "|sparkwindows"=>"Desktopclient"
      case _ => "Other"
    }
  }

  def register(sqlContext :SQLContext) = {
    sqlContext.udf.register("ep1", ep1)
  }
}


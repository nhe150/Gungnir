package util

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf

object UDFUtil {
  lazy val testUsers = loadTestUser

  def loadTestUser = {
    var testUsers =  mutable.HashSet[String]()
    val stream = getClass.getResourceAsStream("/testuser.txt")
    for (line <- scala.io.Source.fromInputStream(stream).getLines)
      testUsers.add(line.trim())
    testUsers
  }


  def testUser_(uid : String) =
    {
      if (testUsers.contains(uid) ) 1  else 0
    }

  def testUser =  udf[Int, String](testUser_)

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
    sqlContext.udf.register("testUser", testUser)
  }
}


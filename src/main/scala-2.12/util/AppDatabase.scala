package util

import com.datastax.driver.core.{Cluster, PreparedStatement, ResultSet, Row}
import com.datastax.driver.core

import scala.collection.JavaConversions._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util

import com.cisco.gungnir.config.ConfigProvider
import org.apache.commons.cli.{GnuParser, HelpFormatter, Option, Options, ParseException}
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON

object AppDatabase {

  val log: Logger = LoggerFactory.getLogger(AppDatabase.getClass);

  def get(url: String,
          connectTimeout: Int = 5000,
          readTimeout: Int = 5000,
          requestMethod: String = "GET"): String =
  {
    import java.net.{URL, HttpURLConnection}
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close
    content
  }

  def fillOrgIdRegionTable(session: core.Session, region: String) = {
    var table = "ci_region"
    val select = "SELECT " + "*" + " FROM " + table;
    val rs: ResultSet = session.execute(select);
    val rows: util.List[Row] = rs.all()
    val endpoints = getRestEndpoints(session, region)
    val insert = "insert into " + table + " (orgid,region) values (?,?);"
    val insertStatement: PreparedStatement = session.prepare(insert)
    for(row <- rows) {
      if(row.getString("region")==null){
        var orgId = row.getString("orgid")
        var region = getRegion(orgId, endpoints)
        if(!region.isEmpty){
          insertRegionForGivenOrgid(orgId, region, session, insertStatement)
        }
      }
    }
  }

  def getRegion(orgId: String, endpoints: util.List[Row]): String = {
    val maxTries = 10;
    var count = 0;
    val port = 8087;
    val path = "/v1/api/todo/region/" + orgId;
    var region = ""
    while(region.isEmpty && count<maxTries){
      var endpoint = endpoints.get(count%endpoints.size()).getString("machine")
      var url = "http://" + endpoint + ":" + port + path
      try {
        var content = JSON.parseFull(get(url))
        if(content != None){
          region = content.get.asInstanceOf[Map[String, String]]("region")
        }
      } catch {
        case ioe: java.io.IOException =>  // handle this
        case ste: java.net.SocketTimeoutException => // handle this
      }
      count = count + 1
    }
    region
  }

  def getRestEndpoints(session: core.Session, region: String): util.List[Row] = {
    var table = "inv_hosts_v3"
    val select = "SELECT " + "*" + " FROM " + table + " where region= '" + region + "'";
    val rs: ResultSet = session.execute(select);
    val rows: util.List[Row] = rs.all()
    rows
  }

  def insertRegionForGivenOrgid(orgId: String, region: String, session: core.Session, insertStatement: PreparedStatement) = {
    try {
      if(orgId != null && orgId.length() > 0){
        session.execute(insertStatement.bind(orgId,region));
      }
    } catch {
      case ex   => {
        log.error("FAFL_ERROR while executing insert with values orgid=" + orgId + " and region=" + region)
      }
    }
  }

  def run(configProvider: ConfigProvider) = {
    val ip: String = configProvider.retrieveAppConfigValue("cassandra.host")
    val keyspace = configProvider.retrieveAppConfigValue("app.keyspace")
    val user = configProvider.retrieveAppConfigValue("cassandra.username")
    val pass = configProvider.retrieveAppConfigValue("cassandra.password")
    val region = configProvider.retrieveAppConfigValue("app.region")
    val ips = ip.split(",")
    val cas = new CassandraClusterConn(ips, keyspace, user, pass)
    fillOrgIdRegionTable(cas.session, region)
    cas.close()
  }

  def main(args: Array[String]): Unit = {
    val options = new Options

    val config = new Option("c", "config", true, "config file")
    config.setRequired(true)
    options.addOption(config)
    val parser = new GnuParser
    val formatter = new HelpFormatter
    var configFile = ""

    try
      configFile = parser.parse(options, args).getOptionValue("config")
    catch {
      case e: ParseException =>
        System.out.println(e.getMessage)
        formatter.printHelp("AppDatabase", options)
        System.exit(1)
        return
    }

    val spark = SparkSession.builder.appName("AppDatabase").getOrCreate
    val configProvider = new ConfigProvider(spark, configFile)
    run(configProvider)
  }
}

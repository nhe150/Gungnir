import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql._
import _root_.util.Constants
import org.apache.spark.sql.api.java.UDF1


object DataMigrationCassandra {
  val usage = """
    Usage: --configFile_from CassandraConfigFile --configFile_to CassandraConfigFile [--pdate pdate] [--relation_name relation_name]
  """
  def main(args: Array[String]): Unit = {
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--configFile_from" :: value :: tail =>
          nextOption(map ++ Map('configFile_from -> value.toString), tail)
        case "--configFile_to" :: value :: tail =>
          nextOption(map ++ Map('configFile_to -> value.toString), tail)
        case "--table_name" :: value :: tail =>
          nextOption(map ++ Map('table_name -> value.toString), tail)
        case "--pdate" :: value :: tail =>
          nextOption(map ++ Map('pdate -> value.toString), tail)
        case "--relation_name" :: value :: tail =>
          nextOption(map ++ Map('relation_name -> value.toString), tail)
        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(),arglist)
    println(options)
    val constants_from = new Constants(options.get('configFile_from).get.toString)
    val constants_to = new Constants(options.get('configFile_to).get.toString)
    val tablename = options.get('table_name).get.toString
    val pdate = options.getOrElse('pdate, null)
    val relation_name = options.getOrElse('relation_name, null)

    val spark = SparkSession.builder
      .config("spark.cassandra.read.timeout_ms", "3600000")
      .config("spark.cassandra.output.consistency.level", "ANY")
      .appName("Migration of Spark Data").getOrCreate

    spark.conf.set("spark.sql.session.timeZone", "GMT")

    spark.udf.register("convertTime", new TimeConverter, DataTypes.StringType)

    val sqlContext = spark.sqlContext
    // set params for the particular cluster
    sqlContext.setConf("ClusterOne/spark.cassandra.connection.host", constants_from.CassandraHosts)
    sqlContext.setConf("ClusterOne/spark.cassandra.auth.username", constants_from.CassandraUsername)
    sqlContext.setConf("ClusterOne/spark.cassandra.auth.password", constants_from.CassandraPassword)

    sqlContext.setConf("ClusterTwo/spark.cassandra.connection.host", constants_to.CassandraHosts)
    sqlContext.setConf("ClusterTwo/spark.cassandra.auth.username", constants_to.CassandraUsername)
    sqlContext.setConf("ClusterTwo/spark.cassandra.auth.password", constants_to.CassandraPassword)

    if("spark_agg".equals(tablename)) {
      copyData(sqlContext, constants_from.CassandraKeySpace, constants_from.CassandraTableAgg, constants_to.CassandraKeySpace, constants_to.CassandraTableAgg, pdate, relation_name)
    }  else if("spark_data".equals(tablename)){
      copyData(sqlContext, constants_from.CassandraKeySpace, constants_from.CassandraTableData, constants_to.CassandraKeySpace, constants_to.CassandraTableData, pdate, relation_name)
    } else if("license".equals(tablename)) {
      copyData(sqlContext, constants_from.CassandraKeySpace, constants_from.CassandraTableLic, constants_to.CassandraKeySpace, constants_to.CassandraTableLic, pdate, relation_name)
    } else {

    }

  }

  def copyData(sqlContext: SQLContext, keyspace_from: String, tablename_from: String, keyspace_to: String, tablename_to: String, pdate: Any, relation_name: Any) : Unit = {
    //Read from ClusterOne
    val dfFromClusterOne = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "cluster" -> "ClusterOne",
        "keyspace" -> keyspace_from,
        "table" -> tablename_from
      ))
      .load

    dfFromClusterOne.show()
    val dfToClusterTwo = spark_filter(dfFromClusterOne, tablename_from, pdate, relation_name)

    dfToClusterTwo.show()

    //Write to ClusterTwo
    dfToClusterTwo
      .write
      .mode(SaveMode.Append)
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "cluster" -> "ClusterTwo",
        "keyspace" -> keyspace_to,
        "table" -> tablename_to
      ))
      .save
  }

  def spark_filter(df: DataFrame, tablename: String, pdate: Any, relation_name: Any): DataFrame = {
    var result = df
    if(relation_name != null){
      result = df.where("relation_name = '" + relation_name + "'")
    }

    if(pdate != null){
      if("spark_agg".equals(tablename)){
        result = df.where("convertTime(unix_timestamp(time_stamp)) = '" + pdate + "'")
      } else if("spark_data".equals(tablename) || "license".equals(tablename)){
        result = df.where("pdate = '" + pdate + "'")
      } else {

      }
    }

    result
  }

  class TimeConverter extends UDF1[Long, String] {
    @throws[Exception]
    override def call(unixtimeStamp: Long): String = {
      val date = new Date(unixtimeStamp * 1000L)
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
      sdf.format(date)
    }
  }
}

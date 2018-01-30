import org.apache.spark.sql.{SaveMode, SparkSession}
import util.Constants
import org.apache.spark.sql.cassandra._

object DataMigration {
  def main(args: Array[String]): Unit = {
    val configFile = args(0)
    val constants = new Constants(configFile)
    val spark = SparkSession.builder
      .config("spark.cassandra.connection.host", constants.CassandraHosts)
      .config("spark.cassandra.auth.username", constants.CassandraUsername)
      .config("spark.cassandra.auth.password", constants.CassandraPassword)
      .config("spark.cassandra.read.timeout_ms", "3600000")
      .config("spark.cassandra.output.consistency.level", "ANY")
      .appName("Migration of Spark Data").getOrCreate

    spark.sparkContext.setLogLevel(constants.logLevel)
    spark.conf.set("spark.sql.session.timeZone", "GMT")

    val results = spark
      .read
      .cassandraFormat(constants.CassandraTableData,constants.CassandraKeySpace)
      .load()

    results
      .write
      .mode(SaveMode.Overwrite)
      .cassandraFormat(constants.CassandraTableData,constants.CassandraKeySpaceAmer)
      .save()

  }
}

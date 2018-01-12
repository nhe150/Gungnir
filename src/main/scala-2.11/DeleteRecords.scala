import org.apache.spark.sql.SparkSession
import util.Constants
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.SqlRowWriter

object DeleteRecords {
  def main(args: Array[String]): Unit = {
    implicit val rowWriter = SqlRowWriter.Factory
    val configFile = args(0)
    val constants = new Constants(configFile)
    val spark = SparkSession.builder.config("spark.cassandra.connection.host", constants.CassandraHosts)
      .config("spark.cassandra.auth.username", constants.CassandraUsername)
      .config("spark.cassandra.auth.password", constants.CassandraPassword)
      .appName("delete weekly records").getOrCreate

    spark.sparkContext.setLogLevel(constants.logLevel)
    spark.conf.set("spark.sql.session.timeZone", "GMT")

    val results = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> constants.CassandraTableAgg, "keyspace" -> constants.CassandraKeySpace ))
      .load()
      .where("period = 'weekly'")
      .where("date_format(time_stamp,'EEEE') = 'Monday'")
      .select("eventkey","time_stamp")

    results.rdd.deleteFromCassandra(constants.CassandraKeySpace,constants.CassandraTableAgg)

  }
}

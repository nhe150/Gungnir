package util

import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.SqlRowWriter

object Cassandra extends Serializable {
  def deleteRecords(df: DataFrame, keyspace: String, table: String) = {
    implicit val rowWriter = SqlRowWriter.Factory
    df.rdd.deleteFromCassandra(keyspace, table)
  }

  def readRecord(spark: SparkSession, keyspace: String, table: String): Dataset[Row] = {
    val df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "keyspace" -> keyspace, "table" -> table ))
      .load()

    df
  }

  def writeRecord(dataset: Dataset[Row], keyspace: String, table: String) = {
    dataset.
      write
      .mode(SaveMode.Append)
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "keyspace" -> keyspace, "table" -> table ))
      .save()
  }
}

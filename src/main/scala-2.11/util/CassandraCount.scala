package util

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
object cassandraCount {
  def main(args: Array[String]) {
    // 1. Create a conf for the Spark context
    // In this example, Spark master and Cassandra nodes info are provided in a separate count.conf file.
    val conf = new SparkConf().setAppName("Counting rows of a cassandra table")

    // 2. Create a Spark context.
    val sc = new SparkContext(conf)

    // 3. Create an rdd that connects to the Cassandra table "schema_keyspaces" of the keyspace "system_schema".
    val rdd = sc.cassandraTable("system_schema", "keyspaces")

    // 4. Count the number of rows.
    val num_row = rdd.count()
    println("\n\n Number of rows in system_schema.keyspaces: " + num_row + "\n\n")

    // 5. Stop the Sspark context.
    sc.stop
  }
}
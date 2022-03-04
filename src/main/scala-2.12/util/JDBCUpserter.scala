package util

import com.cisco.gungnir.utils.Util
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

object JDBCUpserter extends Serializable {

  case class JDBCInfo(val oracleConfig: java.util.Map[String, String], driver: String, schema: StructType, pk: String) extends  Serializable


  def dowork(dataset : Dataset[Row], @transient sparkSession: SparkSession, info: JDBCInfo) {

    val tableName = info.oracleConfig.get("dbtable")
    implicit val encoder = org.apache.spark.sql.Encoders.kryo[Iterator[Row]]
    

    dataset.mapPartitions(Iterator(_)).foreach { batch =>

      var prop = new Properties()
      prop.put("user", info.oracleConfig.get("user"))
      prop.put("password", info.oracleConfig.get("password"))
      if (info.driver.contains("oracle")) {
        prop.put("oracle.jdbc.timezoneAsRegion", "false")
      }

      Class.forName(info.driver)
      val dbc: Connection = {
        DriverManager.getConnection(info.oracleConfig.get("url"), prop)
      }

      val batchSize = if ( info.oracleConfig.get("batchSize") == null )  5000 else  info.oracleConfig.get("batchSize").toInt

      dbc.setAutoCommit(false)

      val st: Statement = dbc.createStatement()

      try {
        batch.grouped(batchSize).foreach { rowBatch =>
          rowBatch.foreach { statement =>
            val upsertStr = Util.getInsertSQLStr(info.schema, tableName, statement, info.pk, info.driver.contains("oracle"))
            //println("upsert:" + upsertStr)
            if( ! upsertStr.isEmpty )
              st.addBatch(upsertStr)
          }
          st.executeBatch()
          dbc.commit()
        }
      }
      finally {
        dbc.close()
      }
    }
  }



}

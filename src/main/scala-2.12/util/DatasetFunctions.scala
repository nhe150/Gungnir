package util
import scala.util.Try
import org.apache.spark.sql.DataFrame

object DatasetFunctions {
  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
}

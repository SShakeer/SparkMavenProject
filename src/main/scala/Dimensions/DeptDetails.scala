package Dimensions
import Utilities.Utils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, count}

/**
 * Created by shakeer on Jan, 2022
 **/
class DeptDetails extends Utils{
  def DeptTotal(df_dept: DataFrame): (DataFrame) = {
    try {
      log.info("Calculating Dept wise total")
      val df_all_cols2 = df_dept.select("Deptno", "Dname", "Location")
      log.info("Aggregation completed")
      df_all_cols2
    } catch {
      case e: Exception => log.error("Error in dept details",e)
        spark.emptyDataFrame
    }
  }
}

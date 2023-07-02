package Dimensions

import Utilities.Utils
import org.apache.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by shakeer on Jan, 2022
 **/
class EmpDetails extends Utils(){
  def EmpTotal(df_emp: DataFrame): (DataFrame) = {
    try {
      log.info("Calculating Dept wise total")
      val df_all_cols = df_emp.groupBy("deptno")
        .agg(sum(col("sal")).as("Total_Salary"))
        .withColumn("State", lit("KA"))
        .withColumn("Province", lit("Bangalore"))
      val df_all_emp_nt = df_all_cols.select("Deptno", "province", "Total_Salary", "State")
      log.info("Aggregation completed")
      df_all_emp_nt
    } catch {
      case e: Exception => log.error("Error in emp details",e)
        spark.emptyDataFrame
    }
  }
}

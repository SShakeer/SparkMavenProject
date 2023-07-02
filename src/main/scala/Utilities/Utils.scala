package Utilities

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
/**
 * Created by shakeer on Jan, 2022
 **/
class Utils {
  val log =Logger.getLogger(this.getClass)

  val spark=SparkSession
    .builder()
    .appName("BDF Non-trade-presc")
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()

  def readCsvFile(filename: String,HeaderExists: String,inferschema:String) = {
    try {
      spark.read.format("com.databricks.spark.csv").option("header",HeaderExists).option("inferschema", inferschema).load(filename)
    }
    catch {
      case e:Exception=>log.error("Error in readCsvFile",e)
        spark.emptyDataFrame
    }
  }

  val df_emp=readCsvFile("/src/main/resources/Emp.csv","true","true")
  val df_dept=readCsvFile("/src/main/resources/department.csv","true","true")

}

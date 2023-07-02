package Driver
import Utilities.Utils
import Dimensions._
import org.apache.spark.SparkFiles
import java.io.File

/**
 * Created by shakeer on Jan, 2022
 **/
object MainMethod extends Utils {
  def main(args: Array[String]): Unit = {

    spark.sparkContext.addFile(args(0))
    val fileName = SparkFiles.get(args(0).substring(args(0).lastIndexOf("/") + 1, args(0).length))
    val myConfigFile = new File(fileName)
    val jobType = args(1).toUpperCase()
    val emp_instance = new EmpDetails
    val dept_instance= new DeptDetails
    try {
      jobType match {
        case job if (job == "EMP") => {
          val emp_det = emp_instance.EmpTotal(df_emp)
        }
        case job if (job == "DEPT") => {
          val dept_det = dept_instance.DeptTotal(df_dept)
        }
        case _ => {
          println("Invalid jobtype....")
        }
      }
    }
      catch {
        case t: Throwable => println(s"........job failed with errors..............+$t")
          throw new Exception("----------------Pipeline FAILED------------------")
    }
  }
}
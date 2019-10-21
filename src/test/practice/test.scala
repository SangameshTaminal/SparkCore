

package test.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import java.util._
import org.apache.spark.sql.functions._

object test {
  def main(args:Array[String])=
  {
    val spark =
SparkSession.builder.appName("Lab011").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val df1=spark.read.format("csv")
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lastname","age","prof")

    import spark.implicits._
    val rdd=spark.sparkContext.textFile("file:/home/hduser/hive/data/custs")
    val rdd1=rdd.map(x=>x.split(",")).filter(x1 => x1.length==4).map(x=> (x(0).toInt,x(1),x(2),x(3).toInt))
    val df0 =rdd1.toDF
    //  df0.write.mode("overwrite").csv("/home/hduser/test")

    df0.show(false)

    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("user","root")
    prop.put("password","root")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")

    df0.write.mode("overwrite").jdbc("jdbc:mysql://localhost/custdb","customer33334",prop)
   // df.show()
    println("data written in mysql")
  }
}
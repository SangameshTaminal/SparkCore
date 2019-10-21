package test.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types
import org.apache.spark.sql.catalyst.plans.logical.With
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}


object sparkSQL2 {
  def main(args:Array[String]) {
  val spark = SparkSession.builder().appName("SQL-2").master("local[*]").getOrCreate()
  val sql1 = SQLContext
  val df = spark.read.format("csv").option("inferschema",true).load("file:/home/hduser/hive/data/custs")
  import spark.implicits._
  val rdd = spark.sparkContext.textFile("file:/home/hduser/hive/data/custs")
  val rdd1 = rdd.map(x=>x.split(",")).filter(x=>x.length==5)
  val rdd2 = rdd1.map(x=> customer(x(0).toInt,x(1),x(2),x(3).toInt,x(4))) //in df compiletime saftey is not present
  
  val df1 = rdd2.toDF()
  val df2 = df1.filter($"custprofession" ==="Pilot" && $"custage">50)
  df2.write.mode("overwrite").parquet("file:/home/hduser/parquetsqldf")
  
  val ds = rdd2.toDS()
  val ds1 = ds.filter(x=>x.custprofession=="Pilot" && x.custage>50) //dataset offers compiletime saftey.
  ds1.write.mode("overwrite").orc("file:/home/hduser/orcsqlds")
  
//  val rdd3 = ds.as[customer]
//  val rdd4 = df2.rdd
  println("DS to RDD")
  //rdd4.foreach(println)
  //val ds3 = rdd4.filter(x=>x=="Pilot").toDF
  //STRUCT
  val structtype = StructType(Array(StructField("first_name",StringType,true),StructField("last_name",StringType,true),
      StructField("company_name", StringType,
      true),StructField("address", StringType, true),StructField("city", StringType,
      true),StructField("country", StringType, true),StructField("state", StringType,
      true),StructField("zip", StringType, true),StructField("age", IntegerType,
      true),StructField("phone1", StringType, true),StructField("phone2", StringType,
      true),StructField("email", StringType, true),StructField("website", StringType, true)));  
      val st = spark.read.option("delimiter",",").option("inferschema",true).option("header",true).schema(structtype).csv("file:/home/hduser/sparkdata/usdata.csv")
      st.createOrReplaceTempView("test1")
      println("CSV")
      spark.sql("select first_name from test1").show(10,false)
      val sc = st.write.option("delimiter",",").mode("overwrite").parquet("file:/home/hduser/ttt")
      val sc1 = spark.read.parquet("file:/home/hduser/ttt/*.parquet")
      sc1.createOrReplaceTempView("test")
      println("PARQUET")
      spark.sql("select first_name from test").show()
      println("DONE?")
    val parquetagecatdf =spark.read.parquet("file:/home/hduser/sparkdata/agecategory.parquet")
    parquetagecatdf.createOrReplaceTempView("parquetagecat")
//spark.sql("SELECT max(age),min(age),count(distinct agecat) FROM parquetagecat ").show()


  }
  
}


package test.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types
import org.apache.spark.sql.catalyst.plans.logical.With


object sparkSQL1 {
  def main(args:Array[String]) {
  val sparkConf = new SparkConf().setAppName("SQL1").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
 
  val sql = new HiveContext(sc)
  val spark = SparkSession.builder().getOrCreate()
  sc.setLogLevel("ERROR")
  val df = sql.read.format("csv").option("delimiter",",").load("file:/home/hduser/hive/data/txns")
  .toDF("transid","transdt","custid","salesamt","category","prodname","state","city","payment")
  //df.show(false)
  
  val df1 = df.filter(col("state") === "Omaha") //using '===' coz we are comparing column and string
  //df1.show(false)
  //OR
  import sql.implicits._
  val df2 = df.filter($"state" === "Omaha")//to use $ we have to import implicits
  //df2.show(false)
  //OR
  val df3  = df.where ("state = 'Omaha' ")
  val df4 = df1.agg(max("salesamt").alias("Max Amt"),min("salesamt"))
  //df4.show
  
  val sql1 = spark.sql("show databases")
  sql1.show()
  
  df3.write.format("parquet").saveAsTable("default.test1")
  val df5 = df1.groupBy("category","transdt").agg(sum("salesamt"))
  //df5.show
  
  val df6 = df1.select("transid", "custid") //to select required column
  //df6.show
  
  //rename existing colum
  val df7 = df1.withColumnRenamed("state", "City")//
  val df8 = df1.withColumnRenamed("city","state")
  
  val df9 = df1.withColumn("Country",lit(" "))
  //df9.show
  
  val df10 = sql.read.format("csv").option("inferschema",true)
  .load("file:/home/hduser/hive/data/custs")
  .toDF("EmpID","FName","LName","Age","Profession")
  val df11 = df10.filter($"Profession" ==="Pilot").count()
  println("Count"+df11)
  
  val df12 = df10.groupBy("Profession").agg(count("EmpID")).orderBy(asc("Profession")).na.fill("NA")
  //df12.show(100)
  
  val df13 = df10.createTempView("temptbl")
  sql.sql("""select Profession,count(EmpID) from 
            temptbl group by Profession""").write.mode("overwrite").csv("file:/home/hduser/sql.csv") //to write sql in multiple line we can use '"""'
  sql.sql("""select FName,LName,EmpID, case when Age>40 then 'OLDAGE' else 'MEDIUMAGE' end as AGED
           from temptbl""").write.mode("overwrite").parquet("file:/home/hduser/sql")
  sql.sql("""select FName,LName,EmpID, case when Age=21 then 'OLDAGE' else 'MEDIUMAGE' end as AGED
           from temptbl""").write.mode("overwrite").orc("file:/home/hduser/sql1")//orc = parquet/1.5
  sql.sql("""select FName,LName,EmpID, case when Age>40 then 'OLDAGE' else 'MEDIUMAGE' end as AGED
           from temptbl""").write.mode("overwrite").json("file:/home/hduser/sql3")
 //          sql.sql("""select FName,LName,EmpID, case when Age>40 then 'OLDAGE' else 'MEDIUMAGE' end as AGED
 //          from temptbl""").write.format("avro").mode("overwrite").save("file:/home/hduser/avroFile")
  val df14 = spark.read.format("parquet").load("/home/hduser/sql/*.parquet")
  df14.show(10)
  val df15 = spark.read.format("orc").load("/home/hduser/sql1/*.orc").count()
  println(df15)
  }
  
}
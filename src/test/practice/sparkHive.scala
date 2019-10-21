package test.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types
import org.apache.spark.sql.catalyst.plans.logical.With
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import java.util._

object sparkHive {
  
  def main(args:Array[String]) {
     
      val spark = SparkSession.builder().appName("SQL-2")
      .config("hive.metastore.uris","thift://localhost:9083").enableHiveSupport().master("local[*]").getOrCreate()
      
      val properties = new Properties()
      properties.put("user","root")
      properties.put("password","root")
      properties.setProperty("driver", "com.mysql.jdbc.Driver") 
      spark.sparkContext.setLogLevel("ERROR")
      spark.sql("create database if not exists cust")
      spark.sql("use cust")
      spark.sql(""" create table if not exists customer1(id int,fname string,lname string,age int,profession string)
         row format delimited fields terminated by ","
         stored as textfile """)  
      spark.sql("load data local inpath '/home/hduser/hive/data/custs' overwrite into table customer1")
      val values = spark.sql("select * from customer1 where age between 30 and 35")
      values.show(10,false)
      val values1 = spark.table("customer1") //load data from table to df 
      //read data from file load it to hive tbl
      val read  = spark.read.format("csv").option("inferschema",true)
      .load("/home/hduser/hive/data/custs")
      //read.write.saveAsTable("test1")
      
      //read data from hive and store in mysql
      val store = spark.sql("select * from customer")
      //or
      val store1 = spark.table("customer")
      //storing value to db
      store.write.mode("overwrite").jdbc("jdbc:mysql://localhost/cust", "customer1",properties )
     
  }
}


//when hive is not running we will get  java.lang.reflect.InvocationTargetException exception
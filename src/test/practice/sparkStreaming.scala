package test.practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//or
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds,StreamingContext}
//or
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SQLContext
import java.util.Properties

object sparkStreaming {
  def main(args:Array[String]) {
  
    //val spark = SparkSession.builder().appName("Streaming").enableHiveSupport().master("local[*]").getOrCreate()
    val conf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sparkStreaming = new StreamingContext(sc,Seconds(10))
    val sqlc = new SQLContext(sc)
    //to read from a file source
    //val stream = sparkStreaming.textFileStream("file:/home/hduser/streaming/test")
    //val ds1 = stream.map(x=>x.split(",")).filter(x=>x.contains("who")).count()
    //this will not look into file instead it will listen to port number 9999(nc -lk 9999)
    val stream = sparkStreaming.socketTextStream("localhost", 9999)
    val ds2 = stream.map(x=>x.split(",")).flatMap(x=>x).map(x=>(x,1))
    val ds3 = ds2.reduceByKey(_+_)
    //or 
    //val ds3 = ds2.reduceByKey((a,b) =>(a+b))
    stream.print()
    ds3.print()
    
    //load data into sql from socket.
     import sqlc.implicits._
    stream.foreachRDD( rdd=>
      {
        if(!rdd.isEmpty()) {
          val rdd1 = rdd.map(x=>x.split(",")).filter(x=>x.length==5).map(x=>customer(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
          val df = rdd1.toDF()
          val prop = new Properties()
          prop.put("user", "root")
          prop.put("password","root")
          prop.setProperty("driver", "com.mysql.jdbc.Driver") 
          df.write.mode("append").jdbc("jdbc:mysql://localhost/cust", "stream", prop)
        }
    }
    )
    // By Sangamesh TaminalsparkStreaming.start()
    sparkStreaming.awaitTermination() //after this many sec it will terminate the pgm.
    
  }
  
}

case class customer(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)
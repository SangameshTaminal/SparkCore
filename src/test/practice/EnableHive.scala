package test.practice

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import org.apache.hadoop.hive.ql.parse.spark.SparkCompiler
import org.apache.spark.sql.catalyst.parser.ParseException
//import org.apache.spark.sql.catalyst.analysis.AnalysisError
import  java.lang.RuntimeException

object EnableHive {
  def main(args:Array[String]) {
    val spark = SparkSession.builder.appName("EnableHive")
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .enableHiveSupport().master("local[*]").getOrCreate()
                //val sconf = new SparkConf().setMaster("local[*]").setAppName("SQL")
//    val sc = new SparkContext(sconf)
//    val sqlCon = new HiveContext(sc)
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._ 
        val data = spark.read.format("csv").load("/home/hduser/hive/data/custs")
        val data1 = data filter(x=> (x(4)=="Pilot"))
    data1.toDF().write.format("parquet").saveAsTable("custdb.ttt4")
           spark.sql("select * from cust.ttt").show(10)   
    
    
    
    
    
    
    
    
    
    
    
    
   try{
     println("Entering try")
                spark.sql("select * from cust.cc").show
   }
    catch {
      case c :(RuntimeException)=> {
        error("RunTime")
      }
//      case d :(AnalysisError)=> {
//        error("ANALYSIS")
//      }
      case b: (ParseException) => {
        error("Parse Exception, Error in syntax")
      }
      case a: (HiveException) => 
        {
          error("Hive exception expected")
        }
      
      
        
    } finally {
      println("Finally handled")
    }
    println("Hi, I'm here")
  }
}
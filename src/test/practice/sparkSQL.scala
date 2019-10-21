package test.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf

case class Auction(auctionid: String, bid: Float, bidtime: Float, bidder: String, bidderrate:
Integer, openbid: Float, price: Float, item: String, daystolive: Integer)

object sparkSQL {
  def main(args:Array[String]) {
    //System.setProperty("hive.metastore.uris", "thrift://METASTORE:9083");
    val sconf = new SparkConf().setMaster("local[*]").setAppName("SQL")
    val sc = new SparkContext(sconf)
    val sqlCon = new HiveContext(sc)
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._ //implicits object gives implicit conversions for converting scala objects (incl. RDDs) into a Dataset, DataFrame, Columns
    sc.setLogLevel("ERROR")
    val sparkSession = SparkSession.builder.config("hive.metastore.uris", "thrift://localhost:9083")
                        .enableHiveSupport.getOrCreate()
    //thrift://localhost:9083
    val auction = sc.textFile("file:/home/hduser/sparkdata/auctiondata")
    val ebay = auction.map(x=>x.split("~")).map(p => Auction(p(0), p(1).toFloat, p(2).toFloat,
                p(3), p(4).toInt, p(5).toFloat, p(6).toFloat, p(7), p(8).toInt))
    //ebay.foreach(println)         
    //System.setProperty("hive.metastore.uris", "thrift://METASTORE:9083");
    println("HIVEENABLE")
    sparkSession.sql("show databases").show()
    val df = spark.createDataFrame(ebay)
    //val df1 = spark.toDF(ebay)
    val auction1 = df.select("auctionid").count()
    println(auction1)
    df.write.mode("overwrite").json("file:/home/hduser/auct.json")// mode("overwrite") overwrites the file with new content
    val emp = sc.textFile("file:/home/hduser/empinfo11")
    val dept = sc.textFile("file:/home/hduser/deptinfo11")
    val emp1 = emp.map(x=>x.split(",")).filter(x=>x.length==2).map(x=>(x(0),x(1)))
    val emp3 = emp.map(x=>x.split(",")).filter(x=>x.length>2).map(x=>(x(0),x(1),x(2),x(3)))
    emp1.foreach(x=>println(x)) //have to use toList() inorder to print values of RDD[Arr:String]
    val emp2 = spark.createDataFrame(emp1)
    val emp4 = spark.createDataFrame(emp3)
    emp2.write.mode("overwrite").json("file:/home/hduser/dummy.json")
    emp4.write.mode("overwrite").orc("file:/home/hduser/dummy.csv")
    val df1 = spark.read.format("orc").load("file:/home/hduser/dummy.csv")
    df1.show()
    val csv = sqlCon.read.option("inferschema","true").option("delimiter",",")
    .json("file:/home/hduser/auct.json")
    csv.printSchema()
    csv.show(10,false)
    csv.createTempView("json")
    def add(a:Int,b:Int)={
      a+b
    }
    spark.udf.register("add",add _)
    spark.sql("select add(10,20) from json").show
    spark.sql("select concat(first_name,'',last_name) from json").show()
  }
  
}
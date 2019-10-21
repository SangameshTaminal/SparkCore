package test.practice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object rddpgm {
  
  def main(args:Array[String]) {
    val spark = 
      SparkSession.builder.appName("RDD").master("local").getOrCreate()
      val sparkContext = spark.sparkContext
      sparkContext.setLogLevel("ERROR")
      val test = spark.read.textFile("file:/home/hduser/sparkdata/empdata.txt").rdd
      val mapFile = test.map(l=>l.length)
      mapFile.foreach(println)
      val test1 = spark.read.textFile("file:/home/hduser/sparkdata/empdata.txt").rdd //rdd from a file.
      val mapFile1 = test1.flatMap(l=>l.split(","))  //rdd from another rdd.
      println("FlatMap")
      mapFile1.foreach(println)
      val test2 = test1.filter(l=>l.length>20)
      test2.foreach(println)
      val data = spark.sparkContext.parallelize(Array(('A',1),('b',2),('c',3)))
val data2 =spark.sparkContext.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
val result = data.join(data2)
println(result.collect().mkString(","))
 val data1 = spark.sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
 data.foreach(println)
 
 
 val rdd1 = spark.sparkContext.parallelize(List(("maths", 80),("science", 90),("kannada",85)))
val additionalMarks = ("extra", 4)
val sum = rdd1.fold(additionalMarks){ (acc,marks) => val add = acc._2 + marks._2 + acc._2
("total", add)
}
println(sum)
val linesFile1 = spark.read.textFile("file:/home/hduser/sparkdata/empdata.txt") //read->sql method converted to DataSet
val linesFile2 = sparkContext.textFile("file:/home/hduser/sparkdata/empdata.txt") //converted to RDD
val rdd = sparkContext.parallelize(1 to 20)
val te = rdd filter{x=>x%2==0}
println("even")
te.foreach(println)
val secthird = sparkContext.textFile("file:/home/hduser/hive/data/txns")
val split = secthird.map(x=>x.split(","))
val st = split.map(x=>(x(7),1))
val st1 = st.reduceByKey((x,y)=>x+y)
st1.foreach(println)
val i = spark.read.textFile("file:/home/hduser/i.txt")
val inc = i filter(l=>l.contains("learning"))
val ct = inc.count() 
println("number of lines having spark in it are" +ct)
val rdd2 = sparkContext.textFile("file:/home/hduser/test3/inc1.txt")
val rdd3 = sparkContext.textFile("file:/home/hduser/test3/inc2.txt")
val rdd4 = sparkContext.textFile("file:/home/hduser/test3/inc3.txt")
val rdd23 = rdd2.union(rdd3)
val rdd234 = rdd23.union(rdd4)
rdd234.foreach(n=>println(n))
val flat = rdd234.flatMap(l=>l.split(" "))
val count = flat.count()
println(count)
val eachcnt = rdd234.flatMap(l=>l.split(" ")).map(m=>(m,1))
eachcnt.foreach(println)
val ctx = new org.apache.spark.sql.SQLContext(sparkContext)
    import ctx.implicits._
val emp = sparkContext.textFile("file:/home/hduser/empinfo11")
val dept = sparkContext.textFile("file:/home/hduser/deptinfo11")
val emp1 = emp.map(x=>x.split(",")).filter(x=>x.length==4).map(x=> (x(2),(x(0),x(1),x(3))))
//emp1.foreach(x=>println(x))
  val dept1 = dept.map(x=>x.split(",")).map(x=>(x(0),(x(1))))
//dept1.foreach(x=>println(x))
val join = emp1.join(dept1)
join.foreach(x=>println(x))
//broadcast variables.
val bcv = sparkContext.broadcast(dept1.collectAsMap())
val jrdd = emp1.map(x=>(x._2,(bcv.value.get(x._1))))
jrdd.foreach(x=>println(x))
val cust = sparkContext.textFile("file:/home/hduser/hive/data/custs",2)
//val fil = cust.map(x=>x.split(",").contains("Pilot"))
val fil1 = cust.filter(x=>x.toLowerCase().contains("pilot"))
var sum1=0
fil1.foreach (x=> {
    println(x)
  sum1 = sum1+1
})  
println(fil1.count)
var sumacc=sparkContext.longAccumulator("Accumulator")
fil1.foreach (x=> {
    println(x)
    sumacc.add(1)
  println(sumacc.value)
})
println(fil1.count)
val emp2 = spark.sparkContext.textFile("file:/home/hduser/empinfo11")
val emp3 = emp2.map(x=>x.split(",")).map(x=>x.length>3)//.toDF().write.mode("overwrite").parquet("file:/home/hduser/emp3csv")
println("emp3")
emp3.foreach(println)
//val emp4 = emp2.map(x=>x.split(",")).map(x=>x.length==2).toDS().write.mode("overwrite").json("file:/home/hduser/emp4json")
//val emp5 = spark.read.parquet("file:/home/hduser/emp3csv/*.parquet")
//emp5.show()
val emp4 = sparkContext.textFile("file:/home/hduser/empinfo11")
val emp5 = emp4.map(x=>x.split(","))  

val udf1 = udf(dayOfweek) //regisetering UDF
//spark.udf.register(dayOfWeek) if you want to register for spark sql

val contwoToAna = conTwoCol1 _ //converting from normal function to ananymous function since normal functions cant be 
                               //registered as udf and hence cannot be passed as parameter
val udf2 = udf(contwoToAna)

val udf3 = udf(ageVal)


val emp6 = emp5.filter(x=>x.length==4).toDF()//.write.mode("overwrite").json("file:/home/hduser/emp3csv")
//val emp8 = emp6.write.mode("overwrite").csv("file:/home/hduser/emp4csv")
val emp7 = emp6.write.mode("overwrite").orc("file:/home/hduser/emp3csv")
val emp8 = spark.read.format("csv").load("file:/home/hduser/frnds12").toDF("ID","Name","DOB")
val emp9 = emp8.withColumn("Date", dayofmonth(col("DOB"))).withColumn("Month", month(col("DOB")))
val emp11 = emp8.withColumn("Date", dayofmonth(col("DOB"))).withColumn("Month", udf1((col("DOB"))))//using udf for day of week
val emp10 = emp9.drop("ID")
println("Day")
emp11.show()
val emp18  = emp8.withColumn("ConcatinatedColumn",udf2(col("ID"),col("Name")))
val emp19  = emp8.withColumn("AgeVal",udf3(col("ID")))
emp8.printSchema()
emp18.show(true)
emp10.show(true)
emp19.show()
}
  
  //creating UDF
  //this is ananymous function
  def dayOfweek = (s:String) => {
  val dayformat = new java.text.SimpleDateFormat("yyyy-mm-dd")
  val days = Array("Sun","Mon","Tue","Wed","Thu","Fri","Sat")
  val day = days(dayformat.parse(s).getDay).toString()
  val day1 = days(dayformat.parse(s).getDay)
  println(day1)
  day
}
  
 //concat two column
  //ananymous function
  def conTwoCol=(s:String,s1:String) =>{
        s+ " " + s1   
  }
  //class function
  def conTwoCol1 (s:String,s1:String) ={
        s+ " " + s1   
  }
  
  def ageVal =(age:Int)=> {
    if(age>1)
    true
    else false
  }
}


case class dept(id:Int,name:String,dept:String,city:String)

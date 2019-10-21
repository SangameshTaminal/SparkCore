package test.practice

object practice {
  
  def main(args:Array[String]):Unit =
  {
    //1st
    val x = 2000;
    val y = 200;
    val z = x/y;
    println(z);
    var a = 2000;
    var x1 = a/y;
     a = x1;
     println(a);
     
//     2nd
       var k=10;
       val l=30;
//       k="Hello"  error: type mismatch;
      // l=20;error: reassignment to val
       //3rd
       var d =10;
       var e=50;
       var f=30;
       if(d>e && d>f) println("d is greater");
       else if(e>f && e>d) println("e is greater");
       else println("f is greater");
       //5th
       val course1=Array("bigdata","spark","datascience","machinelearning");
       for(i<-0 to course1.length-1 by 1) {
         if(course1(i)=="bigdata") {
           println("Bigdata fees is 10000");
         }
         if(course1(i)=="spark") {
           println("Bigdata fees is 15000");
         }
         if(course1(i)=="datascience") {
           println("Bigdata fees is 40000");
         }
         if(course1(i)=="machinelearning") {
           println("Bigdata fees is 60000");
         }
       }
      //6th
       //using for
       for(i<-0 to 10 by 1) {
         if(i%2 == 0) {
           println(i + " Number is even");
         }
         else
           println(i + "Number is odd");
       }
//       var j=100;
//       while(j%2==0) {
//         println("Even");
//       }
       //7th
       for(i<-0 to 20 by 3) {
         println(i);
       }
       var cube = 4;
       for(i<-1 to 4 by 1){
         cube =i*i*i;
       }
       println(cube);
         //8th
//         val k1=10;
//         val k2=20.0;
//         val k3='c';
//       val k4="HI";
//       var myRes = k1 match{
//         case 1 => k1;
//       }
//       println(myRes);
       //11th
       val arr = Array(2,3,1,5,4);
       var larg=arr(0);
       for(i<-0 to arr.length-1 by 1){
         if(larg < arr(i)) {
           larg=arr(i);
         }
       }
       println(larg);
       
  }
}

import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}






object April19 {

  def main (args: Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.OFF)


    val conf=new SparkConf().setMaster("local[2]").setAppName("Errko App")
    val ssc=new StreamingContext(conf,Seconds(5))

    ssc.checkpoint("/Users/benyahiasadok/IdeaProjects/April13/src/main/April19-checkin")


    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines flatMap {line => line.split(" ")}
    val wordpairs=words.map (x=>(x,1))
    val numbers=words map (x=> x.toInt)
    val windowLen = 30
    val slidingInterval = 10
    val countByValueAndWindow = words.countByValueAndWindow(Seconds(windowLen), Seconds(slidingInterval))
   // countByValueAndWindow.checkpoint(Seconds(10))
  // val sumLAST30seconds=numbers.reduceByWindow({(n1,n2)=>n1+n2}, Seconds(windowLen),Seconds(slidingInterval))
   //countByValueAndWindow.print()
    // val sumLast30Seconds=numbers.reduceByWindow({(n1, n2)=> n1+n2}, Seconds(windowLen), Seconds(slidingInterval))
    //sumLAST30seconds.print()

val sumLast30sec=wordpairs.reduceByKeyAndWindow(_+_,_-_,Seconds(30),Seconds(10))
    sumLast30sec.print()




/*
    val window = words.window(Seconds(windowLen), Seconds(slidingInterval))
    val longestWord = window.reduce { (word1, word2) =>           if (word1.length > word2.length) word1 else word2 }
    val countByWindow = words.countByWindow(Seconds(windowLen), Seconds(slidingInterval))
    longestWord.print()

    countByWindow.print()



    /*

    val updateCount=(xs:Seq[Int],prevSate:Option[Int])=>{

  prevSate.map{c=>Some(c+xs.sum)}.getOrElse(Some(xs.sum))
 /* prevSate match
    {case None=> Some(xs.sum)
  case Some(x)=>Some (x+xs.sum)}
*/
}




    val wordcount1=pairedwords.updateStateByKey(updateCount)

    //val worddcount=pairedwords.reduceByKey(_+_)
    wordcount1.print(5)
    /*
    val file=ssc.textFileStream("/Users/benyahiasadok/IdeaProjects/April13/src/main/April19")



    file.foreachRDD(t=>{

    val test=t.flatMap(r=>r.split("")).map (w=>(w,1)).reduceByKey(_+_)
      test.saveAsTextFile("/Users/benyahiasadok/IdeaProjects/April13/src/main/4testing/April19-output")}
      )

*/
*/ */
ssc.start()
    ssc.awaitTermination()
  }
}

import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}

object AprilStream13 {


  def main (args: Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.OFF)


    val conf=new SparkConf().setMaster("local[4]").setAppName("Ludovic App")
    val ssc=new StreamingContext(conf,Seconds(5))
ssc.checkpoint("/Users/benyahiasadok/IdeaProjects/April13/src/main/Cheking")
    val lines1=ssc.socketTextStream("localhost",9999)


    val words = lines1 flatMap {line => line.split(" ")}
    val numbers = words map {x => x.toInt}
    val windowLen = 30
    val slidingInterval = 10
    val sumLast30Seconds=numbers.reduceByWindow({(n1, n2)=> n1+n2}, Seconds(windowLen), Seconds(slidingInterval))
    sumLast30Seconds.print()



    /*

    val updateSate=(xs:Seq[Int],prevSate: Option[Int])=>
    {
      prevSate match
      {
        case None => Some(xs.sum)
        case Some(x)=>Some(x+xs.sum)
      }


    }

val updateSate=(counts:Seq[Int],prevcount: Option[Int])=>{

      prevcount.map{c=>Some(c+counts.sum)}.getOrElse(Some (counts.sum))


    }




    val updatedCount=wordsPairs.updateStateByKey(updateSate)

    val SortedupdatedCount=updatedCount.transform(r=>r.sortBy((y)=>y))
   // val word2= words1.countByValue().transform{rdd => rdd.sortBy((w)=> w)}

 SortedupdatedCount.print()



    val WordLenPair1=words1.map(w=>(w.length(),w))
    val wordByLen1=WordLenPair1.groupByKey()

   // creating the second Dstream


    val lines2=ssc.socketTextStream("localhost",9999)
    val words2=lines2.flatMap(l=>l.split(" "))
    val WordLenPair2=words1.map(w=>(w.length(),w))
    val wordByLen2=WordLenPair2.groupByKey()

val WordSameLength=wordByLen1.join(wordByLen2)
    WordSameLength.print()



val wordsLength=words.map(w=>(w,w.length))
    val longestWord= wordsLength reduce { (w1,w2)=>if (w1._2>w2._2) w1  else w2}
    val OnlyWords=longestWord.map(w=>w._1)
    OnlyWords.print()



    val conf=new SparkConf().setMaster("local[4]").setAppName("Errki first streaming App")
    val ssc=new StreamingContext(conf,Seconds(5))
    val lines=ssc.socketTextStream("localhost",9999)
    val words= lines.flatMap(l=> l.split(" "))
    val wordCounts = words.countByValue()
    val sorted = words.transform{rdd => rdd.sortBy((w)=> w)}

     */













    //words.print()
    //sorted.print()
    ssc.start()
    ssc.awaitTermination()







  }

}

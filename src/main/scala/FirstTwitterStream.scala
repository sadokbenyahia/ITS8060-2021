
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
object FirstTwitterStream {


  def main (args: Array[String]):Unit= {

    Logger.getLogger("org").setLevel(Level.OFF)



    // define twitter conf

    val consumerKey = "0LiL3IZyQhHa3wtWD5w00k97M"
    val consumerSecret = "v8zlBUhrh92dvTkOy05k2FHAIykPXSMnm6SR0xCweyNTsZymgt"
    val accessToken = "1251902462241517570-8HhWzHZdNurIMq6ksoMxqPy38DsUHI"
    val accessTokenSecret = "BddFuHswZ7VWEdqJTKHPIvqU175JpEhigrZe6VfGAPyS7"





    //    val filters=List("#Android")
    System.setProperty("twitter4j.oauth.consumerKey", "0LiL3IZyQhHa3wtWD5w00k97M")
    System.setProperty("twitter4j.oauth.consumerSecret", "v8zlBUhrh92dvTkOy05k2FHAIykPXSMnm6SR0xCweyNTsZymgt")
    System.setProperty("twitter4j.oauth.accessToken", "1251902462241517570-8HhWzHZdNurIMq6ksoMxqPy38DsUHI")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "BddFuHswZ7VWEdqJTKHPIvqU175JpEhigrZe6VfGAPyS7")

    val conf = new SparkConf().setMaster("local[2]").setAppName("Ludovic App")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("/Users/benyahiasadok/IdeaProjects/April13/src/main/Cheking")
    val tweets0 = TwitterUtils.createStream(ssc, None)



val hashTags=tweets0.flatMap(status=>status.getText.split(" ").filter(_.startsWith("#")))
    hashTags.print()
//hashTags.checkpoint(Seconds(10))
  val top60=hashTags.map(s=>(s,1)).reduceByKeyAndWindow(_+_,_-_,Seconds(60)).map{case(topic,count)=>(count,topic)}.transform(_.sortByKey(false))

top60.foreachRDD(rdd=>{

  val top5=rdd.take(5)
  println("\n popular topics in last 60 seconds (%s total):".format(rdd.count()))
  rdd.foreach{case (count,tag)=>println("%s (%s tweets)".format(tag,count))}



})

      ssc.start()
    ssc.awaitTermination()
  }
}
/*
/// getting top 5






//  val aggStream=tweets0.filter(tweet => tweet.getLang()=="en").flatMap(x=>x.getText.split(" ")).filter(_.startsWith(
//  "#")).map(x=>(x,1)).reduceByKeyAndWindow(_+_,_-_,Seconds(15),Seconds(10),5)

//aggStream.checkpoint(Seconds(10))
// aggStream.print()

aggStream.foreachRDD((rdd,time)=>{
 val count=rdd.count
  if (count>0) {
    val dt = new Date(time.milliseconds)
    println(s"\n \n $dt rddcount=$count \n Top 5 words \n")
    val top5=rdd.sortBy(_._2,ascending = false).take(5)
      top5.foreach{ case (word, count)=> println(s"[$word]-$count")
    }
  }
}
)




//perform action on the stream
tweets.foreachRDD((rdd, time) => {
//group tweets by who tweeted it.
val groupedByUser = rdd.groupBy(f => f.getUser)
groupedByUser.foreach(f => {
//how many tweets they sent
val total = f._2.size
//how much positive feedback they got, as a double to prevent truncation in score.
var feedback = 0d
System.out.println("User: " + f._1.getScreenName)
System.out.println("_Tweets: " + f._2.size)
f._2.foreach(e =>
  feedback += e.getFavoriteCount)
  System.out.println("_Score: " + feedback / total)
})
})






val tweets=TwitterUtils.createStream(ssc,None,filters)
val hashtags=tweets.flatMap(t=>t.getHashtagEntities)
val androidHash=hashtags.map(h=>h.getText.toLowerCase).filter(h=>(!h.equals("android")))
androidHash.countByValue().print()




}
}

 */
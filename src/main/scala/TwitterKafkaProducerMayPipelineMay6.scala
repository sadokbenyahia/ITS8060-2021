import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterKafkaProducerMayPipelineMay6 {


  def main (args:Array[String]): Unit={


    Logger.getLogger("org").setLevel(Level.OFF)
   // if (args.length == 0) {
     // System.out.println("Enter the name of the topic");
     // return;
    //}
    //al topicName=args(0)

  // set the scene for ingesting form Twitter


    //    val filters=List("#Android")
    System.setProperty("twitter4j.oauth.consumerKey", "0LiL3IZyQhHa3wtWD5w00k97M")
    System.setProperty("twitter4j.oauth.consumerSecret", "v8zlBUhrh92dvTkOy05k2FHAIykPXSMnm6SR0xCweyNTsZymgt")
    System.setProperty("twitter4j.oauth.accessToken", "1251902462241517570-8HhWzHZdNurIMq6ksoMxqPy38DsUHI")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "BddFuHswZ7VWEdqJTKHPIvqU175JpEhigrZe6VfGAPyS7")


    // setting the scene for Twitter
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("sample-structured-streaming")
      .getOrCreate()


    val sc=spark.sparkContext
    val ssc=new StreamingContext(sc,Seconds(5))
    val tweetStream=TwitterUtils.createStream(ssc,None)
    val tweetSream=tweetStream.map(t=>(t.getUser.getScreenName,(t.getText,t.getLang)))
   // val tweetSreamEN=tweetStream.filter(t=>t.getLang=="en").map(t=>(t.getText,t.getUser.getScreenName))


    // set the scene for my produccer
    val props = new Properties()


    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384");
    // buffer.memory controls the total amount of memory available to the producer for buffering
    props.put("buffer.memory", "33554432");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


// start processing tweets

    tweetSream.foreachRDD(rdd=>{

      rdd.foreachPartition(l=>{

        var producer=new KafkaProducer[String,String](props)
        l.foreach{tweet=>

          val UserTweet =tweet._1
          val textTweet=tweet._2._1
          val TweetLang=tweet._2._2
          println(s"($textTweet,$UserTweet")
          val record=new ProducerRecord("topicName","key",s"$UserTweet,$TweetLang,$textTweet")
          producer.send(record)


        }
      })
    }


    )

ssc.start()
    ssc.awaitTermination()


  }
}

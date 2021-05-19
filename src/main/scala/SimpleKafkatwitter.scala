import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SimpleKafkatwitter {

  def main (args: Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.OFF)
    if (args.length == 0) {
      System.out.println("Enter the name of the topic");
      return;
    }
val topicName=args(0)

    val consumerKey = "0LiL3IZyQhHa3wtWD5w00k97M"
    val consumerSecret = "v8zlBUhrh92dvTkOy05k2FHAIykPXSMnm6SR0xCweyNTsZymgt"
    val accessToken = "1251902462241517570-8HhWzHZdNurIMq6ksoMxqPy38DsUHI"
    val accessTokenSecret = "BddFuHswZ7VWEdqJTKHPIvqU175JpEhigrZe6VfGAPyS7"





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

    val ssc = new StreamingContext(sc, Seconds(5))
    //ssc.checkpoint("/Users/benyahiasadok/IdeaProjects/April13/src/main/Cheking")
    val tweets0 = TwitterUtils.createStream(ssc, None)



    val TweentsEn=tweets0.filter(tweet => tweet.getLang()=="en").map(status=>(status.getText,status.getUser.getScreenName))

    val TweentsFr=tweets0.filter(tweet => tweet.getLang()=="fr").map(status=>(status.getText,status.getUser.getScreenName))

    //  val aggStream=tweets0..flatMap(x=>x.getText.split(" ")).filter(_.startsWith(


// setting the scene for my producer


    val props = new Properties()


    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384");
    // buffer.memory controls the total amount of memory available to the producer for buffering
    props.put("buffer.memory", "33554432");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")



    val producer = new KafkaProducer[String, String](props)

    //  EsSpark.saveToEs(top60,"spark/docs")

// preparing the scene for processing tweets
    TweentsFr.foreachRDD(rdd=> {


      rdd.foreachPartition(l => {
        val producer = new KafkaProducer[String, String](props)

        l.foreach { tweet => {

          val text = tweet._1
          val user = tweet._2
           println(s"\n ")
          val record = new ProducerRecord(topicName, "key", s"($user--: $text)")
          producer.send(record).get()
          println(s"\n ($user--: $text)")
        }

        }
        producer.close()
      })




      /*





      }

    )


*/
    })
    ssc.start()
    ssc.awaitTermination()



  }


}

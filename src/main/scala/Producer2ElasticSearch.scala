import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions


object Producer2ElasticSearch  {


  def main(args : Array[String]):Unit=
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    if (args.length == 0) {
      System.out.println("Enter the name of the topic");
      return;
    }
    val topicName = args(0)

  //  Logger.getLogger("org").setLevel(Level.OFF)
// kafka settings

    val props = new Properties()


    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384");
    // buffer.memory controls the total amount of memory available to the producer for buffering
    props.put("buffer.memory", "33554432");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")







    // define twitter conf



    //    val filters=List("#Android")
    System.setProperty("twitter4j.oauth.consumerKey", "0LiL3IZyQhHa3wtWD5w00k97M")
    System.setProperty("twitter4j.oauth.consumerSecret", "v8zlBUhrh92dvTkOy05k2FHAIykPXSMnm6SR0xCweyNTsZymgt")
    System.setProperty("twitter4j.oauth.accessToken", "1251902462241517570-8HhWzHZdNurIMq6ksoMxqPy38DsUHI")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "BddFuHswZ7VWEdqJTKHPIvqU175JpEhigrZe6VfGAPyS7")

    val spark = SparkSession.builder()
     // .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "username")
      //.config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "password")
      .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .master("local[2]")
      .appName("sample-structured-streaming")
      .getOrCreate()
 //   val conf=new SparkConf().setMaster("local[4]").setAppName("Errki first streaming App")
   // conf.set("es.index.auto.create", "true")
   // conf.set("es.nodes", "127.0.0.1")
    //conf.set("es.port", "9200")


val sc=spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(5))
    //ssc.checkpoint("/Users/benyahiasadok/IdeaProjects/April13/src/main/Cheking")
    val tweets0 = TwitterUtils.createStream(ssc, None)


    val hashTags=tweets0.flatMap(status=>status.getText.split(" ").filter(_.startsWith("#")))
  //  hashTags.print()
    //hashTags.checkpoint(Seconds(10))
 // @SerialVersionUID(100L)
  case class Tweeet(tag: String, count:Int) extends Serializable

val tweetNews=  tweets0.map(t=>(t.getUser.getScreenName,t.getText,t.getLang))
    val top60=hashTags.map(s=>(s,1)).reduceByKey(_+_)
      .map{case(topic,count)=>(count,topic)}
      //.map(elem=>Tweeet(elem._2,elem._1)).map(elem=>Tweeet(elem.tag,elem.count))
      //transform(_.sortByKey(false))

  //  val top60=hashTags.map(s=>(s,1)).reduceByKeyAndWindow(_+_,_-_,Seconds(60)).map{case(topic,count)=>(count,topic)}.transform(_.sortByKey(false))
  //  EsSpark.saveToEs(top60,"spark/docs")

    val schema = new StructType()
      .add(StructField("tag", StringType, true))
      .add(StructField("count", IntegerType, true))

    val sqlContext = new SQLContext(sc)

    import org.elasticsearch.spark.sql._

    tweetNews.foreachRDD (
      rdd => {

rdd.saveAsTextFile("src/main/may11")


     //   EsSpark.saveToEs(rdd, "spark100/docs")

        val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())

        val rdd2df = sqlContext.createDataFrame(rdd).toDF("User", "Text","Language")
        rdd2df.show()


        //Index the Word, Count attributes to ElasticSearch Index. You don't need to create any index in Elastic Search

        rdd2df.saveToEs("kafkawordcount_v2/kwc")





        //df.write.format("org.elasticsearch.spark.sql").mode("append").option("es.resource","wissem").option("es.nodes", "http://localhost:9200").save()
        println("\n popular topics in last 60 seconds (%s total):".format(rdd.count()))
        rdd.foreachPartition(elem=>{
          val producer = new KafkaProducer[String, String](props)
          elem.foreach {t =>
            {val record = new ProducerRecord(topicName, "key", s"(${t._1},${t._2},${t._3}")
             producer.send(record)
          }

        }})



      }









  )





    ssc.start()
    ssc.awaitTermination()

  }
}

/*
*/
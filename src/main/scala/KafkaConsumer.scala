import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaConsumer{

  def main(args:Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("SparkKafkaStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(15))
    val sc = ssc.sparkContext

    val topicsSet = Set("test")

    // val kafkaParams = giveMeKafkaProps(Array("localhost:9092", "console-consumer-23162"))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("test"), kafkaParams)
    )

    val stream1 = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("test1"), kafkaParams)
    )


    val LinesFr=stream.flatMap(l=>l.value.split(" ")).map(x => (x, 1L)).reduceByKey(_+_).map{case (k,c)=>(c,k)}
    val LinesEn=stream1.flatMap(l=>l.value.split(" ")).map(x => (x, 1L)).reduceByKey(_+_).map{case (k,c)=>(c,k)}
    val sameLength=LinesFr.join(LinesEn)
   // sameLength.print()
    val same=sameLength.groupByKey()
    println("connection stream =======================")
  //  same.print()





    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //val producer = new KafkaProducer[String, String](props)

    same.foreachRDD(rdd => {

      rdd.foreachPartition{ elem =>{
        val producer = new KafkaProducer[String, String](props)
        elem.foreach{p=>{

          val key=p._1
          var list =""
          for (elem1 <- p._2) list+= elem1._1 +"--"+elem1._2 +" "
println(s"($key,$list")
          val record = new ProducerRecord[String, String]("Vowel", "key", s"($key,$list")
          producer.send(record)
        }

          producer.close()

        }





       }


      }



    })










    ssc.start()

    ssc.awaitTermination()

  }}

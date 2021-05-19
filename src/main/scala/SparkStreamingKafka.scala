import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingKafka {



  def main(args:Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("SparkKafkaStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(15))
    val sc = ssc.sparkContext



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

    val stream1 = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("test1"), kafkaParams)
    )



    val linesFR =stream.flatMap(l=>l.value().split(" ")).map(w=>(w,1)).reduceByKey(_+_).map{case (k,c)=>(c,k)}
    val linesEN=stream1.flatMap(l=>l.value().split(" ")).map(w=>(w,1)).reduceByKey(_+_).map{case (k,c)=>(c,k)}
    val same=linesFR.join(linesEN)
    val sameCount=same.groupByKey()




    // prepare my producer
    val props = new Properties()


    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384");
    // buffer.memory controls the total amount of memory available to the producer for buffering
    props.put("buffer.memory", "33554432");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    // start processing stream


sameCount.foreachRDD(rdd=>{

  rdd.foreachPartition{elem=>{
    val producer=new KafkaProducer[String,String](props)
    elem.foreach{p=>{
      val key=p._1
      var listWords=" "
      for (pair <-p._2) listWords+=pair._1 +"--"+pair._2 +" "
      println(s"($key,$listWords)")
      val record=new ProducerRecord[String,String]("Andres","key",s"($key,$listWords)")
      producer.send(record)

    }
producer.close()

    }
  }

  }

}




)


    ssc.start()
    ssc.awaitTermination()

  }

}

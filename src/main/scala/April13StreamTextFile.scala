import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object April13StreamTextFile {



  def main (args: Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.OFF)
    val conf=new SparkConf().setMaster("local[4]").setAppName("Errki first streaming App")
    val ssc=new StreamingContext(conf,Seconds(3))
    /*
    val lines=ssc.textFileStream("/Users/benyahiasadok/IdeaProjects/April13/src/April13")
    val words= lines.flatMap(l=> l.split("," ))

*/






    val file = ssc.textFileStream("/Users/benyahiasadok/IdeaProjects/April13/src/main/4testing")
    file.foreachRDD(t => {
      val test = t.flatMap(line => line.split(" ")).map(word => (word,1)) .reduceByKey(_+_)
      test.saveAsTextFile("/Users/benyahiasadok/IdeaProjects/April13/src/test") })


    ssc.start()
    ssc.awaitTermination()
}}

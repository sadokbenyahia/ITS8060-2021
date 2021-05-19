
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions


object KafkaStructuredStreaming {

  def main (args: Array[String]): Unit=
    {
      Logger.getLogger("org").setLevel(Level.OFF)
  //    val sc=SparkSession.builder().master("local[*]").appName("Spark07032021_2 ").getOrCreate()
  val sc = SparkSession.builder()
    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "username")
    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "password")
    .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
    .config(ConfigurationOptions.ES_PORT, "9200")
    .master("local[2]")
    .appName("sample-structured-streaming")
    .getOrCreate()

      val df = sc.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "hengaTopic")
      .option("startingOffsets", "earliest") // From starting
      .load()


      df.printSchema()
      // plz notice that key and value are binary



      //df.show(false)
      //org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();;
val personCasttoString=df.selectExpr("CAST(value as STRING)")

      val schema = new StructType()
        .add("id",IntegerType)
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType)
        .add("dob_year",IntegerType)
        .add("dob_month",IntegerType)
        .add("gender",StringType)
        .add("salary",IntegerType)
val personDF=personCasttoString.select(from_json(col("value"),schema).as ("data")).select("data.*")
      //val person = df.selectExpr("CAST(value AS STRING)")
       // .select(from_json(col("value"), schema).as("data"))
        //.select("data.*")

/* output to console
      val query = personDF.writeStream
          .format("console")
           .outputMode("append")
           .start()
            .awaitTermination()
*/
      personDF.writeStream
        .outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("checkpointLocation", "/Users/benyahiasadok/IdeaProjects/April13/src/main/4testing")
        .start("index-name/doc-type").awaitTermination()

//output to elasticSearch


/*
      /**
       *uncomment below code if you want to write it to kafka topic.
       */
      df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", "192.168.1.100:9092")
        .option("topic", "josn_data_topic")
        .start()
        .awaitTermination()

*/
    }



}

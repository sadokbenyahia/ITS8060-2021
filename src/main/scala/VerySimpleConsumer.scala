import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

object VerySimpleConsumer {
 def main(args:Array[String]):Unit= {
    if(args.length == 0)
    {
      System.out.println("Enter the name of the topic")
      return;
    }

  val topicName = args(0)
    //Step 1.0: test of Arguments
    val props = new Properties()
    //Step 2.0: Create a Properties object
    // Step 3.0: Add configurations to Properties object
    props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

     val consumer =
       new KafkaConsumer[String, String](props)
     consumer.subscribe(java.util.Collections.singletonList(topicName))




   while (true){
  val records = consumer.poll(100)





       records.forEach{r=>{
         println(r.key)
         println(r.partition())
         println(r.topic())
         println (r.timestamp())
         println (r.value())
       }

         }

    //   val iterator = records.iterator()
  //while(iterator.hasNext())
   //{
    // println(iterator.next)

   //}
     }
}
}

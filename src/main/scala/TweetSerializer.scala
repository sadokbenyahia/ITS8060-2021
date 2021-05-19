


import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import java.util
import org.apache.kafka.common.serialization.Serializer


class TweetSerializer extends Serializer[Tweet]{

  override def configure(configs: util.Map[String,_],isKey: Boolean):Unit = {

  }

//case class Tweet(User:String, Text:String, language:String, RetweetCount:Int, CreationTime:Date)


  override def serialize(topic:String, data:Tweet):Array[Byte] = {
    try {
      val byteOut = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(byteOut)
      objOut.writeObject(data)
      objOut.close()
      byteOut.close()
      byteOut.toByteArray
    }
    catch {
      case ex:Exception => throw new Exception(ex.getMessage)
    }
  }

  override def close():Unit = {

  }


}

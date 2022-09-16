import org.json4s.{DefaultFormats}
import org.json4s.native.Serialization.{read, write}
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

object Json4SSerde {
  implicit val formats = DefaultFormats

  class JsonSerializer[T >: Null <: Any : Manifest] extends Serializer[T] {
    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
    override def serialize(topic: String, data: T): Array[Byte] = {
      val dataStr = write(data)
      val dataBytes = stringSerde.serializer().serialize(topic, dataStr)
      dataBytes
    }
    override def close(): Unit = ()
  }

  class JsonDeserializer[T >: Null <: Any : Manifest] extends Deserializer[T]  {
    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
    override def close(): Unit = ()
    override def deserialize(topic: String, data: Array[Byte]): T = {
      val dataStr = stringSerde.deserializer().deserialize(topic, data)
      val dataT= read[T](dataStr)
      dataT
    }
  }

  class JsonSerde[T >: Null <: Any : Manifest] extends Serde[T] {
    override def deserializer(): Deserializer[T] = new JsonDeserializer[T]
    override def serializer(): Serializer[T] = new JsonSerializer[T]
    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
    override def close(): Unit = ()
  }
}
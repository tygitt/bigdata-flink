package cn.brc.lraj.trackingEvent.deserialization

import java.nio.charset.StandardCharsets

import cn.brc.lraj.trackingEvent.bean.TrackingEvent
import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

class TrackingEventBeanDeserialization extends KafkaDeserializationSchema[TrackingEvent] {

  private val logger = LoggerFactory.getLogger(classOf[TrackingEventBeanDeserialization])

  override def isEndOfStream(t: TrackingEvent): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): TrackingEvent = {
    val gson = new Gson()
    val message = consumerRecord.value()
    var bean: TrackingEvent = null
    val str1 = new String(message, StandardCharsets.UTF_8)
    try {
      val str = new StringBuilder(str1)
      str.insert(str.indexOf("expandData") + "expandData".length + 2, "'").insert(str.indexOf("}") + 1, "'")
      bean = gson.fromJson[TrackingEvent](str.toString(), classOf[TrackingEvent])
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error("数据解析错误"+ "--" + str1)
    }
    bean
  }

  override def getProducedType: TypeInformation[TrackingEvent] = TypeInformation.of(new TypeHint[TrackingEvent]() {})
}

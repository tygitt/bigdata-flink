package cn.brc.lraj.trackingEvent.sink

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import cn.brc.lraj.trackingEvent.bean.TrackingEvent
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import redis.clients.jedis.{HostAndPort, JedisCluster}

class RedisLocationUvSink(set: util.HashSet[HostAndPort] , sdf: SimpleDateFormat) extends RichSinkFunction[TrackingEvent]{

  val redisLocationHashName = "lraj_location_uv_hash"
  val redisLocationUvSetPreName = "locationUv_"
  var jedis: JedisCluster = _


  override def open(parameters: Configuration): Unit = {
    try{
      jedis = new JedisCluster(set)
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  override def invoke(value: TrackingEvent, context: SinkFunction.Context[_]): Unit = {
    try{
      val locationSetName = redisLocationUvSetPreName + value.location_province + "_" + value.location_city + "_" + sdf.format(new Date()).substring(0, 10)
      val locationName = value.location_province + "_" + value.location_city
      jedis.sadd(locationSetName, value.device_id)
      jedis.hset(redisLocationHashName, locationName, locationSetName)
      jedis.expire(locationSetName, 60 * 60 * 24 * 2)
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  override def close(): Unit = {
    try{
      if(jedis != null) jedis.close()
    }
  }
}

package cn.brc.lraj.trackingEvent.sink

import java.util

import cn.brc.lraj.trackingEvent.bean.TrackingEvent
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
 * 自定义redis sink，使用redis的set数据结构去重，统计uv
 * @param set
 */
class RedisSink(set: util.HashSet[HostAndPort]) extends RichSinkFunction[TrackingEvent]{

  var jedis: JedisCluster = _

  override def open(parameters: Configuration): Unit = {
    try{
      jedis = new JedisCluster(set)
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  /**
   * 埋点推送数据的gmtTime中的日期字段，作为redis字段中的key
   * 埋点推送数据的device_id作为统计uv的唯一标识
   * redis中的key样式为：2021-02-03_lraj_uv
   * @param value
   * @param context
   */
  override def invoke(value: TrackingEvent, context: SinkFunction.Context[_]): Unit = {
    try{
      val key = value.gmtTime.substring(0, 10) + "_lraj_uv"
      val v = value.device_id
      jedis.sadd(key, v)
      jedis.expire(key, 60 * 60 * 24 * 2)
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  override def close(): Unit = {
    super.close()
    try{
      if(jedis != null) jedis.close()
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}

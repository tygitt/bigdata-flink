package cn.brc.lraj.trackingEvent.sink

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
 * 统计项目当天的uv
 * (String,String,String) = (项目id, 唯一标识("设备id"),gmtTime)
 * @param set
 */
class RedisProjectUvSink(set: util.HashSet[HostAndPort]) extends RichSinkFunction[(String, String, String)]{

  var jedis: JedisCluster = _
  val redisHashName = "lraj_project_uv_hash"
  val redisProjectUvSetPreName = "projectUv_"

  override def open(parameters: Configuration): Unit = {
    try{
      jedis = new JedisCluster(set)
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  override def invoke(value: (String, String, String), context: SinkFunction.Context[_]): Unit = {
    try{
      val setName = redisProjectUvSetPreName + value._1 + "_" + value._3.substring(0, 10)
      jedis.sadd(setName, value._2)
      jedis.hset(redisHashName, value._1, setName)
      jedis.expire(setName, 60 * 60 * 24 * 2)
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

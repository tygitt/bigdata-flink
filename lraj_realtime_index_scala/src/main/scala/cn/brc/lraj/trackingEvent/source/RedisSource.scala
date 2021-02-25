package cn.brc.lraj.trackingEvent.source

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
 * 周期从redis中获取当天uv信息
 * @param set
 * @param sdf
 */
class RedisSource(set: util.HashSet[HostAndPort], sdf:SimpleDateFormat) extends RichSourceFunction[(String, Int, String)] {

  var jedis: JedisCluster = _
  var isRunning = true

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    try{
      jedis = new JedisCluster(set)
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  /**
   * 周期读取redis中的当日uv统计结果
   * @param ctx
   */
  override def run(ctx: SourceFunction.SourceContext[(String, Int, String)]): Unit = {
    while (isRunning) {
      try {
        val date = new Date()
        val key = sdf.format(date).substring(0, 10) + "_lraj_uv"
        val v: Int = jedis.scard(key).toInt
        ctx.collect((key, v, sdf.format(date)))
        Thread.sleep(1000 * 60 * 5)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

}

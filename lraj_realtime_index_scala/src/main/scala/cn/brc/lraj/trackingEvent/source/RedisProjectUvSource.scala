package cn.brc.lraj.trackingEvent.source

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.JavaConversions._

/**
 * 读取redis中的project uv数量
 * (String, String, Int, String) = (uv_date, project_id, today_uv, create_time)
 * @param set
 */
class RedisProjectUvSource(set: util.HashSet[HostAndPort], sdf:SimpleDateFormat) extends RichSourceFunction[(String, String, Int, String)] {

  var jedis: JedisCluster = _
  //var sdf: SimpleDateFormat = _
  var isRunning = true

  val redisHashName = "lraj_project_uv_hash"
  val redisProjectUvSetPreName = "projectUv_"

  override def open(parameters: Configuration): Unit = {
    try {
      jedis = new JedisCluster(set)
      //sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  override def run(ctx: SourceFunction.SourceContext[(String, String, Int, String)]): Unit = {
    while (isRunning) {
      try {
        Thread.sleep(1000 * 60 * 5)
        val list = jedis.hvals(redisHashName)
        list.toList
          .filter(_.split("_")(2).equals(sdf.format(new Date).substring(0, 10)))
          .foreach(setName => {
            val strings = setName.split("_")
            val uvDate = strings(2)
            val projectId = strings(1)
            val today_uv = jedis.scard(setName).toInt
            ctx.collect((uvDate, projectId, today_uv, sdf.format(new Date)))
          })
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

package cn.brc.lraj.trackingEvent.source

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.JavaConversions._

/**
 * (String, String, String, Int, String) = (uv_date, province, city, today_uv, create_time)
 * @param set
 * @param sdf
 */
class RedisLocationUvSource(set: util.HashSet[HostAndPort], sdf: SimpleDateFormat) extends RichSourceFunction[(String, String, String, Int, String)]{

  var isRunning = true

  val redisLocationHashName = "lraj_location_uv_hash"

  var jedis: JedisCluster = _


  override def open(parameters: Configuration): Unit = {
    try{
      jedis = new JedisCluster(set)
    }
  }

  override def run(ctx: SourceFunction.SourceContext[(String, String, String, Int, String)]): Unit = {
    while (isRunning) {
      try {
        Thread.sleep(1000 * 60 * 5)
        val date = new Date()
        val list = jedis.hvals(redisLocationHashName)
        list.toList
          .filter(_.split("_")(3).equals(sdf.format(new Date).substring(0, 10)))
          .foreach(setName => {
            val strings = setName.split("_")
            val uvDate = strings(3)
            val province = strings(1)
            val city = strings(2)
            val today_uv = jedis.scard(setName).toInt
            ctx.collect((uvDate, province, city, today_uv, sdf.format(date)))
          })
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  override def cancel(): Unit = isRunning = false
}

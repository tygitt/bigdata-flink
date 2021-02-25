package cn.brc.lraj.trackingEvent

import java.sql.Date
import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import cn.brc.lraj.trackingEvent.bean.{ExpandData, TrackingEvent}
import cn.brc.lraj.trackingEvent.deserialization.TrackingEventBeanDeserialization
import cn.brc.lraj.trackingEvent.sink.{MySqlLocationUvSink, MySqlProjectUvSink, MySqlSink, RedisLocationUvSink, RedisProjectUvSink, RedisSink}
import cn.brc.lraj.trackingEvent.source.{RedisLocationUvSource, RedisProjectUvSource, RedisSource}
import com.google.gson.Gson
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import redis.clients.jedis.HostAndPort

object RealtimeIndex {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val url = "jdbc:mysql://10.0.22.160:3306/tracking_event?useUnicode=true&characterEncoding=UTF8"
  val user = "brc"
  val password = "BRC@1234"
  val topic = "topic_tracking_event"
  val boostrapServer = "cdh-kafka-97:9092, cdh-kafka-98:9092, cdh-kafka-99:9092"

  val set = new util.HashSet[HostAndPort]()
  set.add(new HostAndPort("10.0.22.157", 6390))
  set.add(new HostAndPort("10.0.22.158", 6390))
  set.add(new HostAndPort("10.0.22.159", 6390))
  set.add(new HostAndPort("10.0.22.157", 6391))
  set.add(new HostAndPort("10.0.22.158", 6391))
  set.add(new HostAndPort("10.0.22.159", 6391))

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(30000L)

    val trackingEventDS: DataStream[TrackingEvent] = env.addSource(getKafkaSource(topic, boostrapServer)).filter(_ != null)

    processPv(trackingEventDS)

    processUv(env, trackingEventDS)

    processProjectUv(env, trackingEventDS)

    processLocationUv(env, trackingEventDS)

    env.execute("lraj realtime index...")
  }

  /**
   * 城市项目uv统计，周期写入mysql
   * @param env
   * @param ds
   */
  def processLocationUv(env: StreamExecutionEnvironment, ds: DataStream[TrackingEvent]):Unit = {

    ds.filter(item => item.location_province != null && item.location_province.length != 0)
      .addSink(new RedisLocationUvSink(set, sdf))

    val locationUvDS: DataStream[(String, String, String, Int, String)] = env.addSource(new RedisLocationUvSource(set, sdf))

    locationUvDS
      .addSink(new MySqlLocationUvSink(url, user, password))
      .name("uv_location_mysql_sink")
  }

  /**
   * 计算各个项目的当日uv，周期写入mysql
   * @param env
   * @param ds
   */
  def processProjectUv(env: StreamExecutionEnvironment, ds: DataStream[TrackingEvent]): Unit = {
    val projectDeviceDS = ds.map(item => {
      val gson = new Gson()
      val expandData = gson.fromJson[ExpandData](item.expandData, classOf[ExpandData])
      (expandData.project_id, item.device_id, item.gmtTime)
    })

    projectDeviceDS
      .filter(expandData => expandData._1 != null && expandData._1.length != 0 )
      .addSink(new RedisProjectUvSink(set))

    env.addSource(new RedisProjectUvSource(set, sdf))
      .addSink(new MySqlProjectUvSink(url, user, password))
      .name("uv_project_mysql_sink")

  }

  /**
   * 计算uv，周期将今日累计uv写入MySql
   * @param env
   * @param ds
   */
  def processUv(env: StreamExecutionEnvironment, ds: DataStream[TrackingEvent]):Unit = {
    // 将数据写入Redis Set，来进行去重
    ds.addSink(new RedisSink(set))

    val redisDS: DataStream[(String, Int, String)] = env.addSource(new RedisSource(set, sdf))
    redisDS
      .map(item => (item._1.substring(0, 10), item._2, item._3))
      .addSink(new MySqlSink(url, user, password, "uv"))
      .name("uv_mysql_sink")
  }

  /**
   * 计算pv，并周期将数据写入MySql
   * @param ds
   */
  def processPv(ds: DataStream[TrackingEvent]): Unit = {
    val pvResultDS = ds.map(_.tracking_id -> 1)
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
      .sum(1)
      .map(item => Tuple3(item._1, item._2, sdf.format(new Date(System.currentTimeMillis() + (8 * 60 * 60 * 1000)))))

    // 将计算结果写入mysql
    pvResultDS
      .addSink(new MySqlSink(url, user, password, "pv"))
      .name("pv_mysql_sink")
  }

  /**
   * 对接kafka
   * @param topic
   * @param bootstrapServer
   * @return
   */
  def getKafkaSource(topic: String, bootstrapServer: String): FlinkKafkaConsumer011[TrackingEvent] = {
    val prop = new Properties()
    prop.put("bootstrap.servers", bootstrapServer)
    prop.put("group.id", "lraj_realtime_group")
    prop.put("enable.auto.commit", "true")
    prop.put("auto.commit.interval.ms", "1000")
    prop.put("session.timeout.ms", "30000")
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    new FlinkKafkaConsumer011[TrackingEvent](topic, new TrackingEventBeanDeserialization(), prop)
  }
}

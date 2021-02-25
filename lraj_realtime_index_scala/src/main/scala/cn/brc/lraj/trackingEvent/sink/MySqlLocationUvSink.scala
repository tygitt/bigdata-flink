package cn.brc.lraj.trackingEvent.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MySqlLocationUvSink(url: String, username: String, password: String) extends RichSinkFunction[(String, String, String, Int, String)]{

  var conn: Connection = _
  var stat: PreparedStatement = _

  val sql = "insert into tracking_event_location_uv(uv_date, province, city, today_uv, create_time)" + " values(?,?,?,?,?)"


  override def open(parameters: Configuration): Unit = {
    try{
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(url, username, password)
      stat = conn.prepareStatement(sql)
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  override def invoke(value: (String, String, String, Int, String), context: SinkFunction.Context[_]): Unit = {
    try{
      stat.setString(1, value._1)
      stat.setString(2, value._2)
      stat.setString(3, value._3)
      stat.setInt(4, value._4)
      stat.setString(5, value._5)
      stat.addBatch()
      stat.execute()
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  override def close(): Unit = {
    try {
      if (stat != null) {
        stat.close()
      }
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}

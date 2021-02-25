package cn.brc.lraj.trackingEvent.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MySqlProjectUvSink(url: String, username: String, password: String) extends RichSinkFunction[(String, String, Int, String)] {

  var conn: Connection = _
  var stat: PreparedStatement = _
  val sql: String = "insert into tracking_event_project_uv(uv_date, project_id, today_uv, create_time)" + " values(?,?,?,?)"

  override def open(parameters: Configuration): Unit = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(url, username, password)
      stat = conn.prepareStatement(sql)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  override def invoke(value: (String, String, Int, String), context: SinkFunction.Context[_]): Unit = {
    try {
      stat.setString(1, value._1)
      stat.setString(2, value._2)
      stat.setInt(3, value._3)
      stat.setString(4, value._4)
      stat.addBatch()
      stat.executeBatch()
    } catch {
      case e: Exception => e.printStackTrace()
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

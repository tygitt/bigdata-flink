package cn.brc.lraj.trackingEvent.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MySqlSink(url: String, username: String, password: String, pvOrUv: String) extends RichSinkFunction[(String, Int, String)] {

  var conn: Connection = _
  var stat: PreparedStatement = _
  val sqlPv: String = "insert into tracking_event_pv(tracking_id, pv_per_five_min, create_time)" + " values (?,?,?)"
  val sqlUv: String = "insert into tracking_event_uv(uv_date, today_uv, create_time)" + " values (?,?,?)"

  override def open(parameters: Configuration): Unit = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(url, username, password)
      pvOrUv match {
        case "pv" => stat = conn.prepareStatement(sqlPv)
        case "uv" => stat = conn.prepareStatement(sqlUv)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  override def invoke(value: (String, Int, String), context: SinkFunction.Context[_]): Unit = {
    try {
      stat.setString(1, value._1)
      stat.setInt(2, value._2)
      stat.setString(3, value._3)
      stat.addBatch()
      stat.executeBatch()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  override def close(): Unit = {
    try{
      if(stat != null){
        stat.close()
      }
      if(conn != null){
        conn.close()
      }
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}

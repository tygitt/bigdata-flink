package cn.brc.trackingEvent.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class HiveUtil implements Serializable {

    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(HiveUtil.class);

    public static Connection conn = null;

    public static PreparedStatement stat = null;

    public static void registerConn() {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://cdh-rm-100:10000";
//        String url = "jdbc:hive2://bgdata00:10000";
        String user = "hive";
        String pass = "";
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, pass);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("get hive connection error....");
        }
    }

    public static void addPartition(String database, String tableName, String partition) {
        registerConn();
        try {
            stat = conn.prepareStatement("alter table " + database + "." + tableName + " add if not exists partition (ds='" + partition + "')");
            stat.execute();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("add partition failed....");
        } finally {
            closeConn();
        }
    }

    public static void closeConn() {
        try {
            if (stat != null) {
                stat.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("conn close error...");
        }
    }

//    public static void main(String[] args) throws Exception {
//        registerConn();
//        addPartition("brc_iot_prod", "ods_tracking_event_data", "20210201");
//        closeConn();
//    }
}

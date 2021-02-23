package cn.brc.trackingEvent.sink;

import cn.brc.trackingEvent.bean.TrackingEventBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class HiveSink extends RichSinkFunction<TrackingEventBean> {

    private Connection conn;

    private PreparedStatement pstat;

    private String sql;

    private static Connection getConn(){

        Connection conn = null;
        try{
            String jdbc = "org.apache.hive.jdbc.HiveDriver";
            String url = "jdbc:hive2://bgdata00:10000/default";
            String user = "hive";
            String pass = "";

            Class.forName(jdbc);
            conn = DriverManager.getConnection(url, user, pass);
        } catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = getConn();

        sql = "insert into internal_table1 partition (ds = '20210101') " +
//                " (imei,\n" +
//                "imsi,\n" +
//                "phone_wifi_mac,\n" +
//                "router_mac,\n" +
//                "device_id,\n" +
//                "tracking_id,\n" +
//                "os_version,\n" +
//                "browser_type,\n" +
//                "browser_ver,\n" +
//                "u_uid,\n" +
//                "mobile_operators_desc,\n" +
//                "network_desc,\n" +
//                "device_desc,\n" +
//                "app_version,\n" +
//                "app_channel,\n" +
//                "appkey,\n" +
//                "device_language,\n" +
//                "ip,\n" +
//                "geo_position,\n" +
//                "location_province,\n" +
//                "location_city,\n" +
//                "location_address,\n" +
//                "gmtTime,\n" +
//                "mac,\n" +
//                "person_id,\n" +
//                "expandData) " +
                "values (?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?\n" +
                "?)";

        pstat = conn.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(pstat != null){
            pstat.close();
        }
        if(conn != null){
            conn.close();
        }
    }

    @Override
    public void invoke(TrackingEventBean value, Context context) throws Exception {

        pstat.setString(1,value.getImei());
        pstat.setString(2,value.getImsi());
        pstat.setString(3,value.getPhone_wifi_mac());
        pstat.setString(4,value.getRouter_mac());
        pstat.setString(5,value.getDevice_id());
        pstat.setString(6,value.getTracking_id());
        pstat.setString(7,value.getOs_version());
        pstat.setString(8,value.getBrowser_type());
        pstat.setString(9,value.getBrowser_ver());
        pstat.setString(10,value.getU_uid());
        pstat.setString(11,value.getMobile_operators_desc());
        pstat.setString(12,value.getNetwork_desc());
        pstat.setString(13,value.getDevice_desc());
        pstat.setString(14,value.getApp_version());
        pstat.setString(15,value.getApp_channel());
        pstat.setString(16,value.getAppkey());
        pstat.setString(17,value.getDevice_language());
        pstat.setString(18,value.getIp());
        pstat.setString(19,value.getGeo_position());
        pstat.setString(20,value.getLocation_province());
        pstat.setString(21,value.getLocation_city());
        pstat.setString(22,value.getLocation_address());
        pstat.setString(23,value.getGmtTime());
        pstat.setString(24,value.getMac());
        pstat.setString(25,value.getPerson_id());
        pstat.setString(26,value.getExpandData());

        pstat.execute();
    }
}

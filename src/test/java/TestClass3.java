import cn.brc.trackingEvent.bean.TrackingEventBean;
import com.google.gson.Gson;
import org.codehaus.jettison.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TestClass3 {

    public static void main(String[] args) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");

        String s1 = sdf.format(new Date());

        System.out.println(s1.substring(8));

        System.out.println(s1.substring(0, 8));

//        Gson gson = new Gson();
//        String json = "\n" +
//                "{\"imei\":\"imei10008\",\"imsi\":\"imsi10008\",\"phone_wifi_mac\":\"\",\"router_mac\":\"\",\"device_id\":\"\",\"tracking_id\":\"\",\"os_version\":\"\",\"browser_type\":\"\",\"browser_ver\":\"\",\"u_uid\":\"\",\"mobile_operators_desc\":\"\",\"network_desc\":\"\",\"device_desc\":\"\",\"app_version\":\"\",\"app_channel\":\"null\",\"appkey\":\"\",\"device_language\":\"\",\"ip\":\"\",\"geo_position\":\"\",\"location_province\":\"\",\"location_city\":\"\",\"location_address\":\"\",\"gmtTime\":\"\",\"mac\":\"\",\"person_id\":\"\",\"expandData\":{\"page_id\":\"page_id10008\",\"project_id\":\"project_id10008\",\"role\":\"role\",\"lbcx\":\"lbcx\",\"vcr_id\":\"vcr_id\",\"vr_id\":\"vr_id\"}}";
//
//        StringBuilder str = new StringBuilder(json);
//        str.insert(str.indexOf("expandData") + "expandData".length() + 2, "'");
//        str.insert(str.indexOf("}") + 1, "'");
//        String s = str.toString();
//
//        String json1 = "{\"imei\":\"imei10008\",\"imsi\":\"imsi10008\"}";
//
//        System.out.println(gson.fromJson(json1, TrackingEventBean.class));
//
//        TrackingEventBean bean = gson.fromJson(s, TrackingEventBean.class);
//        System.out.println(bean);

    }
}

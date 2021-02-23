import cn.brc.trackingEvent.bean.TrackingEventBean;
import com.google.gson.Gson;

public class TestClass7 {

    public static void main(String[] args) {
        String str1 = "{\"imei\":\"\",\"imsi\":\"\",\"phone_wifi_mac\":\"\",\"router_mac\":\"\",\"browser_type\":\"\",\"browser_ver\":\"\",\"u_uid\":\"\",\"mobile_operators_desc\":\"\",\"network_desc\":\"wifi\",\"app_channel\":\"\",\"channel\":\"APP\",\"ip\":\"\",\"gmtTime\":\"2021-02-00 19:44:19.952\",\"mac\":\"\",\"person_id\":\"13783486339\",\"device_id\":\"02:00:00:00:00:00\",\"os_version\":\"10\",\"device_desc\":\"OPPO\",\"device_language\":\"zh\",\"app_version\":\"2.1.2\",\"appKey\":\"蓝人爱家\",\"location_province\":\"河南省\",\"location_city\":\"郑州市\",\"location_address\":\"河南省郑州市新密市郑州曲梁产业集聚区高家\",\"location_district\":\"新密市\",\"geo_position\":\"34.547288,113.644274\",\"tracking_id\":\"LRAJ_APP_HOMEPAGE_SW\",\"expandData\":{\"project_id\":\"\",\"lbcx\":\"\",\"vcr_id\":\"\",\"vr_id\":\"\",\"role\":\"蓝光员工\"}}";

        Gson gson = new Gson();

        StringBuilder str = new StringBuilder(str1);

        str.insert(str.indexOf("expandData") + "expandData".length() + 2, "'").insert(str.indexOf("}") + 1, "'");

        String s = str.toString();

        String s1 = "abc";
        TrackingEventBean bean = null;

        try {
            bean = gson.fromJson(s1, TrackingEventBean.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(bean);

    }
}

package cn.brc.trackingEvent.bean;

/**
 * 埋点数据
 */
public class TrackingEventBean {
    private String imei;
    private String imsi;
    private String phone_wifi_mac;
    private String router_mac;
    private String device_id;
    private String tracking_id;
    private String os_version;
    private String browser_type;
    private String browser_ver;
    private String u_uid;
    private String mobile_operators_desc;
    private String network_desc;
    private String device_desc;
    private String app_version;
    private String app_channel;
    private String channel;
    private String ip;
    private String geo_position;
    private String location_province;
    private String location_city;
    private String location_district;
    private String location_address;
    private String gmtTime;
    private String mac;
    private String person_id;
    private String appkey;
    private String device_language;
    private String expandData;

    public TrackingEventBean(String imei, String imsi, String phone_wifi_mac, String router_mac, String device_id, String tracking_id, String os_version, String browser_type, String browser_ver, String u_uid, String mobile_operators_desc, String network_desc, String device_desc, String app_version, String app_channel, String channel, String ip, String geo_position, String location_province, String location_city, String location_district, String location_address, String gmtTime, String mac, String person_id, String appkey, String device_language, String expandData) {
        this.imei = imei;
        this.imsi = imsi;
        this.phone_wifi_mac = phone_wifi_mac;
        this.router_mac = router_mac;
        this.device_id = device_id;
        this.tracking_id = tracking_id;
        this.os_version = os_version;
        this.browser_type = browser_type;
        this.browser_ver = browser_ver;
        this.u_uid = u_uid;
        this.mobile_operators_desc = mobile_operators_desc;
        this.network_desc = network_desc;
        this.device_desc = device_desc;
        this.app_version = app_version;
        this.app_channel = app_channel;
        this.channel = channel;
        this.ip = ip;
        this.geo_position = geo_position;
        this.location_province = location_province;
        this.location_city = location_city;
        this.location_district = location_district;
        this.location_address = location_address;
        this.gmtTime = gmtTime;
        this.mac = mac;
        this.person_id = person_id;
        this.appkey = appkey;
        this.device_language = device_language;
        this.expandData = expandData;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getLocation_district() {
        return location_district;
    }

    public void setLocation_district(String location_district) {
        this.location_district = location_district;
    }

    public TrackingEventBean() {
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public String getPhone_wifi_mac() {
        return phone_wifi_mac;
    }

    public void setPhone_wifi_mac(String phone_wifi_mac) {
        this.phone_wifi_mac = phone_wifi_mac;
    }

    public String getRouter_mac() {
        return router_mac;
    }

    public void setRouter_mac(String router_mac) {
        this.router_mac = router_mac;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getTracking_id() {
        return tracking_id;
    }

    public void setTracking_id(String tracking_id) {
        this.tracking_id = tracking_id;
    }

    public String getOs_version() {
        return os_version;
    }

    public void setOs_version(String os_version) {
        this.os_version = os_version;
    }

    public String getBrowser_type() {
        return browser_type;
    }

    public void setBrowser_type(String browser_type) {
        this.browser_type = browser_type;
    }

    public String getBrowser_ver() {
        return browser_ver;
    }

    public void setBrowser_ver(String browser_ver) {
        this.browser_ver = browser_ver;
    }

    public String getU_uid() {
        return u_uid;
    }

    public void setU_uid(String u_uid) {
        this.u_uid = u_uid;
    }

    public String getMobile_operators_desc() {
        return mobile_operators_desc;
    }

    public void setMobile_operators_desc(String mobile_operators_desc) {
        this.mobile_operators_desc = mobile_operators_desc;
    }

    public String getNetwork_desc() {
        return network_desc;
    }

    public void setNetwork_desc(String network_desc) {
        this.network_desc = network_desc;
    }

    public String getDevice_desc() {
        return device_desc;
    }

    public void setDevice_desc(String device_desc) {
        this.device_desc = device_desc;
    }

    public String getApp_version() {
        return app_version;
    }

    public void setApp_version(String app_version) {
        this.app_version = app_version;
    }

    public String getApp_channel() {
        return app_channel;
    }

    public void setApp_channel(String app_channel) {
        this.app_channel = app_channel;
    }

    public String getAppkey() {
        return appkey;
    }

    public void setAppkey(String appkey) {
        this.appkey = appkey;
    }

    public String getDevice_language() {
        return device_language;
    }

    public void setDevice_language(String device_language) {
        this.device_language = device_language;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getGeo_position() {
        return geo_position;
    }

    public void setGeo_position(String geo_position) {
        this.geo_position = geo_position;
    }

    public String getLocation_province() {
        return location_province;
    }

    public void setLocation_province(String location_province) {
        this.location_province = location_province;
    }

    public String getLocation_city() {
        return location_city;
    }

    public void setLocation_city(String location_city) {
        this.location_city = location_city;
    }

    public String getLocation_address() {
        return location_address;
    }

    public void setLocation_address(String location_address) {
        this.location_address = location_address;
    }

    public String getGmtTime() {
        return gmtTime;
    }

    public void setGmtTime(String gmtTime) {
        this.gmtTime = gmtTime;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getPerson_id() {
        return person_id;
    }

    public void setPerson_id(String person_id) {
        this.person_id = person_id;
    }

    public String getExpandData() {
        return expandData;
    }

    public void setExpandData(String expandData) {
        this.expandData = expandData;
    }

    @Override
    public String toString() {
        return "TrackingEventBean{" +
                "imei='" + imei + '\'' +
                ", imsi='" + imsi + '\'' +
                ", phone_wifi_mac='" + phone_wifi_mac + '\'' +
                ", router_mac='" + router_mac + '\'' +
                ", device_id='" + device_id + '\'' +
                ", tracking_id='" + tracking_id + '\'' +
                ", os_version='" + os_version + '\'' +
                ", browser_type='" + browser_type + '\'' +
                ", browser_ver='" + browser_ver + '\'' +
                ", u_uid='" + u_uid + '\'' +
                ", mobile_operators_desc='" + mobile_operators_desc + '\'' +
                ", network_desc='" + network_desc + '\'' +
                ", device_desc='" + device_desc + '\'' +
                ", app_version='" + app_version + '\'' +
                ", app_channel='" + app_channel + '\'' +
                ", appkey='" + appkey + '\'' +
                ", device_language='" + device_language + '\'' +
                ", ip='" + ip + '\'' +
                ", geo_position='" + geo_position + '\'' +
                ", location_province='" + location_province + '\'' +
                ", location_city='" + location_city + '\'' +
                ", location_address='" + location_address + '\'' +
                ", gmtTime='" + gmtTime + '\'' +
                ", mac='" + mac + '\'' +
                ", person_id='" + person_id + '\'' +
                ", expandData='" + expandData + '\'' +
                '}';
    }
}

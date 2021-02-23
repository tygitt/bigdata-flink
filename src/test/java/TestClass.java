import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import scala.collection.concurrent.TNode;

import java.net.URI;
import java.util.Map;

public class TestClass {

    public static final String hiveTblPros1 = "alter table kafka_real_time set TBLPROPERTIES ('is_generic'='false')";
    public static final String hiveTblPros2 = "alter table kafka_real_time set TBLPROPERTIES ('sink.partition-commit.policy.kind'='metastore,success-file')";
    public static final String hiveTblPros3 = "alter table kafka_real_time set TBLPROPERTIES ('partition-time-extractor.timestamp-pattern' = '$dt')";
    public static final String hiveTblPros4 = "alter table kafka_real_time set TBLPROPERTIES ('sink.partition-commit.delay'='5s')";
    public static final String hiveTblPros5 = "alter table kafka_real_time set TBLPROPERTIES ('sink.partition-commit.trigger'='process-time') ";

    public static final String KAFKA_TABLE_DDL = "" +
            "create table tracking_event_realtime(" +
            "        imei                    string ,    -- 国际移动用户识别码   \n" +
            "        imsi                    string ,    -- 移动设备国际身份码\n" +
            "        phone_wifi_mac          string ,    -- 手机wifi mac地址\n" +
            "        router_mac              string ,    -- 路由器mac地址\n" +
            "        device_id               string ,    -- 设备id\n" +
            "        tracking_id             string ,    -- 埋点id\n" +
            "        os_version              string ,    -- 操作系统版本\n" +
            "        browser_type            string ,    -- 浏览器类型\n" +
            "        browser_ver             string ,    -- 浏览器版本\n" +
            "        u_uid                   string ,    -- web端生成的cookie信息 \n" +
            "        mobile_operators_desc   string ,    -- 运营商信息\n" +
            "        network_desc            string ,    -- 网络环境\n" +
            "        device_desc             string ,    -- 设备描述信息\n" +
            "        app_version             string ,    -- app当前版本\n" +
            "        app_channel             string ,    -- app渠道标识\n" +
            "        appkey                  string ,    -- appkey号\n" +
            "        device_language         string ,    -- 设备语言\n" +
            "        ip                      string ,    -- 移动端ip地址\n" +
            "        geo_position            string ,    -- 经纬度\n" +
            "        location_province       string ,    -- 定位省份\n" +
            "        location_city           string ,    -- 定位城市\n" +
            "        location_address        string ,    -- 定位地址\n" +
            "        gmtTime                 string ,    -- gmt时间 \n" +
            "        mac                     string ,    -- mac地址      \n" +
            "        person_id               string ,    -- 用户id（蓝人爱家）\n" +
            "        expandData              ROW(page_id string, project_id string, role string, lbcx string, vcr_id string, vr_id string)       -- 扩展数据 \n" +
            ") WITH(\n " +
            "    'format.type' = 'json',  -- json格式，和topic中的消息格式保持一致\n" +
            "    'connector.type' = 'kafka',    -- 指定连接类型是kafka\n" +
            "    'connector.version' = '0.11',  -- 与安装的kafka版本要一致\n" +
            "    'connector.topic' = 'topic_test',              -- 之前创建的topic \n" +
            "    'connector.properties.group.id' = 'test_1',    -- 消费者组，相关概念可自行百度\n" +
            "    'connector.startup-mode' = 'latest-offset',    --指定从最新消费\n" +
            "    'connector.properties.zookeeper.connect' = '10.8.197.30:2181,10.8.197.31:2181,10.8.197.32:2181',  -- zk地址\n" +
            "    'connector.properties.bootstrap.servers' = '10.8.197.30:9092,10.8.197.31:9092,10.8.197.32:9092',  -- broker地址\n" +
            "'format.json-schema' = '{                           \n" +
            "  \"type\": \"object\",                             \n" +
            "  \"properties\": {                                 \n" +
            "    \"imei\": {type: \"string\"},                   \n" +
            "    \"imsi\": {type: \"string\"},                   \n" +
            "    \"phone_wifi_mac\": {type:\"string\"},          \n" +
            "    \"router_mac\": {type: \"string\"},             \n" +
            "    \"device_id\": {type: \"string\"},              \n" +
            "    \"tracking_id\": {type: \"string\"},            \n" +
            "    \"os_version\": {type: \"string\"},             \n" +
            "    \"browser_type\": {type: \"string\"},           \n" +
            "    \"browser_ver\": {type: \"string\"},            \n" +
            "    \"u_uid\": {type: \"string\"},                  \n" +
            "    \"mobile_operators_desc\": {type: \"string\"},  \n" +
            "    \"network_desc\": {type: \"string\"},           \n" +
            "    \"device_desc\": {type: \"string\"},            \n" +
            "    \"app_version\": {type: \"string\"},            \n" +
            "    \"app_channel\": {type: \"string\"},            \n" +
            "    \"appkey\": {type: \"string\"},                 \n" +
            "    \"device_language\": {type: \"string\"},               \n" +
            "    \"ip\": {type: \"string\"},                     \n" +
            "    \"geo_position\": {type: \"string\"},           \n" +
            "    \"location_province\": {type: \"string\"},      \n" +
            "    \"location_city\": {type: \"string\"},          \n" +
            "    \"location_address\": {type: \"string\"},       \n" +
            "    \"gmtTime\": {type: \"string\"},                \n" +
            "    \"mac\": {type: \"string\"},                    \n" +
            "    \"person_id\": {type: \"string\"}, \n" +
            "    \"expandData\": {\n" +
            "      type: \"object\",\n" +
            "      \"properties\": {\n" +
            "        \"page_id\": {type:\"string\"},                \n" +
            "        \"project_id\": {type:\"string\"},             \n" +
            "        \"role\": {type:\"string\"},                   \n" +
            "        \"lbcx\": {type:\"string\"},                   \n" +
            "        \"vcr_id\": {type:\"string\"},                 \n" +
            "        \"vr_id\": {type:\"string\"}   \n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}'" +
            ")";

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(10000);
//
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
//
//
//        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        String name = "myhive";
//
//        String defaultDatabase = "default";
//
//        String hiveConfDir = "/Users/tianyang/IdeaProjects/kafka_to_hive/src/main/resources/hive_conf";
////        String hiveConfDir = "/opt/servers/flink-1.9.2/externalConf/hive_conf";
//
//        String version = "1.1.0";
//
//        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
//
//        tEnv.registerCatalog("myhive", hiveCatalog);
//        tEnv.useCatalog("myhive");
//
////        tEnv.sqlUpdate(hiveTblPros1);
////        tEnv.sqlUpdate(hiveTblPros2);
////        tEnv.sqlUpdate(hiveTblPros5);
//
//
//        Table table = tEnv.sqlQuery("select * from table1");
//        table.printSchema();
//        DataStream<Row> stream = tEnv.toAppendStream(table, Row.class);
//        stream.print();
//
//        tEnv.sqlUpdate("insert into table1 values (3,'flink')");
//
//
//        env.execute("");
//        org.apache.commons.math3.stat.descriptive.rank.Percentile
    }
}

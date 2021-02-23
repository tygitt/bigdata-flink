import cn.brc.trackingEvent.bean.TrackingEventBean;
import cn.brc.trackingEvent.source.TimeSource;
import cn.brc.trackingEvent.util.HiveUtil;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestClass5 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);

        String topic = "topic_tracking_event_test";
        // String testTopic = "topic_test";

        DataStreamSource<String> kafkaSource = env.addSource(getKafkaSource(topic)).setParallelism(3);

        SingleOutputStreamOperator<String> filterDS = kafkaSource.filter(json ->
                json.contains("expandData") && json.contains("{") && json.contains("}") && json.contains("\"")
        );

        SingleOutputStreamOperator<String> mapData = filterDS.map(item -> {
            StringBuilder str = new StringBuilder(item);
            str.insert(str.indexOf("expandData") + "expandData".length() + 2, "'").insert(str.indexOf("}") + 1, "'");
            return str.toString();
        });

        SingleOutputStreamOperator<TrackingEventBean> trackingEventBeanDS = mapData.map(item -> {
            Gson gson = new Gson();
            return gson.fromJson(item, TrackingEventBean.class);
        });

        SingleOutputStreamOperator<String> strDS = trackingEventBeanDS.map(bean -> {
            StringBuffer sb = new StringBuffer();
            sb.append(bean.getImei()).append("|");
            sb.append(bean.getImsi()).append("|");
            sb.append(bean.getPhone_wifi_mac()).append("|");
            sb.append(bean.getRouter_mac()).append("|");
            sb.append(bean.getDevice_id()).append("|");
            sb.append(bean.getTracking_id()).append("|");
            sb.append(bean.getOs_version()).append("|");
            sb.append(bean.getBrowser_type()).append("|");
            sb.append(bean.getBrowser_ver()).append("|");
            sb.append(bean.getU_uid()).append("|");
            sb.append(bean.getMobile_operators_desc()).append("|");
            sb.append(bean.getNetwork_desc()).append("|");
            sb.append(bean.getDevice_desc()).append("|");
            sb.append(bean.getApp_version()).append("|");
            sb.append(bean.getApp_channel()).append("|");
            sb.append(bean.getAppkey()).append("|");
            sb.append(bean.getDevice_language()).append("|");
            sb.append(bean.getIp()).append("|");
            sb.append(bean.getGeo_position()).append("|");
            sb.append(bean.getLocation_province()).append("|");
            sb.append(bean.getLocation_city()).append("|");
            sb.append(bean.getLocation_address()).append("|");
            sb.append(bean.getGmtTime()).append("|");
            sb.append(bean.getMac()).append("|");
            sb.append(bean.getPerson_id()).append("|");
            sb.append(bean.getExpandData());
            return sb.toString();
        });

        // strDS.print();
        strDS.addSink(getHdfsSink());

        // -------------- 触发hive-jdbc --------------
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");

        DataStreamSource<Long> timeSource = env.addSource(new TimeSource()).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<Long, Integer>> mapDS = timeSource.map(item -> Tuple2.of(item, 1))
                .returns(Types.TUPLE(Types.LONG, Types.INT)).setParallelism(1);

        KeyedStream<Tuple2<Long, Integer>, Tuple> keyedDS = mapDS.keyBy(1);

        keyedDS.process(new KeyedProcessFunction<Tuple, Tuple2<Long, Integer>, Object>() {
            @Override
            public void processElement(Tuple2<Long, Integer> value, Context ctx, Collector<Object> out) throws Exception {
                String s = sdf.format(new Date(value.f0));
                if (s.substring(8).equals("2020")) {
                    HiveUtil.addPartition("aiot_db", "ods_event_tracking_lraj_internal", s.substring(0, 8));
                }
            }
        }).setParallelism(1);

        // 执行任务
        env.execute("tracking event sink hive...");
    }


    /**
     * 对接kafka数据
     * @param topic
     * @return
     */
    public static FlinkKafkaConsumer011<String> getKafkaSource(String topic) {
        Properties prop = new Properties();
//        prop.put("bootstrap.servers", "bgdata00:9092,bgdata01:9092,bgdata02:9092");
        prop.put("bootstrap.servers", "cdh-kafka-97:9092,cdh-kafka-98:9092,cdh-kafka-99:9092");
        prop.put("group.id", "test_1");
        prop.put("enable.auto.commit", "true");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);
        return consumer;
    }

    /**
     * 写入hdfs指定目录
     * @return
     */
    public static StreamingFileSink<String> getHdfsSink() {
        String path = "hdfs://cdh-rm-101:8020/user/hive/warehouse/aiot_db.db/ods_event_tracking_lraj_internal";
        String path2 = "hdfs://bgdata00:8020/user/hive/warehouse/internal_table";
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("'ds'=yyyyMMdd"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(10L))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(10L))
                                .withMaxPartSize(1024 * 5)
                                .build()
                ).build();

        return sink;
    }
}

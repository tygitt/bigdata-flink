package cn.brc.trackingEvent;

import cn.brc.trackingEvent.bean.TrackingEventBean;
import cn.brc.trackingEvent.source.TimeSource;
import cn.brc.trackingEvent.util.HiveUtil;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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

public class KafkaToHive {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);

        String topic = "topic_tracking_event";

        DataStreamSource<TrackingEventBean> kafkaSource = env.addSource(getKafkaSource(topic));

        SingleOutputStreamOperator<TrackingEventBean> trackingEventBeanDS = kafkaSource.filter(Objects::nonNull);

        SingleOutputStreamOperator<String> strDS = trackingEventBeanDS.map(bean -> {
            String s = bean.getImei() + "|" +
                    bean.getImsi() + "|" +
                    bean.getPhone_wifi_mac() + "|" +
                    bean.getRouter_mac() + "|" +
                    bean.getDevice_id() + "|" +
                    bean.getTracking_id() + "|" +
                    bean.getOs_version() + "|" +
                    bean.getBrowser_type() + "|" +
                    bean.getBrowser_ver() + "|" +
                    bean.getU_uid() + "|" +
                    bean.getMobile_operators_desc() + "|" +
                    bean.getNetwork_desc() + "|" +
                    bean.getDevice_desc() + "|" +
                    bean.getApp_version() + "|" +
                    bean.getApp_channel() + "|" +
                    bean.getChannel() + "|" +
                    bean.getIp() + "|" +
                    bean.getGeo_position() + "|" +
                    bean.getLocation_province() + "|" +
                    bean.getLocation_city() + "|" +
                    bean.getLocation_district() + "|" +
                    bean.getLocation_address() + "|" +
                    bean.getGmtTime() + "|" +
                    bean.getMac() + "|" +
                    bean.getPerson_id() + "|" +
                    bean.getAppkey() + "|" +
                    bean.getDevice_language() + "|" +
                    bean.getExpandData();
            return s;
        });

        strDS.addSink(getHdfsSink());

        // -------------- 触发hive-jdbc添加hive表分区 --------------
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");

        DataStreamSource<Long> timeSource = env.addSource(new TimeSource()).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<Long, Integer>> mapDS = timeSource.map(item -> Tuple2.of(item, 1))
                .returns(Types.TUPLE(Types.LONG, Types.INT)).setParallelism(1);

        KeyedStream<Tuple2<Long, Integer>, Tuple> keyedDS = mapDS.keyBy(1);

        keyedDS.process(new KeyedProcessFunction<Tuple, Tuple2<Long, Integer>, Object>() {
            @Override
            public void processElement(Tuple2<Long, Integer> value, Context ctx, Collector<Object> out) {
                String s = sdf.format(new Date(value.f0));
                // 每天凌晨1点添加当天分区元数据信息
                if (s.substring(8).equals("0100")) {
                    HiveUtil.addPartition("brc_iot_prod", "ods_tracking_event_data", s.substring(0, 8));
                }
            }
        }).setParallelism(1);

        // 执行任务
        env.execute("tracking event sink hive...");
    }

    /**
     * 对接kafka数据
     * @param
     * @return
     */
    public static FlinkKafkaConsumer011<TrackingEventBean> getKafkaSource(String topic) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "cdh-kafka-97:9092,cdh-kafka-98:9092,cdh-kafka-99:9092");
        prop.put("group.id", "flink_tracking_event");
        prop.put("enable.auto.commit", "true");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new FlinkKafkaConsumer011<>(topic, new TrackingEventBeanDeserializationSchema(), prop);
    }

    /**
     * 写入hdfs指定目录
     * @return
     */
    public static StreamingFileSink<String> getHdfsSink() {
        String path = "hdfs://cdh-rm-101:8020/user/hive/warehouse/brc_iot_prod.db/ods_tracking_event_data";
//        String path2 = "hdfs://bgdata00:8020/user/hive/warehouse/internal_table";
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("'ds'=yyyyMMdd"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.HOURS.toMillis(1L))
                                .withInactivityInterval(TimeUnit.HOURS.toMillis(1L))
                                .withMaxPartSize(1024 * 1024 * 64)
                                .build()
                ).build();

        return sink;
    }
}

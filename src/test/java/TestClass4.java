import cn.brc.trackingEvent.bean.TrackingEventBean;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestClass4 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);


        DataStreamSource<String> kafkaSource = env.addSource(getKafkaSource("topic_test"));

        kafkaSource.print();

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

        trackingEventBeanDS.print();


        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(new Path("hdfs://bgdata00:8020/test/user/eee/"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("'ds'=yyyyMMdd"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(30L))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(10L))
                                .withMaxPartSize(1024 * 1024)
                                .build()
                ).build();

        mapData.addSink(sink);

//        kafkaSource.process(new KeyedProcessFunction<String,String,String>(){
//
//            ValueState<Long> curTimerTsState = super.getRuntimeContext().getState(new ValueStateDescriptor<Long>("cur-timer-ts", Long.class));
//
//            @Override
//            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
//
//            }
//
//            @Override
//            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
//                super.onTimer(timestamp, ctx, out);
//                String currentKey = ctx.getCurrentKey();
//
//            }
//        });

        env.execute("tracking event sink hive...");
    }


    public static FlinkKafkaConsumer011<String> getKafkaSource(String topic) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "bgdata00:9092,bgdata01:9092,bgdata02:9092");
        prop.put("group.id", "test_1");
        prop.put("enable.auto.commit", "true");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);
    }

    /**
     * 添加hive表分区元数据信息
     */
    public static void addPartition() {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://bgdata00:10000/default";
        String user = "hive";
        String pass = "";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String format = sdf.format(new Date());

        String sql = "alter table internal_table add if not exists partition(ds = " + format + ")";

        Connection conn = null;
        PreparedStatement stat = null;
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, pass);
            stat = conn.prepareStatement(sql);
            stat.execute();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stat != null) {
                    stat.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}

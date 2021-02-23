import cn.brc.trackingEvent.bean.TrackingEventBean;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class TestClass6 {

    public static void main(String[] args) throws  Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
//        prop.put("bootstrap.servers", "10.0.22.97:9092,10.0.22.98:9092,10.0.22.99:9092");
        prop.put("group.id", "test1");
        prop.put("enable.auto.commit", "true");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer011<TrackingEventBean> consumer = new FlinkKafkaConsumer011<TrackingEventBean>("test", new MyKafkaSchema(), prop);
        DataStreamSource<TrackingEventBean> source = env.addSource(consumer);
        source.print();


        env.execute();
    }
}

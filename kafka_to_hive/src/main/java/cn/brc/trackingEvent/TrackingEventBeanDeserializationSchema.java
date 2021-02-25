package cn.brc.trackingEvent;

import cn.brc.trackingEvent.bean.TrackingEventBean;
import com.google.gson.Gson;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class TrackingEventBeanDeserializationSchema implements KafkaDeserializationSchema<TrackingEventBean> {

    private final Logger logger = LoggerFactory.getLogger(TrackingEventBeanDeserializationSchema.class);

    @Override
    public boolean isEndOfStream(TrackingEventBean trackingEventBean) {
        return false;
    }

    @Override
    public TrackingEventBean deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
        Gson gson = new Gson();
        byte[] value = consumerRecord.value();

        TrackingEventBean bean = new TrackingEventBean();

        if(value != null){
            // kafka中消息体
            String message = new String(value, StandardCharsets.UTF_8);
            try{
                StringBuilder str = new StringBuilder(message);
                str.insert(str.indexOf("expandData") + "expandData".length() + 2, "'").insert(str.indexOf("}") + 1, "'");
                String beanStr = str.toString();
                bean = gson.fromJson(beanStr, TrackingEventBean.class);
            } catch (Exception e){
                e.printStackTrace();
                logger.error("数据解析错误");
            }
        }
        return bean;
    }

    @Override
    public TypeInformation<TrackingEventBean> getProducedType() {
        return TypeInformation.of(new TypeHint<TrackingEventBean>() {});
    }
}

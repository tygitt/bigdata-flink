import cn.brc.trackingEvent.util.HiveUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Date;
import java.text.SimpleDateFormat;

public class MyTrigger extends KeyedProcessFunction<String, String,String> {

    ValueState<Long> timeState = null;

    ValueState<String> serverState = null;

    SimpleDateFormat sdf = null;

    @Override
    /**
     * 初始化状态信息
     */
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        timeState = super.getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-state", Long.class));

        serverState = super.getIterationRuntimeContext().getState(new ValueStateDescriptor<String>("server-state", String.class));

        sdf = new SimpleDateFormat("HHmmss");
    }

    @Override
    /**
     * 处理每条流入的数据
     */
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

    }

    @Override
    /**
     * 定时回调方法，触发逻辑并注册下一个执行逻辑
     */
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        if(sdf.format(new Date(System.currentTimeMillis())).equals("112000")){
            HiveUtil.addPartition("default","internal","20210112");
        }
    }
}

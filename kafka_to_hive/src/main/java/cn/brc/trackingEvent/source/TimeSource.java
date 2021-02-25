package cn.brc.trackingEvent.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class TimeSource extends RichSourceFunction<Long> {

    Boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while(true) {
            ctx.collect(System.currentTimeMillis());
            Thread.sleep(10000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

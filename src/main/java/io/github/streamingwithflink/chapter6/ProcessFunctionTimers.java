package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ProcessFunctionTimers
 *
 * @author xinsheng2.zhao
 * @version Id: ProcessFunctionTimers.java, v 0.1 2023/2/11 21:04 xinsheng2.zhao Exp $
 */
public class ProcessFunctionTimers {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<SensorReading> readings = env.addSource(new SensorSource());

        DataStream<String> warnings = readings.keyBy(r -> r.id).process(new TempIncreaseAlertFunction());

        warnings.print();

        env.execute("Monitor sensor temperatures.");
    }

    /**
     * 如果某传感器的温度在1秒内（处理时间）持续增加则发出警告。
     */
    public static class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {

        // 存储最近一次的传感器温度度数
        ValueState<Double> lastTemp;

        // 存储当前活动计时器的时间戳
        ValueState<Long> currentTimer;

        /**
         * 经验：值状态的初始化操作必须在open()方法中进行。
         */
        @Override public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemp", Types.DOUBLE));
            currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));
        }

        /**
         * 注解：这里的out并没有收集任何事件。
         */
        @Override public void processElement(SensorReading sensorReading, KeyedProcessFunction<String, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
            // 读取前一个温度，注：存在拆包时的空指针问题，所以使用包装类型接收值。
            Double prevTemp = lastTemp.value();
            // 更新最近一次温度
            lastTemp.update(sensorReading.temperature);

            // 获取当前活动计时器的时间戳，注：存在拆包时的空指针问题，所以使用包装类型接收值。
            Long curTimerTimestamp = currentTimer.value();
            if (prevTemp == null || sensorReading.temperature < prevTemp) {
                if (curTimerTimestamp == null) {
                    return;
                }
                // 温度下降，删除当前计时器
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                currentTimer.clear();
            } else if (sensorReading.temperature > prevTemp && curTimerTimestamp == null) {
                // 温度升高并且尚未设置计时器
                // 以当前时间+1秒设置处理器时间计时器
                long nextTimer = ctx.timerService().currentProcessingTime() + 1000;
                ctx.timerService().registerProcessingTimeTimer(nextTimer);
                // 记住当前的定时器
                currentTimer.update(nextTimer);
            }

            // out.collect(ctx.getCurrentKey() + " " + ctx.timerService().currentProcessingTime() + " " + sensorReading.temperature);
        }

        @Override public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("Temperature of sensor '" + ctx.getCurrentKey() + "' monotonically increased for 1 second.");
            currentTimer.clear();
        }

        @Override public void close() throws Exception {
            super.close();
        }
    }
}

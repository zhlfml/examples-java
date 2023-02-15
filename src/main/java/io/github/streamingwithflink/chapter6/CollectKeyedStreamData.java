package io.github.streamingwithflink.chapter6;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.SensorSource;
import io.github.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * 需求：先按照传感器ID分组，找出每5秒钟内的每个传感器的最高温度，再汇总取前10个最高的温度并输出结果。
 *
 * @author xinsheng2.zhao
 * @version Id: CollectKeyedStreamData.java, v 0.1 2023/2/12 18:03 xinsheng2.zhao Exp $
 */
public class CollectKeyedStreamData {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        env.addSource(new SensorSource()).assignTimestampsAndWatermarks(new SensorTimeAssigner()).keyBy(r -> r.id).timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<SensorReading, Tuple2<String, Double>, Tuple2<String, Double>>() {

                    @Override public Tuple2<String, Double> createAccumulator() {
                        return new Tuple2<>("", 0.0);
                    }

                    @Override public Tuple2<String, Double> add(SensorReading value, Tuple2<String, Double> accumulator) {
                        accumulator.f0 = value.id;
                        accumulator.f1 = Math.max(value.temperature, accumulator.f1);
                        return accumulator;
                    }

                    @Override public Tuple2<String, Double> getResult(Tuple2<String, Double> accumulator) {
                        return accumulator;
                    }

                    @Override public Tuple2<String, Double> merge(Tuple2<String, Double> a, Tuple2<String, Double> b) {
                        Tuple2<String, Double> merged = new Tuple2<>();
                        merged.f0 = a.f0;
                        merged.f1 = Math.max(a.f1, b.f1);
                        return merged;
                    }
                })
                // 收集所有分组后的元素，并提取温度最高的前10个传感器。
                .timeWindowAll(Time.seconds(1))
                .process(new ProcessAllWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, TimeWindow>() {

                    final int top = 10;

                    @Override public void process(ProcessAllWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, TimeWindow>.Context context,
                                                  Iterable<Tuple2<String, Double>> elements, Collector<Tuple2<String, Double>> out) throws Exception {
                        int cnt = 0;
                        PriorityQueue<Tuple2<String, Double>> queue = new PriorityQueue<>(Comparator.comparing((Tuple2<String, Double> a) -> a.f1));
                        for (Tuple2<String, Double> element : elements) {
                            cnt++;
                            queue.offer(element);
                            if (queue.size() > top) {
                                queue.poll();
                            }
                        }

                        while (!queue.isEmpty()) {
                            out.collect(queue.poll());
                        }

                        System.out.println("elements size: " + cnt + ", window's end: " + context.window().getEnd());
                    }

                }).print();

        env.execute("my job");
    }
}

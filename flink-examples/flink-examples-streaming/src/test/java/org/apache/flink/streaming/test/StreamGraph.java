package org.apache.flink.streaming.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class StreamGraph {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.generateSequence(1, 10000000)
                .map(x -> x.toString())
                .addSink(
                        new SinkFunction<String>() {
                            @Override
                            public void invoke(String value) {
                                if(Integer.valueOf(value) < 100){
                                    System.out.println(value);
                                }
                            }
                        });

        env.execute("test");
    }
}

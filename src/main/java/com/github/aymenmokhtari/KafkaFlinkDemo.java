package com.github.aymenmokhtari;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Properties;

public class KafkaFlinkDemo {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties p = new Properties();

        p.setProperty("bootstrap.servers"  , "127.0.0.1:9002");
        DataStream<String> kafkaData = env.addSource(new FlinkKafkaConsumer<String>("test" , new SimpleStringSchema(), p));
        kafkaData.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {

            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for(String word : words)
                    out.collect(new Tuple2<String, Integer>(word , 1));
            }
        });
    }
}

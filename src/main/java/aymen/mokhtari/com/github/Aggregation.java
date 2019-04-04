package aymen.mokhtari.com.github;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = env.readTextFile("/home/aymen/avg1");

        DataStream<Tuple4<String, String, String, Integer>> mapped= data.map(new Splitter());

        mapped.keyBy(0).sum(3).writeAsText("/home/aymen/out1");

    }

    public static  class  Splitter implements MapFunction<String , Tuple4<String , String , String , Integer >> {

        public Tuple4<String, String, String, Integer> map(String value) throws Exception {
            String[] words = value.split(",") ;
            return new Tuple4<String, String, String, Integer >(words[1] , words[2] , words[3] , Integer.parseInt(words[4]) );

        }
    }
}

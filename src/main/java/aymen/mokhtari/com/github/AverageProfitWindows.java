package aymen.mokhtari.com.github;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AverageProfitWindows {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        DataStream<String> data = env.socketTextStream("localhost" , 9090);


        DataStream<Tuple5<String , String , String , Integer , Integer>> mapped = data.map(new Splitter());

        DataStream<Tuple5<String , String , String , Integer , Integer>> reduced = mapped
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .reduce(new Reduce1());

        reduced.writeAsText("/home/aymen/www");

        env.execute();



    }

    public static  class  Splitter implements MapFunction<String , org.apache.flink.api.java.tuple.Tuple5<String , String , String , Integer , Integer>> {

        public org.apache.flink.api.java.tuple.Tuple5<String, String, String, Integer, Integer> map(String value) throws Exception {
            String[] words = value.split(",") ;
            return new Tuple5<String, String, String, Integer, Integer>(words[1] , words[2] , words[3] , Integer.parseInt(words[4]) , 1);

        }
    }
    public static  class  Reduce1 implements ReduceFunction<Tuple5 <String ,String , String , Integer , Integer>> {

        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current, Tuple5<String, String, String, Integer, Integer> pre_result) throws Exception {
            return new Tuple5<String, String, String, Integer, Integer>(current.f0 , current.f1 , current.f2 , current.f3+pre_result.f3 , current.f4 + pre_result.f4);
        }
    }

}

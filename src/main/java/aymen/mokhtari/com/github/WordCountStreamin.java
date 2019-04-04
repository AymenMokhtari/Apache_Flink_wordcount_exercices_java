package aymen.mokhtari.com.github;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreamin {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.socketTextStream("localhost" , 9999);

        DataStream<Tuple2<String,Integer>> counts = text.filter(new FilterFunction<String>() {
            public boolean filter(String value) throws Exception {
                return value.startsWith("N");
            }
        })
                .map(new Tokenizer())
                .keyBy(0).sum(1);
        counts.print();
    }

    public static final  class  Tokenizer implements MapFunction<String ,Tuple2<String , Integer>>
    {

        public Tuple2<String, Integer> map(String value) throws Exception {
            return new Tuple2<String, Integer>(value , 1);
        }
    }



}

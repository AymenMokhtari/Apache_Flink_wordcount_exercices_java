package aymen.mokhtari.com.github;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class JoinExample {
    public static void main(String[] args) throws Exception {

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params  = ParameterTool.fromArgs(args);
        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //Read person file and generate tuples out of each string read

        DataSet<Tuple2<Integer , String>> personSet = env.readTextFile(params.get("input1"))
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    public Tuple2<Integer, String> map(String value) throws Exception {
                        String[] words = value.split(",");

                        return new Tuple2<Integer, String>(Integer.parseInt(words[0]) , words[1]);
                    }
                });
        // Read location file and generate tuples out of each string read

        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2"))
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    public Tuple2<Integer, String> map(String value) throws Exception {
                        String[] words  = value.split(",");

                        return new Tuple2<Integer, String>(Integer.parseInt(words[0]) , words[1]);
                    }
                });

        //join datasets on person id ;
        // joined format will be <id, person , name , state>

        DataSet<Tuple3<Integer,String , String>> joined = personSet.join(locationSet).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) throws Exception {
                        return new Tuple3<Integer, String, String>(person.f0 , person.f1 , location.f1); // return tuple of (1 John DC)
                    }
                });

        joined.writeAsCsv(params.get("output"), "\n"  , " ");
        env.execute("Join Example");

    }
}

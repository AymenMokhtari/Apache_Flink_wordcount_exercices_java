package aymen.mokhtari.com.github;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple2 ;

public class WordCount
{
    public static void main(String[] args)
            throws Exception
    {
        //set up the execution environment
        final ExecutionEnvironment env  = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        // read the text file from given input path
        DataSet<String> text =  env.readTextFile(params.get("input")) ;
        //filter all the names starting with N
        DataSet<String> filtered = text.filter(new FilterFunction<String>() {
            public boolean filter(String value) throws Exception {
                return value.startsWith("N");
            }
        });

        //Returns a tuple of (name,1)
        DataSet<Tuple2<String , Integer>> tokenized = filtered.map(new Tokenizer()); // tokenize = [(Noman , 1) (Nipun ,1)( Naman,1) }
        //group by the tuple field "0" and sum up tuple field "1"
        DataSet<Tuple2<String,Integer>> counts  = tokenized.groupBy(0).sum(1);

        //emit result

        if(params.has("output")) {
            counts.writeAsCsv(params.get("output") , "\n" ," ");
            //execute program
            env.execute("WordCount Example");
        }


    }
    public static final  class  Tokenizer implements MapFunction<String ,Tuple2<String , Integer>>
    {

        public Tuple2<String, Integer> map(String value) throws Exception {
            return new Tuple2<String, Integer>(value , 1);
        }
    }

}


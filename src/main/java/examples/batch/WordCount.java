package examples.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount
{
    static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>>
    {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception
        {
            String patten = "[a-zA-Z0-9-_']+";
            Pattern r = Pattern.compile(patten);
            Matcher m = r.matcher(value.toLowerCase());
            while (m.find())
            {
                String token = m.group();
                if (token.length() > 0)
                {
                    out.collect(Tuple2.of(token, 1));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile("words.md");

        DataSet<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .groupBy(0)
                        .sum(1);
        counts.writeAsCsv("wordcount_output", "\n", " ");
        env.execute("WordCount");
//    counts.
    }
}

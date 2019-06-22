package examples.stream.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount
{
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment senv=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String>text=senv.readTextFile("words.txt");
        DataStream<Tuple2<String,Integer>>result=text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>()
                {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception
                    {
                        String pattern="[a-zA-Z0-9-_']+";
                        Pattern p=Pattern.compile(pattern);
                        Matcher m=p.matcher(value.toLowerCase());
                        while(m.find())
                        {
                            String word=m.group();
                            if(word.length()>0)
                            {
                                out.collect(Tuple2.of(word,1));
                            }
                        }
                    }
                })
                .keyBy(0)
                .sum(1);

        result.print();
        senv.execute("WorldCount");
    }
}

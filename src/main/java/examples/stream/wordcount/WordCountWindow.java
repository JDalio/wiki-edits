package examples.stream.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountWindow
{
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> result = senv
                .socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>()
                {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception
                    {
                        String[] words = value.split(" ");
                        for (String word : words)
                        {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(20))
                .sum(1);
        result.print();
        senv.execute("WordCountWindow");

    }
}

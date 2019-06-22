package wikiedits;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikipediaAnalysis
{
    static class SumAggregate implements AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>
    {
        @Override//initial state
        public Tuple2<String, Long> createAccumulator()
        {
            return Tuple2.of("", 0L);
        }

        @Override
        public Tuple2<String, Long> add(WikipediaEditEvent event, Tuple2<String, Long> sumAggregator)
        {
            sumAggregator.f0 = event.getUser();
            sumAggregator.f1 += event.getByteDiff();
            return sumAggregator;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b)
        {
            a.f1 += b.f1;
            return a;
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> res)
        {
            return res;
        }
    }

    public static void main(String[] args) throws Exception
    {
        //create the StreamExecutionEnvironment
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //create a source reading from the wikipedia IRC log
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        //key the stream, every event/stream has a key
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy(new KeySelector<WikipediaEditEvent, String>()
                {
                    @Override
                    public String getKey(WikipediaEditEvent event) throws Exception
                    {
                        return event.getUser();
                    }
                });

        //aggregate the sum of edited bytes for every five minutes per user
        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(10))
                .aggregate(new SumAggregate());

        result
                .map(new MapFunction<Tuple2<String, Long>, String>()
                {
                    @Override
                    public String map(Tuple2<String, Long> tuple) throws Exception
                    {
                        return tuple.toString();
                    }
                })
                .addSink(new FlinkKafkaProducer010<String>("localhost:9092", "wiki-result", new SimpleStringSchema()));
        see.execute();
    }
}

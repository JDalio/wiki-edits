package test;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test
{
    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String,Integer>>grade=env.readCsvFile("grade.csv").fieldDelimiter(" ").lineDelimiter("\n").types(String.class, Integer.class);
        DataSet<Tuple2<String,String>>gender=env.readCsvFile("gender.csv").fieldDelimiter(" ").lineDelimiter("\n").types(String.class, String.class);
        DataSet<Tuple2<Tuple2<String,String>,Tuple2<String,Integer>>> students=gender
                .join(grade).where(0).equalTo(0)
                .projectFirst(0,1)
                .projectSecond(1);
//                .with(new FlatJoinFunction<Tuple2<String, String>, Tuple2<String, Integer>, Tuple2<Tuple2<String, String>, Tuple2<String, Integer>>>()
//                {
//                    @Override
//                    public void join(Tuple2<String, String> gender, Tuple2<String, Integer> grade, Collector<Tuple2<Tuple2<String, String>, Tuple2<String, Integer>>> out) throws Exception
//                    {
//                        if(grade.f1>300)
//                        {
//                            out.collect(Tuple2.of(gender,grade));
//                        }
//                    }
//                })
        students.print();
//        DataSet<Tuple3<String,Integer,Integer>>students=env.readCsvFile("grade.csv").fieldDelimiter(" ").lineDelimiter("\n").types(String.class,Integer.class,Integer.class);
//        students.groupBy(0).maxBy(1,2).print();
//        DataSet<Tuple1<String>> input = env.readTextFile("words.txt").flatMap(new ExplodeArticleFlatMap());

//        DataSet<Tuple2<String, Integer>> combineResult = input
//                .groupBy(0)
//                .combineGroup(new WordsCombineGroup());
//
//        DataSet<Tuple2<String, Integer>> reduceResult = combineResult
//                .groupBy(0)
//                .reduceGroup(new WordsReduceGroup())
//                .reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>()
//                {
//                    Tuple2<String, Integer> max = Tuple2.of("", 0);
//                    Tuple2<String, Integer> min = Tuple2.of("", Integer.MAX_VALUE);
//                    Tuple2<String, Integer> sum = Tuple2.of("Total", 0);
//
//                    @Override
//                    public void reduce(Iterable<Tuple2<String, Integer>> words, Collector<Tuple2<String, Integer>> out) throws Exception
//                    {
//                        for (Tuple2<String, Integer> word : words)
//                        {
//                            if (word.f1 > max.f1)
//                            {
//                                max.f0 = word.f0;
//                                max.f1 = word.f1;
//                            }
//                            if (word.f1 < min.f1)
//                            {
//                                min.f0 = word.f0;
//                                min.f1 = word.f1;
//                            }
//                            sum.f1 += word.f1;
//                        }
//
//                        out.collect(max);
//                        out.collect(min);
//                        out.collect(sum);
//                    }
//                });
//        reduceResult.print();
//                .flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>()
//                {
//                    @Override
//                    public void flatMap(Tuple2<String, Integer> word, Collector<Tuple2<String, Integer>> out) throws Exception
//                    {
//                        if(word.f1>max.f1)
//                        {
//                            max.f0 = word.f0;
//                            max.f1 = word.f1;
//                        }
//                        if (word.f1 < min.f1)
//                        {
//                            min.f0 = word.f0;
//                            min.f1 = word.f1;
//                        }
//                        sum.f1 += word.f1;
//                    }
//                });
//                .mapPartition(new MapPartitionFunction<Tuple2<String, Integer>, Tuple2<String,Integer>>()
//                {
//                    Tuple2<String,Integer>max=Tuple2.of("",0);
//                    Tuple2<String,Integer>min=Tuple2.of("",Integer.MAX_VALUE);
//                    Tuple2<String,Integer>sum=Tuple2.of("Total",0);
//                    @Override
//                    public void mapPartition(Iterable<Tuple2<String, Integer>> words, Collector<Tuple2<String,Integer>> out) throws Exception
//                    {
//                        for(Tuple2<String,Integer>word:words)
//                        {
//                            if(word.f1>max.f1)
//                            {
//                                max.f0=word.f0;
//                                max.f1=word.f1;
//                            }
//                            if(word.f1<min.f1)
//                            {
//                                min.f0=word.f0;
//                                min.f1=word.f1;
//                            }
//                            sum.f1+=word.f1;
//                        }
//                        out.collect(max);
//                        out.collect(min);
//                        out.collect(sum);
//                    }
//                });

//                .filter(new FilterFunction<Tuple2<String, Integer>>()
//                {
//                    @Override
//                    public boolean filter(Tuple2<String, Integer> word) throws Exception
//                    {
//                        if (word.f0.equals("is"))
//                            return true;
//                        return false;
//                    }
//                });
//        DataSet<Tuple2<String,Integer>>max=countResult.aggregate(Aggregations.MAX,1);
//        max.print();
//        output.mapPartition(new ResultPartitionMap()).print();
    }

    //************************************************************************
    // USER FUNCTION
    //************************************************************************

    //    public static final class ResultPartitionMap implements MapPartitionFunction<Tuple2<String,Integer>,Tuple2<String,Integer>>
//    {
//        @Override
//        public void mapPartition(Iterable<Tuple2<String, Integer>> words, Collector<Tuple2<String, Integer>> out) throws Exception
//        {
//
//        }
//    }
    public static final class WordsCombineGroup implements GroupCombineFunction<Tuple1<String>, Tuple2<String, Integer>>
    {
        @Override
        public void combine(Iterable<Tuple1<String>> words, Collector<Tuple2<String, Integer>> out) throws Exception
        {
            String key = "";
            int count = 0;
            for (Tuple1<String> word : words)
            {
                key = word.f0;
                count++;
            }
            out.collect(Tuple2.of(key, count));
        }
    }

    public static final class WordsReduceGroup implements GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>
    {
        @Override
        public void reduce(Iterable<Tuple2<String, Integer>> words, Collector<Tuple2<String, Integer>> out) throws Exception
        {
            String key = "";
            int count = 0;
            for (Tuple2<String, Integer> word : words)
            {
                key = word.f0;
                count += word.f1;
            }
            out.collect(Tuple2.of(key, count));
        }
    }

    public static final class ExplodeArticleFlatMap implements FlatMapFunction<String, Tuple1<String>>
    {
        @Override
        public void flatMap(String values, Collector<Tuple1<String>> out) throws Exception
        {
            Pattern p = Pattern.compile("[a-zA-Z0-9-_']+");
            Matcher m = p.matcher(values);
            while (m.find())
            {
                out.collect(Tuple1.of(m.group().toLowerCase()));
            }
        }
    }

    public static final class SumGradeReduceGroup implements GroupReduceFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>
    {
        @Override
        public void reduce(Iterable<Tuple3<String, Integer, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception
        {
            Tuple2<String, Integer> grade = new Tuple2<>();
            grade.f1 = 0;
            for (Tuple3<String, Integer, Integer> value : values)
            {
                grade.f0 = value.f0;
                grade.f1 += value.f1 + value.f2;
            }
            out.collect(grade);

        }
    }

    public static final class SumPerNoteMap implements MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>
    {
        @Override
        public Tuple2<String, Integer> map(Tuple3<String, Integer, Integer> value) throws Exception
        {
            return Tuple2.of(value.f0, value.f1 + value.f2);
        }
    }

    public static final class SumGradeReduce implements ReduceFunction<Tuple2<String, Integer>>
    {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> in1, Tuple2<String, Integer> in2) throws Exception
        {
            return Tuple2.of(in1.f0, in1.f1 + in2.f1);
        }
    }

    public static final class GenderMapPartition implements MapPartitionFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>
    {
        @Override
        public void mapPartition(Iterable<Tuple2<String, String>> values, Collector<Tuple3<String, String, Integer>> out) throws Exception
        {
            for (Tuple2<String, String> stu : values)
            {
                int age = new Random().nextInt(100);
                out.collect(Tuple3.of(stu.f0, stu.f1, age));
            }
        }
    }

    public static final class AgeFilterFunction implements FilterFunction<Tuple3<String, String, Integer>>
    {
        @Override
        public boolean filter(Tuple3<String, String, Integer> stu) throws Exception
        {
            if (stu.f2 < 50)
            {
                return true;
            }
            return false;
        }
    }
}

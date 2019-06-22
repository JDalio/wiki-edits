package examples.batch;

import examples.batch.util.PageRankData;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class PageRank
{
    private static final double DAMPENING_FACTOR = 0.85;
    private static final double EPSILON = 0.0001;

    public static void main(String[] args) throws Exception
    {
        ParameterTool params = ParameterTool.fromArgs(args);

        final int numPages = params.getInt("numPages", PageRankData.getNumberOfPages());
        final int maxIterations = params.getInt("iterations", 10);

        // *************************************************************************
        //     PROGRAM
        // *************************************************************************

        //get execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);
        //total page nodes
        DataSet<Long> pagesInput = getPagesDataSet(env, params);
        //page graph
        DataSet<Tuple2<Long, Long>> linksInput = getLinksDataSet(env, params);
        //initiate the page nodes with 1/page nums
        DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput
                .map(new RankAssigner(1.0d / numPages));

        DataSet<Tuple2<Long, Long[]>> adjacencyListInput =
                linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

        IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(maxIterations);

        DataSet<Tuple2<Long, Double>> newRanks = iteration
                .join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
                .groupBy(0).aggregate(Aggregations.SUM, 1)
                .map(new Dampener(DAMPENING_FACTOR, numPages));

        DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(
                newRanks,
                newRanks.join(iteration).where(0).equalTo(0)
                        .filter(new EpsilonFilter()));

        if (params.has("output"))
        {
            finalPageRanks.writeAsCsv(params.get("output"), "\n", " ");
            env.execute("Basic Page Rank Example");
        } else
        {
            System.out.println("\"Printing result to stdout. Use --output to specify output path.\"");
            finalPageRanks.print();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class RankAssigner implements MapFunction<Long, Tuple2<Long, Double>>
    {
        Tuple2<Long, Double> outPageWithRank;

        public RankAssigner(double rank)
        {
            this.outPageWithRank = new Tuple2<>(-1L, rank);
        }

        @Override
        public Tuple2<Long, Double> map(Long page) throws Exception
        {
            outPageWithRank.f0 = page;
            return outPageWithRank;
        }
    }

    public static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>>
    {
        private final ArrayList<Long> neighbors = new ArrayList<>();

        @Override
        public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) throws Exception
        {
            neighbors.clear();
            Long id = 0L;
            for (Tuple2<Long, Long> n : values)
            {
                id = n.f0;
                neighbors.add(n.f1);
            }
            out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
        }
    }

    public static final class JoinVertexWithEdgesMatch implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>>
    {
        @Override
        public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> out) throws Exception
        {
            Long[] neighbors = value.f1.f1;
            Double rank = value.f0.f1;
            Double rankDistribute = rank / (double) neighbors.length;
            for (Long neighbor : neighbors)
            {
                out.collect(new Tuple2<Long, Double>(neighbor, rankDistribute));
            }
        }
    }

    public static final class Dampener implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>
    {
        private final double dampening;
        private final double randomJump;

        public Dampener(double dampening, double numVertices)
        {
            this.dampening = dampening;
            this.randomJump = (1 - dampening) / numVertices;
        }

        @Override
        public Tuple2<Long, Double> map(Tuple2<Long, Double> value) throws Exception
        {
            value.f1 = (value.f1 * dampening) + randomJump;
            return value;
        }
    }

    public static final class EpsilonFilter implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>>
    {
        @Override
        public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) throws Exception
        {
            return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
        }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static DataSet<Long> getPagesDataSet(ExecutionEnvironment env, ParameterTool params)
    {
        if (params.has("pages"))
        {
            return env.readCsvFile(params.get("pages"))
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Long.class)
                    .map(new MapFunction<Tuple1<Long>, Long>()
                    {
                        @Override
                        public Long map(Tuple1<Long> longTuple1) throws Exception
                        {
                            return longTuple1.f0;
                        }
                    });
        } else
        {
            System.out.println("Executing PageRank example with default pages data set.");
            System.out.println("Use --pages to specify file input.");
            return PageRankData.getDefaultPagesDataSet(env);
        }
    }

    private static DataSet<Tuple2<Long, Long>> getLinksDataSet(ExecutionEnvironment env, ParameterTool params)
    {
        if (params.has("links"))
        {
            return env.readCsvFile(params.get("links"))
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Long.class, Long.class);
        } else
        {
            System.out.println("Executing PageRank example with default links data set.");
            System.out.println("Use --links to specify file input.");
            return PageRankData.getDefaultEdgeDataSet(env);
        }
    }
}

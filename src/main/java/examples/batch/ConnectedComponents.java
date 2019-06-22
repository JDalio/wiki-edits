package examples.batch;

import examples.batch.util.ConnectedComponentsData;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class ConnectedComponents
{
    public static void main(String[] args) throws Exception
    {
        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final int maxIteration = params.getInt("iterations", 1000000000);
        env.getConfig().setGlobalJobParameters(params);
        //build the undirected graph
        DataSet<Long> vertices = getVertexDataSet(env, params);
        DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env, params).flatMap(new UndirectEdge());

        DataSet<Tuple2<Long, Long>> vertexWithInitialId = vertices.map(new DuplicateValue<Long>());

        //work set is vertexWithInitialId set, with each iteration, if the element within doesn't change, it'll be added in the
        //solution set
        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
                vertexWithInitialId.iterateDelta(vertexWithInitialId, maxIteration, 0);

        DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset().join(edges).where(0).equalTo(0)
                .with(new NeighborWithComponentIDJoin())
                .groupBy(0)
                .aggregate(Aggregations.MIN, 1)
                .join(iteration.getSolutionSet()).where(0).equalTo(0)
                .with(new ComponentIdFilter());
        DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);

        // emit result
        if (params.has("output")) {
            result.writeAsCsv(params.get("output"), "\n", " ");
            // execute program
            env.execute("Connected Components Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            result.print();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static final class UndirectEdge implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>
    {
        Tuple2<Long, Long> invertedEdge = new Tuple2<>();

        @Override
        public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) throws Exception
        {
            invertedEdge.f0 = edge.f1;
            invertedEdge.f1 = edge.f0;
            out.collect(edge);
            out.collect(invertedEdge);
        }
    }

    public static final class NeighborWithComponentIDJoin implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>
    {
        @Override
        public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) throws Exception
        {
            return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
        }
    }

    public static final class DuplicateValue<T> implements MapFunction<T, Tuple2<T, T>>
    {
        @Override
        public Tuple2<T, T> map(T vertex) throws Exception
        {
            return new Tuple2<T, T>(vertex, vertex);
        }
    }

    public static final class ComponentIdFilter implements FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>
    {
        @Override
        public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old, Collector<Tuple2<Long, Long>> out) throws Exception
        {
            if (candidate.f1 < old.f1)
            {
                out.collect(candidate);
            }
        }
    }
    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static DataSet<Long> getVertexDataSet(ExecutionEnvironment env, ParameterTool params)
    {
        if (params.has("vertices"))
        {
            return env.readCsvFile(params.get("vertices")).types(Long.class).map(
                    new MapFunction<Tuple1<Long>, Long>()
                    {
                        @Override
                        public Long map(Tuple1<Long> value) throws Exception
                        {
                            return value.f0;
                        }
                    }
            );
        } else
        {
            System.out.println("Executing Connected Components example with default vertices data set.");
            System.out.println("Use --vertices to specify file input.");
            return ConnectedComponentsData.getDefaultVertexDataSet(env);
        }
    }

    private static DataSet<Tuple2<Long, Long>> getEdgeDataSet(ExecutionEnvironment env, ParameterTool params)
    {
        if (params.has("edges"))
        {
            return env.readCsvFile(params.get("edges")).fieldDelimiter(" ").lineDelimiter("\n").types(Long.class, Long.class);
        } else
        {
            System.out.println("Executing Connected Components example with default edges data set.");
            System.out.println("Use --edges to specify file input.");
            return ConnectedComponentsData.getDefaultEdgeDataSet(env);
        }
    }
}

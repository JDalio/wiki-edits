package examples.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

public class BasicOpt
{
    public static void main(String[] args) throws Exception
    {
        //examples.table
        ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment btableEnv = BatchTableEnvironment.getTableEnvironment(benv);

        //Type Info
        String[] fieldNames = new String[]{"framework", "time", "starnum"};
        TypeInformation<String> stringType = TypeInformation.of(String.class);
        TypeInformation<String> dateTimeType = TypeInformation.of(String.class);
        TypeInformation<Integer> integerType = TypeInformation.of(Integer.class);
        TypeInformation[] fieldTypes = new TypeInformation[]{stringType, dateTimeType, integerType};

        //register a examples.table source
        TableSource csvSource = new CsvTableSource("star.csv", fieldNames, fieldTypes);
        btableEnv.registerTableSource("frameworkStar", csvSource);

        //register a examples.table sink
        TableSink tableSink = new CsvTableSink("star-sink.csv", ",");
        btableEnv.registerTableSink("csvSinkTable",fieldNames,fieldTypes,tableSink);

        Table frameworkStar=btableEnv.scan("frameworkStar");
        Table frameworkInOrder=frameworkStar.orderBy("framework");

        frameworkInOrder.insertInto("csvSinkTable");


    }

}

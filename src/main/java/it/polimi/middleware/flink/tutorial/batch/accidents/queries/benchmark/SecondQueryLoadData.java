package it.polimi.middleware.flink.tutorial.batch.accidents.queries.benchmark;

import it.polimi.middleware.flink.tutorial.batch.accidents.queries.Query;
import it.polimi.middleware.flink.tutorial.batch.accidents.utils.AccidentField;
import it.polimi.middleware.flink.tutorial.batch.accidents.utils.Functions;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.core.fs.FileSystem;

public class SecondQueryLoadData extends Query {

    public SecondQueryLoadData(ExecutionEnvironment env, String data, String outputFile) {
        super(env, data, outputFile);
    }


    @Override
    public JobExecutionResult execute() throws Exception {
        final String contributingFactorFields = AccidentField.getFields(
                AccidentField.NUMBER_OF_CYCLIST_KILLED,
                AccidentField.NUMBER_OF_MOTORIST_KILLED,
                AccidentField.NUMBER_OF_PEDESTRIANS_KILLED,
                AccidentField.NUMBER_OF_PERSONS_KILLED,
                AccidentField.CONTRIBUTING_FACTOR_VEHICLE_1,
                AccidentField.CONTRIBUTING_FACTOR_VEHICLE_2,
                AccidentField.CONTRIBUTING_FACTOR_VEHICLE_3,
                AccidentField.CONTRIBUTING_FACTOR_VEHICLE_4,
                AccidentField.CONTRIBUTING_FACTOR_VEHICLE_5
        );

        final DataSet<Tuple9<Integer, Integer, Integer, Integer, String, String, String, String, String>> contributingFactors = this.env
                .readCsvFile(this.data)
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .includeFields(contributingFactorFields)
                .types(
                        Integer.class,
                        Integer.class,
                        Integer.class,
                        Integer.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class
                );

        contributingFactors.first(1).print();

        env.execute();

        return null;
    }
}

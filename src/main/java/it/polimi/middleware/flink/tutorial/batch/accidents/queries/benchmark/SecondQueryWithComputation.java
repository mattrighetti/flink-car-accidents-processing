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

public class SecondQueryWithComputation extends Query {

    public SecondQueryWithComputation(ExecutionEnvironment env, String data, String outputFile) {
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

        final DataSet<Tuple3<String, Integer, Integer>> contributingFactor1 = contributingFactors
                .map(tuple -> {
                    String contributingFactorName = tuple.f4;
                    int isLethal = (tuple.f0 != 0 || tuple.f1 != 0 || tuple.f2 != 0 || tuple.f3 != 0) ? 1 : 0;
                    return Tuple3.of(contributingFactorName, isLethal, 1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        final DataSet<Tuple3<String, Integer, Integer>> contributingFactor2 = contributingFactors
                .map(tuple -> {
                    String contributingFactorName = tuple.f5;
                    int isLethal = (tuple.f0 != 0 || tuple.f1 != 0 || tuple.f2 != 0 || tuple.f3 != 0) ? 1 : 0;
                    return Tuple3.of(contributingFactorName, isLethal, 1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        final DataSet<Tuple3<String, Integer, Integer>> contributingFactor3 = contributingFactors
                .map(tuple -> {
                    String contributingFactorName = tuple.f6;
                    int isLethal = (tuple.f0 != 0 || tuple.f1 != 0 || tuple.f2 != 0 || tuple.f3 != 0) ? 1 : 0;
                    return Tuple3.of(contributingFactorName, isLethal, 1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        final DataSet<Tuple3<String, Integer, Integer>> contributingFactor4 = contributingFactors
                .map(tuple -> {
                    String contributingFactorName = tuple.f7;
                    int isLethal = (tuple.f0 != 0 || tuple.f1 != 0 || tuple.f2 != 0 || tuple.f3 != 0) ? 1 : 0;
                    return Tuple3.of(contributingFactorName, isLethal, 1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        final DataSet<Tuple3<String, Integer, Integer>> contributingFactor5 = contributingFactors
                .map(tuple -> {
                    String contributingFactorName = tuple.f8;
                    int isLethal = (tuple.f0 != 0 || tuple.f1 != 0 || tuple.f2 != 0 || tuple.f3 != 0) ? 1 : 0;
                    return Tuple3.of(contributingFactorName, isLethal, 1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        final DataSet<Tuple3<String, Integer, Integer>> groupedContributingFactors = contributingFactor1
                .union(contributingFactor2)
                .union(contributingFactor3)
                .union(contributingFactor4)
                .union(contributingFactor5)
                .map(tuple -> {
                    if (tuple.f0.isEmpty()) {
                        // "no name", isLethal, 1
                        return Tuple3.of("No name", tuple.f1, tuple.f2);
                    }
                    // contributing factor name, isLethal, 1
                    return Tuple3.of(tuple.f0, tuple.f1, tuple.f2);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        //System.out.println("ContributingFactorName, NumberLethalAccidents, NumberAccidents, Percentage");

        groupedContributingFactors
                // group by contributing factor name
                .groupBy(0)
                // number of lethal accidents and number of rows (number of accidents)
                .reduce(new Functions.Tuple3Sum())
                // return percentage of lethal over total number of accidents
                .map(new Functions.LethalPercentage())
                .first(1).print();

        env.execute();

        return null;
    }
}

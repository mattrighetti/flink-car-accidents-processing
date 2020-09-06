package it.polimi.middleware.flink.tutorial.batch.accidents.queries.benchmark;

import it.polimi.middleware.flink.tutorial.batch.accidents.queries.Query;
import it.polimi.middleware.flink.tutorial.batch.accidents.utils.AccidentField;
import it.polimi.middleware.flink.tutorial.batch.accidents.utils.Functions;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;

import java.util.Date;

public class FirstQueryWithComputation extends Query {

    private IntCounter numLines = new IntCounter();

    public FirstQueryWithComputation(ExecutionEnvironment env, String data, String outputFile) {
        super(env, data, outputFile);
    }


    // First Query :
    // Number of lethal accidents per week throughout the entire dataset
    @Override
    public JobExecutionResult execute() throws Exception {

        // timestamp

        final String firstQueryFields = AccidentField.getFields(
                AccidentField.DATE,
                AccidentField.NUMBER_OF_CYCLIST_KILLED,
                AccidentField.NUMBER_OF_MOTORIST_KILLED,
                AccidentField.NUMBER_OF_PEDESTRIANS_KILLED,
                AccidentField.NUMBER_OF_PERSONS_KILLED
        );

        final DataSet<Tuple5<String, Integer, Integer, Integer, Integer>> lethalAccidentsData = this.env
                .readCsvFile(this.data)
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .includeFields(firstQueryFields)
                .types(
                        String.class,
                        Integer.class,
                        Integer.class,
                        Integer.class,
                        Integer.class
                );

        //

        // Lethal accidents
        final DataSet<Date> lethalAccidentsDates = lethalAccidentsData
                .filter(new Functions.LethalAccidents())
                .map(new Functions.DateParser());

        final DataSet<Tuple3<Integer, Integer, Integer>> lethalAccidentsPerWeek = lethalAccidentsDates
                .map(new Functions.DateToWeekNumber()) // return year and week number
                // group by year and week number
                .groupBy(0, 1)
                // count number of rows
                .sum(2);

        lethalAccidentsPerWeek.first(1).print();

        return env.execute();
    }
}

package it.polimi.middleware.flink.tutorial.batch.accidents.queries.sql;

import it.polimi.middleware.flink.tutorial.batch.accidents.utils.AccidentField;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.Query;
import it.polimi.middleware.flink.tutorial.batch.accidents.utils.Functions;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.table.api._
//import org.apache.flink.table.api.java._

import java.util.Date;

public class FirstQuerySQL extends Query {

    public FirstQuerySQL(ExecutionEnvironment env, String data) {
        super(env, data);
    }


    // First Query :
    // Number of lethal accidents per week throughout the entire dataset
    @Override
    public void execute() throws Exception {

        final String firstQueryFields = AccidentField.getFields(
                AccidentField.DATE,
                AccidentField.NUMBER_OF_CYCLIST_KILLED,
                AccidentField.NUMBER_OF_MOTORIST_KILLED,
                AccidentField.NUMBER_OF_PEDESTRIANS_KILLED,
                AccidentField.NUMBER_OF_PERSONS_KILLED
        );

        //BatchTableEnvironment tEnv = BatchTableEnvironment.

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

        // Lethal accidents
        final DataSet<Date> lethalAccidentsDates = lethalAccidentsData
                .filter(new Functions.LethalAccidents())
                .map(new Functions.DateParser());

        lethalAccidentsDates
                .map(new Functions.DateToWeekNumber())
                .groupBy(0, 1)
                .sum(2)
                .print();
    }
}

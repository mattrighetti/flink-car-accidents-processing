package it.polimi.middleware.flink.tutorial.batch.accidents;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CarAccidents {

    // First Query :
    // Number of lethal accidents per week throughout the entire dataset
    public static void firstQuery(ExecutionEnvironment env, String data) throws Exception {

        final String firstQueryFields = AccidentFiled.getFields(
                AccidentFiled.DATE,
                AccidentFiled.NUMBER_OF_CYCLIST_KILLED,
                AccidentFiled.NUMBER_OF_MOTORIST_KILLED,
                AccidentFiled.NUMBER_OF_PEDESTRIANS_KILLED,
                AccidentFiled.NUMBER_OF_PERSONS_KILLED
        );

        final DataSet<Tuple5<String, Integer, Integer, Integer, Integer>> lethalAccidentsData = env
                .readCsvFile(data)
                .includeFields(firstQueryFields)
                .types(
                        String.class,
                        Integer.class,
                        Integer.class,
                        Integer.class,
                        Integer.class
                );

        // Number of Dates in entire dataset
        final DataSet<Date> accidentsDates = lethalAccidentsData
                .map(new DateParser());

        // Lethal accidents
        final DataSet<Date> lethalAccidentsDates = lethalAccidentsData
                .filter(new LethalAccidents())
                .map(new DateParser());

        // Cross operation to get an instance of type -> | min_date | max_date |
        final DataSet<Tuple2<Date, Date>> minMaxDates = accidentsDates
                .cross(lethalAccidentsDates)
                .groupBy(0, 1)
                .min(0)
                .max(1);

        minMaxDates.print();

        // Number of total weeks
        final DataSet<Long> numOfWeeks = minMaxDates
                .map(new MillisecondsSince1970())
                .map(i -> {
                    long millisecondsWindow = i.f1 - i.f0;
                    long millisecondsInWeek = (long) 6.048e8;
                    return millisecondsWindow / millisecondsInWeek;
                });
    }

    public static void secondQuery(ExecutionEnvironment environment, String data) throws Exception {

    }

    public static void thirdQuery(ExecutionEnvironment environment, String data) throws Exception {

    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String data = params.get("nypd_data_file", "files/car-accidents/NYPD_Motor_Vehicle_Collisions.csv");

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        firstQuery(env, data);
    }

    // --- Functions ---

    /**
     * Function that parses String into Date
     */
    public static class DateParser implements MapFunction<Tuple5<String, Integer, Integer, Integer, Integer>, Date> {
        @Override
        public Date map(Tuple5<String, Integer, Integer, Integer, Integer> in) {
            Date date = null;

            try {
                date = new SimpleDateFormat("dd/MM/yyyy").parse(in.f0);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            return date;
        }
    }

    /**
     * Function that transforms Date into milliseconds from 1970
     */
    public static class MillisecondsSince1970 implements MapFunction<Tuple2<Date, Date>, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> map(Tuple2<Date, Date> in) throws Exception {
            return new Tuple2<>(in.f0.getTime(), in.f1.getTime());
        }
    }

    /**
     * Function that filters only lethal accidents
     */
    public static class LethalAccidents implements FilterFunction<Tuple5<String, Integer, Integer, Integer, Integer>> {
        @Override
        public boolean filter(Tuple5<String, Integer, Integer, Integer, Integer> in) {
            return in.f1 != 0 || in.f2 != 0 || in.f3 != 0 || in.f4 != 0;
        }
    }

}

package it.polimi.middleware.flink.tutorial.batch.accidents;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

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

        System.out.println("Mask: " + firstQueryFields);

        final DataSet<Tuple5<String, Integer, Integer, Integer, Integer>> lethalAccidentsData = env
                .readCsvFile(data)
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
                .filter(new LethalAccidents())
                .map(new DateParser());

        lethalAccidentsDates
                .map(new DateToWeekNumber())
                .groupBy(0, 1)
                .sum(2)
                .print();
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
     * Function that filters only lethal accidents
     */
    public static class LethalAccidents implements FilterFunction<Tuple5<String, Integer, Integer, Integer, Integer>> {
        @Override
        public boolean filter(Tuple5<String, Integer, Integer, Integer, Integer> in) {
            return in.f1 != 0 || in.f2 != 0 || in.f3 != 0 || in.f4 != 0;
        }
    }

    public static class DateToWeekNumber implements MapFunction<Date, Tuple3<Integer, Integer, Integer>> {
        @Override
        public Tuple3<Integer, Integer, Integer> map(Date date) {
            Calendar calendar = new GregorianCalendar();
            calendar.setTime(date);
            int year = calendar.get(Calendar.YEAR);
            int numberOfWeek = calendar.get(Calendar.WEEK_OF_YEAR);
            return new Tuple3<>(year, numberOfWeek, 1);
        }
    }


}

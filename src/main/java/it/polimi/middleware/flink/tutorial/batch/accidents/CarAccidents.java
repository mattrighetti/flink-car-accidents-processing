package it.polimi.middleware.flink.tutorial.batch.accidents;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
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

        final String firstQueryFields = AccidentField.getFields(
                AccidentField.DATE,
                AccidentField.NUMBER_OF_CYCLIST_KILLED,
                AccidentField.NUMBER_OF_MOTORIST_KILLED,
                AccidentField.NUMBER_OF_PEDESTRIANS_KILLED,
                AccidentField.NUMBER_OF_PERSONS_KILLED
        );

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

    public static void secondQuery(ExecutionEnvironment env, String data) throws Exception {
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

        final DataSet<Tuple9<Integer, Integer, Integer, Integer, String, String, String, String, String>> contributingFactors = env
                .readCsvFile(data)
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
                        return Tuple3.of("No name", tuple.f1, tuple.f2);
                    }

                    return Tuple3.of(tuple.f0, tuple.f1, tuple.f2);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        groupedContributingFactors
            .groupBy(0)
            .reduce(new DoubleFieldSum())
            .map(new LethalPercentage())
            .print();

    }

    public static void thirdQuery(ExecutionEnvironment env, String data) throws Exception {

        final String thirdQueryFields = AccidentField.getFields(
                AccidentField.DATE,
                AccidentField.BOROUGH,
                AccidentField.NUMBER_OF_CYCLIST_KILLED,
                AccidentField.NUMBER_OF_MOTORIST_KILLED,
                AccidentField.NUMBER_OF_PEDESTRIANS_KILLED,
                AccidentField.NUMBER_OF_PERSONS_KILLED
        );

        final DataSet<Tuple6<String, String, Integer, Integer, Integer, Integer>> lethalAccidentsDateAndBorough = env
                .readCsvFile(data)
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .includeFields(thirdQueryFields)
                .types(
                        String.class,
                        String.class,
                        Integer.class,
                        Integer.class,
                        Integer.class,
                        Integer.class
                );

        final DataSet<Tuple4<String, String, Integer, Integer>> boroughNumberOfAccidents = lethalAccidentsDateAndBorough
                .map(tuple -> {
                    int isLethal = (tuple.f2 != 0 || tuple.f3 != 0 || tuple.f4 != 0 || tuple.f5 != 0) ? 1 : 0;
                    return Tuple4.of(tuple.f1, tuple.f0, 1, isLethal);
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT, Types.INT));

        final DataSet<Tuple5<String, Integer, Integer, Integer, Integer>> boroughNumberOfAccidentsPerWeek = boroughNumberOfAccidents
                .map(tuple -> {
                    Date date = new SimpleDateFormat("dd/MM/yyyy").parse(tuple.f1);
                    Calendar calendar = new GregorianCalendar();
                    calendar.setTime(date);
                    int year = calendar.get(Calendar.YEAR);
                    int numberOfWeek = calendar.get(Calendar.WEEK_OF_YEAR);

                    return Tuple5.of(tuple.f0, year, numberOfWeek, tuple.f2, tuple.f3);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT, Types.INT, Types.INT));

        boroughNumberOfAccidentsPerWeek
                .filter(tuple -> !tuple.f0.isEmpty())
                .groupBy(0, 1, 2)
                .reduce(new DoubleSum())
                .project(0, 1, 2, 4)
                .print();

        // Average of number of lethal accidents per week
        // grouped by BOROUGH, YEAR
        final DataSet<Tuple3<String, Integer, Float>> averageLethalPerWeekGroupedByYear = boroughNumberOfAccidentsPerWeek
                .filter(tuple -> !tuple.f0.isEmpty())
                .groupBy(0, 1)
                .sum(4)
                .map(tuple -> {
                    float accidentsPerWeekAverage = (float) (((float) tuple.f4) / 52.0);
                    return Tuple3.of(tuple.f0, tuple.f1, accidentsPerWeekAverage);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.FLOAT));

        averageLethalPerWeekGroupedByYear.print();

        // Average of number of lethal accidents per week
        // grouped by BOROUGH
        final DataSet<Tuple2<String, Float>> averageLethalPerWeekTotal = averageLethalPerWeekGroupedByYear
                .map(tuple -> Tuple4.of(tuple.f0, tuple.f1, tuple.f2, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.FLOAT, Types.INT))
                .groupBy(0)
                .reduce(new DoubleSumAverage())
                .map(tuple -> {
                    float averageAccidentsPerWeekInYears = tuple.f2 / tuple.f3;
                    return Tuple2.of(tuple.f0, averageAccidentsPerWeekInYears);
                })
                .returns(Types.TUPLE(Types.STRING,Types.FLOAT));

        averageLethalPerWeekTotal.print();

    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String data = params.get("nypd_data_file", "files/car-accidents/NYPD_Motor_Vehicle_Collisions.csv");
        final int query = params.getInt("query", 1);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        switch (query) {
            case 1:
                firstQuery(env, data);
                break;
            case 2:
                secondQuery(env, data);
                break;
            case 3:
                thirdQuery(env, data);
                break;
            default:
                System.out.println("You have to enter a valid --query param (1, 2, 3)");
                break;
        }
    }


    // --- Functions ---

    /**
     * Function that sums two fields simultaneously with a groupBy
     */
    public static class DoubleFieldSum implements ReduceFunction<Tuple3<String, Integer, Integer>> {
        @Override
        public Tuple3<String, Integer, Integer>
        reduce(Tuple3<String, Integer, Integer> t0, Tuple3<String, Integer, Integer> t1) throws Exception {
            return Tuple3.of(t0.f0, t0.f1 + t1.f1, t0.f2 + t1.f2);
        }
    }

    public static class DoubleSum implements ReduceFunction<Tuple5<String, Integer, Integer, Integer, Integer>> {
        @Override
        public Tuple5<String, Integer, Integer, Integer, Integer> reduce(Tuple5<String, Integer, Integer, Integer, Integer> t0, Tuple5<String, Integer, Integer, Integer, Integer> t1) throws Exception {
            return Tuple5.of(t0.f0, t0.f1, t0.f2, t0.f3 + t1.f3, t0.f4 + t1.f4);
        }
    }

    public static class DoubleSumAverage implements ReduceFunction<Tuple4<String, Integer, Float, Integer>> {
        @Override
        public Tuple4<String, Integer, Float, Integer> reduce(Tuple4<String, Integer, Float, Integer> t0, Tuple4<String, Integer, Float, Integer> t1) throws Exception {
            return Tuple4.of(t0.f0, t0.f1, t0.f2 + t1.f2, t0.f3 + t1.f3);
        }
    }

    public static class LethalPercentage implements MapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, String>> {
        @Override
        public Tuple3<String, Integer, String>
        map(Tuple3<String, Integer, Integer> in) throws Exception {
            float percentage = (float) (((float) in.f1) / ((float) in.f2) * 100.0);
            String percentageString = String.format("%.2f%%", percentage);
            return Tuple3.of(in.f0, in.f2, percentageString);
        }
    }

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

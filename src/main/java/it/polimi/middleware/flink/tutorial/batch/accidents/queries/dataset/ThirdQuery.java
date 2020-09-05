package it.polimi.middleware.flink.tutorial.batch.accidents.queries.dataset;

import it.polimi.middleware.flink.tutorial.batch.accidents.queries.Query;
import it.polimi.middleware.flink.tutorial.batch.accidents.utils.AccidentField;
import it.polimi.middleware.flink.tutorial.batch.accidents.utils.Functions;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class ThirdQuery extends Query {

    public ThirdQuery(ExecutionEnvironment env, String data) {
        super(env, data);
    }

    @Override
    public void execute() throws Exception {

        final String thirdQueryFields = AccidentField.getFields(
                AccidentField.DATE,
                AccidentField.BOROUGH,
                AccidentField.NUMBER_OF_CYCLIST_KILLED,
                AccidentField.NUMBER_OF_MOTORIST_KILLED,
                AccidentField.NUMBER_OF_PEDESTRIANS_KILLED,
                AccidentField.NUMBER_OF_PERSONS_KILLED
        );

        final DataSet<Tuple6<String, String, Integer, Integer, Integer, Integer>> lethalAccidentsDateAndBorough = this.env
                .readCsvFile(this.data)
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
                    // borough, date, 1, isLethal
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

                    // borough, year, week number, 1, isLethal
                    return Tuple5.of(tuple.f0, year, numberOfWeek, tuple.f2, tuple.f3);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT, Types.INT, Types.INT));

        final DataSet<Tuple4<String, Integer, Integer, Integer>> boroughNumberOfLethalAccidentsPerWeek = boroughNumberOfAccidentsPerWeek
                .filter(tuple -> !tuple.f0.isEmpty())
                // group by boroguh and week (week identified by year and week number)
                .groupBy(0, 1, 2)
                // count number of tuples
                .reduce(new Functions.Tuple5Sum())
                .project(0, 1, 2, 4); // return 4 because it is the number of lethal accidents

        boroughNumberOfLethalAccidentsPerWeek.print();

        final DataSet<Tuple5<String, Integer, Integer, Integer, Integer>> averageLethalAccidentsPerWeekPerBorough = boroughNumberOfLethalAccidentsPerWeek
                .filter(tuple -> !tuple.f0.isEmpty())
                // borough, year, week number, number of lethal accidents, 1
                .map(tuple -> Tuple5.of(tuple.f0, tuple.f1, tuple.f2, tuple.f3, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT, Types.INT, Types.INT)); // used later to count how many weeks we have for each borough

        averageLethalAccidentsPerWeekPerBorough.print();

        averageLethalAccidentsPerWeekPerBorough
                .groupBy(0) // group by borough
                .reduce(new Functions.Tuple5Sum()) // get the total number of rows (weeks) and total number of lethal accidents
                .map(tuple -> {
                    // tuple.f3 = total accidents in that borough
                    // tuple.f4 = number of lethal accidents in that borough
                    float avg;
                    try {
                        avg = (float) tuple.f3 / tuple.f4; // lethal accidents over number of weeks for which we have data
                    } catch (ArithmeticException e) {
                        avg = 0;
                    }
                    return Tuple4.of(tuple.f0, tuple.f3, tuple.f4, avg); // borough, total lethal accidents, number of weeks, average
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT, Types.FLOAT))
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
                .reduce(new Functions.Tuple4Sum())
                .map(tuple -> {
                    float averageAccidentsPerWeekInYears = tuple.f2 / tuple.f3;
                    return Tuple2.of(tuple.f0, averageAccidentsPerWeekInYears);
                })
                .returns(Types.TUPLE(Types.STRING,Types.FLOAT));

        averageLethalPerWeekTotal.print();

    }

}

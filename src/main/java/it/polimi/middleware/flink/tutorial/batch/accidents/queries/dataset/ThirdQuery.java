package it.polimi.middleware.flink.tutorial.batch.accidents.queries.dataset;

import it.polimi.middleware.flink.tutorial.batch.accidents.queries.Query;
import it.polimi.middleware.flink.tutorial.batch.accidents.utils.AccidentField;
import it.polimi.middleware.flink.tutorial.batch.accidents.utils.Functions;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


public class ThirdQuery extends Query {

    public ThirdQuery(ExecutionEnvironment env, String data, String outputFile) {
        super(env, data, outputFile);
    }

    @Override
    public JobExecutionResult execute() throws Exception {

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

        final DataSet<Tuple5<String, Integer, Integer, Integer, Integer>> boroughNumberOfAccidentsWithWeekNumber = boroughNumberOfAccidents
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

        //
        //  Compute for each (borough, year, week)
        //  - the total number of accidents
        //  - the avg/ratio of lethal accidents over total accidents
        //
        final DataSet<Tuple5<String, Integer, Integer, Integer, Integer>> boroughNumberOfLethalAccidentsPerWeek = boroughNumberOfAccidentsWithWeekNumber
                .filter(tuple -> !tuple.f0.isEmpty())
                // group by boroguh, year and week number (week identified by year and week number)
                .groupBy(0, 1, 2)
                .reduce(new Functions.Tuple5Sum())
                // borough, yaer, week, num accidents, num lethal accidents (group by borough and week)
                // => 1 row for each triplet (borough, year, week)
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT, Types.INT, Types.INT));

        final DataSet<Tuple5<String, Integer, Integer, Integer, Float>> boroughNumberOfLethalAccidentsAveragePerWeek = boroughNumberOfLethalAccidentsPerWeek
                .map(tuple -> {
                    // borough, year, week, number of accidents, avg (percentage/ratio) lethal accidents
                    return Tuple5.of(tuple.f0, tuple.f1, tuple.f2, tuple.f3, (float)tuple.f4/tuple.f3);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT, Types.INT, Types.FLOAT));

        boroughNumberOfLethalAccidentsAveragePerWeek
                .writeAsCsv(outputFile, "\n", ",", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute();
        return null;
    }

}

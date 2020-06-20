package it.polimi.middleware.flink.tutorial.batch.accidents.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class Functions {

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

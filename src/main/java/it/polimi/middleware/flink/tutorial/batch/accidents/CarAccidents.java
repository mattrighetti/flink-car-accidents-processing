package it.polimi.middleware.flink.tutorial.batch.accidents;

import it.polimi.middleware.flink.tutorial.batch.accidents.queries.Query;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.benchmark.FirstQueryLoadData;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.benchmark.FirstQueryWithComputation;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.benchmark.SecondQueryLoadData;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.benchmark.SecondQueryWithComputation;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.dataset.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;


public class CarAccidents {

    private static int queryNumber;
    private static String data;
    private static String outputFile;
    private static ExecutionEnvironment env;

    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        data = params.get("nypd_data_file", "files/car-accidents/NYPD_Motor_Vehicle_Collisions.csv");
        queryNumber = params.getInt("query", 1);
        outputFile = params.get("output", "./output.txt");
        env = ExecutionEnvironment.getExecutionEnvironment();
        JobExecutionResult executionResult;

        Query query;
        try {
            query = getQuery(queryNumber);
            executionResult = query.execute();

            //System.out.println("Exec time: " + executionResult.getNetRuntime());
            //System.out.println(executionResult.getAllAccumulatorResults());

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println(e.getMessage());
        }


    }

    private static Query getQuery(int queryNumber) throws Exception {
        switch (queryNumber) {
            case 1:
                return new FirstQuery(env, data, outputFile);
            case 2:
                return new SecondQuery(env, data, outputFile);
            case 3:
                return new ThirdQuery(env, data, outputFile);
            case 4:
                return new ThirdQuery2(env, data, outputFile);
            default:
                throw new Exception("You have to enter a valid --query param (1, 2, 3, 4)");
        }
    }
}

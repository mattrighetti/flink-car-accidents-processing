package it.polimi.middleware.flink.tutorial.batch.accidents;

import it.polimi.middleware.flink.tutorial.batch.accidents.queries.Query;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.benchmark.*;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.dataset.FirstQuery;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.dataset.SecondQuery;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.dataset.ThirdQuery;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CarAccidentsBenchmark {

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

            Logger LOG = LoggerFactory.getLogger(CarAccidentsBenchmark.class);
            LOG.info("Exec time: " + executionResult.getNetRuntime());

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println(e.getMessage());
        }


    }

    private static Query getQuery(int queryNumber) throws Exception {
        switch (queryNumber) {
            case 1:
                return new FirstQueryLoadData(env, data, outputFile);
            case 2:
                return new FirstQueryWithComputation(env, data, outputFile);
            case 3:
                return new SecondQueryLoadData(env, data, outputFile);
            case 4:
                return new SecondQueryWithComputation(env, data, outputFile);
            case 5:
                return new ThirdQueryLoadData(env, data, outputFile);
            case 6:
                return new ThirdQueryWithComputation(env, data, outputFile);
            case 7:
                return new ThirdQuery2WithComputation(env, data, outputFile);
            default:
                throw new Exception("You have to enter a valid --query param benchmark (1, 2, 3, 4, 5, 6, 7)");
        }
    }
}

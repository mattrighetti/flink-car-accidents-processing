package it.polimi.middleware.flink.tutorial.batch.accidents;

import it.polimi.middleware.flink.tutorial.batch.accidents.queries.Query;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.dataset.SecondQuery;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.dataset.ThirdQuery;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.dataset.ThirdQuery2;
import it.polimi.middleware.flink.tutorial.batch.accidents.queries.sql.ThirdQuerySQL;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import it.polimi.middleware.flink.tutorial.batch.accidents.queries.dataset.FirstQuery;


public class CarAccidents {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String data = params.get("nypd_data_file", "files/car-accidents/NYPD_Motor_Vehicle_Collisions.csv");
        final int queryNumber = params.getInt("query", 1);
        final String outputFile = params.get("output", "./output.txt");
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Query query;

        switch (queryNumber) {
            case 1:
                query = new FirstQuery(env, data, outputFile);
                query.execute();
                break;
            case 2:
                query = new SecondQuery(env, data, outputFile);
                query.execute();
                break;
            case 3:
                query = new ThirdQuery(env, data, outputFile);
                query.execute();
                break;
            case 4:
                query = new ThirdQuery2(env, data, outputFile);
                query.execute();
                break;
            default:
                System.out.println("You have to enter a valid --query param (1, 2, 3, 4)");
                break;
        }
    }
}

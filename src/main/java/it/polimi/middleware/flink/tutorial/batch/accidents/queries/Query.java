package it.polimi.middleware.flink.tutorial.batch.accidents.queries;

import org.apache.flink.api.java.ExecutionEnvironment;

public abstract class Query {

    public ExecutionEnvironment env;
    public String data;

    // Abstract constructor
    public Query(ExecutionEnvironment env, String data){
        this.env = env;
        this.data = data;
    }

    // abstract method that implements the query
    public abstract void execute() throws Exception;

}

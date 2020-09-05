package it.polimi.middleware.flink.tutorial.batch.accidents.queries;

import org.apache.flink.api.java.ExecutionEnvironment;

public abstract class Query {

    protected ExecutionEnvironment env;
    protected String data;
    protected String outputFile;

    // Abstract constructor
    public Query(ExecutionEnvironment env, String data, String outputFile){
        this.env = env;
        this.data = data;
        this.outputFile = outputFile;
    }

    // abstract method that implements the query
    public abstract void execute() throws Exception;

}

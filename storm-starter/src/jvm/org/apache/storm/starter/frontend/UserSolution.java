package org.apache.storm.starter.frontend;

import org.apache.storm.starter.profiling.GraphParser;
import org.moeaframework.core.Solution;
import org.moeaframework.core.variable.EncodingUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by VagelisAkis on 29/1/2017.
 */
public class UserSolution {
    private int workers;
    private Map<String, Integer> executors;

    public UserSolution() {
        executors = new HashMap<>();
    }

    public UserSolution(GraphParser g, Solution s) {
        executors = new HashMap<>();
        int[] config = EncodingUtils.getInt(s);
        int i = 0;

        workers = config[i++];
        for(String spoutName : g.getSpoutMap().keySet())
            executors.put(spoutName, config[i++]);
        for(String boltName : g.getBoltMap().keySet())
            executors.put(boltName, config[i++]);
    }

    public void print() {
        System.out.println("***SOLUTION***");
        System.out.println("Workers: " + workers);
        for(String operator : executors.keySet())
            System.out.println("Operator: " + operator + " number of executors: " + executors.get(operator));
        System.out.println("**************");
    }

    public Map<String, Integer> getExecutors() {
        return executors;
    }

    public void setExecutors(Map<String, Integer> executors) {
        this.executors = executors;
    }

    public void addExecutor(String name, int executors) {
        this.executors.put(name, executors);
    }
}

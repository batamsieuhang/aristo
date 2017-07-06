package org.apache.storm.starter.profiling;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by vgol on 25/12/2016.
 */
public class Constraints {

    private Map<String, Double> throughput;
    private Integer currentWorkers;
    private Integer constraintWorkers;
    private Integer maxWorkers;

    public Constraints(int maxWorkers) {
        currentWorkers = 0;
        this.maxWorkers = maxWorkers;
        throughput = new HashMap<>();
    }

    public boolean hasNext() {
        if(constraintWorkers == null)
            return currentWorkers < maxWorkers;
        else
            return currentWorkers < constraintWorkers;
    }

    public Integer getNext() {
        return ++currentWorkers;
    }

    public Map<String, Double> getThroughput() {
        return throughput;
    }

    public void setThroughput(Map<String, Double> throughput) {
        this.throughput = throughput;
    }

    public void addThroughput(String spoutname, Double throughput) {
        this.throughput.put(spoutname, throughput);
    }

    public Integer getConstraintWorkers() {
        return constraintWorkers;
    }

    public void setConstraintWorkers(Integer constraintWorkers) {
        this.constraintWorkers = constraintWorkers;
    }

    public Integer getMaxWorkers() {
        return maxWorkers;
    }

    public void setMaxWorkers(Integer maxWorkers) {
        this.maxWorkers = maxWorkers;
    }
}

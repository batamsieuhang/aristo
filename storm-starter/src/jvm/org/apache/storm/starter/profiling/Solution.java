package org.apache.storm.starter.profiling;

import java.util.HashMap;
import java.util.Map;

public class Solution {

    private Double throughput;
    private Double remainingTime;
    private Integer workers;
    private Map<String, Integer> threads;
    private Integer notCompleteFlag;
//    private Double deviation;

    public Solution(int workers) {
        threads = new HashMap<>();
        this.workers = workers;
        notCompleteFlag = 0;
    }

    public void print() {
        System.out.println("*************************");
        System.out.println("Throughput: " + throughput);
        System.out.println("Remaining Time: " + remainingTime);
        System.out.println("Not Complete Solution: " + notCompleteFlag);
        System.out.println("Workers: " + workers);
        System.out.println("Topology executors:");
        for(String operator : threads.keySet())
            System.out.println(operator + " -> " + threads.get(operator));
        System.out.println("*************************");
    }

    public Double getThroughput() {
        return throughput;
    }

    public void setThroughput(Double throughput) {
        this.throughput = throughput;
    }

    public Double getRemainingTime() {
        return remainingTime;
    }

    public void setRemainingTime(Double remainingTime) {
        this.remainingTime = remainingTime;
    }

    public Integer getWorkers() {
        return workers;
    }

    public void setWorkers(Integer workers) {
        this.workers = workers;
    }

    public Map<String, Integer> getThreads() {
        return threads;
    }

    public Integer getOperatorThreads(String name) {
        return threads.get(name);
    }

    public void setThreads(Map<String, Integer> threads) {
        this.threads = threads;
    }

    public void addThread(String operator, Integer threadCount) {
        threads.put(operator, threadCount);
    }

    public Integer getNotCompleteFlag() {
        return notCompleteFlag;
    }

    public void setNotCompleteFlag(Integer notCompleteFlag) {
        this.notCompleteFlag = notCompleteFlag;
    }
}

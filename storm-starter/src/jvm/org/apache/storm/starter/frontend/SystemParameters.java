package org.apache.storm.starter.frontend;

/**
 * Created by VagelisAkis on 29/1/2017.
 */
public class SystemParameters {

    private int maxWorkers;
    private int maxThreads;

    public SystemParameters() {
    }

    public SystemParameters(int maxWorkers, int maxThreads) {
        this.maxWorkers = maxWorkers;
        this.maxThreads = maxThreads;
    }

    public int getMaxWorkers() {
        return maxWorkers;
    }

    public void setMaxWorkers(int maxWorkers) {
        this.maxWorkers = maxWorkers;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }
}

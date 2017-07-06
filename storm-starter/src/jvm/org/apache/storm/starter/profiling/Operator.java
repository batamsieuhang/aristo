package org.apache.storm.starter.profiling;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.IOException;
import java.util.ArrayList;

public class Operator {

    private String name;
    private MetricsPredictor predictor;
    private Double targetThroughput;
    private Integer sources;
    private Double IORatio;
    private ArrayList<Operator> neighbors;

    public Operator(String name) {
        this.name = name;

        try{
            predictor = new MetricsPredictor(this.name);
        }
        catch(IOException e) {
            e.printStackTrace();
        }

        targetThroughput = 0d;
        neighbors = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public MetricsPredictor getPredictor() {
        return predictor;
    }

    public void setPredictor(MetricsPredictor predictor) {
        this.predictor = predictor;
    }

    public Double getTargetThroughput() {
        return targetThroughput;
    }

    public void setTargetThroughput(Double targetThroughput) {
        this.targetThroughput = targetThroughput;
    }

    public void addTargetThroughput(Double targetThroughput) {
        this.targetThroughput += targetThroughput;
    }

    public Integer getSources() {
        return sources;
    }

    public void setSources(Integer sources) {
        this.sources = sources;
    }

    public Double getIORatio() {
        if(IORatio == null)
            IORatio = predictor.getIORatio();
        return IORatio;
    }

    public void setIORatio(Double IORatio) {
        this.IORatio = IORatio;
    }

    public ArrayList<Operator> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(ArrayList<Operator> neighbors) {
        this.neighbors = neighbors;
    }

    public void addNeighbor(Operator neighbor) {
        neighbors.add(neighbor);
    }
}

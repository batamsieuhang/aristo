package org.apache.storm.starter.frontend;

/**
 * Created by VagelisAkis on 29/1/2017.
 */
public class UserParameters {

    private Double dataset;
    private Double deadline;
    private Double budget;
    private Double cost; //per second lease cost

    public UserParameters() {
    }

    public UserParameters(Double dataset, Double deadline, Double budget, Double cost) {
        this.dataset = dataset;
        this.deadline = deadline;
        this.budget = budget;
        this.cost = cost;
    }

    public Double getDataset() {
        return dataset;
    }

    public void setDataset(Double dataset) {
        this.dataset = dataset;
    }

    public Double getDeadline() {
        return deadline;
    }

    public void setDeadline(Double deadline) {
        this.deadline = deadline;
    }

    public Double getBudget() {
        return budget;
    }

    public void setBudget(Double budget) {
        this.budget = budget;
    }

    //TODO : could be changed to getCost(workers, time, ...) to represent different cost programs
    public Double getCost() {
        return cost;
    }

    public void setCost(Double cost) {
        this.cost = cost;
    }
}

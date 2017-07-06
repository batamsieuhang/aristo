package org.apache.storm.starter.profiling.cost;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by vgol on 16/1/2017.
 */
public class Container {
    private Integer id;
    private double watermark;
    private double capacity;
    private Map<String, Double> tupleCount;
    private Set<String> assigns;

    public Container() {
        watermark = 0;
        capacity = 1;
        tupleCount = new HashMap<>();
        assigns = new HashSet<>();
    }

    public Container(int id){
        this.id = id;
        watermark = 0;
        capacity = 1;
        tupleCount = new HashMap<>();
        assigns = new HashSet<>();
    }

    public Container(double capacity) {
        this.capacity = capacity;
        tupleCount = new HashMap<>();
        assigns = new HashSet<>();
    }

    public Container(int id, double capacity) {
        this.id = id;
        this.capacity = capacity;
        tupleCount = new HashMap<>();
        assigns = new HashSet<>();
    }

    public double getWatermark() {
        return watermark;
    }

    public void setWatermark(double watermark) throws IllegalArgumentException{
        if(watermark > capacity)
            throw new IllegalArgumentException("Watermark can't be set higher than Container capacity");
        this.watermark = watermark;
    }

    public void increaseWatermark(double increase) throws IllegalStateException{
        watermark += increase;
        if(watermark > capacity)
            throw new IllegalStateException("Watermark exceeded Container capacity");
    }

    public double getCapacity() {
        return capacity;
    }

    public void setCapacity(double capacity) {
        this.capacity = capacity;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Map<String, Double> getTupleCount() {
        return tupleCount;
    }

    public void setTupleCount(Map<String, Double> tupleCount) {
        this.tupleCount = tupleCount;
    }

    public void addTupleCount(String operator, double dx) {
        if(!tupleCount.containsKey(operator)) {
            tupleCount.put(operator, dx);
        }
        else {
            Double count = tupleCount.get(operator);
            tupleCount.put(operator, count + dx);
//            count += dx;
        }
    }

    public void assignOperator(String operator) {
        if(!assigns.contains(operator))
            assigns.add(operator);
    }

    public boolean checkAssign(String operator){
        return assigns.contains(operator);
    }

    public void printStatus() {
        System.out.println("Container Id: " + id);
        System.out.println("Container Capacity: " + capacity);
        System.out.println("Container Watermark: " + watermark);
    }

    public void printStatusVerbose() {
        System.out.println("Container Id: " + id);
        System.out.println("Capacity: " + capacity);
        System.out.println("Watermark: " + watermark);
        System.out.println("Operator participation");
        for(String operator : tupleCount.keySet())
            System.out.println("Operator: " + operator + " Tuple count: " + tupleCount.get(operator));
    }
}

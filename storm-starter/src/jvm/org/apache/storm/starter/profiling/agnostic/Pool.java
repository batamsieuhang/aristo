package org.apache.storm.starter.profiling.agnostic;

/**
 * Created by vgol on 10/3/2017.
 */
public class Pool {
    private String id;
    private double watermark;
    private double capacity;
    private double tupleCount;

    public Pool() {
        watermark = 0;
        capacity = 1;
        tupleCount = 0;
    }

    public Pool(double capacity) {
        watermark = 0;
        this.capacity = capacity;
        tupleCount = 0;
    }

    public Pool(String id, int threads, int workers) {
        this.id = id;
        watermark = 0;
        this.capacity = Math.min(threads, workers);
        tupleCount = 0;
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

    public double getTupleCount() {
        return tupleCount;
    }

    public void setTupleCount(double tupleCount) {
        this.tupleCount = tupleCount;
    }

    public void addTupleCount(double dx) {
        tupleCount += dx;
    }

    public void printStatus() {
        System.out.println("Container Id: " + id);
        System.out.println("Container Capacity: " + capacity);
        System.out.println("Container Watermark: " + watermark);
    }
}

package org.apache.storm.starter.profiling.cost;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by vgol on 16/1/2017.
 */
public class Cluster {

    private int workerNumber;
    private Map<Integer, Container> containers;
    private int pointer;

    public Cluster(int workernumber) {
        this.workerNumber = workernumber;

        containers = new HashMap<>();
        for(int i = 0; i < workernumber; i++)
            containers.put(i,new Container(i));

        pointer = 0;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    public void setWorkerNumber(int workerNumber) {
        this.workerNumber = workerNumber;
    }

    public Map<Integer, Container> getContainers() {
        return containers;
    }

    public void setContainers(Map<Integer, Container> workers) {
        this.containers = workers;
    }

    public Container getNextContainer() {
        pointer = (pointer+1) % workerNumber;
        return containers.get(pointer);
    }

    public Container getContainer(int pointer) {
        return containers.get(pointer);
    }

    public int getPointer() {
        return pointer;
    }

    public void setPointer(int pointer) {
        this.pointer = pointer;
    }

    public void printStatus() {
        System.out.println("Worker number: " + workerNumber);
        for(Container c : containers.values())
            c.printStatus();
    }

    public void printStatusVerbose() {
        System.out.println("Worker number: " + workerNumber);
        for(Container c : containers.values())
            c.printStatusVerbose();
    }

    public static void main(String args[]) {
        Cluster c = new Cluster(4);

        Container con;
        for(int i=0;i<4;i++) {
            con = c.getNextContainer();
            con.setWatermark(i);
        }


        for(int i=0;i<12;i++)
            System.out.println(c.getNextContainer().getWatermark());
    }
}

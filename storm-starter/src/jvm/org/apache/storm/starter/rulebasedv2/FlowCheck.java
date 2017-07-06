package org.apache.storm.starter.rulebasedv2;

import org.apache.storm.starter.metrics.BoltMetrics;
import org.apache.storm.starter.metrics.BoltMetricsUpdater;
import org.apache.storm.starter.metrics.SpoutMetrics;
import org.apache.storm.starter.metrics.SpoutMetricsUpdater;
import org.apache.storm.starter.rulebased.BoltMetricsComparator;
import org.apache.storm.starter.rulebased.ComponentNode;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class FlowCheck {


    private String  topologyName;
    private int moves;
    private int changes;
    private int cores;
    private int workers;
    private int maxWorkers;
    private int executors;

    private static Map<String, ComponentNode> spoutMap;
    private static Map<String, ComponentNode> boltMap;
    private Map<String, Double> targetThroughput;

    private Map<String, SpoutMetrics> currentSpoutStats;
    private Map<String, SpoutMetrics> previousSpoutStats;
    private Map<String, BoltMetrics> currentBoltStats;
    private Map<String, BoltMetrics> previousBoltStats;
    private Map<String, Integer> severity;
    private int maxSeverity;

    private RebalanceMove previousConfig;
    private RebalanceMove currentConfig;

    private boolean rebalanced;
    private boolean checkNeeded;



    public FlowCheck(String topologyName, Map<String, ComponentNode> spoutMap, Map<String, ComponentNode> boltMap, Map<String, Double> throughput) {

        this.topologyName = topologyName;

        moves = 0;
        changes = 0;
        cores = 2;
        workers = 1;
        maxWorkers = 4;

        this.spoutMap = spoutMap;
        this.boltMap = boltMap;

        this.targetThroughput = throughput;

        executors = countThreads();


        currentSpoutStats = new HashMap<String, SpoutMetrics>();

        previousSpoutStats = new HashMap<String, SpoutMetrics>();
        severity = new HashMap<String, Integer>();
        for (String key : spoutMap.keySet()) {
            severity.put(key, 0);
            previousSpoutStats.put(key, new SpoutMetrics(((SpoutMetricsUpdater)spoutMap.get(key).getNode().getComponentUpdater()).getSpoutMetrics()));
        }
        maxSeverity = 2;

        currentBoltStats = new HashMap<>();

        previousBoltStats = new HashMap<>();
        for (String key : boltMap.keySet())
            previousBoltStats.put(key, new BoltMetrics( ((BoltMetricsUpdater)boltMap.get(key).getNode().getComponentUpdater()).getBoltMetrics() ));

        previousConfig = new RebalanceMove();
        currentConfig = new RebalanceMove();

        rebalanced = false;

        checkNeeded = false;
    }


    public void initFlowCheck() {

        for (String key : boltMap.keySet())
            currentBoltStats.put(key, new BoltMetrics( ((BoltMetricsUpdater)boltMap.get(key).getNode().getComponentUpdater()).getBoltMetrics() ));

        changes = 0;

        Map<String, ComponentNode> needCheck = new HashMap<>();

        for (String key : spoutMap.keySet()) {

            currentSpoutStats.put(key, ((SpoutMetricsUpdater)spoutMap.get(key).getNode().getComponentUpdater()).getSpoutMetrics());
            System.out.println("Spout: " + key + " target throughput: " + targetThroughput.get(key) + " current throughput: " + currentSpoutStats.get(key).getAckedRate());


            if(targetThroughput.get(key) > currentSpoutStats.get(key).getAckedRate()) {

                if(initSpoutCheck(spoutMap.get(key))) {
                    needCheck.put(key, spoutMap.get(key));
                    checkNeeded = true;
                }

            }
        }

        //TODO if needed keep history stats when replacing in previous hashmap
        for (String key : spoutMap.keySet())
            previousSpoutStats.put(key, new SpoutMetrics(((SpoutMetricsUpdater)spoutMap.get(key).getNode().getComponentUpdater()).getSpoutMetrics()));


        if(!checkNeeded)
            return;

        try {

            if ((executors) > (cores * workers)) {
                if (workers < maxWorkers) {

                    System.out.println("Added worker");
                    previousConfig.setWorkers(workers);
                    currentConfig.setWorkers(++workers);

                    System.out.println("Rebalance");
                    currentConfig.commitRebalance(topologyName);

                    rebalanced = true;
                } else
                    System.out.println("Cannot add more workers");

                return;
            }


            for (ComponentNode spout : needCheck.values())
                    initBoltCheck(spout);


            if (changes > 0) {

                if ((executors + changes) > (cores * workers)) {
                    if(workers < maxWorkers) {

                        System.out.println("Added worker");
                        previousConfig.setWorkers(workers);
                        currentConfig.setWorkers(++workers);
                    }
                    else
                        System.out.println("Cannot add more workers");
                }

                System.out.println("Rebalance");
                currentConfig.commitRebalance(topologyName);

                rebalanced = true;
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }


    public static int compareSpoutStats(SpoutMetrics previous, SpoutMetrics current) {
            int severity = 0;

            System.out.println("throughput: " + previous.getAckedRate() + "-->" + current.getAckedRate());
            System.out.println("latency: " + previous.getCompleteLatency() + "-->" + current.getCompleteLatency());

            if(current.getAckedRate() > previous.getAckedRate()) {

                System.out.println("Relative increase: " + (current.getAckedRate() - previous.getAckedRate()) / previous.getAckedRate());

                if((current.getAckedRate() - previous.getAckedRate()) / previous.getAckedRate() > 0.05)
                    return -1;
                else
                    severity++;

            }
            else {

                System.out.println("Relative decrease: " + (previous.getAckedRate() - current.getAckedRate()) / previous.getAckedRate());

                if((previous.getAckedRate() - current.getAckedRate()) / previous.getAckedRate() > 0.02)
                    severity++;
            }

            return severity;

    }


    public boolean initSpoutCheck(ComponentNode root) {
        System.out.println("Checking: " + root.getNode().getComponentId());

        int tempSeverity = severity.get(root.getNode().getComponentId());

        System.out.println("Current severity: " + tempSeverity);

        tempSeverity += compareSpoutStats(previousSpoutStats.get(root.getNode().getComponentId()), currentSpoutStats.get(root.getNode().getComponentId()));
        System.out.println("New severity: " + tempSeverity);

        if (tempSeverity < 0)
            tempSeverity = 0;

        severity.put(root.getNode().getComponentId(), tempSeverity);

        if (tempSeverity > maxSeverity)
            return true;

        return false;
    }




    public void initBoltCheck(ComponentNode root) {

        //put the bolts in a priority queue based on capacity
        PriorityQueue<ComponentNode> queue = new PriorityQueue<ComponentNode>(11, new BoltMetricsComparator());

        for (ComponentNode neighbor : root.getNeighbors()) {
            //inspectFlow(neighbor);
            queue = recursiveInspect(neighbor, queue);
        }


        //iterate through the queue while the capacity is greater than 0.8
        //if none of the bolts have capacity problem then we add a thread to the bolt with the biggest one (prediction that there is a [potential bottleneck)
        ComponentNode bolt = queue.poll();
        BoltMetrics boltStats = ((BoltMetricsUpdater) bolt.getNode().getComponentUpdater()).getBoltMetrics();
        do {

            if (!currentConfig.getComponents().containsKey(bolt.getNode().getComponentId())) {
                System.out.println(boltStats.getId() + " capacity: " + boltStats.getCapacity());
                System.out.println(boltStats.getId() + " threads: " + boltStats.getExecutors() + "-->" + (boltStats.getExecutors()+1));
                previousConfig.addComponent(bolt.getNode().getComponentId(), boltStats.getExecutors());
                currentConfig.addComponent(bolt.getNode().getComponentId(), boltStats.getExecutors() + 1);
                changes++;
            }

            bolt = queue.poll();
            if(bolt == null)
                return;
            boltStats = ((BoltMetricsUpdater) bolt.getNode().getComponentUpdater()).getBoltMetrics();
        }
        while(boltStats.getCapacity() > 0.8 );


    }


    public PriorityQueue<ComponentNode> recursiveInspect(ComponentNode bolt, PriorityQueue<ComponentNode> pq) {

        pq.add(bolt);

        for (ComponentNode neighbor : bolt.getNeighbors()) {
            pq = recursiveInspect(neighbor, pq);
        }

        return pq;
    }


    public static int countThreads() {

        int threads = 0;

        for (String key : spoutMap.keySet()) {
            threads += ((SpoutMetricsUpdater)spoutMap.get(key).getNode().getComponentUpdater()).getSpoutMetrics().getExecutors();
        }

        for (String key : boltMap.keySet()) {
            threads += ((BoltMetricsUpdater)boltMap.get(key).getNode().getComponentUpdater()).getBoltMetrics().getExecutors();
        }

        return threads;
    }


    public void rebalanceInit() throws Exception{

        for (String key : spoutMap.keySet()) {
            severity.put(key, 0);
            previousSpoutStats.put(key, new SpoutMetrics(((SpoutMetricsUpdater)spoutMap.get(key).getNode().getComponentUpdater()).getSpoutMetrics()));
        }

        moves++;
        previousConfig = currentConfig;
        currentConfig = new RebalanceMove();

        executors = countThreads();

        rebalanced = false;

        checkNeeded = false;
    }


    public void inspectFlow(ComponentNode bolt){

        BoltMetrics boltStats = ((BoltMetricsUpdater)bolt.getNode().getComponentUpdater()).getBoltMetrics();

        if(boltStats.getCapacity() > 0.9 && !currentConfig.getComponents().containsKey(bolt.getNode().getComponentId())) {
            System.out.println("Added Thread " + boltStats.getId());
            previousConfig.addComponent(bolt.getNode().getComponentId(), boltStats.getExecutors());
            currentConfig.addComponent(bolt.getNode().getComponentId(), boltStats.getExecutors()+1);
            changes++;
        }

        for (ComponentNode neighbor : bolt.getNeighbors()) {
            inspectFlow(neighbor);
        }

    }

    public boolean isRebalanced() {
        return rebalanced;
    }

    public void setRebalanced(boolean rebalanced) {
        this.rebalanced = rebalanced;
    }
}

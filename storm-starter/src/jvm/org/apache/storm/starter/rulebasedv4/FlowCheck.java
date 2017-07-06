package org.apache.storm.starter.rulebasedv4;

import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.starter.metrics.BoltMetrics;
import org.apache.storm.starter.metrics.BoltMetricsUpdater;
import org.apache.storm.starter.metrics.SpoutMetrics;
import org.apache.storm.starter.metrics.SpoutMetricsUpdater;
import org.apache.storm.starter.rulebased.BoltMetricsComparator;
import org.apache.storm.starter.rulebased.ComponentNode;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.*;

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

    private List<TopologyConfiguration> previousConfigs;
    private List<Double> histThroughput;
    private List<Double> histLatency;
    private Double avgThroughput;
    private Double avgLatency;
    private int count;
    private boolean killTopology;

    private boolean rebalanced;
    private boolean checkNeeded;

    private long start,stop;

    private OutputWriter writer;
    TopologyConfiguration conf;

    private String boltIndex = "split";



    public FlowCheck(String topologyName, Map<String, ComponentNode> spoutMap, Map<String, ComponentNode> boltMap, Map<String, Double> throughput) {

        this.topologyName = topologyName;

        moves = 0;
        changes = 0;
        cores = 2;
        workers = 1;
        maxWorkers = 7;

        this.spoutMap = spoutMap;
        this.boltMap = boltMap;

        this.targetThroughput = throughput;

        executors = countThreads();


        currentSpoutStats = new HashMap<String, SpoutMetrics>();

        previousSpoutStats = new HashMap<String, SpoutMetrics>();
        severity = new HashMap<String, Integer>();
        severity.put(boltIndex, 0);
        for (String key : spoutMap.keySet()) {
            previousSpoutStats.put(key, new SpoutMetrics(((SpoutMetricsUpdater)spoutMap.get(key).getNode().getComponentUpdater()).getSpoutMetrics()));
        }
        maxSeverity = 2;

        currentBoltStats = new HashMap<>();

        previousBoltStats = new HashMap<>();
        for (String key : boltMap.keySet())
            previousBoltStats.put(key, new BoltMetrics( ((BoltMetricsUpdater)boltMap.get(key).getNode().getComponentUpdater()).getBoltMetrics() ));

        previousConfig = new RebalanceMove();
        currentConfig = new RebalanceMove();

        previousConfigs = new ArrayList<>();
        conf = new TopologyConfiguration(workers, previousSpoutStats, previousBoltStats);
        previousConfigs.add(conf);
        histThroughput = new ArrayList<>();
        histLatency = new ArrayList<>();

        avgThroughput = 0d;
        avgLatency = 0d;
        count = 0;

        killTopology = false;

        rebalanced = false;

        checkNeeded = false;

        start = System.currentTimeMillis();

        writer = new OutputWriter(topologyName);
    }


    public void initFlowCheck() {

        for (String key : spoutMap.keySet())
            currentSpoutStats.put(key, ((SpoutMetricsUpdater) spoutMap.get(key).getNode().getComponentUpdater()).getSpoutMetrics());

        for (String key : boltMap.keySet())
            currentBoltStats.put(key, new BoltMetrics( ((BoltMetricsUpdater)boltMap.get(key).getNode().getComponentUpdater()).getBoltMetrics() ));

        changes = 0;

        Map<String, ComponentNode> needCheck = new HashMap<>();


        System.out.println("Index: " + boltIndex + " target throughput: " + targetThroughput.get(boltIndex) + " current throughput: " + currentBoltStats.get(boltIndex).getAckedRate());


        if(targetThroughput.get(boltIndex) > currentBoltStats.get(boltIndex).getAckedRate()) {

            if(initSpoutCheck(boltMap.get(boltIndex))) {
                for(String key : spoutMap.keySet())
                    needCheck.put(key, spoutMap.get(key));

                checkNeeded = true;
            }

        }
        else {
            killTopology = true;
            OutputWriter out = new OutputWriter(topologyName+"_hist");
            try {
                writer.write(conf, currentBoltStats.get(boltIndex).getAckedRate(), 0d, 0l);
                writer.write("Target reached: " + targetThroughput.get(boltIndex));
                out.write(previousConfigs, histThroughput, histLatency);
                killTopology();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            return;
        }


        //TODO if needed keep history stats when replacing in previous hashmap
        for (String key : spoutMap.keySet())
            previousSpoutStats.put(key, new SpoutMetrics(((SpoutMetricsUpdater)spoutMap.get(key).getNode().getComponentUpdater()).getSpoutMetrics()));

        for (String key : boltMap.keySet())
            previousBoltStats.put(key, new BoltMetrics( ((BoltMetricsUpdater)boltMap.get(key).getNode().getComponentUpdater()).getBoltMetrics() ));


        if(!checkNeeded)
            return;

        try {

            for (ComponentNode spout : needCheck.values())
                initBoltCheck(spout);


            if(changes == 0) {
                if (workers < maxWorkers) {

                    System.out.println("Added worker");
                    previousConfig.setWorkers(workers);
                    currentConfig.setWorkers(++workers);

                    System.out.println("Rebalance");
                    currentConfig.commitRebalance(topologyName);

                    rebalanced = true;
                } else
                    System.out.println("Cannot add more workers");
            }
            else if (changes > 0) {

                if ((executors + changes) > (cores * workers)) {
                    if(workers < maxWorkers) {

                        System.out.println("Added worker : " + workers + "-->" + (workers +1));
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
            else if (changes == -1) { //TODO better changes<0 cause we check multiple roots
                if(workers > 1) {

                    System.out.println("Reduced workers : " + workers + "-->" + (workers - 1));
                    previousConfig.setWorkers(workers);
                    currentConfig.setWorkers(--workers);

                    System.out.println("Rebalance");
                    currentConfig.commitRebalance(topologyName);

                    rebalanced = true;

                }
                else
                    System.out.println("Cannot reduce workers(Probable spout issue)");
            }

            if(rebalanced == true) {
                histThroughput.add(avgThroughput/count);
                histLatency.add(avgLatency/count);
            }

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }


    public int compareSpoutStats(BoltMetrics previous, BoltMetrics current) {
        int severity = 0;

        System.out.println("throughput: " + previous.getAckedRate() + "-->" + current.getAckedRate());

        avgThroughput += current.getAckedRate();
        count++;

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

        tempSeverity += compareSpoutStats(previousBoltStats.get(root.getNode().getComponentId()), currentBoltStats.get(root.getNode().getComponentId()));
        System.out.println("New severity: " + tempSeverity);

        if (tempSeverity < 0)
            tempSeverity = 0;

        severity.put(root.getNode().getComponentId(), tempSeverity);

        if (tempSeverity > maxSeverity)
            return true;

        return false;
    }




    public void initBoltCheck(ComponentNode root) {

        PriorityQueue<ComponentNode> queue = recursiveInspect(root);


        //iterate through the queue while the capacity is greater than 0.8
        ComponentNode bolt = queue.poll();
        BoltMetrics boltStats = ((BoltMetricsUpdater) bolt.getNode().getComponentUpdater()).getBoltMetrics();

        while(boltStats.getMaxCapacity() > 0.8 ) {

            if (!currentConfig.getComponents().containsKey(bolt.getNode().getComponentId())) {
                System.out.println(boltStats.getId() + " capacity: " + boltStats.getMaxCapacity());
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


        //if nothing has match capacity then we need worker or more spouts
        if(currentConfig.getComponents().isEmpty()) {
            System.out.println("No bottleneck found");
            double totalCapacity = 0;
            for (ComponentNode b : boltMap.values()) {
                boltStats = ((BoltMetricsUpdater) b.getNode().getComponentUpdater()).getBoltMetrics();

                totalCapacity += boltStats.getCapacity();
            }
            System.out.println("Total capacity : " + totalCapacity);

            if(totalCapacity < 0.6) {
                changes = -1;
                return;
            }
        }

    }


    public PriorityQueue<ComponentNode> recursiveInspect(ComponentNode root) {

        PriorityQueue<ComponentNode> pq = new PriorityQueue<ComponentNode>(11, new BoltMetricsComparator());

        Queue<ComponentNode> q = new LinkedList<>();
        Set<String> visited = new HashSet<>();

        for (ComponentNode neighbor : root.getNeighbors()) {
            if(!visited.contains(neighbor.getNode().getComponentId())) {
                q.add(neighbor);
                visited.add(neighbor.getNode().getComponentId());
            }
        }

        ComponentNode temp;
        while(!q.isEmpty()) {
            temp = q.poll();

            pq.add(temp);

            for (ComponentNode neighbor : temp.getNeighbors()) {
                if(!visited.contains(neighbor.getNode().getComponentId())) {
                    q.add(neighbor);
                    visited.add(neighbor.getNode().getComponentId());
                }
            }
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

        severity.put(boltIndex, 0);

        for (String key : spoutMap.keySet()) {
            previousSpoutStats.put(key, new SpoutMetrics(((SpoutMetricsUpdater)spoutMap.get(key).getNode().getComponentUpdater()).getSpoutMetrics()));
        }

        for (String key : boltMap.keySet())
            previousBoltStats.put(key, new BoltMetrics( ((BoltMetricsUpdater)boltMap.get(key).getNode().getComponentUpdater()).getBoltMetrics() ));

        moves++;
        previousConfig = currentConfig;
        currentConfig = new RebalanceMove();

        executors = countThreads();

        stop = System.currentTimeMillis();
        writer.write(conf, avgThroughput/count, avgLatency/count, stop-start);

        conf = new TopologyConfiguration(workers, previousSpoutStats, previousBoltStats);
        previousConfigs.add(conf);

        avgThroughput = 0d;
        avgLatency = 0d;
        count = 0;

        killTopology = false;

        rebalanced = false;

        checkNeeded = false;

        start = System.currentTimeMillis();
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

    public void killTopology() throws Exception {

        Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);

        client.killTopologyWithOpts(topologyName, opts);
    }

    public boolean isRebalanced() {
        return rebalanced;
    }

    public boolean isKilled() {
        return killTopology;
    }

    public void setRebalanced(boolean rebalanced) {
        this.rebalanced = rebalanced;
    }
}

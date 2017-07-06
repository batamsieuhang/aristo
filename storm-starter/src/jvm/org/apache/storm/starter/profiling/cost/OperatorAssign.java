package org.apache.storm.starter.profiling.cost;

import org.apache.storm.starter.profiling.Operator;

import java.util.Map;
import java.util.PriorityQueue;

/**
 * Created by VagelisAkis on 17/1/2017.
 */
public class OperatorAssign {

    private String name;
    private int threads;
    private Map<String, Integer> threadMap;
    private double executeLatency;
    private PriorityQueue<Container>  assign;
    private double di;

    public OperatorAssign(Operator op, int threads, Map<String, Integer> threadMap, double dinput, Cluster cluster) throws Exception{
        name = op.getName();
        this.threads = threads;
        this.threadMap = threadMap;

        executeLatency = op.getPredictor().getExecuteLatency(cluster.getWorkerNumber(),threads, op.getSources());
        System.out.println("Weka prediction for: " + name + " execute latency: " + executeLatency);

//        System.out.println(op.getIORatio());
        int destinations = 0;
        if(!op.getNeighbors().isEmpty()) {
            //TODO in simple topologies we batch them together, but if we have multiple different destinations do we put double the penalty?
            for(Operator o : op.getNeighbors())
                destinations += threadMap.get(o.getName());
            executeLatency += calculateDataTransferLatency(cluster.getWorkerNumber(), op.getIORatio(), destinations);
        }
        executeLatency /= 1000;

        System.out.println("Operator: " + name + " execute latency: " + executeLatency);

        di = dinput;

        assign = new PriorityQueue<>(threads, new ContainerComparator());
        for(int i=0;i<Math.min(threads, cluster.getWorkerNumber());i++)
            assign.add(cluster.getNextContainer());

    }

    public OperatorAssign(Operator op, int threads, Map<String, Integer> threadMap, Cluster cluster) throws Exception{
        name = op.getName();
        this.threads = threads;
        this.threadMap = threadMap;

        executeLatency = op.getPredictor().getSpoutExecuteLatency();

        int destinations = 0;
        if(!op.getNeighbors().isEmpty()) {
            //TODO in simple topologies we batch them together, but if we have multiple different destinations do we put double the penalty?
            for(Operator o : op.getNeighbors())
                destinations += threadMap.get(o.getName());

            executeLatency += calculateDataTransferLatency(cluster.getWorkerNumber(), destinations, op.getPredictor().getSpoutTransferLatency());
        }
        executeLatency /= 1000;

        System.out.println("Operator: " + name + " execute latency: " + executeLatency);

        di = 1;

        assign = new PriorityQueue<>(threads, new ContainerComparator());
        for(int i=0;i<Math.min(threads, cluster.getWorkerNumber());i++)
            assign.add(cluster.getNextContainer());
    }

    public OperatorAssign(String name, int threads, double el, double dinput, Cluster cluster) {
        this.name = name;
        this.threads = threads;
        executeLatency = el;
        di = dinput;

        if(name.equals("FastRandomSentenceSpout"))
            executeLatency = 0.0000091;
        if(name.equals("MatrixSizeGeneratorSpout"))
            executeLatency = 0.0000076;
//        System.out.println("Operator: " + name + " execute latency: " + executeLatency);

        assign = new PriorityQueue<>(threads, new ContainerComparator());
        for(int i=0;i<Math.min(threads, cluster.getWorkerNumber());i++)
            assign.add(cluster.getNextContainer());
    }

    private Double calculateDataTransferLatency(int workers, double IORatio, int destinations) {
        //penalty = emitDelay + routingDelay*differentDestinations
        //total penalty = penalty*emits
//        double penalty = 0.0027 + 0.0002*Math.min(destinations, Math.max(workers-threads, 0)); //WC
//        double penalty = 0.005 + 0.0005*Math.min(destinations, Math.max(workers-threads, 0)); //MX
//        double penalty = 0.0022 + 0.0002*Math.min(destinations, Math.max(workers-threads, 0)); //CS

        double penalty = calculateNetwork(workers, destinations);

//        penalty += 0.000015*threads*destinations;

        if(workers > 1)
//            return penalty / IORatio; //not good for CS(we already calculate in calculate network multiple destinations)
        return penalty;
        else
            return 0d;
    }

    private Double calculateDataTransferLatency(int workers, int destinations, double transferLatency) {

//        double penalty = transferLatency*(1 + Math.min(destinations, Math.max(workers-threads, 0)));
        double penalty = calculateNetwork(workers, destinations, transferLatency);

        if(workers > 1)
            return penalty;
        else
            return 0d;
    }

    private Double calculateNetwork(int workers, int destinations) {

        //per executor thread
        int externalStreams = destinations-(destinations/workers);
        System.out.println(externalStreams);

        int locality = Math.min(destinations, Math.max(workers-threads, 0));
        System.out.println(locality);

//        double baseTime = 0.002; //WC
//        double streamPenalty = 0.0001*externalStreams + 0.0004*locality; //WC
//        double baseTime = 0.005; //MX
//        double streamPenalty = 0.0005*externalStreams + 0.0015*locality; //MX
        double baseTime = 0.003; //CS
        double streamPenalty = 0.001*externalStreams+ 0.00075*locality; //CS

        double penalty = baseTime + streamPenalty;

        System.out.println(penalty);

        return penalty;
    }

    private Double calculateNetwork(int workers, int destinations, double latency) {

        //per executor thread
        int externalStreams = destinations-(destinations/workers);
        System.out.println(externalStreams);

        int locality = Math.min(destinations, Math.max(workers-threads, 0));
        System.out.println(locality);

//        double baseTime = latency; //WC
//        double streamPenalty = 0.0005*externalStreams + 0.001*locality; //WC
//        double baseTime = latency; //MX
//        double streamPenalty = 0.001*externalStreams + 0.001*locality; //MX
        double baseTime = latency; //CS
        double streamPenalty = 0.00025*externalStreams + 0.00025*locality; //CS

        double penalty = baseTime + streamPenalty;

        System.out.println(penalty);

        return penalty;
    }

    private void refreshAssign() {
        PriorityQueue temp = new PriorityQueue<>(threads, new ContainerComparator());
        for(Container c : assign)
            temp.add(c);
        assign = temp;
    }

    public void executeStep() {
        double input = di;
        double inputStep;
        double dt;
        Container container;
        while(input > 0) {
            refreshAssign();
//            System.out.println("di = " + input);
            inputStep = Math.min(1, input--);
            dt = inputStep * executeLatency;
//            System.out.println("dt = " + dt);

            container = assign.poll();
//            System.out.println("operator: " + this.name +" --> container: " + container.getId());
//            System.out.println("watermark: " + container.getWatermark() + " --> " + (container.getWatermark() + dt));
            container.increaseWatermark(dt);

            container.addTupleCount(name, inputStep);
            assign.add(container);
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public double getExecuteLatency() {
        return executeLatency;
    }

    public void setExecuteLatency(double executeLatency) {
        this.executeLatency = executeLatency;
    }

    public PriorityQueue<Container> getAssign() {
        return assign;
    }

    public void setAssign(PriorityQueue<Container> assign) {
        this.assign = assign;
    }

    public double getDi() {
        return di;
    }

    public void setDi(double di) {
        this.di = di;
    }

    public static void main(String args[]) {
        Cluster c = new Cluster(4);
        PriorityQueue<Container> pq = new PriorityQueue<>(11, new ContainerComparator());

        Container con;
        for(int i=0;i<4;i++) {
            con = c.getNextContainer();
            con.setWatermark(i);
            pq.add(con);
        }

        con = pq.poll();
        System.out.println(con.getWatermark());
        con.setWatermark(10);
        pq.add(con);
        con = pq.peek();
        System.out.println(con.getWatermark());
    }
}

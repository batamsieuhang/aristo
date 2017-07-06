package org.apache.storm.starter.profiling.cost;

import org.apache.storm.starter.profiling.Operator;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Created by VagelisAkis on 17/1/2017.
 */
public class OperatorAssignv2 {

    private String name;
    private int threads;
    private Map<String, Integer> threadMap;
    private double executeLatency;
    private PriorityQueue<Container>  assign;
    private double di;
    private Operator op;
    private boolean isSpout;
    private int workers;
    private Cluster cluster;

    public OperatorAssignv2(Operator op, int threads, Map<String, Integer> threadMap, double dinput, Cluster cluster) throws Exception{
        name = op.getName();
        this.threads = threads;
        this.threadMap = threadMap;
        this.op = op;
        this.cluster = cluster;
        workers = cluster.getWorkerNumber();
        isSpout = false;

        di = dinput;

        assign = new PriorityQueue<>(threads, new ContainerComparator());
        Container c;
        for(int i=0;i<Math.min(threads, cluster.getWorkerNumber());i++) {
            c = cluster.getNextContainer();
            c.assignOperator(op.getName());
            assign.add(c);
        }

    }

    public OperatorAssignv2(Operator op, int threads, Map<String, Integer> threadMap, Cluster cluster) throws Exception{
        name = op.getName();
        this.threads = threads;
        this.threadMap = threadMap;
        this.op = op;
        this.cluster = cluster;
        workers = cluster.getWorkerNumber();
        isSpout = true;

        di = 2;

        assign = new PriorityQueue<>(threads, new ContainerComparator());
        Container c;
        for(int i=0;i<Math.min(threads, cluster.getWorkerNumber());i++) {
            c = cluster.getNextContainer();
            c.assignOperator(op.getName());
            assign.add(c);
        }
    }

    public void calculateLatency() throws Exception{
        if(isSpout) {
            executeLatency = op.getPredictor().getSpoutExecuteLatency();

            int destinations = 0;
            if(!op.getNeighbors().isEmpty()) {
                for(Operator o : op.getNeighbors())
                    destinations += threadMap.get(o.getName());

                executeLatency += calculateDataTransferLatency(workers, destinations, op.getPredictor().getSpoutTransferLatency());
            }
            executeLatency /= 1000;

            System.out.println("Operator: " + name + " execute latency: " + executeLatency);
        }
        else {
            executeLatency = op.getPredictor().getExecuteLatency(workers, threads, op.getSources());
            System.out.println("Weka prediction for: " + name + " execute latency: " + executeLatency);

//        System.out.println(op.getIORatio());
            int destinations = 0;
            if (!op.getNeighbors().isEmpty()) {
                for (Operator o : op.getNeighbors())
                    destinations += threadMap.get(o.getName());
                executeLatency += calculateDataTransferLatency(workers, op.getIORatio(), destinations);
            }
            executeLatency /= 1000;

            System.out.println("Operator: " + name + " execute latency: " + executeLatency);
        }
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
//        int externalStreams = destinations-(destinations/workers); //needs check
//        System.out.println(externalStreams);

        int external = 0;
        int local = 0;
        double locality = 0;
        double externalStreams = 0;
        if (!op.getNeighbors().isEmpty()) {
            for (Operator o : op.getNeighbors()) {
                local = 0;
                external = 0;
                for(Container c : cluster.getContainers().values()) {
                    if(c.checkAssign(o.getName()) && c.checkAssign(name)) {
                        local++;
                        external += Math.min(threads-1, workers-1);
                    }
                    else if(c.checkAssign(o.getName()) && !c.checkAssign(name)) {
                        external += threads;
                    }
                    else if(!c.checkAssign(o.getName()) && c.checkAssign(name)) {
                        local--;
                    }
                }
                System.out.println(name + " " + o.getName() + " external: " + external + " local: " + local);
                externalStreams += (double)external/threads;
                locality += local;
            }
            locality /= op.getNeighbors().size();
            externalStreams /= op.getNeighbors().size();
//            externalStreams = Math.min(externalStreams, workers-1);
        }


        System.out.println(externalStreams);
        System.out.println(locality);

//        double baseTime = 0.002; //WC
//        double streamPenalty = 0.0001*externalStreams + 0.0004*locality; //WC
//        double baseTime = 0.005; //MX
//        double streamPenalty = 0.0005*externalStreams + 0.0015*locality; //MX

        /*double baseTime = 0.0032; //CS
//        double streamPenalty =  0.0006*externalStreams - 0.00085*locality; //CS

//        double penalty = baseTime + streamPenalty;
        double penalty;
        if(locality > 0)
            penalty = baseTime + 0.0023*externalStreams - 0.0014*locality;
        else
            penalty = baseTime + 0.0024*externalStreams - 0.0009*locality;

        penalty = penalty<0.0055 ? 0.0055 : penalty;*/ //CS

        double penalty = 0;
        if(name.equals("MontageAggregator")) {
            double baseTime = 0.0006; //MG

            /*if (locality > 0)
                penalty = baseTime + 0.0012 * externalStreams - 0.0006 * locality;
            else
                penalty = baseTime + 0.0006 * externalStreams - 0.0006 * locality;*/

            if (locality > 0)
                penalty = baseTime + 0.0010 * externalStreams - 0.0007 * locality;
            else
                penalty = baseTime + 0.0005 * externalStreams - 0.0004 * locality;

            penalty = penalty < 0.0008 ? 0.0008 : penalty;

            //for starvation problems
//            penalty = threads == 1 && external > 1 ? 0.0025 : penalty;
//            penalty = 0.0013;
        }
        if(name.equals("MontageGeneral")) {
            double baseTime = 0.0012; //MG
//        double streamPenalty =  0.0006*externalStreams - 0.00085*locality; //MG

//        double penalty = baseTime + streamPenalty;
//            double penalty;
            if (locality > 0)
                //penalty = baseTime + 0.0023 * externalStreams - 0.0014 * locality;
                penalty = baseTime + 0.0012 * externalStreams - 0.0009 * locality;
            else
                //penalty = baseTime + 0.0008 * externalStreams - 0.0008 * locality;
                penalty = baseTime + 0.0005 * externalStreams - 0.0008 * locality;

            penalty = penalty < 0.002 ? 0.002 : penalty;

            //for starvation problems
//            penalty = threads == 1 && external > 1 ? 0.004 : penalty;
//            penalty = penalty > 0.002 ? 0.002 : penalty;
//            penalty = 0.0036;
        }

        System.out.println(penalty);

        return penalty;
    }

    private Double calculateNetwork(int workers, int destinations, double latency) {

        //per executor thread
//        int externalStreams = destinations-(destinations/workers);
//        System.out.println(externalStreams);

        int external = 0;
        int local = 0;
        double locality = 0;
        double externalStreams = 0;
        if (!op.getNeighbors().isEmpty()) {
            for (Operator o : op.getNeighbors()) {
                local = 0;
                external = 0;
                for(Container c : cluster.getContainers().values()) {
                    if(c.checkAssign(o.getName()) && c.checkAssign(name)) {
                        local++;
                        external += Math.min(threads-1, workers-1);
                    }
                    else if(c.checkAssign(o.getName()) && !c.checkAssign(name)) {
                        external += threads;
                    }
                    else if(!c.checkAssign(o.getName()) && c.checkAssign(name)) {
                        local--;
                    }
                }
                System.out.println(name + " " + o.getName() + " external: " + external + " local: " + local);
                externalStreams += (double)external/threads;
                locality += local;
            }
            locality /= op.getNeighbors().size();
            externalStreams /= op.getNeighbors().size();
//            externalStreams = Math.min(externalStreams, workers-1);
        }

        System.out.println(externalStreams);
        System.out.println(locality);

//        double baseTime = latency; //WC
//        double streamPenalty = 0.0005*externalStreams + 0.001*locality; //WC
//        double baseTime = latency; //MX
//        double streamPenalty = 0.001*externalStreams + 0.001*locality; //MX
        /*double baseTime = 0.0006;//latency; //CS
//        double streamPenalty = 0.0001*externalStreams - 0.00015*locality; //CS

//        double penalty = baseTime + streamPenalty;

        double penalty;
        if(locality > 0)
            penalty = baseTime + 0.0002*externalStreams - 0.00005*locality;
        else
            penalty = baseTime + 0.0001*externalStreams - 0.0001*locality;*/ //CS

        double baseTime = 0.0008;//latency; //MG
//        double streamPenalty = 0.0001*externalStreams - 0.00015*locality; //MG

//        double penalty = baseTime + streamPenalty;

        double penalty;
        if(locality > 0)
            penalty = baseTime + 0.0002*externalStreams - 0.00005*locality;
        else
            penalty = baseTime + 0.0002*externalStreams - 0.0001*locality;

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

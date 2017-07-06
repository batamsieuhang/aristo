package org.apache.storm.starter.profiling.agnostic;

import org.apache.storm.starter.profiling.Operator;

import java.util.Map;

/**
 * Created by vgol on 10/3/2017.
 */
public class PoolAssign {

    private Pool pool;
    private Pool global;
    private String name;
    private int threads;
    private Map<String, Integer> threadMap;
    private double executeLatency;
    private double di;
    private Operator op;
    private boolean isSpout;
    private int workers;

    public PoolAssign(Operator op, int threads, Map<String, Integer> threadMap, double dinput, int workers, Pool global) throws Exception{

        name = op.getName();
        this.op = op;
        this.threads = threads;
        this.threadMap = threadMap;
        this.workers = workers;

        isSpout = false;

        di = dinput;

        pool = new Pool(this.name, this.threads, this.workers);
        this.global = global;

    }

    public PoolAssign(Operator op, int threads, Map<String, Integer> threadMap, int workers, Pool global) throws Exception{

        name = op.getName();
        this.op = op;
        this.threads = threads;
        this.threadMap = threadMap;
        this.workers = workers;

        isSpout = true;

        di = 1;

        pool = new Pool(this.name, this.threads, this.workers);
        this.global = global;

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

    public void executeStep() {
        double input = di;
        double inputStep;
        double dt;
        while(input > 0) {
//            System.out.println("di = " + input);
            inputStep = Math.min(1, input--);
            dt = inputStep * executeLatency;
//            System.out.println("dt = " + dt);

            global.increaseWatermark(dt);
            pool.increaseWatermark(dt);
        }
    }

    public Pool getPool() {
        return pool;
    }

    public void setPool(Pool pool) {
        this.pool = pool;
    }
}

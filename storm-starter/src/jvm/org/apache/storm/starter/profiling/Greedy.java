package org.apache.storm.starter.profiling;

import org.apache.storm.shade.org.apache.zookeeper.Op;
import org.apache.storm.shade.org.eclipse.jetty.util.security.Constraint;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class Greedy {

    private GraphParser graph;
    private Constraints constraints;

    public Greedy(Constraints constraints) throws Exception{
        graph = new GraphParser();
        this.constraints = constraints;
    }

    public Solution parallelizeTopology() throws Exception{
        int workers = 0;
        Solution solution = null;

        for(String spout : constraints.getThroughput().keySet())
            graph.getSpoutMap().get(spout).setTargetThroughput(constraints.getThroughput().get(spout));

        while(constraints.hasNext()) {
            workers = constraints.getNext();

            if(constraints.getThroughput().isEmpty())
                solution = maximizeThroughput(workers);
            else
                solution = limitedThroughput(workers);

            if(solution.getNotCompleteFlag() == 0)
                break;
        }

        return solution;
    }

    public Solution limitedThroughput(Integer workers) throws Exception{
        for(Operator op: graph.getBoltMap().values())
            op.setTargetThroughput(0d);

        Solution solution = new Solution(workers);

        Double totalTime = workers.doubleValue();

        System.out.println("Total time:" + totalTime);

        Queue<Operator> queue = new LinkedList<>();
        for(Operator root : graph.getSpoutMap().values()) {
            int threads = (int) Math.ceil(root.getTargetThroughput() / root.getPredictor().getSpoutEmitRate());
            solution.addThread(root.getName(), threads);

            for(Operator neighbor : root.getNeighbors()) {
                neighbor.addTargetThroughput(root.getTargetThroughput() / root.getNeighbors().size());
                neighbor.setSources(threads);
                queue.add(neighbor);
            }
        }

        Operator temp;
        MetricsPredictor predictor;
        double operatorExecuteLatency;
        double availableTime;
        double requiredTime;
        double emitRate;
        int executors;
        double transferTime = 0.002;

        while(!queue.isEmpty()) {
            temp = queue.poll();
            predictor = temp.getPredictor();

            System.out.println(temp.getName());

            for(executors = 1; executors <= workers; executors++) {
                if(executors > workers)
                    availableTime = workers;
                else
                    availableTime = executors;

                operatorExecuteLatency = predictor.getExecuteLatency(workers, executors, temp.getSources());

                System.out.println("Prediction:" + operatorExecuteLatency);

                requiredTime = (operatorExecuteLatency * temp.getTargetThroughput()) / 1000; //add IORatio*penalty for data transfer + context switch


                if(workers > 1 && !temp.getNeighbors().isEmpty())
                    requiredTime += ((transferTime / temp.getIORatio()) * temp.getTargetThroughput()) / 1000;

                System.out.println("Required time:" + requiredTime);

                if(requiredTime <= availableTime) {
                    totalTime -= requiredTime;
                    solution.addThread(temp.getName(), executors);
                    break;
                }

            }

            if(!solution.getThreads().containsKey(temp.getName())) {
                solution.setNotCompleteFlag(1);
                return solution;
            }

            if(totalTime < 0) {
                solution.setNotCompleteFlag(1);
                solution.setRemainingTime(totalTime);
                return solution;
            }


            emitRate = temp.getTargetThroughput() / temp.getIORatio();
            System.out.println("Emit rate:" + emitRate);
            for(Operator neighbor : temp.getNeighbors()) {
                neighbor.addTargetThroughput(emitRate / temp.getNeighbors().size());
                neighbor.setSources(executors);
                queue.add(neighbor);
            }


        }

        solution.setRemainingTime(totalTime);

        return solution;
    }

    public Solution maximizeThroughput(Integer workers) {
        Solution solution = new Solution(workers);

        Double totalTime = workers.doubleValue();

        System.out.println("Total time:" + totalTime);

        Double totalLatency = 0d;



        return solution;
    }

    public GraphParser getGraph() {
        return graph;
    }

    public Constraints getConstraints() {
        return constraints;
    }


    public static void main(String args[]) {
        Constraints c = new Constraints(4);
        c.setConstraintWorkers(4);
        c.addThroughput("FastRandomSentenceSpout", 10000d);
//        c.addThroughput("MatrixSizeGeneratorSpout", 80000d);

        try {
            Greedy g = new Greedy(c);
            Solution s = g.parallelizeTopology();
            s.print();

//            ThroughputCalculator tc = new ThroughputCalculator();
//            System.out.println(tc.calculateMaxThroughput(g.getGraph().getSpoutMap().get("FastRandomSentenceSpout"), 1));


        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}

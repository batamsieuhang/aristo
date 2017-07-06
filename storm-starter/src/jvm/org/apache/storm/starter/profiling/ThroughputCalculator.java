package org.apache.storm.starter.profiling;

import org.apache.storm.starter.frontend.SystemParameters;
import org.apache.storm.starter.profiling.agnostic.Pool;
import org.apache.storm.starter.profiling.agnostic.PoolAssign;
import org.apache.storm.starter.profiling.cost.Cluster;
import org.apache.storm.starter.profiling.cost.OperatorAssign;
import org.apache.storm.starter.profiling.cost.OperatorAssignv2;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

public class ThroughputCalculator {



    public ThroughputCalculator(){}

    public Double calculateMaxThroughput(Operator root, Integer workers) throws Exception{
        int totalTime = workers;

        double totalLatency = 0;
        Map<String, Double> inputRatioMap = new HashMap<>();
        Queue<Operator> queue = new LinkedList<>();

        for(Operator neighbor : root.getNeighbors())
            queue.add(neighbor);

        Operator temp;
        double executeLatency;
        double outputRatio;
        double inputRatio;
        double kappa;
        while(!queue.isEmpty()) {
            temp = queue.poll();
            executeLatency = temp.getPredictor().getExecuteLatency(workers, 1, 1);

            if(inputRatioMap.containsKey(temp.getName()))
                executeLatency *= inputRatioMap.get(temp.getName());

            if(!temp.getNeighbors().isEmpty())
                executeLatency += calculateDataTransferLatency(workers, temp.getIORatio());

            totalLatency += executeLatency;

            outputRatio = 1/temp.getIORatio();
            for(Operator neighbor : temp.getNeighbors()) {
                kappa = outputRatio / temp.getNeighbors().size();

                if(inputRatioMap.containsKey(neighbor.getName()))
                    inputRatio = inputRatioMap.get(neighbor.getName());
                else
                    inputRatio = 0;
                inputRatio += kappa;
                inputRatioMap.put(neighbor.getName(), inputRatio);

                queue.add(neighbor);
            }
        }

        return (totalTime / totalLatency) * 1000;
    }

    private Double calculateDataTransferLatency(int workers, double IORatio) {
        double penalty = 0.0025;

        if(workers > 1)
            return penalty / IORatio;
        else
            return 0d;

    }


    //TODO complete the iteration and do a binary search for throughput values
    public Double calculateThroughput(Operator root, Solution solution) throws Exception{
        int totalTime = solution.getWorkers();
        int workers = solution.getWorkers();

        Double throughput = calculateMaxThroughput(root, workers);

        Map<String, Double> inputMap = new HashMap<>();
        Map<String, Integer> sourcesMap = new HashMap<>();
        Queue<Operator> queue = new LinkedList<>();

        for(Operator neighbor : root.getNeighbors()) {
            inputMap.put(neighbor.getName(), throughput / root.getNeighbors().size());
            sourcesMap.put(neighbor.getName(), solution.getOperatorThreads(root.getName()));
            queue.add(neighbor);
        }

        Operator temp;
        double executeLatency;
        double availableTime;
        double requiredTime;
        double outputRatio;
        int sources;
        int threads;
        double inputThroughput;
        while(!queue.isEmpty()) {
            temp = queue.poll();
            threads = solution.getOperatorThreads(temp.getName());
            sources = sourcesMap.get(temp.getName());
            inputThroughput = inputMap.get(temp.getName());
            if (threads > workers)
                availableTime = workers;
            else
                availableTime = threads;

            executeLatency = temp.getPredictor().getExecuteLatency(workers, threads, sources);
            if (!temp.getNeighbors().isEmpty())
                executeLatency += calculateDataTransferLatency(workers, temp.getIORatio());

            requiredTime = executeLatency * inputThroughput;
        }

        return throughput;
    }

    public Double calculateThroughputAgnostic(Map<String,Operator> roots, Solution solution) throws Exception {

        List<PoolAssign> operators = new ArrayList<>();

        Map<String, Double> inputMap = new HashMap<>();
        Queue<Operator> queue = new LinkedList<>();

        int workers = solution.getWorkers();

        Pool global = new Pool(workers);

        int operatorCount = 0;
        PoolAssign assign;
        Operator temp;
        int threads;
        double input;
        double output;
        double tempin;

        for(Operator root : roots.values()) {
            assign = new PoolAssign(root, solution.getOperatorThreads(root.getName()), solution.getThreads(), workers, global);
            operators.add(operatorCount++, assign);

            for (Operator neighbor : root.getNeighbors()) {
                if(!inputMap.containsKey(neighbor.getName()))
                    tempin = 0d;
                else
                    tempin = inputMap.get(neighbor.getName());

                tempin += 1d / root.getNeighbors().size();
                inputMap.put(neighbor.getName(), tempin);

                neighbor.setSources(solution.getOperatorThreads(root.getName()));
                queue.add(neighbor);
            }
        }

        while(!queue.isEmpty()) {
            temp = queue.poll();
            threads = solution.getOperatorThreads(temp.getName());
            input = inputMap.get(temp.getName());

            assign = new PoolAssign(temp, threads, solution.getThreads(), input, workers, global);
            operators.add(operatorCount++, assign);

            output = input / temp.getIORatio();
            for(Operator neighbor : temp.getNeighbors()) {
                if(!inputMap.containsKey(neighbor.getName()))
                    tempin = 0d;
                else
                    tempin = inputMap.get(neighbor.getName());

                tempin += output / temp.getNeighbors().size();
                inputMap.put(neighbor.getName(), tempin);


                neighbor.setSources(solution.getOperatorThreads(temp.getName()));
                queue.add(neighbor);

            }
        }

        int i;
        double throughput = 0;

        for(i=0; i<operatorCount; i++)
            operators.get(i).calculateLatency();

        while(true) {
            try{
                for(i=0; i<operatorCount; i++)
                    operators.get(i).executeStep();

                throughput++;
            }
            catch(IllegalStateException e) {
//                System.out.println(e.getMessage());
                break;
            }
        }

        global.printStatus();
        for(PoolAssign p : operators)
            p.getPool().printStatus();

        return throughput;
    }


    public Double calculateThroughputPrecise(Map<String,Operator> roots, Solution solution) throws  Exception{
        Cluster cluster = new Cluster(solution.getWorkers());

        Map<String, Double> inputMap = new HashMap<>();
        Queue<Operator> queue = new LinkedList<>();

        List<OperatorAssign> operators = new ArrayList<>();

        //additional for spout
        int operatorCount = 0;
        OperatorAssign assign;
        Operator temp;
        int threads;
        double input;
        double output;
        double tempin;

        for(Operator root : roots.values()) {
            assign = new OperatorAssign(root, solution.getOperatorThreads(root.getName()), solution.getThreads(), cluster);
            operators.add(operatorCount++, assign);

            for (Operator neighbor : root.getNeighbors()) {
                if(!inputMap.containsKey(neighbor.getName()))
                    tempin = 0d;
                else
                    tempin = inputMap.get(neighbor.getName());

                tempin += 1d / root.getNeighbors().size();
                inputMap.put(neighbor.getName(), tempin);

                neighbor.setSources(solution.getOperatorThreads(root.getName()));
                queue.add(neighbor);
            }
        }

        while(!queue.isEmpty()) {
            temp = queue.poll();
            threads = solution.getOperatorThreads(temp.getName());
            input = inputMap.get(temp.getName());

            assign = new OperatorAssign(temp, threads, solution.getThreads(), input, cluster);
            operators.add(operatorCount++, assign);

            output = input / temp.getIORatio();
            for(Operator neighbor : temp.getNeighbors()) {
                if(!inputMap.containsKey(neighbor.getName()))
                    tempin = 0d;
                else
                    tempin = inputMap.get(neighbor.getName());

                tempin += output / temp.getNeighbors().size();
                inputMap.put(neighbor.getName(), tempin);


                neighbor.setSources(solution.getOperatorThreads(temp.getName()));
                queue.add(neighbor);

            }
        }

        int i;
        double throughput = 0;

        while(true) {
            try{
                for(i=0; i<operatorCount; i++)
                    operators.get(i).executeStep();

                throughput++;
            }
            catch(IllegalStateException e) {
//                System.out.println(e.getMessage());
                break;
            }
        }

        cluster.printStatusVerbose();

        return throughput;
    }


    public Double calculateThroughputPrecise(Operator root, Solution solution) throws  Exception{
        Cluster cluster = new Cluster(solution.getWorkers());

        Map<String, Double> inputMap = new HashMap<>();
        Queue<Operator> queue = new LinkedList<>();

        List<OperatorAssign> operators = new ArrayList<>();

        //additional for spout
        int operatorCount = 0;
        OperatorAssign assign;
//        assign = new OperatorAssign(root.getName(), solution.getOperatorThreads(root.getName()), 0.0091/1000, 1d, cluster);
        assign = new OperatorAssign(root, solution.getOperatorThreads(root.getName()), solution.getThreads(), cluster);
        operators.add(operatorCount++, assign);

        for(Operator neighbor : root.getNeighbors()) {
            inputMap.put(neighbor.getName(), 1d / root.getNeighbors().size());
            neighbor.setSources(solution.getOperatorThreads(root.getName()));
            queue.add(neighbor);
        }

//        int operatorCount = 0;
        Operator temp;
        int threads;
        double input;
        double output;
        double tempin;
//        OperatorAssign assign;
        while(!queue.isEmpty()) {
            temp = queue.poll();
            threads = solution.getOperatorThreads(temp.getName());
            input = inputMap.get(temp.getName());

            assign = new OperatorAssign(temp, threads, solution.getThreads(), input, cluster);
            operators.add(operatorCount++, assign);

            output = input / temp.getIORatio();
            for(Operator neighbor : temp.getNeighbors()) {
                if(!inputMap.containsKey(neighbor.getName()))
                    tempin = 0d;
                else
                    tempin = inputMap.get(neighbor.getName());

                tempin += output / temp.getNeighbors().size();
                inputMap.put(neighbor.getName(), tempin);


                neighbor.setSources(solution.getOperatorThreads(temp.getName()));
                queue.add(neighbor);

            }
        }

        int i;
        double throughput = 0;

        while(true) {
            try{
                for(i=0; i<operatorCount; i++)
                    operators.get(i).executeStep();

                throughput++;
            }
            catch(IllegalStateException e) {
//                System.out.println(e.getMessage());
                break;
            }
        }

        cluster.printStatusVerbose();

        return throughput;
    }

    public Double calculateThroughputPrecisev2(Operator root, Solution solution) throws  Exception{
        Cluster cluster = new Cluster(solution.getWorkers());

        Map<String, Double> inputMap = new HashMap<>();
        Queue<Operator> queue = new LinkedList<>();
        Map<String, Integer> visited = new HashMap<>();

        List<OperatorAssignv2> operators = new ArrayList<>();

        //additional for spout
        int operatorCount = 0;
        OperatorAssignv2 assign;
//        assign = new OperatorAssign(root.getName(), solution.getOperatorThreads(root.getName()), 0.0091/1000, 1d, cluster);
        assign = new OperatorAssignv2(root, solution.getOperatorThreads(root.getName()), solution.getThreads(), cluster);
        operators.add(operatorCount++, assign);

        for(Operator neighbor : root.getNeighbors()) {
            inputMap.put(neighbor.getName(), 2d / root.getNeighbors().size());
            neighbor.setSources(solution.getOperatorThreads(root.getName()));
            queue.add(neighbor);
            visited.put(neighbor.getName(), 1);
        }

//        int operatorCount = 0;
        Operator temp;
        int threads;
        double input;
        double output;
        double tempin;
//        OperatorAssign assign;
        while(!queue.isEmpty()) {
            temp = queue.poll();
            threads = solution.getOperatorThreads(temp.getName());
            input = inputMap.get(temp.getName());

            assign = new OperatorAssignv2(temp, threads, solution.getThreads(), input, cluster);
            operators.add(operatorCount++, assign);

            output = input / temp.getIORatio();
            for(Operator neighbor : temp.getNeighbors()) {
                if(!inputMap.containsKey(neighbor.getName()))
                    tempin = 0d;
                else
                    tempin = inputMap.get(neighbor.getName());

                tempin += output / temp.getNeighbors().size();
                inputMap.put(neighbor.getName(), tempin);

                neighbor.setSources(solution.getOperatorThreads(temp.getName()));
                if(visited.containsKey(neighbor.getName()))
                    continue;

                queue.add(neighbor);
                visited.put(neighbor.getName(), 1);
            }
        }

        int i;
        double throughput = 0;

        for(i=0; i<operatorCount; i++)
            operators.get(i).calculateLatency();

        while(true) {
            try{
                for(i=0; i<operatorCount; i++)
                    operators.get(i).executeStep();

                throughput++;
            }
            catch(IllegalStateException e) {
//                System.out.println(e.getMessage());
                break;
            }
        }

        cluster.printStatusVerbose();

        return throughput;
    }


    public static void main(String args[]) throws Exception{
        GraphParser graph = new GraphParser();

        Solution solution = new Solution(7);

//        solution.addThread("FastRandomSentenceSpout", 1);
//        solution.addThread("SplitSentence", 3);
//        solution.addThread("WordCount", 3);

//        solution.addThread("MatrixSizeGeneratorSpout", 1);
//        solution.addThread("MatrixCreatorBolt", 1);
//        solution.addThread("MatrixInverterBolt", 1);

//        solution.addThread("CyberSpout", 2);
//        solution.addThread("CyberDistribute", 2);
//        solution.addThread("CyberGeneral", 6);
//        solution.addThread("CyberGeneral2", 6);

        solution.addThread("MontageSpout", 3);
        solution.addThread("MontageGeneral", 2);
        solution.addThread("MontageAggregator", 3);
        solution.addThread("MontageGeneral2", 1);

        ThroughputCalculator tc = new ThroughputCalculator();
//        System.out.println(tc.calculateThroughputPrecise(graph.getSpoutMap(), solution));

//        System.out.println(tc.calculateThroughputPrecise(graph.getSpoutMap().get("FastRandomSentenceSpout"), solution));
//        System.out.println(tc.calculateThroughputPrecise(graph.getSpoutMap().get("MatrixSizeGeneratorSpout"), solution));
//        System.out.println(tc.calculateThroughputPrecisev2(graph.getSpoutMap().get("CyberSpout"), solution));
        System.out.println(tc.calculateThroughputPrecisev2(graph.getSpoutMap().get("MontageSpout"), solution));

//        Agnostic calculation
//        System.out.println(tc.calculateThroughputAgnostic(graph.getSpoutMap(), solution));


       /* File file = new File("WCEvaluation5.txt");

        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        String comma = ",";

        for(int w = 1; w <= 7; w++) {
            for (int i = 1; i <= w; i++) {
                for (int j = 1; j <= w; j++) {
                    for (int k = 1; k <= w; k++) {
                        solution = new Solution(w);
                        solution.addThread("FastRandomSentenceSpout", i);
                        solution.addThread("SplitSentence", j);
                        solution.addThread("WordCount", k);
                        writer.println(w + comma + i + comma + j + comma + k + " : " + tc.calculateThroughputPrecise(graph.getSpoutMap().get("FastRandomSentenceSpout"), solution));
                    }
                }
            }
        }
        writer.close();*/


        /*File file = new File("MXEvaluation5.txt");

        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        String comma = ",";

        for(int w = 1; w <= 4; w++) {
            for (int i = 1; i <= w; i++) {
                for (int j = 1; j <= w; j++) {
                    for (int k = 1; k <= w; k++) {
                        solution = new Solution(w);
                        solution.addThread("MatrixSizeGeneratorSpout", i);
                        solution.addThread("MatrixCreatorBolt", j);
                        solution.addThread("MatrixInverterBolt", k);
                        writer.println(w + comma + i + comma + j + comma + k + " : " + tc.calculateThroughputPrecise(graph.getSpoutMap().get("MatrixSizeGeneratorSpout"), solution));
                    }
                }
            }
        }
        writer.close();*/

        /*File file = new File("CSEvaluation13_5_2.txt");

        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        String comma = ",";

        for(int w = 5; w <= 5; w++) {
            for (int i = 4; i <= w; i++) {
                for (int j = 1; j <= w; j++) {
                    for (int k = 1; k <= w; k++) {
//                        for (int l = 1; l <= w; l++) {
                            solution = new Solution(w);
                            solution.addThread("CyberSpout", i);
                            solution.addThread("CyberDistribute", j);
                            solution.addThread("CyberGeneral", k);
                            solution.addThread("CyberGeneral2", k);
                            writer.println(w + comma + i + comma + j + comma + k + comma + k + " : " + tc.calculateThroughputPrecisev2(graph.getSpoutMap().get("CyberSpout"), solution));
//                        }
                    }
                }
            }
        }
        writer.close();*/

        /*File file = new File("MGEvaluation15_567.txt");

        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        String comma = ",";

        for(int w = 7; w <= 7; w++) {
            for (int i = 4; i <= 4; i++) {
                for (int j = 1; j <= 1; j++) {
                    for (int k = 1; k <= w; k++) {
                        for (int l = 1; l <= k; l++) {
                            solution = new Solution(w);
                            solution.addThread("MontageSpout", i);
                            solution.addThread("MontageGeneral", j);
                            solution.addThread("MontageAggregator", k);
                            solution.addThread("MontageGeneral2", l);
                            writer.println(w + comma + i + comma + j + comma + k + comma + l + " : " + tc.calculateThroughputPrecisev2(graph.getSpoutMap().get("MontageSpout"), solution));
                        }
                    }
                }
            }
        }
        writer.close();*/
    }
}

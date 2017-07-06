package org.apache.storm.starter.profiling.cost;

import org.apache.storm.starter.profiling.GraphParser;
import org.apache.storm.starter.profiling.ThroughputCalculator;
import org.moeaframework.core.Solution;
import org.moeaframework.core.variable.BinaryIntegerVariable;
import org.moeaframework.core.variable.BinaryVariable;
import org.moeaframework.core.variable.EncodingUtils;
import org.moeaframework.core.variable.RealVariable;
import org.moeaframework.problem.AbstractProblem;

/**
 * Created by vgol on 23/1/2017.
 */
public class CostEvaluator extends AbstractProblem {

    ThroughputCalculator calculator;
    GraphParser graph;

    public CostEvaluator() throws Exception{
        super(4,2,0);
        calculator = new ThroughputCalculator();
        graph = new GraphParser();
    }

    @Override
    public Solution newSolution() {
        Solution solution = new Solution(4, 2, 0);

        solution.setVariable(0, new BinaryIntegerVariable(1, 4));
        solution.setVariable(1, new BinaryIntegerVariable(1, 4));
        solution.setVariable(2, new BinaryIntegerVariable(1, 4));
        solution.setVariable(3, new BinaryIntegerVariable(1, 4));

        return solution;
    }

    @Override
    public void evaluate(Solution solution) {
        int[] config = EncodingUtils.getInt(solution);

        for(int i=0;i<config.length;i++)
            System.out.print(config[i] + " ");
        System.out.print("\n");

        org.apache.storm.starter.profiling.Solution s = new org.apache.storm.starter.profiling.Solution(config[0]);

        s.addThread("FastRandomSentenceSpout", config[1]);
        s.addThread("SplitSentence", config[2]);
        s.addThread("WordCount", config[3]);

        try {
            solution.setObjective(0, -calculator.calculateThroughputPrecise(graph.getSpoutMap().get("FastRandomSentenceSpout"), s));
            solution.setObjective(1, config[0]);
        }
        catch(Exception e) {
            solution.setObjective(0, 0);
            solution.setObjective(1, 0);
        }

    }

    /*@Override
    public void evaluate(Solution solution) {
//        double[] temp = EncodingUtils.getReal(solution);
//        for(int i=0;i<temp.length;i++)
//            System.out.print(temp[i] + " ");
//        System.out.print("\n");
        int[] config = EncodingUtils.getInt(solution);

        for(int i=0;i<config.length;i++)
            System.out.print(config[i] + " ");
        System.out.print("\n");

        if(config[0] ==4 && config[1] ==4 && config[2] ==4 && config[3] ==4)
            System.out.println("********HIT*********");


        org.apache.storm.starter.profiling.Solution s = new org.apache.storm.starter.profiling.Solution(config[0]);

        s.addThread("FastRandomSentenceSpout", config[1]);
        s.addThread("SplitSentence", config[2]);
        s.addThread("WordCount", config[3]);

        try {
            solution.setObjective(0, -calculator.calculateThroughputPrecise(graph.getSpoutMap().get("FastRandomSentenceSpout"), s));
            solution.setObjective(0, -config[0]*config[1]*config[2]*config[3]);
        }
        catch(Exception e) {
            solution.setObjective(0, 0);
        }

        if(config[0] ==4 && config[1] ==4 && config[2] ==4 && config[3] ==4)
            solution.setObjective(0, -40000);

//        solution.setConstraint(0, config[1] <= config[0] ? 0.0 : 1);
//        solution.setConstraint(1, config[2] <= 2*config[0] ? 0.0 : 1);
//        solution.setConstraint(2, config[3] <= 2*config[0] ? 0.0 : 1);

    }*/
}

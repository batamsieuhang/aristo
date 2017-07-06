package org.apache.storm.starter.profiling.cost.evaluators;

import org.apache.storm.starter.frontend.SystemParameters;
import org.apache.storm.starter.frontend.UserParameters;
import org.apache.storm.starter.profiling.GraphParser;
import org.apache.storm.starter.profiling.ThroughputCalculator;
import org.moeaframework.core.Solution;
import org.moeaframework.core.variable.BinaryIntegerVariable;
import org.moeaframework.core.variable.EncodingUtils;
import org.moeaframework.problem.AbstractProblem;

public class TimeEvaluator extends AbstractProblem{

    private ThroughputCalculator calculator;
    private GraphParser graph;
    private UserParameters up;
    private SystemParameters sp;
    private int variables;
    private int objectives;
    private int constraints;

    public TimeEvaluator(GraphParser graph, UserParameters up, SystemParameters sp) throws Exception{
        super(graph.getSpoutMap().size() + graph.getBoltMap().size() + 1, 1, 1);
        variables = graph.getSpoutMap().size() + graph.getBoltMap().size() + 1;
        objectives = 1;
        constraints = 1;

        this.graph = graph;
        calculator = new ThroughputCalculator();

        this.up = up;
        this.sp = sp;
    }

    @Override
    public Solution newSolution() {
        Solution solution = new Solution(variables, objectives, constraints);

        solution.setVariable(0, new BinaryIntegerVariable(1, sp.getMaxWorkers()));
        for(int i=1; i<variables; i++)
            solution.setVariable(i, new BinaryIntegerVariable(1, sp.getMaxThreads()));

        return solution;
    }

    @Override
    public void evaluate(Solution solution) {
        int[] config = EncodingUtils.getInt(solution);

        for(int i=0;i<config.length;i++)
            System.out.print(config[i] + " ");
        System.out.print("\n");

        int var = 0;
        org.apache.storm.starter.profiling.Solution s = new org.apache.storm.starter.profiling.Solution(config[var++]);
        for(String spoutName : graph.getSpoutMap().keySet())
            s.addThread(spoutName, config[var++]);
        for(String boltName : graph.getBoltMap().keySet())
            s.addThread(boltName, config[var++]);

        try {
            double throughput = calculator.calculateThroughputPrecise(graph.getSpoutMap(), s);
            double time = up.getDataset() / throughput;
            double money = up.getCost() * config[0] * time;
            solution.setObjective(0, money);
            solution.setConstraint(0, time <= up.getDeadline() ? 0.0 : 1.0);
        }
        catch(Exception e) {
            solution.setObjective(0, 0);
            solution.setConstraint(0,1.0);
        }

    }
}

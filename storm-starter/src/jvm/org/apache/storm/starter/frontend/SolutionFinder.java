package org.apache.storm.starter.frontend;

import org.apache.storm.starter.profiling.GraphParser;
import org.apache.storm.starter.profiling.cost.CostEvaluator;
import org.apache.storm.starter.profiling.cost.evaluators.MoneyEvaluator;
import org.apache.storm.starter.profiling.cost.evaluators.MoneyTimeEvaluator;
import org.apache.storm.starter.profiling.cost.evaluators.ParetoEvaluator;
import org.apache.storm.starter.profiling.cost.evaluators.TimeEvaluator;
import org.moeaframework.Executor;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.Problem;
import org.moeaframework.core.Solution;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by VagelisAkis on 29/1/2017.
 */
public class SolutionFinder {
    private UserParameters userParameters;
    private SystemParameters systemParameters;
    private GraphParser graph;

    public SolutionFinder(UserParameters userParameters, SystemParameters systemParameters) throws Exception{
        this.userParameters = userParameters;
        this.systemParameters = systemParameters;
        graph = new GraphParser();
    }

    public List<UserSolution> findSolution() throws Exception{
        List<UserSolution> out = null;

        if(userParameters.getBudget() == null && userParameters.getDeadline() != null) {
            System.out.println("Time Constraint");
//            out= findTimeSolution();
            out = findSolution(new TimeEvaluator(graph, userParameters, systemParameters));
        }
        else if(userParameters.getBudget() != null && userParameters.getDeadline() == null) {
            System.out.println("Money Constraint");
            out = findSolution(new MoneyEvaluator(graph, userParameters, systemParameters));
        }
        else if(userParameters.getBudget() == null && userParameters.getDeadline() == null) {
            System.out.println("Pareto Curve");
            out = findSolution(new ParetoEvaluator(graph, userParameters, systemParameters));
        }
        else if(userParameters.getBudget() != null && userParameters.getDeadline() != null) {
            System.out.println("Time+money Constraint");
            out = findSolution(new MoneyTimeEvaluator(graph, userParameters, systemParameters));
        }

        return out;
    }

    public List<UserSolution> findSolution(Problem problem) throws Exception{
        List<UserSolution> out = new ArrayList<>();

        NondominatedPopulation result = new Executor()
                .withAlgorithm("NSGAII")
                .withProblem(problem)
                .withMaxEvaluations(500)
                .run();

        for(Solution s : result) {
            if(!s.violatesConstraints())
                out.add(new UserSolution(graph, s));
        }

        return out;
    }

    public List<UserSolution> findTimeSolution() throws Exception{
        List<UserSolution> out = new ArrayList<>();

        NondominatedPopulation result = new Executor()
                .withAlgorithm("NSGAII")
                .withProblem(new TimeEvaluator(graph, userParameters, systemParameters))
                .withMaxEvaluations(5000)
                .run();

        for(Solution s : result) {
            if(!s.violatesConstraints())
                out.add(new UserSolution(graph, s));
        }

        return out;
    }

    public static void main(String args[]) {
        UserParameters user = new UserParameters(100000d, null, null, 1d);
        SystemParameters system = new SystemParameters(7, 7);

        List<UserSolution> solutions;
        try {
            SolutionFinder sf = new SolutionFinder(user, system);
            solutions = sf.findSolution();

            for(UserSolution s : solutions)
                s.print();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

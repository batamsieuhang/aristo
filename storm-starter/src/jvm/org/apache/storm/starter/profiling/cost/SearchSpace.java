package org.apache.storm.starter.profiling.cost;

import org.apache.storm.starter.moea.CostFunction;
import org.moeaframework.Executor;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.Solution;

/**
 * Created by vgol on 23/1/2017.
 */
public class SearchSpace {

    public static void main(String args[]) throws Exception{

        NondominatedPopulation result = new Executor()
                .withAlgorithm("NSGAII")
                .withProblem(new CostEvaluator())
//                .withProblemClass(CostEvaluator.class)
                .withMaxEvaluations(500)
                .run();

        for (Solution solution : result) {

            for(int i=0;i<solution.getNumberOfVariables();i++)
                System.out.print(solution.getVariable(i) + "/");

            System.out.print("solution: " + solution.getObjective(1) + " " + (-solution.getObjective(0)) + "\n");

        }

    }
}

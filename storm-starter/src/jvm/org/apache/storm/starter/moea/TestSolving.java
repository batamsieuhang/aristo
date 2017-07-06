package org.apache.storm.starter.moea;

import org.apache.storm.starter.MoeaTest;
import org.moeaframework.Executor;
import org.moeaframework.core.NondominatedPopulation;
import org.moeaframework.core.Solution;

/**
 * Created by VagelisAkis on 16/1/2017.
 */
public class TestSolving {

    public static void main(String args[]) {
        NondominatedPopulation result = new Executor()
                .withProblemClass(CostFunction.class)
                .withAlgorithm("NSGAII")
                .withMaxEvaluations(1000000)
                .run();

        for (Solution solution : result) {

            System.out.println("Variables");
            for(int i=0;i<solution.getNumberOfVariables();i++)
                System.out.print(i + ": " + solution.getVariable(i) + "\n");

            System.out.println("Constraints");
            for(int i=0;i<solution.getNumberOfConstraints();i++)
                System.out.print(i + ": " + solution.getConstraint(i) + "\n");


            System.out.println("Solution");
            System.out.print(solution.getObjective(0) +"\n");
        }
    }
}

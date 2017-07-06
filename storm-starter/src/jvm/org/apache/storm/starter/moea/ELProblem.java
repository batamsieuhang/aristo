package org.apache.storm.starter.moea;

import org.apache.storm.starter.profiling.MetricsPredictor;
import org.moeaframework.core.Solution;
import org.moeaframework.core.variable.EncodingUtils;
import org.moeaframework.core.variable.RealVariable;
import org.moeaframework.problem.AbstractProblem;

import java.io.IOException;

/**
 * Created by VagelisAkis on 16/1/2017.
 */
public class ELProblem extends AbstractProblem{

    private MetricsPredictor predictor;
    private String operatorName;
    private int workers;

    public ELProblem() {
        super(2,1);
        operatorName = "SplitSentence";
        try {
            predictor = new MetricsPredictor(operatorName);
        }
        catch(IOException e) {
            e.printStackTrace();
        }

        workers = 4;
    }

    @Override
    public Solution newSolution() {
        Solution solution = new Solution(getNumberOfVariables(), getNumberOfObjectives());

        solution.setVariable(0, new RealVariable(1, 2*workers));
        solution.setVariable(1, new RealVariable(1, workers));

        return solution;
    }

    @Override
    public void evaluate(Solution solution) {
        int[] x = EncodingUtils.getInt(solution);
        double[] f = new double[numberOfObjectives];

        try {
            f[0] = predictor.getExecuteLatency(workers, x[0], x[1]);
        }
        catch(Exception e) {
            e.printStackTrace();
        }

        solution.setObjectives(f);
    }
}

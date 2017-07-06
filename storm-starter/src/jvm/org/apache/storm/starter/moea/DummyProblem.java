package org.apache.storm.starter.moea;

import org.moeaframework.core.Solution;
import org.moeaframework.core.variable.EncodingUtils;
import org.moeaframework.core.variable.RealVariable;
import org.moeaframework.problem.AbstractProblem;

/**
 * Created by VagelisAkis on 16/1/2017.
 */
public class DummyProblem extends AbstractProblem{

    public DummyProblem() {
        super(2,1);
    }

    public Solution newSolution() {
        Solution solution = new Solution(getNumberOfVariables(), getNumberOfObjectives());

        solution.setVariable(0, new RealVariable(-1, 1));
        solution.setVariable(1, new RealVariable(-1, 1));

        return solution;
    }

    @Override
    public void evaluate(Solution solution) {
        double[] x = EncodingUtils.getReal(solution);
        double[] f = new double[numberOfObjectives];

        f[0] = x[0] + x[1];

        solution.setObjectives(f);
    }
}

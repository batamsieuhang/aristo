package org.apache.storm.starter.moea;

import org.moeaframework.core.Solution;
import org.moeaframework.core.variable.EncodingUtils;
import org.moeaframework.core.variable.RealVariable;
import org.moeaframework.problem.AbstractProblem;

/**
 * Created by VagelisAkis on 17/1/2017.
 */
public class CostFunction extends AbstractProblem{

    private double t1 = 1;
    private double t2 = 2;
    private double t3 = 3;
    private double t4 = 4;
    private double e = 1;


    public CostFunction() {
        super(8,1,7);
    }
    @Override
    public Solution newSolution() {
        Solution solution = new Solution(8, 1, 7);

        solution.setVariable(0, new RealVariable(0, 1000));
        solution.setVariable(1, new RealVariable(0, 500));
        solution.setVariable(2, new RealVariable(0, 500));
        solution.setVariable(3, new RealVariable(0, 334));
        solution.setVariable(4, new RealVariable(0, 334));
        solution.setVariable(5, new RealVariable(0, 334));
        solution.setVariable(6, new RealVariable(0, 250));
        solution.setVariable(7, new RealVariable(0, 250));

        return solution;
    }

    @Override
    public void evaluate(Solution solution) {
        double[] o = EncodingUtils.getReal(solution);

        double f1 = o[0]*t1 + o[4]*t3;
        double f2 = o[1]*t2 + o[5]*t3;
        double f3 = o[2]*t2 + o[6]*t4;
        double f4 = o[3]*t3 + o[7]*t4;

        double s1 = o[0];
        double s2 = o[1] + o[2];
        double s3 = o[3] + o[4] + o[5];
        double s4 = o[6] + o[7];

        double c1 = Math.abs(s1 - s2);
        double c2 = Math.abs(s1 - s3);
        double c3 = Math.abs(s1 - s4);

        solution.setObjective(0, -s1);

        solution.setConstraint(0, c1 <= e ? 0 : c1);
        solution.setConstraint(1, c2 <= e ? 0 : c2);
        solution.setConstraint(2, c3 <= e ? 0 : c3);

        solution.setConstraint(3, f1 <= 1000 ? 0 : f1);
        solution.setConstraint(4, f2 <= 1000 ? 0 : f2);
        solution.setConstraint(5, f3 <= 1000 ? 0 : f3);
        solution.setConstraint(6, f4 <= 1000 ? 0 : f4);

//        if(f1<=1000 && f2<=1000 && f3<=1000 && f4<=1000)
//            solution.setConstraint(3, 0);
//        else
//            solution.setConstraint(3, 1);

//        if(f1>900 || f2>900 || f3>900 || f4>900)
//            solution.setConstraint(4, 0);
//        else
//            solution.setConstraint(4, 1);


    }


}

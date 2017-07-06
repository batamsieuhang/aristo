package org.apache.storm.starter.profiling.cost;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by vgol on 18/1/2017.
 */
public class TestCost {

    public static void main(String args[]) {
        Cluster cluster = new Cluster(4);

        List<OperatorAssign> list = new ArrayList<>();
        OperatorAssign op;

        op = new OperatorAssign("1", 1, 0.001, 1, cluster);
        list.add(0, op);
        op = new OperatorAssign("2", 2, 0.002, 1, cluster);
        list.add(1, op);
        op = new OperatorAssign("3", 3, 0.003, 1, cluster);
        list.add(2, op);
        op = new OperatorAssign("4", 2, 0.004, 1, cluster);
        list.add(3, op);

        int i;
        double count = 0;
        while(true) {
//        for(int k=0;k<2;k++) {
            try{
//                cluster.printStatus();

//                System.out.println(count);
                for(i=0;i<4;i++) {
                    list.get(i).executeStep();
                }
//                System.out.println("List size: " + list.size());

//                cluster.printStatus();

                count++;
            }
            catch(IllegalStateException e) {
                System.out.println(e.getMessage());
                break;
            }
        }

        System.out.println("Throughput: " + count);
        cluster.printStatusVerbose();
    }
}

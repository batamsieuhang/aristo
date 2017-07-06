package org.apache.storm.starter.test;

import java.util.Comparator;
import java.util.PriorityQueue;

public class testO1 {

    private static testobj obj;

    public static void main(String args[]) {

        obj = new testobj(0,0);
        testO2 call = new testO2(obj);

        for (int i=0;i<10;i++){
            obj.updateX();
            call.test();
            obj.updateY();
            call.test();
        }

        PriorityQueue<testobj> pq= new PriorityQueue<testobj>(11, new Comparator<testobj>() {
            @Override
            public int compare(testobj o1, testobj o2) {
                if(o1.getX() > o2.getX())
                    return -1;
                if(o1.getX() < o2.getX())
                    return 1;
                return 0;
            }
        }
        );

        pq.add(new testobj(1,1));
        pq.add(new testobj(2,2));
        System.out.println("PQ: " + pq.peek().getX());


    }
}

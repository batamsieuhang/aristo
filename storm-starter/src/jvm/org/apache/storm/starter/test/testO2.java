package org.apache.storm.starter.test;

/**
 * Created by sagapiou on 18/8/2016.
 */
public class testO2 {

    private testobj last;

    public testO2(testobj obj) {

        last = obj;
    }

    public void test(){
        System.out.println(last.getX());
        System.out.println(last.getY());
    }
}

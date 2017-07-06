package org.apache.storm.starter.test;

/**
 * Created by VagelisAkis on 21/7/2016.
 */
public class testI2 implements testInterface{

    private int x;

    public testI2(int x) {
        this.x =x;
    }

    public int getX() {
        return x;
    }



    public void add1() {
        x++;
    }

    public void print() {
        System.out.println(x);
    }
}

package org.apache.storm.starter.test;

/**
 * Created by VagelisAkis on 21/7/2016.
 */
public class test1 {
    private int x;
    private test2 t2;

    public test1 (int x) {
        this.x = x;
        t2 = new test2(this);
    }

    public int getX() {
        return x;
    }

    public test2 getT2() {
        return t2;
    }
}

package org.apache.storm.starter.test;

/**
 * Created by VagelisAkis on 21/7/2016.
 */
public class test2 {
    private int y;

    public test2(test1 kappa) {
        y = kappa.getX() + 1;
        //System.out.println(kappa.getT2().getY()); Null pointer Exception
    }

    public int getY() {
        return y;
    }
}

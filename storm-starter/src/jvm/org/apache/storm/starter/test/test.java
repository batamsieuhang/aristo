package org.apache.storm.starter.test;

/**
 * Created by VagelisAkis on 21/7/2016.
 */
public class test {

    public static void main(String args[]) {
        test1 k = new test1(3);
        test3 k2 = new test3(k);
        System.out.println(k.getX() + " " + k.getT2().getY());

        testInterface x;
        x = new testI1(1,2);
        x.print();

        x = new testI2(5);
        x.print();
        testI2 x2 = (testI2) x;
        System.out.print(x2.getX());
    }
}

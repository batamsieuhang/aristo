package org.apache.storm.starter.test;

/**
 * Created by VagelisAkis on 21/7/2016.
 */
public class testI1 implements testInterface{

    private int x;
    private int y;

    public testI1(int x, int y) {
        this.x =x;
        this.y = y;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public void add1() {
        x++;
        y++;
    }

    public void print() {
        System.out.println(x + " " + y);
    }
}

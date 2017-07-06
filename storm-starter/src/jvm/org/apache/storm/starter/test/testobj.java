package org.apache.storm.starter.test;

/**
 * Created by sagapiou on 18/8/2016.
 */
public class testobj {
    private int x;
    private int y;

    public testobj(int x,int y){
        this.x = x;
        this.y = y;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public void updateX() {
        x++;
    }

    public void updateY() {
        y++;
    }
}

package org.apache.storm.starter.test;

public class test3 {

    Class c;
    Object k;

    public test3(Object c) {
        this.c = c.getClass();
        k = this.c.cast(c);
        System.out.println(k.getClass() + " " + k);
    }



}

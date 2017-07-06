package org.apache.storm.starter.profiling;

import javax.swing.plaf.multi.MultiInternalFrameUI;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by VagelisAkis on 10/10/2016.
 */
public class MultiDimensionalPoint implements Serializable{

    private Map<String, Double> values;

    public MultiDimensionalPoint() {
        values = new HashMap<>();
    }

    public void addDimension(String key, Double value) {
        this.values.put(key, value);
    }

    public Double getValue(String key) {
        return this.values.get(key);
    }

    public Set<String> getKeys() {
        return this.values.keySet();
    }

    public static void main(String args[]) {
        MultiDimensionalPoint x = new MultiDimensionalPoint();
        x.addDimension("c",100d);
        x.addDimension("a",100d);
        x.addDimension("b",200d);
        for(String s : x.getKeys()){
            System.out.println(s + " " + x.getValue(s));
        }
    }
}

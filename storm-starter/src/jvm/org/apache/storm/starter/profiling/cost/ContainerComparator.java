package org.apache.storm.starter.profiling.cost;

import java.util.Comparator;

/**
 * Created by VagelisAkis on 18/1/2017.
 */
public class ContainerComparator implements Comparator<Container> {

    @Override
    public int compare(Container x, Container y) throws NullPointerException{

        //We want ascending order so that's why we return 1 if X > Y
        if(x.getWatermark() > y.getWatermark())
            return 1;

        if(x.getWatermark() < y.getWatermark())
            return -1;

        return 0;
    }
}

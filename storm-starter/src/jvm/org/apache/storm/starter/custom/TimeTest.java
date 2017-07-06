package org.apache.storm.starter.custom;

import org.apache.storm.utils.Utils;

/**
 * Created by VagelisAkis on 28/6/2016.
 */
public class TimeTest {

    public static void main(String args[]) {
        System.out.println(System.nanoTime());
        long start = System.nanoTime();
        Utils.sleep(1000);
        long end = System.nanoTime();
        System.out.println(System.nanoTime());
        System.out.println(end-start);

        long _periodNano = Math.max(1, 1000000000/20000);
        long _emitAmount = Math.max(1, (long)((20000 / 1000000000.0) * _periodNano));
        System.out.println(_periodNano);
        System.out.println(_emitAmount);

        int min = 1;
        int max = 10;
        int step = 1;
        int direction = 1;
        long interval = 2000000000;
        int current = min + step;
        long nextTime = System.nanoTime() + interval;

        /*while(true) {
            if (nextTime <= System.nanoTime()) {
                current = current + direction*step;
                if(current <= min || current >= max) {
                    direction = -direction;
                    System.out.println(direction);
                }
                nextTime = nextTime + interval;
                System.out.println(current);
            }




        }*/
        long _stepNano = 1000000000;
        long _nextStepTime = System.nanoTime() + _stepNano;
        long _currentRatePerSecond = 2000;
        long _minRatePerSecond = 1000;
        long _maxRatePerSecond = 30000;
        long _stepRatePerSecond = 1000;
        while(true) {
            if (_nextStepTime <= System.nanoTime()){
                _currentRatePerSecond = _currentRatePerSecond + direction*_stepRatePerSecond;
                System.out.println(_currentRatePerSecond + " " + 1000000000/_currentRatePerSecond);

                if(_currentRatePerSecond <= _minRatePerSecond || _currentRatePerSecond >= _maxRatePerSecond)  //care, if it is strted from min or max then we have a problem, especially when min and step are the same then divide by  in 65
                    direction = -direction;

                //_periodNano = Math.max(1, 1000000000/_currentRatePerSecond);
                _nextStepTime = _nextStepTime + _stepNano;
            }

        }

    }
}

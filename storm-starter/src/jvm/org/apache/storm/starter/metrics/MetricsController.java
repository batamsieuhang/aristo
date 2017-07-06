package org.apache.storm.starter.metrics;

public class MetricsController {

    public static void main(String args[]) throws Exception{

        if (args.length != 2) {
            System.out.println("Usage: Topology name, Component name");
        }

        String topologyName = args[0];
        String componentName = args[1];

        ComponentMetricsCreator component = new ComponentMetricsCreator(topologyName, componentName);


        for (int i = 0; i < 1000; i++) {
            component.getComponentUpdater().updateMetrics();
            component.getComponentUpdater().printMetrics();
            Thread.sleep(10 * 1000);
        }

        /*
        if (component.getComponentType() == 1) {

            BoltMetricsUpdater updater = new BoltMetricsUpdater(component);

            for (int i = 0; i < 1000; i++) {
                updater.updateMetrics();
                updater.printMetrics();
                Thread.sleep(10 * 1000);
            }



        }
        else if (component.getComponentType() == 2) {
            //SpoutMetricsUpdater updater = new SpoutMetricsUpdater(component);

            for (int i = 0; i < 1000; i++) {
                updater.updateMetrics();
                updater.printMetrics();
                Thread.sleep(10 * 1000);
            }
        }
        */



    }
}

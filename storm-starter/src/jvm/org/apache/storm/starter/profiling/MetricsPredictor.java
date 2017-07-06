package org.apache.storm.starter.profiling;

import org.apache.storm.starter.metrics.MetricsController;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.classifiers.lazy.KStar;
import weka.classifiers.trees.m5.M5Base;
import weka.core.*;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class MetricsPredictor {

    private String file;
    private Instances instances;
    private Instances filtered;
    private Instances prediction;
    private Classifier classifier;
    private Double IORatio;

    public MetricsPredictor(String operatorName) throws IOException{
        file = operatorName + ".txt";
        instances = new Instances(new BufferedReader(new FileReader(file)));

        classifier = new MultilayerPerceptron();
//        classifier = new KStar();
//        classifier = new LinearRegression(); //bad
    }

    private void filterInstances(int classIndex, int excludedAttribute) throws Exception{
        instances.setClassIndex(classIndex - 1);

        Remove filter = new Remove();

        String indices = "4,5,6,7,8";
        String exclude = excludedAttribute + ",";
        filter.setAttributeIndices(indices.replace(exclude, ""));

        filter.setInputFormat(instances);
        filtered = Filter.useFilter(instances,filter);
        if(classIndex < 4)
            filtered.setClassIndex(classIndex - 1);
        else
            filtered.setClassIndex(3);

//        System.out.println(instances);
//        System.out.println(instances.classIndex());
//        System.out.println(instances.classAttribute());
//        System.out.println(filtered);
//        System.out.println(filtered.classIndex());
//        System.out.println(filtered.classAttribute());
    }

    private void trainDataset() throws Exception{
        classifier.buildClassifier(filtered);
    }

    public double createPrediction(String missing, int value0, int value1, int value2) throws Exception{
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("workers", 0));
        attributes.add(new Attribute("boltExecutors", 1));
        attributes.add(new Attribute("totalExecutors", 2));
        attributes.add(new Attribute(missing, 3));

        DenseInstance inst = new DenseInstance(4);
        FastVector att  = new FastVector(4);

        inst.setValue(attributes.get(0), value0);
        inst.setValue(attributes.get(1), value1);
        inst.setValue(attributes.get(2), value2);
        inst.setMissing(3);

        for(Attribute a : attributes)
            att.addElement(a);

        prediction = new Instances("predictions", att, 0);
        prediction.setClassIndex(3);
        inst.setDataset(prediction);

//        System.out.println(inst.toString());
//        System.out.println(inst.classAttribute());
//        System.out.println(inst.classIndex());

        return classifier.classifyInstance(inst);
    }

    public double createPrediction(String extra, int value0, int value2, double value3) throws Exception{
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("workers", 0));
        attributes.add(new Attribute("boltExecutors", 1));
        attributes.add(new Attribute("totalExecutors", 2));
        attributes.add(new Attribute(extra, 3));

        DenseInstance inst = new DenseInstance(4);
        FastVector att  = new FastVector(4);

        inst.setValue(attributes.get(0), value0);
        inst.setMissing(1);
        inst.setValue(attributes.get(2), value2);
        inst.setValue(attributes.get(3), value3);

        for(Attribute a : attributes)
            att.addElement(a);

        prediction = new Instances("predictions", att, 0);
        prediction.setClassIndex(1);
        inst.setDataset(prediction);

//        System.out.println(inst.toString());
//        System.out.println(inst.classAttribute());
//        System.out.println(inst.classIndex());

        return classifier.classifyInstance(inst);
    }



    public double getExecuteLatency(int workers, int boltExecutors, int spoutExecutors) throws Exception{
        filterInstances(6,6);

        trainDataset();

        return createPrediction("executeLatency", workers, boltExecutors, spoutExecutors);

    }

    public double getExecutors(int workers, int spoutExecutors, double executeLatency) throws Exception{
        filterInstances(2,6);

        trainDataset();

        return createPrediction("executeLatency", workers, spoutExecutors, executeLatency);

    }

    public double getSpoutEmitRate(){
        return instances.get(0).value(1);
    }

    public double getSpoutExecuteLatency() {
        return instances.get(0).value(2);
    }

    public double getSpoutTransferLatency() {
        return instances.get(0).value(3);
    }


    public double getIORatio() {
        if (IORatio == null){
            double input = 0;
            double output = 0;
            int count = 0;
            for (Instance instance : instances) {
                input += instance.value(4);
                output += instance.value(7);
                count++;
            }

            input /= count;
            output /= count;
            IORatio = input / output;
        }
        return IORatio;
    }

    public static void main(String args[]) {
        try {
            MetricsPredictor mp = new MetricsPredictor("SplitSentence");
            System.out.println(mp.getExecuteLatency(4,1,1));

//            System.out.println(mp.getExecutors(1,1,0.0168));

//            System.out.println(mp.getIORatio());

//            GraphParser gp = new GraphParser();
//            gp.printGraph();
//            System.out.println(gp.getSpoutMap().get("FastRandomSentenceSpout").getPredictor().getEmitRate());
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}

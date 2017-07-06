package org.apache.storm.starter.profiling;

import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.Sigar;
import org.w3c.dom.Attr;
import weka.classifiers.Classifier;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by VagelisAkis on 30/10/2016.
 */
public class SplitTrain {

    public  String file = "SplitSentence.txt";
    public Instances instances;
    public Instances filtered;
    public Instances prediction;
    public Classifier classifier = new MultilayerPerceptron();


    public SplitTrain() throws Exception{
        writeFile();

        initInstances();

        filterInstances();

        trainDataset();

        createPrediction();
    }



    public void writeFile() throws Exception {

        OutputToARFF o = new OutputToARFF("", file);
        o.setRelation("SplitSentence");
        o.initializeFileForStorm();
//        o.writeData("1,1,2,267,63258,0.0162,1.013,707360");
//        o.writeData("1,2,3,127,66697,0.0232,0.741,733763");
//        o.writeData("1,3,4,72,60960,0.0202,0.405,682132");
//        o.writeData("2,1,2,1414,64031,0.0158,0.975,711816");
//        o.writeData("2,2,3,827,73592,0.0152,0.542,827007");
//        o.writeData("2,3,4,421,93366,0.0213,0.593,975874");
//        o.writeData("2,2,4,382,88435,0.0219,0.882,985032");
//        o.writeData("3,2,3,22,102111,0.0159,0.786,1117758");
//        o.writeData("3,5,6,23,91971,0.0191,0.335,1010254");
//        o.writeData("3,4,6,133,135222,0.0226,0.664,1490168");
//        o.writeData("3,3,6,419,111005,0.0216,0.731,1217723");
//        o.writeData("4,3,4,11,105475,0.0173,0.529,1160647");
//        o.writeData("4,5,6,27,92632,0.0202,0.347,1019029");
//        o.writeData("4,2,4,528,115902,0.0145,0.733,1155107");
//        o.writeData("4,4,6,41,153246,0.0186,0.667,1678561");
//        o.writeData("4,6,8,25,150514,0.0202,0.481,1650241");
//        o.writeData("4,5,8,104,173555,0.0218,0.687,1943793");
//        o.writeData("4,4,8,275,139378,0.0221,0.679,1524804");

        o.writeData("1,1,1,267,63258,0.0162,1.013,707360");
        o.writeData("1,2,1,127,66697,0.0232,0.741,733763");
        o.writeData("1,3,1,72,60960,0.0202,0.405,682132");
//        o.writeData("1,5,1,267,59258,0.0162,1.013,707360");
//        o.writeData("1,10,1,267,58258,0.0162,1.013,707360");
        o.writeData("2,1,1,1414,64031,0.0158,0.975,711816");
        o.writeData("2,2,1,827,73592,0.0152,0.542,827007");
        o.writeData("2,3,1,421,93366,0.0213,0.593,975874");
//        o.writeData("2,5,1,421,83366,0.0213,0.593,975874");
//        o.writeData("2,10,1,421,79366,0.0213,0.593,975874");
        o.writeData("2,2,2,382,88435,0.0219,0.882,985032");
//        o.writeData("2,5,2,382,84435,0.0219,0.882,985032");
//        o.writeData("2,10,2,382,83435,0.0219,0.882,985032");
        o.writeData("3,2,1,22,102111,0.0159,0.786,1117758");
        o.writeData("3,5,1,23,91971,0.0191,0.335,1010254");
//        o.writeData("3,10,1,23,81971,0.0191,0.335,1010254");
        o.writeData("3,4,2,133,135222,0.0226,0.664,1490168");
//        o.writeData("3,6,2,133,125222,0.0226,0.664,1490168");
//        o.writeData("3,10,2,133,123222,0.0226,0.664,1490168");
        o.writeData("3,3,3,419,111005,0.0216,0.731,1217723");
//        o.writeData("3,6,3,419,111005,0.0216,0.731,1217723");
//        o.writeData("3,11,3,419,111005,0.0216,0.731,1217723");
//        o.writeData("4,3,1,11,105475,0.0173,0.529,1160647");
//        o.writeData("4,5,1,27,92632,0.0202,0.347,1019029");
//        o.writeData("4,8,1,27,82632,0.0202,0.347,1019029");
//        o.writeData("4,11,1,27,90632,0.0202,0.347,1019029");
//        o.writeData("4,2,2,528,115902,0.0145,0.733,1155107");
//        o.writeData("4,4,2,41,153246,0.0186,0.667,1678561");
//        o.writeData("4,6,2,25,150514,0.0202,0.481,1650241");
//        o.writeData("4,10,2,25,140514,0.0202,0.481,1650241");
//        o.writeData("4,5,3,104,173555,0.0218,0.687,1943793");
//        o.writeData("4,7,3,104,163555,0.0218,0.687,1943793");
//        o.writeData("4,9,3,104,163555,0.0218,0.687,1943793");
//        o.writeData("4,4,4,275,139378,0.0221,0.679,1524804");
    }

    public void initInstances() throws Exception{
        instances = new Instances(new BufferedReader(new FileReader(file)));
        instances.setClassIndex(instances.numAttributes() - 7);
    }

    public void filterInstances() throws Exception{
        Remove filter = new Remove();
        filter.setAttributeIndices("4,6-8");
        filter.setInputFormat(instances);
        filtered = Filter.useFilter(instances,filter);
        filtered.setClassIndex(filtered.numAttributes() - 3);
    }

    public void trainDataset() throws Exception{
        classifier.buildClassifier(filtered);
    }

    public void createPrediction() throws Exception{
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("workers", 0));
        attributes.add(new Attribute("boltExecutors", 1));
        attributes.add(new Attribute("totalExecutors", 2));
        attributes.add(new Attribute("throughput", 3));

        DenseInstance inst = new DenseInstance(4);
        FastVector att  = new FastVector(4);

        inst.setValue(attributes.get(0), 2);
        //inst.setValue(attributes.get(1), 3);
        inst.setValue(attributes.get(2), 1);
        inst.setValue(attributes.get(3), 20000);
        inst.setMissing(1);

        for(Attribute a : attributes)
            att.addElement(a);

        prediction = new Instances("predictions", att, 0);
        prediction.setClassIndex(prediction.numAttributes()-3);
        inst.setDataset(prediction);

        System.out.println(inst.toString());
        System.out.println(inst.classAttribute());
        System.out.println(inst.classIndex());

        System.out.println(classifier.classifyInstance(inst));
    }

    public static void main(String args[]) {
        try {
            SplitTrain temp = new SplitTrain();
            System.out.println(temp.instances);
            System.out.println(temp.instances.classIndex());
            System.out.println(temp.filtered);
            System.out.println(temp.filtered.classIndex());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

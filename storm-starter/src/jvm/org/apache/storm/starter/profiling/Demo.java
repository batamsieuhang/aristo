package org.apache.storm.starter.profiling;

import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.functions.*;
import weka.classifiers.lazy.KStar;
import weka.classifiers.meta.ClassificationViaRegression;
import weka.classifiers.meta.RandomSubSpace;
import weka.classifiers.pmml.consumer.GeneralRegression;
import weka.classifiers.pmml.consumer.NeuralNetwork;
import weka.classifiers.pmml.consumer.Regression;
import weka.classifiers.rules.JRip;
import weka.classifiers.rules.OneR;
import weka.classifiers.trees.LMT;
import weka.core.*;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;
import weka.filters.unsupervised.instance.Randomize;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Vector;

/**
 * Created by VagelisAkis on 8/10/2016.
 */
public class Demo {

    public static void main(String args[]) {
        try {
            String file = "yo.txt";

            OutputToARFF o = new OutputToARFF("", file);
            o.setRelation("demo");
            o.initializeFileForStorm();
            o.writeData("1,1,1000,1000,0.1,0.1");
            o.writeData("2,2,2000,2000,0.2,0.2");
            o.writeData("3,3,3000,3000,0.3,0.3");
            o.writeData("4,4,4000,4000,0.4,0.4");


            Instances instances = new Instances(new BufferedReader(new FileReader(file)));
            instances.setClassIndex(instances.numAttributes() - 1);

            Vector<String> filterOptions = new Vector<>();
            Remove filter = new Remove();
            filter.setAttributeIndices("1-2");
            filter.setInputFormat(instances);
            Instances instances2 = Filter.useFilter(instances,filter);
            System.out.println(instances2);

            System.out.println(instances.toString());
            System.out.println(instances.classAttribute());
            System.out.println(instances.classIndex());

            Classifier classifier = new MultilayerPerceptron();
//            Classifier classifier = new KStar();
//            Classifier classifier = new LinearRegression();
//            Classifier classifier = new RandomSubSpace();
            classifier.buildClassifier(instances);

            DenseInstance inst = new DenseInstance(6);
            Attribute att = new Attribute("workers", 0);
            inst.setValue(att, 5);
            att = new Attribute("executors", 1);
            inst.setValue(att, 5);
            att = new Attribute("transferred", 2);
            inst.setValue(att, 5000);
            att = new Attribute("acked", 3);
            inst.setValue(att, 5000);
            att = new Attribute("latency", 4);
            inst.setValue(att, 0.5);
//            att = new Attribute("capacity", 5);
//            inst.setValue(att, 0.5);

            inst.setMissing(5);


            //assign instance to dataset
            FastVector att2  = new FastVector(6);

            att2.addElement(new Attribute("workers", 0));
            att2.addElement(new Attribute("executors", 1));
            att2.addElement(new Attribute("transferred", 2));
            att2.addElement(new Attribute("acked", 3));
            att2.addElement(new Attribute("latency", 4));
            //att2.addElement(new Attribute("capacity", 5));
            att2.addElement(new Attribute("objective", 5));

            Instances dataset = new Instances("instances", att2, 0);
            dataset.setClassIndex(dataset.numAttributes()-1);
            inst.setDataset(dataset);

            System.out.println(inst.toString());
            System.out.println(inst.classAttribute());
            System.out.println(inst.classIndex());

            System.out.println(classifier.classifyInstance(inst));
            for(double d : classifier.distributionForInstance(instances.instance(0)))
                System.out.println(d);


        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}

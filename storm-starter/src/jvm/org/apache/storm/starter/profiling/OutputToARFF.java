package org.apache.storm.starter.profiling;

import org.apache.storm.starter.metrics.BoltMetrics;
import org.apache.storm.starter.metrics.SpoutMetrics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * Created by vgol on 23/9/2016.
 */
public class OutputToARFF {

    private  File file;
    private String relation;
    private List<ARFFAttribute> attributeList;


    public OutputToARFF(String filepath, String filename) {
        file = new File(filepath+filename);
    }

    public OutputToARFF(String filename) {
        file = new File(filename);
    }

    public void initializeFile() throws Exception{
        PrintWriter writer = new PrintWriter(new FileWriter(file, false));

        writer.println("@relation " + relation +"\n");

        for (ARFFAttribute attribute : attributeList)
            writer.println("@attribute " + attribute.getAttributeName() + " " + attribute.getAttributeType());

        writer.println("\n@data");
        writer.close();
    }

    public void initializeFileForStorm() throws IOException{

        PrintWriter writer = new PrintWriter(new FileWriter(file, false));

        writer.println("@relation " + relation +"\n");

        writer.println("@attribute workers numeric");
        writer.println("@attribute boltExecutors numeric");
        writer.println("@attribute totalExecutors numeric");
        writer.println("@attribute totalLatency numeric");
        writer.println("@attribute acked/sec numeric");
        writer.println("@attribute executeLatency numeric");
        writer.println("@attribute avgCapacity numeric");
        writer.println("@attribute emitted/sec numeric");

        writer.println("\n@data");

        writer.close();
    }

    public void writeData(String data) throws IOException{
        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        writer.println(data);
        writer.close();
    }

    public void writeData(int workers, SpoutMetrics spoutMetrics, BoltMetrics boltMetrics) throws IOException{
        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        String comma = ",";
        writer.println(workers + comma + boltMetrics.getExecutors() + comma + spoutMetrics.getExecutors() + comma +
                        spoutMetrics.getCompleteLatency() + comma + spoutMetrics.getAckedRate() + comma +
                        boltMetrics.getExecuteLatency() + comma + boltMetrics.getCapacity() + comma + boltMetrics.getEmitRate());
        writer.close();
    }

    public void writeDataNA(int workers, SpoutMetrics spoutMetrics, BoltMetrics boltMetrics) throws IOException{
        PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        String comma = ",";
        writer.println(workers + comma + boltMetrics.getExecutors() + comma + spoutMetrics.getExecutors() + comma +
                spoutMetrics.getCompleteLatency() + comma + boltMetrics.getAckedRate() + comma +
                boltMetrics.getExecuteLatency() + comma + boltMetrics.getCapacity() + comma + boltMetrics.getEmitRate());
        writer.close();
    }

    public static void main(String args[]){
        try {
            OutputToARFF o = new OutputToARFF("", "yo.txt");
            o.setRelation("new");
            o.initializeFileForStorm();
            o.writeData("yo");

        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public File getFile() {
        return file;
    }

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public List<ARFFAttribute> getAttributeList() {
        return attributeList;
    }

    public void setAttributeList(List<ARFFAttribute> attributeList) {
        this.attributeList = attributeList;
    }

}

package org.apache.storm.starter.profiling;

import org.apache.storm.starter.rulebased.ComponentNode;
import org.apache.storm.starter.rulebasedv2.FlowCheck;

import java.io.*;
import java.util.*;

public class GraphParser {

    private static final String connectionsFileName = "connections3.txt";
    private static final String spoutsFileName = "spouts3.txt";
    private static final String boltsFileName = "bolts3.txt";


    private Map<String, Operator> spoutMap;
    private Map<String, Operator> boltMap;


    public GraphParser() throws Exception{
        createGraph();
    }

    private void createGraph() throws FileNotFoundException{
        spoutMap = new HashMap<String, Operator>();
        boltMap = new HashMap<String, Operator>();

        BufferedReader in;
        String line;
        String name;
        String[] parts;
        Operator source;
        Operator destination;

        //Read the spout operators
        in = new BufferedReader(new FileReader(spoutsFileName));
        try {
            while ((line = in.readLine()) != null) {
                name = line.trim();
                spoutMap.put(name, new Operator(name));
            }

            in.close();
        }
        catch(IOException e) {
            e.printStackTrace();
        }

        //Read the bolt operators
        in = new BufferedReader(new FileReader(boltsFileName));
        try {
            while ((line = in.readLine()) != null) {
                name = line.trim();
                boltMap.put(name, new Operator(name));
            }

            in.close();
        }
        catch(IOException e) {
            e.printStackTrace();
        }

        //Read the connections between the operators
        in = new BufferedReader(new FileReader(connectionsFileName));
        try {
            while ((line = in.readLine()) != null) {

                if ( !line.contains(" ") )
                    throw new IllegalArgumentException("String '" + line + "' does not contain space");

                parts = line.split(" ");

                if (parts.length != 2)
                    throw new IllegalArgumentException("String '" + line + "' should contain exactly 2 vertices");

                if(spoutMap.containsKey(parts[0]))
                    source = spoutMap.get(parts[0]);
                else if(boltMap.containsKey(parts[0]))
                    source = boltMap.get(parts[0]);
                else
                    throw new IllegalArgumentException("No operator with name: '" + parts[0] + "' found");

                if(spoutMap.containsKey(parts[1]))
                    destination = spoutMap.get(parts[1]);
                else if(boltMap.containsKey(parts[1]))
                    destination = boltMap.get(parts[1]);
                else
                    throw new IllegalArgumentException("No operator with name: '" + parts[1] + "' found");

                source.addNeighbor(destination);

            }

            in.close();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void printGraph() {

        Queue<Operator> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();

        for(Operator root : spoutMap.values()) {
            System.out.println(root.getName());

            visited.add(root.getName());
            queue.add(root);
        }

        Operator temp = null;
        while(!queue.isEmpty()) {
            temp = queue.poll();
            for (Operator leaf : temp.getNeighbors()) {

                if (!visited.contains(leaf.getName())) {
                    System.out.println(leaf.getName());

                    visited.add(leaf.getName());
                    queue.add(leaf);
                }
            }
        }
    }

    public Map<String, Operator> getSpoutMap() {
        return spoutMap;
    }

    public Map<String, Operator> getBoltMap() {
        return boltMap;
    }
}

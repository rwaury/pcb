package com.amadeus.pcb.graph;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class SubGraphFinder {

    private static final long SEED = 1337L;

    private static int COUNT = 128;
    private static int SHINGLE_SIZE = 3;

    private final static String DELIM = ",";

    private final static String path = "/home/robert/Amadeus/data/query_result.csv";


    public static void main(String[] args) throws Exception {
        HashMap<String, HashSet<String>> map = new HashMap<String, HashSet<String>>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(path));
            String line;
            String key;
            String value;
            while ((line = br.readLine()) != null) {
                if(line.indexOf("\"") == -1) {
                    String[] connection = line.split(DELIM);
                    key = connection[0];
                    value = connection[1];
                } else {
                    int delimIdx = 0;
                    if(line.indexOf("\"") != 0) {
                        delimIdx = 3;
                    } else {
                        delimIdx = line.indexOf("\"", line.indexOf("\"")+1)+1;
                    }
                    key = line.substring(0,delimIdx);
                    value = line.substring(delimIdx+1);
                    //System.out.println(key + " + " + value);
                }
                if (map.containsKey(key)) {
                    map.get(key).add(value);
                } else {
                    HashSet<String> list = new HashSet<String>();
                    list.add(value);
                    map.put(key, list);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        int consideredCount = 0;
        int ignoredCount = 0;
        HashMap<String, ArrayList<Integer>> hashes = new HashMap<String, ArrayList<Integer>>();
        for(Map.Entry<String, HashSet<String>> e : map.entrySet()) {
            if(e.getValue().size() >= SHINGLE_SIZE) {
                ArrayList<Integer> hashList = shingle(e.getValue(), SHINGLE_SIZE, COUNT);
                hashes.put(e.getKey(), hashList);
                consideredCount++;
            } else {
                ignoredCount++;
            }
        }
        System.out.println("Considered: " + consideredCount + " Ignored: " + ignoredCount);
    }

    private static ArrayList<Integer> shingle(HashSet<String> neighborsIn, int s, int c) {
        ArrayList<String> neighbors = new ArrayList<String>(neighborsIn);
        Collections.sort(neighbors);
        int p = Integer.MAX_VALUE; // happens to be prime
        Random rnd = new Random(SEED);
        ArrayList<Integer> result = new ArrayList<Integer>(c);
        long a = 0;
        long b = 0;
        int x = 0;
        int y = 0;
        int z = 0;
        for(int i = 0; i < c; i++) {
            int[] ys = new int[neighbors.size()];
            int j = 0;
            for(String str : neighbors) {
                x = str.hashCode();
                a = rnd.nextInt(p-1)+1;
                b = rnd.nextInt(p-1)+1;
                y = (int)(((a*x) + b) % p);
                ys[j] = y;
                j++;
            }
            Arrays.sort(ys);
            String hashable = "";
            for(int k = 0; k < s ; k++) {
                hashable += ys[k];
            }
            z = hashable.hashCode();
            result.add(z);
        }
        return result;
    }
}

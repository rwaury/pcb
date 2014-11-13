package com.amadeus.pcb.graph;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class SubGraphFinder {

    private static final long SEED = 1337L;

    private static int COUNT1 = 32;
    private static int SHINGLE_SIZE1 = 3;
    private static int COUNT2 = 32;
    private static int SHINGLE_SIZE2 = 3;


    private final static String DELIM = ",";

    private final static String path = "/home/robert/Amadeus/data/query_result2.csv";


    public static void main(String[] args) throws Exception {
        HashMap<String, HashSet<String>> airportToAirports = new HashMap<String, HashSet<String>>();
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
                if (airportToAirports.containsKey(key)) {
                    airportToAirports.get(key).add(value);
                } else {
                    HashSet<String> list = new HashSet<String>();
                    list.add(value);
                    airportToAirports.put(key, list);
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
        HashMap<String, ArrayList<Integer>> airportToShingles = new HashMap<String, ArrayList<Integer>>();
        HashSet<Integer> distinctShingles = new HashSet<Integer>();
        for(Map.Entry<String, HashSet<String>> e : airportToAirports.entrySet()) {
            if(e.getValue().size() >= SHINGLE_SIZE1) {
                ArrayList<Integer> shingles = shingle(e.getValue(), SHINGLE_SIZE1, COUNT1);
                airportToShingles.put(e.getKey(), shingles);
                distinctShingles.addAll(shingles);
                consideredCount++;
            } else {
                ignoredCount++;
            }
        }
        System.out.println("Considered: " + consideredCount + " Ignored: " + ignoredCount);
        System.out.println("Distinct shingles: " + distinctShingles.size());

        HashMap<Integer, ArrayList<String>> shingleToAirports = new HashMap<Integer, ArrayList<String>>();
        for(Integer s : distinctShingles) {
            ArrayList<String> nodes = getNodes(airportToShingles, s);
            shingleToAirports.put(s, nodes);
        }

        consideredCount = 0;
        ignoredCount = 0;
        HashSet<Integer> distinctMetaShingles = new HashSet<Integer>();
        HashMap<Integer, ArrayList<Integer>> shingleToMetaShingles = new HashMap<Integer, ArrayList<Integer>>();
        for(Map.Entry<Integer, ArrayList<String>> e : shingleToAirports.entrySet()) {
            if(e.getValue().size() >= SHINGLE_SIZE2) {
                ArrayList<Integer> metaShingles = shingle(e.getValue(), SHINGLE_SIZE2, COUNT2);
                shingleToMetaShingles.put(e.getKey(), metaShingles);
                distinctMetaShingles.addAll(metaShingles);
                consideredCount++;
            } else {
                ignoredCount++;
            }
        }
        System.out.println("Considered: " + consideredCount + " Ignored: " + ignoredCount);
        System.out.println("Distinct meta-shingles: " + distinctMetaShingles.size());

        HashMap<Integer, ArrayList<Integer>> metaShingleToShingles = new HashMap<Integer, ArrayList<Integer>>();
        for(Integer ms : distinctMetaShingles) {
            ArrayList<Integer> shingles = getNodes(shingleToMetaShingles, ms);
            metaShingleToShingles.put(ms, shingles);
        }

        // airport - airports
        // airport - shingles
        // shingle - airports
        // shingle - meta-shingles
        // meta-shingle - shingles

        QuickFind qf = new QuickFind(distinctShingles.size());
        qf.createMapping(distinctShingles);
        ArrayList<Integer> list;
        for(Map.Entry<Integer, ArrayList<Integer>> e : metaShingleToShingles.entrySet()) {
            list = e.getValue();
            if(list.size() > 1) {
               int first = list.get(0);
               for (int i = 1; i < list.size(); i++) {
                   qf.union(first, list.get(i));
               }
            }
        }
        HashMap<Integer, ArrayList<Integer>> clusters = qf.getClusters();
        int cCnt = 0;
        for(Map.Entry<Integer, ArrayList<Integer>> e : clusters.entrySet()) {
            HashSet<String> airports = new HashSet<String>();
            for(Integer s : e.getValue()) {
                airports.addAll(shingleToAirports.get(s));
            }
            ArrayList<String> apList = new ArrayList<String>();
            apList.addAll(airports);
            if(airports.size() > 9) {
                System.out.println(e.getKey() + ":");
                System.out.println(airports);
                System.out.println(pairwiseJaccardAvg(apList, airportToAirports));
                cCnt++;
            }
        }
        System.out.println("Components: " + qf.components() + " Components with more than 9 members: " + cCnt);
    }

    private static <C extends Comparable> ArrayList<Integer> shingle(Collection<C> neighborsIn, int s, int c) {
        ArrayList<C> neighbors = new ArrayList<C>(neighborsIn);
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
            a = rnd.nextInt(p-1)+1;
            b = rnd.nextInt(p-1)+1;
            for(C str : neighbors) {
                x = str.hashCode();
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

    private static <C extends Comparable> boolean sharesElements(Collection<C> c1, Collection<C> c2) {
        int size1 = c1.size();
        int size2 = c2.size();
        HashSet<C> union = new HashSet<C>();
        union.addAll(c1);
        union.addAll(c2);
        return (size1+size2) > union.size();
    }

    private static <C extends Comparable> ArrayList<C> getNodes(HashMap<C, ArrayList<Integer>> hashes, Integer s) {
        ArrayList<C> result = new ArrayList<C>();
        for(Map.Entry<C, ArrayList<Integer>> e : hashes.entrySet()) {
            if(e.getValue().contains(s)) {
                result.add(e.getKey());
            }
        }
        return result;
    }

    public static class QuickFind {

        private HashMap<Integer, Integer> mappingIdToIdx;
        private HashMap<Integer, Integer> mappingIdxToId;
        private int[] myID;
        private int myComponents;

        /**
         * Default constructor
         */
        public QuickFind() {
            mappingIdToIdx = null;
            mappingIdxToId = null;
            myID = null;
            myComponents = 0;
        }

        /**
         * Constructor that creates N isolated components
         */
        public QuickFind(int N) {
            initialize(N);
        }

        // instantiate N isolated components 0 through N-1
        public void initialize(int n) {
            mappingIdToIdx  = new HashMap<Integer, Integer>(n);
            mappingIdxToId  = new HashMap<Integer, Integer>(n);
            myComponents = n;
            myID = new int[n];
            for (int i = 0; i < n; i++) {
                myID[i] = i;
            }
        }

        public void createMapping(Collection<Integer> possibleValues) {
            if(possibleValues.size() != this.myID.length) {
                throw new RuntimeException("Invalid input. Expected size: " + this.myID.length + " Got: " + possibleValues.size());
            }
            int idx = 0;
            for(Integer shingle : possibleValues) {
                mappingIdToIdx.put(shingle,idx);
                mappingIdxToId.put(idx,shingle);
                idx++;
            }
        }

        // return number of connected components
        public int components() {
            return myComponents;
        }

        // return id of component corresponding to element x
        public int find(int shingle) {
            int idxX = mappingIdToIdx.get(shingle);
            return myID[idxX];
        }

        // are elements p and q in the same component?
        public boolean connected(int shingle1, int shingle2) {
            int p = mappingIdToIdx.get(shingle1);
            int q = mappingIdToIdx.get(shingle2);
            return myID[p] == myID[q];
        }

        // merge components containing p and q
        public void union(int shingle1, int shingle2) {
            int p = mappingIdToIdx.get(shingle1);
            int q = mappingIdToIdx.get(shingle2);
            if (connected(shingle1, shingle2))
                return;
            int pid = myID[p];
            for (int i = 0; i < myID.length; i++)
                if (myID[i] == pid)
                    myID[i] = myID[q];
            myComponents -= 1;
        }

        public HashMap<Integer, ArrayList<Integer>> getClusters() {
            HashMap<Integer, ArrayList<Integer>> clusters = new HashMap<Integer, ArrayList<Integer>>(this.myComponents);
            for (int i = 0; i < myID.length; i++) {
                int cid = myID[i];
                if(clusters.containsKey(cid)) {
                    clusters.get(cid).add(mappingIdxToId.get(i));
                } else {
                    ArrayList<Integer> cluster = new ArrayList<Integer>();
                    cluster.add(mappingIdxToId.get(i));
                    clusters.put(cid, cluster);
                }
            }
            return clusters;
        }
    }

    private static JInfo pairwiseJaccardAvg(ArrayList<String> nodes, HashMap<String, HashSet<String>> neighbors) {
        double sum = 0.0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        int count = 0;
        for(int i = 0; i < nodes.size(); i++) {
            for(int j = 0; j < nodes.size(); j++) {
                if(i != j) {
                    double jIdx = jaccard(neighbors.get(nodes.get(i)), neighbors.get(nodes.get(j)));
                    sum += jIdx;
                    if (jIdx < min) {
                        min = jIdx;
                    }
                    if (jIdx > max) {
                        max = jIdx;
                    }
                    count++;
                }
            }
        }
        JInfo result = new JInfo();
        result.min = min;
        result.max = max;
        result.avg = sum/count;
        return result;
    }

    private static double jaccard(HashSet<String> a, HashSet<String> b) {
        int aCnt = a.size();
        int bCnt = b.size();
        if(aCnt == 0 ^ bCnt == 0) {
            return 0.0;
        }
        if(aCnt == 0 && bCnt == 0) {
            return 1.0;
        }
        HashSet<String> union = new HashSet<String>();
        union.addAll(a);
        union.addAll(b);
        int uSize = union.size();
        HashSet<String> intersection = new HashSet<String>();
        intersection.addAll(a);
        intersection.retainAll(b);
        int iSize = intersection.size();
        return (double)iSize/(double)uSize;
    }

    public static class JInfo {

        double min;
        double max;
        double avg;

        public JInfo() {}


        @Override
        public String toString() {
            return "min: " + min + " max: " + max + " avg: " + avg;
        }
    }
}

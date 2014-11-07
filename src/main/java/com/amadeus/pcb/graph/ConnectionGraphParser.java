package com.amadeus.pcb.graph;

import com.amadeus.pcb.join.FlightConnectionJoiner;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;

public class ConnectionGraphParser {

    public static class ConnectionParser implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {

        String[] tmp = null;

        @Override
        public Tuple5<String, String, String, Integer, Integer> map(String s) throws Exception {
            Tuple5<String, String, String, Integer, Integer> result = new Tuple5<String, String, String, Integer, Integer>();
            tmp = s.split(",");
            String day = tmp[2].substring(4,10);
            result.f2 = day;
            if(tmp.length == 8 || tmp.length == 7) {
                result.f0 = tmp[0].trim();
                result.f1 = tmp[1].trim();
                result.f3 = 1;//Integer.parseInt(tmp[6].trim());
                result.f4 = 1;
            } else if(tmp.length == 15 || tmp.length == 16) {
                result.f0 = tmp[0].trim();
                result.f1 = tmp[8].trim();
                /*int min = Integer.MAX_VALUE;
                if(min > Integer.parseInt(tmp[6].trim())) {
                    min = Integer.parseInt(tmp[6].trim());
                }
                if(min > Integer.parseInt(tmp[14].trim())) {
                    min = Integer.parseInt(tmp[14].trim());
                }*/
                result.f3 = 2;
                result.f4 = 1;
            } else if( tmp.length == 23 || tmp.length == 24) {
                result.f0 = tmp[0].trim();
                result.f1 = tmp[17].trim();
                /*int min = Integer.MAX_VALUE;
                if(min > Integer.parseInt(tmp[6].trim())) {
                    min = Integer.parseInt(tmp[6].trim());
                }
                if(min > Integer.parseInt(tmp[14].trim())) {
                    min = Integer.parseInt(tmp[14].trim());
                }
                if(min > Integer.parseInt(tmp[22].trim())) {
                    min = Integer.parseInt(tmp[22].trim());
                }*/
                result.f3 = 3;
                result.f4 = 1;
            } else {
                throw new Exception("Wrong number of fields: " + s);
            }
            return result;
        }
    }

    public static class CityCodeJoiner1 implements JoinFunction<Tuple5<String, String, String, Integer, Integer>,
            Tuple7<String, String, String, String, String, Double, Double>, Tuple5<String, String, String, Integer, Integer>> {

        @Override
        public Tuple5<String, String, String, Integer, Integer> join(Tuple5<String, String, String, Integer, Integer> first,
               Tuple7<String, String, String, String, String, Double, Double> second) throws Exception {
            first.f0 = second.f1;
            return first;
        }
    }

    public static class CityCodeJoiner2 implements JoinFunction<Tuple5<String, String, String, Integer, Integer>,
            Tuple7<String, String, String, String, String, Double, Double>, Tuple5<String, String, String, Integer, Integer>> {

        @Override
        public Tuple5<String, String, String, Integer, Integer> join(Tuple5<String, String, String, Integer, Integer> first,
               Tuple7<String, String, String, String, String, Double, Double> second) throws Exception {
            first.f1 = second.f1;
            return first;
        }
    }


    public static void main(String[] args) throws Exception{
        if(!parseParameters(args)) {
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<String, String, String, Integer, Integer>> one = env.readTextFile(inputPath1).map(new ConnectionParser());
        DataSet<Tuple5<String, String, String, Integer, Integer>> two = env.readTextFile(inputPath2).map(new ConnectionParser());
        DataSet<Tuple5<String, String, String, Integer, Integer>> three = env.readTextFile(inputPath3).map(new ConnectionParser());

        DataSet<String> rawAirportInfo = env.readTextFile(oriPath);
        DataSet<Tuple7<String, String, String, String, String, Double, Double>> airportCoordinatesNR = rawAirportInfo.flatMap(new FlightConnectionJoiner.AirportCoordinateExtractor());

        DataSet<Tuple5<String, String, String, Integer, Integer>> flights = one.union(two).union(three);

        DataSet<Tuple5<String, String, String, Integer, Integer>> caps = flights.groupBy(0,1,2,3).sum(4);

        //DataSet<Tuple3<String, String, Integer>> capSum = flights.project(0,1,2).types(String.class, String.class, Integer.class).groupBy(0,1).sum(2);
        //capSum.writeAsCsv(capPath, "\n", "^", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<Tuple5<String, String, String, Integer, Integer>> cc1 = caps.join(airportCoordinatesNR).where(0).equalTo(0).with(new CityCodeJoiner1());
        DataSet<Tuple5<String, String, String, Integer, Integer>> cc2 = cc1.join(airportCoordinatesNR).where(1).equalTo(0).with(new CityCodeJoiner2());

        DataSet<Tuple5<String, String, String, Integer, Integer>> result = cc2.groupBy(0,1,2,3).sum(4);

        result.writeAsCsv(outputPath+"perleg/", "\n", "^", FileSystem.WriteMode.OVERWRITE);

        result.project(0,1,2,4).types(String.class, String.class, String.class, Integer.class).groupBy(0,1,2).sum(3).writeAsCsv(outputPath+"sum/", "\n", "^", FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }

    private static String oriPath;
    private static String inputPath1;
    private static String inputPath2;
    private static String inputPath3;
    private static String outputPath;
    private static String capPath;

    private static boolean parseParameters(String[] args) {
        if (args.length == 0) {
            oriPath = "file:///home/robert/Amadeus/data/ori_por_public.txt";
            inputPath1 = "file:///home/robert/Amadeus/data/resultConnections/one/";
            inputPath2 = "file:///home/robert/Amadeus/data/resultConnections/two/";
            inputPath3 = "file:///home/robert/Amadeus/data/resultConnections/three/";
            outputPath = "file:///home/robert/Amadeus/data/graph/";
            capPath = "file:///home/robert/Amadeus/data/capacity";
            return true;
        }
        if (args.length == 1) {
            oriPath = "hdfs:///user/rwaury/input/ori_por_public.csv";
            inputPath1 = "hdfs:///user/rwaury/output/capacity/";
            inputPath2 = "hdfs:///user/rwaury/output/flights/two/";
            inputPath3 = "hdfs:///user/rwaury/output/flights/three/";
            outputPath = "hdfs:///user/rwaury/output/graph/";
            capPath = "hdfs:///user/rwaury/output/capacity";
            return true;
        }
        return false;
    }
}

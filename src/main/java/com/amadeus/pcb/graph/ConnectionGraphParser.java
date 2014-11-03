package com.amadeus.pcb.graph;

import com.amadeus.pcb.join.FlightConnectionJoiner;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;

public class ConnectionGraphParser {

    public static class ConnectionParser implements MapFunction<String, Tuple4<String, String, Integer, Integer>> {

        String[] tmp = null;

        @Override
        public Tuple4<String, String, Integer, Integer> map(String s) throws Exception {
            tmp = s.split(",");
            Tuple4<String, String, Integer, Integer> result = new Tuple4<String, String, Integer, Integer>();
            if(tmp.length == 8 || tmp.length == 7) {
                result.f0 = tmp[0].trim();
                result.f1 = tmp[1].trim();
                result.f2 = Integer.parseInt(tmp[6].trim());
                result.f3 = 1;
            } else if(tmp.length >= 14 && tmp.length <= 16) {
                result.f0 = tmp[0].trim();
                result.f1 = tmp[8].trim();
                int min = Integer.MAX_VALUE;
                if(min > Integer.parseInt(tmp[6].trim())) {
                    min = Integer.parseInt(tmp[6].trim());
                }
                if(min > Integer.parseInt(tmp[14].trim())) {
                    min = Integer.parseInt(tmp[14].trim());
                }
                result.f2 = min;
                result.f3 = 2;
            } else if( tmp.length >= 21 && tmp.length <= 24) {
                result.f0 = tmp[0].trim();
                result.f1 = tmp[17].trim();
                int min = Integer.MAX_VALUE;
                if(min > Integer.parseInt(tmp[6].trim())) {
                    min = Integer.parseInt(tmp[6].trim());
                }
                if(min > Integer.parseInt(tmp[14].trim())) {
                    min = Integer.parseInt(tmp[14].trim());
                }
                if(min > Integer.parseInt(tmp[22].trim())) {
                    min = Integer.parseInt(tmp[22].trim());
                }
                result.f2 = min;
                result.f3 = 3;
            } else {
                throw new Exception("Wrong number of fields: " + s);
            }
            return result;
        }
    }

    public static class CityCodeJoiner1 implements JoinFunction<Tuple4<String, String, Integer, Integer>,
            Tuple7<String, String, String, String, String, Double, Double>, Tuple4<String, String, Integer, Integer>> {

        @Override
        public Tuple4<String, String, Integer, Integer> join(Tuple4<String, String, Integer, Integer> first,
               Tuple7<String, String, String, String, String, Double, Double> second) throws Exception {
            first.f0 = second.f1;
            return first;
        }
    }

    public static class CityCodeJoiner2 implements JoinFunction<Tuple4<String, String, Integer, Integer>,
            Tuple7<String, String, String, String, String, Double, Double>, Tuple4<String, String, Integer, Integer>> {

        @Override
        public Tuple4<String, String, Integer, Integer> join(Tuple4<String, String, Integer, Integer> first,
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

        DataSet<Tuple4<String, String, Integer, Integer>> one = env.readTextFile(inputPath1).map(new ConnectionParser());
        DataSet<Tuple4<String, String, Integer, Integer>> two = env.readTextFile(inputPath2).map(new ConnectionParser());
        DataSet<Tuple4<String, String, Integer, Integer>> three = env.readTextFile(inputPath3).map(new ConnectionParser());
        // extract GPS coordinates of all known airports
        DataSet<String> rawAirportInfo = env.readTextFile(oriPath);
        DataSet<Tuple7<String, String, String, String, String, Double, Double>> airportCoordinatesNR = rawAirportInfo.flatMap(new FlightConnectionJoiner.AirportCoordinateExtractor());

        DataSet<Tuple4<String, String, Integer, Integer>> flights = one.union(two).union(three);

        DataSet<Tuple4<String, String, Integer, Integer>> caps = flights.groupBy(0,1,3).sum(2);

        DataSet<Tuple4<String, String, Integer, Integer>> cc1 = caps.join(airportCoordinatesNR).where(0).equalTo(0).with(new CityCodeJoiner1());
        DataSet<Tuple4<String, String, Integer, Integer>> cc2 = cc1.join(airportCoordinatesNR).where(1).equalTo(0).with(new CityCodeJoiner2());

        DataSet<Tuple4<String, String, Integer, Integer>> result = cc2.groupBy(0,1,3).sum(2);

        result.writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }

    private static String oriPath;
    private static String inputPath1;
    private static String inputPath2;
    private static String inputPath3;
    private static String outputPath;

    private static boolean parseParameters(String[] args) {
        if (args.length == 0) {
            oriPath = "file:///home/robert/Amadeus/data/ori_por_public.txt";
            inputPath1 = "file:///home/robert/Amadeus/data/resultConnections/one/";
            inputPath2 = "file:///home/robert/Amadeus/data/resultConnections/two/";
            inputPath3 = "file:///home/robert/Amadeus/data/resultConnections/three/";
            outputPath = "file:///home/robert/Amadeus/data/graph/";
            return true;
        }
        if (args.length == 1) {
            oriPath = "hdfs:///user/rwaury/input/ori_por_public.csv";
            inputPath1 = "hdfs:///user/rwaury/output/flights/one/";
            inputPath2 = "hdfs:///user/rwaury/output/flights/two/";
            inputPath3 = "hdfs:///user/rwaury/output/flights/three/";
            outputPath = "hdfs:///user/rwaury/output/graph/";
            return true;
        }
        return false;
    }
}

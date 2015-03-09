package de.tuberlin.dima.old.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;

public class CapExtractor {

    public static final String DELIM = "^";

    public static class Group extends Tuple3<String, String, String> {
        public Group() {super();}

        @Override
        public String toString() {
            return this.f0 + DELIM + this.f1 + DELIM + this.f2;
        }

    }

    public static class ConnectionParser1 implements MapFunction<String, Tuple2<Group, Integer>> {

        String[] tmp = null;

        @Override
        public Tuple2<Group, Integer> map(String s) throws Exception {
            tmp = s.split(",");
            Tuple2<Group, Integer> result = new Tuple2<Group, Integer>();
            Group g = new Group();
            g.f0 = tmp[0];
            g.f1 = tmp[1];
            g.f2 = tmp[4].substring(0,2); // airline ID
            result.f0 = g;
            result.f1 = Integer.parseInt(tmp[6]);
            return result;
        }
    }

    public static class ConnectionParser2 implements MapFunction<String, Tuple3<Group, Group, Integer>> {

        String[] tmp = null;

        @Override
        public Tuple3<Group, Group, Integer> map(String s) throws Exception {
            tmp = s.split(",");
            Tuple3<Group, Group, Integer> result = new Tuple3<Group, Group, Integer>();
            Group g1 = new Group();
            g1.f0 = tmp[0];
            g1.f1 = tmp[1];
            g1.f2 = tmp[4].substring(0,2); // airline ID
            Group g2 = new Group();
            g2.f0 = tmp[8];
            g2.f1 = tmp[9];
            g2.f2 = tmp[12].substring(0,2); // airline ID
            result.f0 = g1;
            result.f1 = g2;
            int min = Integer.MAX_VALUE;
            if(min > Integer.parseInt(tmp[6].trim())) {
                min = Integer.parseInt(tmp[6].trim());
            }
            if(min > Integer.parseInt(tmp[14].trim())) {
                min = Integer.parseInt(tmp[14].trim());
            }
            result.f2 = min;
            return result;
        }
    }

    public static class ConnectionParser3 implements MapFunction<String, Tuple4<Group, Group, Group, Integer>> {

        String[] tmp = null;

        @Override
        public Tuple4<Group, Group, Group, Integer> map(String s) throws Exception {
            tmp = s.split(",");
            Tuple4<Group, Group, Group, Integer> result = new Tuple4<Group, Group, Group, Integer>();
            Group g1 = new Group();
            g1.f0 = tmp[0];
            g1.f1 = tmp[1];
            g1.f2 = tmp[4].substring(0,2); // airline ID
            Group g2 = new Group();
            g2.f0 = tmp[8];
            g2.f1 = tmp[9];
            g2.f2 = tmp[12].substring(0,2); // airline ID
            Group g3 = new Group();
            g3.f0 = tmp[16];
            g3.f1 = tmp[17];
            g3.f2 = tmp[20].substring(0,2); // airline ID
            result.f0 = g1;
            result.f1 = g2;
            result.f2 = g3;
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
            result.f3 = min;
            return result;
        }
    }

    public static void main(String[] args) throws Exception{
        if(!parseParameters(args)) {
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Group, Integer>> one = env.readTextFile(inputPath1).map(new ConnectionParser1());
        DataSet<Tuple2<Group, Integer>> caps1 = one.groupBy("f0.f0", "f0.f1", "f0.f2").sum(1);
        caps1.writeAsCsv(capPath+"one/", "\n", DELIM, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Group, Group, Integer>> two = env.readTextFile(inputPath2).map(new ConnectionParser2());
        DataSet<Tuple3<Group, Group, Integer>> caps2 = two.groupBy("f0.f0", "f0.f1", "f0.f2", "f1.f0", "f1.f1", "f1.f2").sum(2);
        caps2.writeAsCsv(capPath+"two/", "\n", DELIM, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Group, Group, Group, Integer>> three = env.readTextFile(inputPath3).map(new ConnectionParser3());
        DataSet<Tuple4<Group, Group, Group, Integer>> caps3 = three.groupBy("f0.f0", "f0.f1", "f0.f2", "f1.f0", "f1.f1", "f1.f2", "f2.f0", "f2.f1", "f2.f2").sum(3);
        caps3.writeAsCsv(capPath+"three/", "\n", DELIM, FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }

    private static String inputPath1;
    private static String inputPath2;
    private static String inputPath3;
    private static String capPath;

    private static boolean parseParameters(String[] args) {
        if (args.length == 0) {
            inputPath1 = "file:///home/robert/Amadeus/data/resultConnections/one/";
            inputPath2 = "file:///home/robert/Amadeus/data/resultConnections/two/";
            inputPath3 = "file:///home/robert/Amadeus/data/resultConnections/three/";
            capPath = "file:///home/robert/Amadeus/data/capacity/";
            return true;
        }
        if (args.length == 1) {
            inputPath1 = "hdfs:///user/rwaury/output/flights/one/";
            inputPath2 = "hdfs:///user/rwaury/output/flights/two/";
            inputPath3 = "hdfs:///user/rwaury/output/flights/three/";
            capPath = "hdfs:///user/rwaury/output/capacity/";
            return true;
        }
        return false;
    }
}

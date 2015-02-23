package com.amadeus.ti.pcb;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelConnectionBuilder {

    public static final Logger LOG = LoggerFactory.getLogger(ParallelConnectionBuilder.class);

    private static String schedulePath = "hdfs:///user/rwaury/input2/all_catalog_140417.txt";
    private static String oriPath = "hdfs:///user/rwaury/input2/ori_por_public.csv";
    private static String regionPath = "hdfs:///user/rwaury/input2/ori_country_region_info.csv";
    private static String defaultCapacityPath = "hdfs:///user/rwaury/input2/default_capacities.csv";
    private static String capacityPath = "hdfs:///user/rwaury/input2/capacities_2014-07-01.csv";
    private static String mctPath = "hdfs:///user/rwaury/input2/mct.csv";
    private static String outputPath = "hdfs:///user/rwaury/output2/flights/";

    public static final long START = 1399248000000L;
    public static final long END = 1399852799000L;

    // minimum allowed minimum connecting time in minutes
    public static final long MCT_MIN = 10L;
    //public static final long MCT_MAX = 551L; // maximum found in MCT file < 999

    public static final double MAX_DETOUR_TWO_LEG = 2.5;
    public static final double MAX_DETOUR_THREE_LEG = 1.5;

    public static final long WINDOW_SIZE = 48L * 60L * 60L * 1000L; // 48 hours
    //public static final int MAX_WINDOW_ID = (int) Math.ceil((2.0 * (END - START)) / (double) WINDOW_SIZE);

    private static final JoinOperatorBase.JoinHint JOIN_HINT = JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE;

    private static final FileSystem.WriteMode WRITE_MODE = FileSystem.WriteMode.OVERWRITE;


    public static void main(String[] args) throws Exception {
        int phase = 0;
        if (args.length == 1) {
            phase = Integer.parseInt(args[0]);
        }
        // the program is split into two parts (building and parsing all non-stop connections, and finding multi-leg flights)
        if (phase == 0) {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // get all relevant schedule data
            DataSet<Flight> extracted = env.readTextFile(schedulePath).flatMap(new FilteringUTCExtractor());

            // extract coordinates of all known airports
            DataSet<Tuple7<String, String, String, String, String, Double, Double>> airportCoordinatesNR =
                    env.readTextFile(oriPath).flatMap(new AirportCoordinateExtractor());

            // get IATA region information for each airport
            DataSet<Tuple2<String, String>> regionInfo = env.readTextFile(regionPath).map(new RegionExtractor());
            DataSet<Tuple7<String, String, String, String, String, Double, Double>> airportCoordinates =
                    airportCoordinatesNR.join(regionInfo).where(3).equalTo(0).with(new RegionJoiner());

            /*KeySelector<Flight, String> jk1 = new KeySelector<Flight, String>() {
                public String getKey(Flight tuple) {
                    return tuple.getOriginAirport();
                }
            };
            KeySelector<Flight, String> jk2 = new KeySelector<Flight, String>() {
                public String getKey(Flight tuple) {
                    return tuple.getDestinationAirport();
                }
            };*/

            DataSet<Flight> join1 = extracted.join(airportCoordinates, JOIN_HINT)
                    .where("f0.f0").equalTo(0).with(new OriginCoordinateJoiner());
            DataSet<Flight> join2 = join1.join(airportCoordinates, JOIN_HINT)
                    .where("f2.f0").equalTo(0).with(new DestinationCoordinateJoiner());

            // add aircraft capacities (first default then overwrite with airline specific information if available)
            DataSet<Tuple2<String, Integer>> defaultCapacities = env.readCsvFile(defaultCapacityPath).types(String.class, Integer.class);
            DataSet<Flight> join3 = join2.join(defaultCapacities).where("f6").equalTo(0).with(new DefaultCapacityJoiner());

            DataSet<Tuple3<String, String, Integer>> aircraftCapacities = env.readCsvFile(capacityPath).fieldDelimiter('^').ignoreFirstLine()
                    .includeFields(false, true, true, true, false, false, false, false).types(String.class, String.class, Integer.class);

            DataSet<Flight> join4 = join3.coGroup(aircraftCapacities).where("f6", "f4").equalTo(0, 1).with(new CapacityGrouper());

            /*KeySelector<Flight, Tuple3<String, String, Integer>> ml1 = new KeySelector<Flight, Tuple3<String, String, Integer>>() {
                public Tuple3<String, String, Integer> getKey(Flight tuple) {
                    return new Tuple3<String, String, Integer>(tuple.getOriginCity(), tuple.getAirline(), tuple.getFlightNumber());
                }
            };
            KeySelector<Flight, Tuple3<String, String, Integer>> ml2 = new KeySelector<Flight, Tuple3<String, String, Integer>>() {
                 public Tuple3<String, String, Integer> getKey(Flight tuple) {
                     return new Tuple3<String, String, Integer>(tuple.getDestinationCity(), tuple.getAirline(), tuple.getFlightNumber());
                 }
            };*/

            // create multi-leg flights as non-stop flights
            DataSet<Flight> multiLeg2a = join4.join(join4, JOIN_HINT).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg2b = join4.join(join4, JOIN_HINT).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg2 = multiLeg2a.union(multiLeg2b);

            DataSet<Flight> multiLeg3a = multiLeg2.join(join4, JOIN_HINT).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg3b = multiLeg2.join(join4, JOIN_HINT).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg3 = multiLeg3a.union(multiLeg3b);

            DataSet<Flight> multiLeg4a = multiLeg2.join(multiLeg2, JOIN_HINT).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg4b = multiLeg2.join(multiLeg2, JOIN_HINT).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg4 = multiLeg4a.union(multiLeg4b);

            DataSet<Flight> multiLeg5a = multiLeg2.join(multiLeg3, JOIN_HINT).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg5b = multiLeg2.join(multiLeg3, JOIN_HINT).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg5 = multiLeg5a.union(multiLeg5b);

            DataSet<Flight> multiLeg6a = multiLeg3.join(multiLeg3, JOIN_HINT).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg6b = multiLeg3.join(multiLeg3, JOIN_HINT).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg6 = multiLeg6a.union(multiLeg6b);

            DataSet<Flight> multiLeg7a = multiLeg3.join(multiLeg4, JOIN_HINT).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg7b = multiLeg3.join(multiLeg4, JOIN_HINT).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg7 = multiLeg7a.union(multiLeg7b);

            DataSet<Flight> multiLeg8a = multiLeg4.join(multiLeg4, JOIN_HINT).where("f2.f2", "f4", "f5", "f13")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg8b = multiLeg4.join(multiLeg4, JOIN_HINT).where("f2.f2", "f4", "f5", "f14")
                    .equalTo("f0.f2", "f4", "f5", "f12").with(new MultiLegJoiner());
            DataSet<Flight> multiLeg8 = multiLeg8a.union(multiLeg8b);

            DataSet<Flight> singleFltNoFlights = join4.union(multiLeg2).union(multiLeg3).union(multiLeg4).union(multiLeg5).union(multiLeg6).union(multiLeg7).union(multiLeg8);

            FileOutputFormat nonStop = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath + "one"));
            singleFltNoFlights.filter(new NonStopTrafficRestrictionsFilter()).write(nonStop, outputPath + "one", WRITE_MODE);

            FileOutputFormat nonStopFull = new FlightOutput.NonStopFullOutputFormat();
            singleFltNoFlights.write(nonStopFull, outputPath + "oneFull", WRITE_MODE);


            env.execute("Phase 0");
            //System.out.println(env.getExecutionPlan());
        } else if (phase == 1) {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Flight> singleFltNoFlights2 = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull/");

            /*KeySelector<Flight, String> tl1 = new KeySelector<Flight, String>() {
                public String getKey(Flight tuple) {
                    return tuple.getOriginAirport();
                }
            };
            KeySelector<Flight, String> tl2 = new KeySelector<Flight, String>() {
                public String getKey(Flight tuple) {
                    return tuple.getDestinationAirport();
                }
            };*/

            DataSet<Tuple2<Flight, Flight>> twoLegConnections1 = singleFltNoFlights2.join(singleFltNoFlights2, JOIN_HINT)
                    .where("f2.f2", "f13").equalTo("f0.f2", "f12").with(new FilteringConnectionJoiner());
            DataSet<Tuple2<Flight, Flight>> twoLegConnections2 = singleFltNoFlights2.join(singleFltNoFlights2, JOIN_HINT)
                    .where("f2.f2", "f14").equalTo("f0.f2", "f12").with(new FilteringConnectionJoiner());

            DataSet<Tuple2<Flight, Flight>> twoLegConnections = twoLegConnections1.union(twoLegConnections2);

            DataSet<MCTEntry> mctData = env.readTextFile(mctPath).flatMap(new MCTParser());

            DataSet<Tuple2<Flight, Flight>> twoLegConnectionsFiltered = twoLegConnections.coGroup(mctData).where("f0.f2.f0").equalTo(0).with(new MCTFilter());

            FileOutputFormat twoLeg = new FlightOutput.TwoLegFlightOutputFormat(new Path(outputPath + "two"));
            twoLegConnectionsFiltered.write(twoLeg, outputPath + "two", WRITE_MODE);

            FileOutputFormat twoLegFull = new FlightOutput.TwoLegFullOutputFormat();
            twoLegConnectionsFiltered.write(twoLegFull, outputPath + "twoFull", WRITE_MODE);

            /*KeySelector<Tuple2<Flight, Flight>, String> ks0 = new KeySelector<Tuple2<Flight, Flight>, String>() {
                public String getKey(Tuple2<Flight, Flight> tuple) {
                    return tuple.f0.getKey();
                }
            };
            KeySelector<Tuple2<Flight, Flight>, String> ks1 = new KeySelector<Tuple2<Flight, Flight>, String>() {
                public String getKey(Tuple2<Flight, Flight> tuple) {
                    return tuple.f1.getKey();
                }
            };*/

            DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = twoLegConnectionsFiltered.join(twoLegConnectionsFiltered, JOIN_HINT)
                    .where("f1.f0.f0", "f1.f1", "f1.f2.f0", "f1.f3", "f1.f4", "f1.f5").equalTo("f0.f0.f0", "f0.f1", "f0.f2.f0", "f0.f3", "f0.f4", "f0.f5").with(new ThreeLegJoiner());

            FileOutputFormat threeLeg = new FlightOutput.ThreeLegFlightOutputFormat(new Path(outputPath + "three"));
            threeLegConnections.write(threeLeg, outputPath + "three", WRITE_MODE);

            FileOutputFormat threeLegFull = new FlightOutput.ThreeLegFullOutputFormat();
            threeLegConnections.write(threeLegFull, outputPath + "threeFull", WRITE_MODE);

            env.execute("Phase 1");
            //System.out.println(env.getExecutionPlan());
        } else {
            throw new Exception("Invalid parameter! phase: " + phase);
        }
    }
}

package de.tuberlin.dima.ti.pcb;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.util.Properties;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * main connection builder class that defines data flow
 */
public class ParallelConnectionBuilder {

    //public static final Logger LOG = LoggerFactory.getLogger(ParallelConnectionBuilder.class);

    private static final String PROTOCOL = "hdfs:///";

    /** HDFS default paths **/
    private static String schedulePathKey = "SCHEDULE_PATH_PROPERTY_KEY";
    private static String schedulePathDefault = PROTOCOL + "tmp/waury/input/all_catalog_140417.txt";

    private static String oriPathKey = "ORI_PATH_PROPERTY_KEY";
    private static String oriPathDefault = PROTOCOL + "tmp/waury/input/optd_por_public.csv";

    private static String regionPathKey = "REGION_PATH_PROPERTY_KEY";
    private static String regionPathDefault = PROTOCOL + "tmp/waury/input/ori_country_region_info.csv";

    private static String defaultCapacityPathKey = "DEFAULT_CAP_PATH_PROPERTY_KEY";
    private static String defaultCapacityPathDefault = PROTOCOL + "tmp/waury/input/default_capacities.csv";

    private static String capacityPathKey = "CAPACITY_PATH_PROPERTY_KEY";
    private static String capacityPathDefault = PROTOCOL + "tmp/waury/input/capacities_2014-07-01.csv";

    private static String mctPathKey = "MCT_PATH_PROPERTY_KEY";
    private static String mctPathDefault = PROTOCOL + "tmp/waury/input/mct.csv";

    private static String outputPathKey = "OUTPUT_PATH_PROPERTY_KEY";
    private static String outputPathDefault = PROTOCOL + "tmp/waury/output/benchmark/50days/";

    /** date range as 64 bit timestamps **/
    public static final long START = 1399248000000L;
    public static final long END = 1403567999000L;


    /** CB parameters **/
    public static final long MCT_MIN = 10L; // minimum allowed minimum connecting time in minutes
    //public static final long MCT_MAX = 551L; // maximum found in MCT file < 999

    public static final double MAX_DETOUR_TWO_LEG = 2.5;
    public static final double MAX_DETOUR_THREE_LEG = 1.5;

    public static final long WINDOW_SIZE = 48L * 60L * 60L * 1000L; // 48 hours
    //public static final int MAX_WINDOW_ID = (int) Math.ceil((2.0 * (END - START)) / (double) WINDOW_SIZE);

    /** Flink-specific **/
    private static final JoinOperatorBase.JoinHint JOIN_HINT = JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE;

    private static final FileSystem.WriteMode WRITE_MODE = FileSystem.WriteMode.OVERWRITE;

    public static void main(String[] args) throws Exception {
        /*Options options = setupOptions();
        CommandLineParser parser = new GnuParser();
        try {
            CommandLine line = parser.parse(options, args);
        } catch(ParseException e) {
            System.err.println( "Parsing failed. Reason: " + e.getMessage() );
        }*/

        Properties defaults = setupPropertyDefaults();
        Properties properties = new Properties(defaults);
        boolean fullCB = true;

        //executeCBPhase0(properties);
        if(fullCB) {
            executeCBPhase1(properties);
        }

    }

    /**
     * build multi-leg connections from schedule
     *
     * @param properties
     * @throws Exception
     */
    private static void executeCBPhase0(Properties properties) throws Exception {
        String schedulePath = properties.getProperty(schedulePathKey, schedulePathDefault);
        String oriPath = properties.getProperty(oriPathKey, oriPathDefault);
        String regionPath = properties.getProperty(regionPathKey, regionPathDefault);
        String defaultCapacityPath = properties.getProperty(defaultCapacityPathKey, defaultCapacityPathDefault);
        String capacityPath = properties.getProperty(capacityPathKey, capacityPathDefault);

        String outputPath = properties.getProperty(outputPathKey, outputPathDefault);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get all relevant schedule data
        DataSet<Flight> extracted = env.readTextFile(schedulePath).flatMap(new FilteringUTCExtractor());

        // extract coordinates of all known airports
        DataSet<Tuple8<String, String, String, String, String, Double, Double, String>> airportCoordinatesNR =
                env.readTextFile(oriPath).flatMap(new AirportCoordinateExtractor());

        // get IATA region information for each airport
        DataSet<Tuple2<String, String>> regionInfo = env.readTextFile(regionPath).map(new RegionExtractor());
        DataSet<Tuple8<String, String, String, String, String, Double, Double, String>> airportCoordinates =
                airportCoordinatesNR.join(regionInfo).where(3).equalTo(0).with(new RegionJoiner());

        // join with coordinates
        DataSet<Flight> join1 = extracted.join(airportCoordinates, JOIN_HINT)
                .where("f0.f0").equalTo(0).with(new OriginCoordinateJoiner());
        DataSet<Flight> join2 = join1.join(airportCoordinates, JOIN_HINT)
                .where("f2.f0").equalTo(0).with(new DestinationCoordinateJoiner());

        // add aircraft capacities (first default then overwrite with airline specific information if available)
        DataSet<Tuple2<String, Integer>> defaultCapacities = env.readCsvFile(defaultCapacityPath).types(String.class, Integer.class);
        DataSet<Flight> join3 = join2.join(defaultCapacities).where("f6").equalTo(0).with(new DefaultCapacityJoiner());

        DataSet<Tuple3<String, String, Integer>> aircraftCapacities = env.readCsvFile(capacityPath).fieldDelimiter('^').ignoreFirstLine()
                .includeFields(false, true, true, true, false, false, false, false).types(String.class, String.class, Integer.class);

        DataSet<Flight> join4 = join3.coGroup(aircraftCapacities).where("f6","f4").equalTo(0, 1).with(new CapacityGrouper());

        // create multi-leg flights as non-stop flights
        DataSet<Flight> multiLeg2a = join4.join(join4, JOIN_HINT).where("f2.f0", "f4", "f5", "f13")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg2b = join4.join(join4, JOIN_HINT).where("f2.f0", "f4", "f5", "f14")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg2 = multiLeg2a.union(multiLeg2b);

        DataSet<Flight> multiLeg3a = multiLeg2.join(join4, JOIN_HINT).where("f2.f0", "f4", "f5", "f13")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg3b = multiLeg2.join(join4, JOIN_HINT).where("f2.f0", "f4", "f5", "f14")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg3 = multiLeg3a.union(multiLeg3b);

        DataSet<Flight> multiLeg4a = multiLeg2.join(multiLeg2, JOIN_HINT).where("f2.f0", "f4", "f5", "f13")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg4b = multiLeg2.join(multiLeg2, JOIN_HINT).where("f2.f0", "f4", "f5", "f14")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg4 = multiLeg4a.union(multiLeg4b);

        DataSet<Flight> multiLeg5a = multiLeg2.join(multiLeg3, JOIN_HINT).where("f2.f0", "f4", "f5", "f13")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg5b = multiLeg2.join(multiLeg3, JOIN_HINT).where("f2.f0", "f4", "f5", "f14")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg5 = multiLeg5a.union(multiLeg5b);

        /*DataSet<Flight> multiLeg6a = multiLeg3.join(multiLeg3, JOIN_HINT).where("f2.f0", "f4", "f5", "f13")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg6b = multiLeg3.join(multiLeg3, JOIN_HINT).where("f2.f0", "f4", "f5", "f14")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg6 = multiLeg6a.union(multiLeg6b);

        DataSet<Flight> multiLeg7a = multiLeg3.join(multiLeg4, JOIN_HINT).where("f2.f0", "f4", "f5", "f13")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg7b = multiLeg3.join(multiLeg4, JOIN_HINT).where("f2.f0", "f4", "f5", "f14")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg7 = multiLeg7a.union(multiLeg7b);

        DataSet<Flight> multiLeg8a = multiLeg4.join(multiLeg4, JOIN_HINT).where("f2.f0", "f4", "f5", "f13")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg8b = multiLeg4.join(multiLeg4, JOIN_HINT).where("f2.f0", "f4", "f5", "f14")
                .equalTo("f0.f0", "f4", "f5", "f12").with(new MultiLegJoiner());
        DataSet<Flight> multiLeg8 = multiLeg8a.union(multiLeg8b);*/

        DataSet<Flight> singleFltNoFlights = join4.union(multiLeg2).union(multiLeg3).union(multiLeg4).union(multiLeg5);//.union(multiLeg6).union(multiLeg7).union(multiLeg8);

        FileOutputFormat nonStop = new FlightOutput.NonStopFlightOutputFormat(new Path(outputPath + "one"));
        singleFltNoFlights.filter(new NonStopTrafficRestrictionsFilter()).write(nonStop, outputPath + "one", WRITE_MODE);

        FileOutputFormat nonStopFull = new FlightOutput.NonStopFullOutputFormat();
        singleFltNoFlights.write(nonStopFull, outputPath + "oneFull", WRITE_MODE);
        //System.out.println(env.getExecutionPlan());
        env.execute("Phase 0");
    }

    /**
     * build two and three leg connections from multi-leg connections created in executeCBPhase1
     *
     * @param properties
     * @throws Exception
     */
    private static void executeCBPhase1(Properties properties) throws Exception {
        String mctPath = properties.getProperty(mctPathKey, mctPathDefault);
        String outputPath = properties.getProperty(outputPathKey, outputPathDefault);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Flight> singleFltNoFlights2 = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull/");

        DataSet<Tuple2<Flight, Flight>> twoLegConnections1 = singleFltNoFlights2.join(singleFltNoFlights2, JOIN_HINT)
                .where("f2.f2", "f13").equalTo("f0.f2", "f12").with(new FilteringConnectionJoiner());
        DataSet<Tuple2<Flight, Flight>> twoLegConnections2 = singleFltNoFlights2.join(singleFltNoFlights2, JOIN_HINT)
                .where("f2.f2", "f14").equalTo("f0.f2", "f12").with(new FilteringConnectionJoiner());

        DataSet<Tuple2<Flight, Flight>> twoLegConnections = twoLegConnections1.union(twoLegConnections2);

        DataSet<MCTEntry> mctData = env.readTextFile(mctPath).flatMap(new MCTParser());

        // check MCT rules
        DataSet<Tuple2<Flight, Flight>> twoLegConnectionsFiltered = twoLegConnections.coGroup(mctData).where("f0.f2.f0").equalTo(0).with(new MCTFilter());

        FileOutputFormat twoLeg = new FlightOutput.TwoLegFlightOutputFormat(new Path(outputPath + "two"));
        twoLegConnectionsFiltered.write(twoLeg, outputPath + "two", WRITE_MODE);

        FileOutputFormat twoLegFull = new FlightOutput.TwoLegFullOutputFormat();
        twoLegConnectionsFiltered.write(twoLegFull, outputPath + "twoFull", WRITE_MODE);

        DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = twoLegConnectionsFiltered.join(twoLegConnectionsFiltered, JOIN_HINT)
                .where("f1.f0.f0", "f1.f1", "f1.f2.f0", "f1.f3", "f1.f4", "f1.f5").equalTo("f0.f0.f0", "f0.f1", "f0.f2.f0", "f0.f3", "f0.f4", "f0.f5").with(new ThreeLegJoiner());

        FileOutputFormat threeLeg = new FlightOutput.ThreeLegFlightOutputFormat(new Path(outputPath + "three"));
        threeLegConnections.write(threeLeg, outputPath + "three", WRITE_MODE);

        FileOutputFormat threeLegFull = new FlightOutput.ThreeLegFullOutputFormat();
        threeLegConnections.write(threeLegFull, outputPath + "threeFull", WRITE_MODE);

        //System.out.println(env.getExecutionPlan());
        env.execute("Phase 1");
    }

    /** setup helper functions **/

    private static Options setupOptions() {
        Options options = new Options();

        Option schedulePath = OptionBuilder.withArgName("schedulePath").isRequired().hasArg().
                withDescription("path to SSIM7 schedule in CSV format").create("schedule");
        options.addOption(schedulePath);

        //Option oriPath = OptionBuilder.withArgName()
        return options;
    }

    private static Properties setupPropertyDefaults() {
        Properties defaults = new Properties();
        defaults.setProperty(schedulePathKey, schedulePathDefault);
        defaults.setProperty(oriPathKey, oriPathDefault);
        defaults.setProperty(regionPathKey, regionPathDefault);
        defaults.setProperty(defaultCapacityPathKey, defaultCapacityPathDefault);
        defaults.setProperty(capacityPathKey, capacityPathDefault);
        defaults.setProperty(mctPathKey, mctPathDefault);
        defaults.setProperty(outputPathKey, outputPathDefault);
        return defaults;
    }
}

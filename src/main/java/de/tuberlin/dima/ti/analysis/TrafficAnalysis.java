package de.tuberlin.dima.ti.analysis;

import de.tuberlin.dima.ti.pcb.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class TrafficAnalysis {

    // calendar week 19 2014
    public static long firstPossibleTimestamp = 1399248000000L;
    public static long lastPossibleTimestamp = 1399852799000L;

    public static final double SLF = 0.797;

    public static final double WAITING_FACTOR = 1.0;

    public static final int MAX_ITERATIONS = 10;

    public static final double OPTIMIZER_TOLERANCE = 0.000001;

    public static final int MAX_OPTIMIZER_ITERATIONS = 1000;

    public static String AP_GEO_DATA = "AirportGeoDataBroadcastSet";

    public static String INVERTED_COVARIANCE_MATRIX = "InvertedCovarianceMatrixBroadcastSet";

    public static final int OD_FEATURE_COUNT = 6;

    public static final double p1 = 1.0;
    public static final double p15 = 1.5;
    public static final double p2 = 2.0;

    public final static ArrayList<String> countriesWithStates = new ArrayList<String>() {{
        add("AR");
        add("AU");
        add("BR");
        add("CA");
        add("US");
    }};

    public static final boolean US_ONLY = false;
    // black/white hole for non-domestic US traffic
    public final static String NON_US_POINT = "XXX";
    public final static String NON_US_CITY = "XXX";
    public final static String NON_US_STATE = "XX";
    public final static String NON_US_COUNTRY = "XX";
    public final static String NON_US_REGION = "XXX";
    public final static String NON_US_ICAO = "XXXX";
    // somewhere in the Indian Ocean halfway between Australia's west coast and the Kerguelen Islands
    // exact opposite of the mean center of the US population
    public final static double NON_US_LATITUDE = -37.458905;
    public final static double NON_US_LONGITUDE = 87.69223399999998;


    public final static SimpleDateFormat dayFormat = new SimpleDateFormat("ddMMyyyy");

    private static String PROTOCOL = "hdfs:///";

    private static String oriPath = PROTOCOL + "user/rwaury/input2/ori_por_public.csv";
    private static String regionPath = PROTOCOL + "user/rwaury/input2/ori_country_region_info.csv";
    private static String midtPath = PROTOCOL + "user/rwaury/input2/MIDTTotalHits.csv";
    private static String outputPath = PROTOCOL + "user/rwaury/output3/flights/";

    private static final JoinOperatorBase.JoinHint JOIN_HINT = JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE;
    private static final FileSystem.WriteMode OVERWRITE = FileSystem.WriteMode.OVERWRITE;

    public static void main(String[] args) throws Exception {
        ti(true,    p1,     true, false, false);
        ti(true,    p15,    true, false, false);
        ti(true,    p2,     true, false, false);
        ti(false,   p1,     true, false, false);
        ti(false,   p15,    true, false, false);
        ti(false,   p2,     true, false, false);

        ti(true,    p1,     false, true, false);
        ti(true,    p15,    false, true, false);
        ti(true,    p2,     false, true, false);
        ti(false,   p1,     false, true, false);
        ti(false,   p15,    false, true, false);
        ti(false,   p2,     false, true, false);

        ti(true,    p1,     false, false, true);
        ti(true,    p15,    false, false, true);
        ti(true,    p2,     false, false, true);
        ti(false,   p1,     false, false, true);
        ti(false,   p15,    false, false, true);
        ti(false,   p2,     false, false, true);
    }

    public static void ti(boolean useTime, double p, boolean noPartition, boolean DIPartition, boolean fullPartition) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // extract coordinates of all known airports
        DataSet<Tuple8<String, String, String, String, String, Double, Double, String>> airportCoordinatesNR =
                env.readTextFile(oriPath).flatMap(new AirportCoordinateExtractor());

        // get IATA region information for each airport
        DataSet<Tuple2<String, String>> regionInfoPartial = env.readTextFile(regionPath).map(new RegionExtractor());
        //DataSet<Tuple2<String, String>> regionInfoXXX = env.fromElements(new Tuple2<String, String>(NON_US_COUNTRY, NON_US_REGION));
        DataSet<Tuple2<String, String>> regionInfo = regionInfoPartial;//.union(regionInfoXXX);
        //regionInfo.writeAsCsv(outputPath + "regionInfo", "\n", ",", OVERWRITE).setParallelism(1);

        DataSet<Tuple8<String, String, String, String, String, Double, Double, String>> airportCountry =
                airportCoordinatesNR.join(regionInfo).where(3).equalTo(0).with(new RegionJoiner());

        //airportCountry.writeAsCsv(outputPath + "airportCountry", "\n", ",", OVERWRITE).setParallelism(1);

        DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull");
//                .flatMap(new GeoInfoReplacer.GeoInfoReplacerUS1());
        DataSet<Tuple2<Flight, Flight>> twoLegConnections = env.readFile(new FlightOutput.TwoLegFullInputFormat(), outputPath + "twoFull");
//                .flatMap(new GeoInfoReplacer.GeoInfoReplacerUS2());
        DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), outputPath + "threeFull");
//                .flatMap(new GeoInfoReplacer.GeoInfoReplacerUS3());

        DataSet<Itinerary> nonStopItineraries = nonStopConnections.flatMap(new FlightExtractor.FlightExtractor1());
        DataSet<Itinerary> twoLegItineraries = twoLegConnections.flatMap(new FlightExtractor.FlightExtractor2());
        DataSet<Itinerary> threeLegItineraries = threeLegConnections.flatMap(new FlightExtractor.FlightExtractor3());
        DataSet<Itinerary> itineraries = nonStopItineraries.union(twoLegItineraries).union(threeLegItineraries);

        /*DataSet<Tuple3<String, String, Integer>> flightBounds = nonStopConnections.map(new MapFunction<Flight, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(Flight flight) throws Exception {
                Date d = new Date(flight.getDepartureTimestamp());
                return new Tuple3<String, String, Integer>(flight.getAirline() + flight.getFlightNumber(), dayFormat.format(d), flight.getMaxCapacity());
            }
        }).groupBy(0,1).min(2);
        flightBounds.writeAsCsv(outputPath + "flightCapacity", "\n", ",", OVERWRITE).setParallelism(1);*/

        DataSet<String> midtStrings = env.readTextFile(midtPath);
        DataSet<MIDT> midt = midtStrings.flatMap(new MIDTParser()).withBroadcastSet(airportCountry, AP_GEO_DATA)
                .map(new MIDTCompressor()).groupBy(0,1,2,3,4,5,6,7).reduceGroup(new MIDTGrouper());
        DataSet<Tuple5<String, String, String, Integer, Integer>> ODLowerBound = midt.map(new LowerBoundExtractor()).groupBy(0,1,2).sum(3).andSum(4);

        // group sort and first-1 is necessary to exclude multileg connections that yield the same OD (only the fastest is included)
        DataSet<Itinerary> itinerariesWithMIDT = itineraries.groupBy(0,1,2,3,4,5,6,7).sortGroup(10, Order.ASCENDING).first(1).coGroup(midt)
                .where(0,1,2,3,4,5,6,7).equalTo(0,1,2,3,4,5,6,7).with(new ItineraryMIDTMerger());

        DataSet<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> APBoundsAgg = midtStrings.flatMap(new MIDTCapacityEmitter(noPartition, DIPartition, fullPartition))
                .withBroadcastSet(airportCountry, AP_GEO_DATA);
        DataSet<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> APBounds = APBoundsAgg.groupBy(0,1,2,3,4).sum(5).andSum(6);
        //APBounds.writeAsCsv(outputPath + "APBounds", "\n", ",", OVERWRITE).setParallelism(1);

        DataSet<Tuple5<String, String, String, Integer, Integer>> ODMax = itinerariesWithMIDT.groupBy(0,1,2).reduceGroup(new ODMax());
        //ODMax.writeAsCsv(outputPath + "ODMax", "\n", ",", OVERWRITE).setParallelism(1);

        DataSet<Tuple5<String, String, String, Integer, Integer>> ODBounds =
                ODLowerBound.coGroup(ODMax).where(0,1,2).equalTo(0,1,2).with(new ODBoundMerger());
        //ODBounds.writeAsCsv(outputPath + "ODBounds", "\n", ",", OVERWRITE).setParallelism(1);

        DataSet<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> inOutCapa =
                nonStopConnections.flatMap(new APCapacityExtractor(noPartition, DIPartition, fullPartition));

        // in and out loads of airports per day and as intercontinental/intracontinental,
        // international/domestic and interstate/innerstate (three boolean flags (f2-4))
        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> outgoingMarginalsAgg =
                inOutCapa.groupBy(0,1,2,3,4).reduceGroup(new OutgoingMarginalsReducer());
        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> incomingMarginalsAgg =
                inOutCapa.groupBy(0,1,2,3,4).reduceGroup(new IncomingMarginalsReducer());

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> outgoingMarginals =
                outgoingMarginalsAgg.join(APBounds, JOIN_HINT).where(0,1,2,3,4).equalTo(0,1,2,3,4).with(
                new JoinFunction<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>,
                        Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>,
                        Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>>() {
                    @Override
                    public Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> join(
                            Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal,
                            Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> midtBound) throws Exception {
                        int estimateResidual = Math.max(marginal.f5 - midtBound.f5, 0);
                        return new Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>
                                (marginal.f0, marginal.f1, marginal.f2, marginal.f3, marginal.f4, estimateResidual, marginal.f6, marginal.f7);
                    }
                });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> incomingMarginals =
                incomingMarginalsAgg.join(APBounds, JOIN_HINT).where(0,1,2,3,4).equalTo(0,1,2,3,4).with(
                new JoinFunction<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>,
                        Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>,
                        Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>>() {
                    @Override
                    public Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> join(
                            Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal,
                            Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> midtBound) throws Exception {
                        int estimateResidual = Math.max(marginal.f5 - midtBound.f6, 0);
                        return new Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>
                                (marginal.f0, marginal.f1, marginal.f2, marginal.f3, marginal.f4, estimateResidual, marginal.f6, marginal.f7);
                    }
                });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> nonStop =
                nonStopConnections.flatMap(new FlightDistanceExtractor.FlightDistanceExtractor1());

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> twoLeg =
                twoLegConnections.flatMap(new FlightDistanceExtractor.FlightDistanceExtractor2());

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> threeLeg =
                threeLegConnections.flatMap(new FlightDistanceExtractor.FlightDistanceExtractor3());

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> flights = nonStop.union(twoLeg).union(threeLeg);
        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>> distancesWithoutMaxODTraffic =
                flights.groupBy(0,1,2,3,4,5).reduceGroup(new ODDistanceAggregator());

        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>> distances =
                distancesWithoutMaxODTraffic.join(ODBounds).where(0,1,2).equalTo(0,1,2).with(new MaxODCapacityJoiner());

        /* IPF START */
        IterativeDataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> initial =
                outgoingMarginals.union(incomingMarginals).iterate(MAX_ITERATIONS);

        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>> KiFractions = distances
                .join(initial.filter(new IncomingFilter()), JOIN_HINT).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new KJoiner(useTime, p));

        outgoingMarginals = KiFractions.groupBy(0,2,3,4,5).sum(6)
                .join(initial.filter(new OutgoingFilter()), JOIN_HINT).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new KUpdater());

        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>> KjFractions = distances
                .join(outgoingMarginals, JOIN_HINT).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new KJoiner(useTime, p));

        incomingMarginals = KjFractions.groupBy(1,2,3,4,5).sum(6)
                .join(initial.filter(new IncomingFilter()), JOIN_HINT).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new KUpdater());

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> iteration = outgoingMarginals.union(incomingMarginals);
        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> result = initial.closeWith(iteration);
        /* IPF END */

        DataSet<Tuple5<String, String, String, Double, SerializableVector>> trafficMatrix = distances
                .join(result.filter(new OutgoingFilter()), JOIN_HINT).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new TMJoinerOut(useTime, p))
                .join(result.filter(new IncomingFilter()), JOIN_HINT).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new TMJoinerIn())
                .project(0,1,2,6,7);

        DataSet<Tuple5<String, String, String, Double, SerializableVector>> TMWithMIDT =
                trafficMatrix.coGroup(ODBounds).where(0,1,2).equalTo(0,1,2).with(new TMMIDTMerger());

        String timestamp = Long.toString(System.currentTimeMillis());
        TMWithMIDT.project(0,1,3).groupBy(0,1).sum(2).writeAsCsv(outputPath + "trafficMatrix" + timestamp, "\n", ",");

        /*DataSet<Tuple4<String, String, String, LogitOptimizable>> trainedLogit =
                midt.groupBy(0,1,2).reduceGroup(new LogitTrainer());

        DataSet<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> TMWithWeights =
                TMWithMIDT.join(trainedLogit, JOIN_HINT).where(0,1,2).equalTo(0,1,2).with(new WeightTMJoiner());

        DataSet<Tuple5<String, String, String, Double, LogitOptimizable>> allWeighted =
                TMWithWeights.coGroup(TMWithMIDT).where(2).equalTo(2).with(new ODDistanceComparator());

        DataSet<Itinerary> estimate = itinerariesWithMIDT.coGroup(allWeighted).where(0,1,2).equalTo(0,1,2).with(new TrafficEstimator());

        estimate.groupBy(0,1,2).sortGroup(15, Order.DESCENDING).first(1000000000).writeAsCsv(outputPath + "ItineraryEstimate", "\n", ",", OVERWRITE);

        estimate.groupBy(0,1).reduceGroup(new ODSum()).writeAsCsv(outputPath + "ODSum", "\n", ",", OVERWRITE).setParallelism(1);
        */
        //System.out.println(env.getExecutionPlan());
        env.execute("TrafficAnalysis");
    }

    // IPF operator
    private static class KJoiner implements JoinFunction<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>,
            Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>,
            Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>> {

        private boolean useTime;
        private double p;

        public KJoiner(boolean useTime, double p) {
            this.useTime = useTime;
            this.p = p;
        }

        @Override
        public Tuple7<String, String, String, Boolean, Boolean, Boolean, Double> join(
                Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector> distance,
                Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            double KFraction = marginal.f5*marginal.f6*TAUtil.decayingFunction(
                    distance.f6.getVector().getEntry(0), distance.f6.getVector().getEntry(1), distance.f6.getVector().getEntry(2),
                    distance.f6.getVector().getEntry(3), distance.f6.getVector().getEntry(4), distance.f6.getVector().getEntry(5),
                    distance.f3, distance.f4, distance.f5, p , useTime);
            if(Double.isNaN(KFraction) || KFraction < 0.0) {
                KFraction = 0.0;
            }
            return new Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>(distance.f0, distance.f1, distance.f2, distance.f3, distance.f4, distance.f5, KFraction);
        }
    }

    // IPF operator
    private static class KUpdater implements JoinFunction<Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>,
            Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>,
            Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> {

        @Override
        public Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> join(
                Tuple7<String, String, String, Boolean, Boolean, Boolean, Double> Ksum,
                Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            double sum = Ksum.f6;
            //if(Double.isNaN(sum) || sum < 0.0) {
            //    sum = 1.0;
            //}
            marginal.f6 = 1.0/sum;
            return marginal;
        }
    }

    // IPF Operator
    private static class OutgoingFilter implements FilterFunction<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> {
        @Override
        public boolean filter(Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> tuple) throws Exception {
            return tuple.f7.booleanValue();
        }
    }

    // IPF Operator
    private static class IncomingFilter implements FilterFunction<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> {
        @Override
        public boolean filter(Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> tuple) throws Exception {
            return !tuple.f7.booleanValue();
        }
    }

    // IPF result to estimate I
    private static class TMJoinerOut implements JoinFunction<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>,
            Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>,
            Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector>> {

        private boolean useTime;
        private double p;

        public TMJoinerOut(boolean useTime, double p) {
            this.useTime = useTime;
            this.p = p;
        }

        @Override
        public Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector> join(
                Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector> distance,
                Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            double partialDist = marginal.f5*marginal.f6*TAUtil.decayingFunction(
                   distance.f6.getVector().getEntry(0), distance.f6.getVector().getEntry(1), distance.f6.getVector().getEntry(2),
                   distance.f6.getVector().getEntry(3), distance.f6.getVector().getEntry(4), distance.f6.getVector().getEntry(5),
                   distance.f3, distance.f4, distance.f5, p, useTime);
            if(Double.isNaN(partialDist) || partialDist < 0.0) {
                partialDist = 0.0;
            }
            return new Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector>
                    (distance.f0, distance.f1, distance.f2, distance.f3, distance.f4, distance.f5, partialDist, distance.f6);
        }
    }

    // IPF result to estimate II
    private static class TMJoinerIn implements JoinFunction<Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector>,
            Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>,
            Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector>> {

        @Override
        public Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector> join(
                Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector> tmOut,
                Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            double fullDistance = tmOut.f6*marginal.f5*marginal.f6;
            if(Double.isNaN(fullDistance) || fullDistance < 0.0) {
                fullDistance = 0.0;
            }
            return new Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector>
                    (tmOut.f0, tmOut.f1, tmOut.f2, tmOut.f3, tmOut.f4, tmOut.f5, fullDistance, tmOut.f7);
        }
    }

    // emit number of MIDT bookings for an OD
    private static class LowerBoundExtractor implements MapFunction<MIDT, Tuple5<String, String, String, Integer, Integer>> {
        @Override
        public Tuple5<String, String, String, Integer, Integer> map(MIDT midt) throws Exception {
            return new Tuple5<String, String, String, Integer, Integer>(midt.f0, midt.f1, midt.f2, midt.f11, 0);
        }
    }

    // add maximum OD capacity (from CB result) to OD feature vector
    private static class MaxODCapacityJoiner implements JoinFunction<
            Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>,
            Tuple5<String, String, String, Integer, Integer>,
            Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>> {

        @Override
        public Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector> join(
                Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector> distance,
                Tuple5<String, String, String, Integer, Integer> odBound) throws Exception {
            distance.f6.getVector().setEntry(5, (double)Math.max(odBound.f3, odBound.f4));
            return distance;
        }
    }

    // merge TM estimates and training results
    private static class WeightTMJoiner implements JoinFunction<Tuple5<String, String, String, Double, SerializableVector>,
            Tuple4<String, String, String, LogitOptimizable>,
            Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> {

        @Override
        public Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> join(
                Tuple5<String, String, String, Double, SerializableVector> tmEntry,
                Tuple4<String, String, String, LogitOptimizable> logit) throws Exception {
            return new Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>
                    (tmEntry.f0, tmEntry.f1, tmEntry.f2, tmEntry.f3, tmEntry.f4, logit.f3);
        }
    }

    private static class USFilter implements FilterFunction<Tuple8<String, String, String, String, String, Double, Double, String>> {

        @Override
        public boolean filter(Tuple8<String, String, String, String, String, Double, Double, String> geoData) throws Exception {
            return geoData.f3.equals("US");
        }
    }

}

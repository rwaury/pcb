package de.tuberlin.dima.ti.analysis;

import de.tuberlin.dima.ti.pcb.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;

public class ItineraryShareEstimation {


    public static String AP_GEO_DATA = "AirportGeoDataBroadcastSet";

    public final static SimpleDateFormat dayFormat = new SimpleDateFormat("ddMMyyyy");

    private static String PROTOCOL = "hdfs:///";

    private static String oriPath = PROTOCOL + "tmp/waury/input/optd_por_public.csv";
    private static String regionPath = PROTOCOL + "tmp/waury/input/ori_country_region_info.csv";
    private static String midtPath = PROTOCOL + "tmp/waury/input/MIDTTotalHits.csv";
    private static String db1bPath = PROTOCOL + "tmp/waury/input/db1b.csv";
    private static String cbOutputPath = PROTOCOL + "tmp/waury/output/benchmark/50days/";
    private static String outputPath = PROTOCOL + "tmp/waury/output/benchmark/50days/ise/";

    private static final JoinOperatorBase.JoinHint JOIN_HINT = JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE;
    private static final FileSystem.WriteMode OVERWRITE = FileSystem.WriteMode.OVERWRITE;

    public static void main(String[] args) throws Exception {
        ise(false, true, TrainingData.MIDT, Logit.MNL, -1.0);
        /*ise(false, false, TrainingData.MIDT, Logit.MNL, -1.0);

        //ise(true, true, TrainingData.DB1B, Logit.MNL, -1.0);
        //ise(true, false, TrainingData.DB1B, Logit.MNL, -1.0);
        //ise(false, true, TrainingData.DB1B, Logit.MNL, -1.0);
        //ise(false, false, TrainingData.DB1B, Logit.MNL, -1.0);

        ise(true, true, TrainingData.BOTH_MERGED, Logit.MNL, -1.0);
        ise(true, false, TrainingData.BOTH_MERGED, Logit.MNL, -1.0);
        ise(false, true, TrainingData.BOTH_MERGED, Logit.MNL, -1.0);
        ise(false, false, TrainingData.BOTH_MERGED, Logit.MNL, -1.0);

        ise(true, true, TrainingData.BOTH_US_SEPARATE, Logit.MNL, -1.0);
        ise(true, false, TrainingData.BOTH_US_SEPARATE, Logit.MNL, -1.0);
        ise(false, true, TrainingData.BOTH_US_SEPARATE, Logit.MNL, -1.0);
        ise(false, false, TrainingData.BOTH_US_SEPARATE, Logit.MNL, -1.0);


        ise(false, true, TrainingData.MIDT, Logit.PSL, 0.3);
        ise(false, false, TrainingData.MIDT, Logit.PSL, 0.3);
        ise(false, true, TrainingData.MIDT, Logit.PSL, 0.4);
        ise(false, false, TrainingData.MIDT, Logit.PSL, 0.4);
        ise(false, true, TrainingData.MIDT, Logit.PSL, 0.5);
        ise(false, false, TrainingData.MIDT, Logit.PSL, 0.5);
        ise(false, true, TrainingData.MIDT, Logit.PSL, 0.6);
        ise(false, false, TrainingData.MIDT, Logit.PSL, 0.6);
        ise(false, true, TrainingData.MIDT, Logit.PSL, 0.7);
        ise(false, false, TrainingData.MIDT, Logit.PSL, 0.7);
        ise(false, true, TrainingData.MIDT, Logit.PSL, 0.8);
        ise(false, false, TrainingData.MIDT, Logit.PSL, 0.8);
        ise(false, true, TrainingData.MIDT, Logit.PSL, 0.9);
        ise(false, false, TrainingData.MIDT, Logit.PSL, 0.9);
        ise(false, true, TrainingData.MIDT, Logit.PSL, 1.0);
        ise(false, false, TrainingData.MIDT, Logit.PSL, 1.0);


        ise(false, true, TrainingData.MIDT, Logit.CLOGIT, 0.3);
        //ise(false, false, TrainingData.MIDT, Logit.CLOGIT, 0.3);
        ise(false, true, TrainingData.MIDT, Logit.CLOGIT, 0.4);
        //ise(false, false, TrainingData.MIDT, Logit.CLOGIT, 0.4);
        ise(false, true, TrainingData.MIDT, Logit.CLOGIT, 0.5);
        //ise(false, false, TrainingData.MIDT, Logit.CLOGIT, 0.5);
        ise(false, true, TrainingData.MIDT, Logit.CLOGIT, 0.6);
        //ise(false, false, TrainingData.MIDT, Logit.CLOGIT, 0.6);
        ise(false, true, TrainingData.MIDT, Logit.CLOGIT, 0.7);
        //ise(false, false, TrainingData.MIDT, Logit.CLOGIT, 0.7);
        ise(false, true, TrainingData.MIDT, Logit.CLOGIT, 0.8);
        //ise(false, false, TrainingData.MIDT, Logit.CLOGIT, 0.8);
        ise(false, true, TrainingData.MIDT, Logit.CLOGIT, 0.9);
        //ise(false, false, TrainingData.MIDT, Logit.CLOGIT, 0.9);
        ise(false, true, TrainingData.MIDT, Logit.CLOGIT, 1.0);
        //ise(false, false, TrainingData.MIDT, Logit.CLOGIT, 1.0);*/
    }

    public static void ise(boolean spreadOverWeek, boolean useEuclidean, TrainingData td, Logit logit, double beta) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String regressionMethod;
        if(logit == Logit.MNL) {
            regressionMethod = "MNL";
        } else if(logit == Logit.CLOGIT) {
            regressionMethod = "CLOGIT";
        } else if(logit == Logit.PSL) {
            regressionMethod = "PSL";
        } else{
            throw new Exception("Unknown logit parameter: " + Logit.values()[logit.ordinal()]);
        }

        String ticketTrainingData;
        if(td == TrainingData.MIDT) {
            ticketTrainingData = "MIDT-only";
        } else if(td == TrainingData.DB1B) {
            ticketTrainingData = "DB1B-only";
        } else if(td == TrainingData.BOTH_MERGED) {
            ticketTrainingData = "MIDT-DB1B";
        } else if(td == TrainingData.BOTH_US_SEPARATE) {
            ticketTrainingData = "MIDT-DB1B-separate";
        } else {
            throw new Exception("Unknown data usage parameter: " + TrainingData.values()[td.ordinal()]);
        }

        String betaStr = "";
        if(logit != Logit.MNL) {
            betaStr = Double.toString(beta).replace('.', '-');
        }

        String distanceMetric = useEuclidean ? "Euclidean" : "Mahalanobis";

        String description;
        if(spreadOverWeek) {
            description = '_' + regressionMethod + '_' + ticketTrainingData + '_' + "spread" + '_' + distanceMetric;
        } else {
            description = '_' + regressionMethod + '_' + ticketTrainingData + '_' + distanceMetric + '_' + betaStr;
        }

        // extract coordinates of all known airports
        DataSet<Tuple8<String, String, String, String, String, Double, Double, String>> airportCoordinatesNR =
                env.readTextFile(oriPath).flatMap(new AirportCoordinateExtractor());

        // get IATA region information for each airport
        DataSet<Tuple2<String, String>> regionInfo = env.readTextFile(regionPath).map(new RegionExtractor());

        DataSet<Tuple8<String, String, String, String, String, Double, Double, String>> airportCountry =
                airportCoordinatesNR.join(regionInfo).where(3).equalTo(0).with(new RegionJoiner());

        DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), cbOutputPath + "oneFull");

        DataSet<Tuple2<Flight, Flight>> twoLegConnections = env.readFile(new FlightOutput.TwoLegFullInputFormat(), cbOutputPath + "twoFull");

        DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), cbOutputPath + "threeFull");

        DataSet<Itinerary> nonStopItineraries = nonStopConnections.flatMap(new FlightExtractor.FlightExtractor1());
        DataSet<Itinerary> twoLegItineraries = twoLegConnections.flatMap(new FlightExtractor.FlightExtractor2());
        DataSet<Itinerary> threeLegItineraries = threeLegConnections.flatMap(new FlightExtractor.FlightExtractor3());
        DataSet<Itinerary> itineraries = nonStopItineraries.union(twoLegItineraries).union(threeLegItineraries);

        DataSet<Tuple3<String, String, Integer>> flightBounds = nonStopConnections.map(new MapFunction<Flight, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(Flight flight) throws Exception {
                Date d = new Date(flight.getDepartureTimestamp());
                return new Tuple3<String, String, Integer>(flight.getAirline() + flight.getFlightNumber(), dayFormat.format(d), flight.getMaxCapacity());
            }
        }).groupBy(0,1).min(2);

        DataSet<String> midtStrings = env.readTextFile(midtPath);
        DataSet<MIDT> midt = midtStrings.flatMap(new MIDTParser()).withBroadcastSet(airportCountry, AP_GEO_DATA)
                .groupBy(0,1,2,3,4,5,6,7,14,15).reduceGroup(new MIDTGrouper());
        DataSet<Tuple5<String, String, String, Integer, Integer>> ODLowerBound = midt.map(new LowerBoundExtractor()).groupBy(0,1,2).sum(3).andSum(4);

        // group sort and first-1 is necessary to exclude multileg connections that yield the same OD (only the fastest is included)
        DataSet<Itinerary> itinerariesWithMIDT = itineraries.groupBy(0,1,2,3,4,5,6,7).sortGroup(10, Order.ASCENDING).first(1).coGroup(midt)
                .where(0,1,2,3,4,5,6,7,20,21).equalTo(0,1,2,3,4,5,6,7,14,15).with(new ItineraryMIDTMerger());

        DataSet<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> APBoundsAgg = midtStrings.flatMap(new MIDTCapacityEmitter(false, true, false))
                .withBroadcastSet(airportCountry, AP_GEO_DATA);
        DataSet<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> APBounds = APBoundsAgg.groupBy(0,1,2,3,4).sum(5).andSum(6);

        DataSet<Tuple5<String, String, String, Integer, Integer>> ODMax = itinerariesWithMIDT.groupBy(0,1,2).reduceGroup(new ODMax());

        DataSet<Tuple5<String, String, String, Integer, Integer>> ODBounds =
                ODLowerBound.coGroup(ODMax).where(0,1,2).equalTo(0,1,2).with(new ODBoundMerger());

        DataSet<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> inOutCapa =
                nonStopConnections.flatMap(new APCapacityExtractor(false, true, false));

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
                nonStopConnections.flatMap(new FlightDistanceExtractor.FlightDistanceExtractor1(false, true, false));

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> twoLeg =
                twoLegConnections.flatMap(new FlightDistanceExtractor.FlightDistanceExtractor2(false, true, false));

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> threeLeg =
                threeLegConnections.flatMap(new FlightDistanceExtractor.FlightDistanceExtractor3(false, true, false));

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> flights = nonStop.union(twoLeg).union(threeLeg);
        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>> distancesWithoutMaxODTraffic =
                flights.groupBy(0,1,2,3,4,5).reduceGroup(new ODDistanceAggregator());

        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>> distances =
                distancesWithoutMaxODTraffic.join(ODBounds).where(0,1,2).equalTo(0,1,2).with(new MaxODCapacityJoiner());

        /* IPF START */
        IterativeDataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> initial =
                outgoingMarginals.union(incomingMarginals).iterate(TrafficAnalysis.MAX_ITERATIONS);

        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>> KiFractions = distances
                .join(initial.filter(new IncomingFilter()), JOIN_HINT).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new KJoiner(false, 0.5));

        outgoingMarginals = KiFractions.groupBy(0,2,3,4,5).sum(6)
                .join(initial.filter(new OutgoingFilter()), JOIN_HINT).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new KUpdater());

        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>> KjFractions = distances
                .join(outgoingMarginals, JOIN_HINT).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new KJoiner(false, 0.5));

        incomingMarginals = KjFractions.groupBy(1,2,3,4,5).sum(6)
                .join(initial.filter(new IncomingFilter()), JOIN_HINT).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new KUpdater());

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> iteration = outgoingMarginals.union(incomingMarginals);
        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> result = initial.closeWith(iteration);
        /* IPF END */

        DataSet<Tuple5<String, String, String, Double, SerializableVector>> trafficMatrix = distances
                .join(result.filter(new OutgoingFilter()), JOIN_HINT).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new TMJoinerOut(false, 0.5))
                .join(result.filter(new IncomingFilter()), JOIN_HINT).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new TMJoinerIn())
                .project(0,1,2,6,7);

        DataSet<Tuple5<String, String, String, Double, SerializableVector>> TMWithMIDT =
                trafficMatrix.coGroup(ODBounds).where(0, 1, 2).equalTo(0, 1, 2).with(new TMMIDTMerger());

        DataSet<MIDT> trainingData;
        if(td == TrainingData.MIDT) {
            trainingData = midt;
        } else {
            DataSet<MIDT> db1b = env.readTextFile(db1bPath).flatMap(new DB1B2MIDTParser(spreadOverWeek))
                    .withBroadcastSet(airportCountry, AP_GEO_DATA).groupBy(0,1,2,3,4,5,6,7,14,15).reduceGroup(new MIDTGrouper());
            if(td == TrainingData.DB1B) {
                trainingData = db1b;
            } else if(td == TrainingData.BOTH_MERGED) {
                trainingData = midt.union(db1b);
            } else if(td == TrainingData.BOTH_US_SEPARATE) {
                trainingData = db1b.union(midt.filter(new USFilter()).withBroadcastSet(airportCountry, AP_GEO_DATA));
            } else {
                throw new Exception("Unknown data usage parameter: " + TrainingData.values()[td.ordinal()]);
            }
        }

        //TMWithMIDT.project(0,1,3).groupBy(0,1).sum(2).writeAsCsv(outputPath + "trafficMatrix" + description, "\n", ",").setParallelism(1);
        GroupReduceFunction reducer;
        if(logit == Logit.MNL) {
            reducer = new LogitTrainer();
        } else if(logit == Logit.CLOGIT) {
            reducer = new CLogitTrainer(beta);
        } else if(logit == Logit.PSL) {
            reducer = new PSLTrainer(beta);
        } else{
            throw new Exception("Unknown logit parameter: " + Logit.values()[logit.ordinal()]);
        }

        if(logit == Logit.MNL) {
            DataSet<Tuple4<String, String, String, LogitOptimizable>> trainedLogit = trainingData.groupBy(0, 1, 2).reduceGroup(reducer);

            DataSet<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> TMWithWeights =
                    TMWithMIDT.join(trainedLogit, JOIN_HINT).where(0,1,2).equalTo(0,1,2).with(new WeightTMJoiner());

            DataSet<Tuple5<String, String, String, Double, LogitOptimizable>> allWeighted =
                    TMWithWeights.coGroup(TMWithMIDT).where(2).equalTo(2).with(new ODDistanceComparator(useEuclidean));

            DataSet<Itinerary> estimate = itinerariesWithMIDT.coGroup(allWeighted).where(0,1,2).equalTo(0,1,2).with(new TrafficEstimator());

            estimate.map(new Itin2Agg()).groupBy(0,1,2,3,4,5,6).sum(7).writeAsCsv(outputPath + "ItineraryEstimateAgg" + description, "\n", ";", OVERWRITE);

            //estimate.groupBy(0,1,2).sortGroup(15, Order.DESCENDING).first(1000000000).writeAsCsv(outputPath + "ItineraryEstimate", "\n", ",", OVERWRITE);

            estimate.groupBy(0, 1).reduceGroup(new ODSum()).writeAsCsv(outputPath + "ODSum" + description, "\n", ",", OVERWRITE);
        } else {
            DataSet<Tuple4<String, String, String, PSLOptimizable>> trainedLogit = trainingData.groupBy(0,1,2).reduceGroup(reducer).withBroadcastSet(airportCountry, AP_GEO_DATA);

            DataSet<Tuple6<String, String, String, Double, SerializableVector, PSLOptimizable>> TMWithWeights =
                    TMWithMIDT.join(trainedLogit, JOIN_HINT).where(0,1,2).equalTo(0,1,2).with(new PSCFWeightTMJoiner());

            DataSet<Tuple5<String, String, String, Double, PSLOptimizable>> allWeighted =
                    TMWithWeights.coGroup(TMWithMIDT).where(2).equalTo(2).with(new PSCFODDistanceComparator(useEuclidean));

            DataSet<Itinerary> estimate = itinerariesWithMIDT.coGroup(allWeighted).where(0,1,2).equalTo(0,1,2).with(new PSCFTrafficEstimator(logit, beta)).withBroadcastSet(airportCountry, AP_GEO_DATA);

            estimate.map(new Itin2Agg()).groupBy(0,1,2,3,4,5,6).sum(7).writeAsCsv(outputPath + "ItineraryEstimateAgg" + description, "\n", ";", OVERWRITE);

            //estimate.groupBy(0,1,2).sortGroup(15, Order.DESCENDING).first(1000000000).writeAsCsv(outputPath + "ItineraryEstimate", "\n", ",", OVERWRITE);

            estimate.groupBy(0, 1).reduceGroup(new ODSum()).writeAsCsv(outputPath + "ODSum" + description, "\n", ",", OVERWRITE);
        }

        //System.out.println(env.getExecutionPlan());
        env.execute("ItineraryShareEstimation");
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

    // merge TM estimates and training results
    private static class PSCFWeightTMJoiner implements JoinFunction<Tuple5<String, String, String, Double, SerializableVector>,
            Tuple4<String, String, String, PSLOptimizable>,
            Tuple6<String, String, String, Double, SerializableVector, PSLOptimizable>> {

        @Override
        public Tuple6<String, String, String, Double, SerializableVector, PSLOptimizable> join(
                Tuple5<String, String, String, Double, SerializableVector> tmEntry,
                Tuple4<String, String, String, PSLOptimizable> logit) throws Exception {
            return new Tuple6<String, String, String, Double, SerializableVector, PSLOptimizable>
                    (tmEntry.f0, tmEntry.f1, tmEntry.f2, tmEntry.f3, tmEntry.f4, logit.f3);
        }
    }

    private static class USFilter extends RichFilterFunction<MIDT> {

        private HashMap<String, String> airports;

        @Override
        public void open(Configuration parameters) {
            Collection<Tuple8<String, String, String, String, String, Double, Double, String>> broadcastSet =
                    this.getRuntimeContext().getBroadcastVariable(TrafficAnalysis.AP_GEO_DATA);
            this.airports = new HashMap<String, String>(broadcastSet.size());
            for(Tuple8<String, String, String, String, String, Double, Double, String> tuple8 : broadcastSet) {
                this.airports.put(tuple8.f0, tuple8.f3);
            }
        }

        @Override
        public boolean filter(MIDT midt) throws Exception {
            String originCountry = airports.get(midt.f0);
            String destinationCountry = airports.get(midt.f1);
            return (!originCountry.equals("US") || !destinationCountry.equals("US"));
        }
    }

    private static class Itin2Agg implements MapFunction<Itinerary, Tuple8<String, String, String, String, String, String, String, Double>> {

        @Override
        public Tuple8<String, String, String, String, String, String, String, Double> map(Itinerary itinerary) throws Exception {
            String origin = itinerary.f0;
            String hub1 = itinerary.f20;
            String hub2 = itinerary.f21;
            String destination = itinerary.f1;
            String airline1 = itinerary.f3.substring(0,2);
            String airline2 = "";
            if(!itinerary.f4.isEmpty()) {
                airline2 = itinerary.f4.substring(0,2);
            }
            String airline3 = "";
            if(!itinerary.f5.isEmpty()) {
                airline3 = itinerary.f5.substring(0,2);
            }
            double estimate = itinerary.f15;
            return new Tuple8<String, String, String, String, String, String, String, Double>(origin, hub1, hub2, destination, airline1, airline2, airline3, estimate);
        }
    }

}

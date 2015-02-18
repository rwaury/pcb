package com.amadeus.pcb.join;

import cc.mallet.optimize.LimitedMemoryBFGS;
import cc.mallet.optimize.OptimizationException;
import net.didion.jwnl.data.Exc;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.hadoop.util.hash.Hash;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class TrafficAnalysis {

    private static final double SLF = 0.795;

    private static final double WAITING_FACTOR = 1.0;

    private static final int MAX_ITERATIONS = 10;

    private static final double OPTIMIZER_TOLERANCE = 0.000001;

    private static final int MAX_OPTIMIZER_ITERATIONS = 1000;

    private static long firstPossibleTimestamp = 1399248000000L;
    private static long lastPossibleTimestamp = 1399852799000L;

    private static String outputPath = "hdfs:///user/rwaury/output2/flights/";

    private static String AP_COUNTRY_MAPPING = "AirportCountryMappingBroadcastSet";

    private static String INVERTED_COVARIANCE_MATRIX = "InvertedCovarianceMatrixBroadcastSet";

    private static final int OD_FEATURE_COUNT = 5;

    private final static ArrayList<String> countriesWithStates = new ArrayList<String>() {{
        add("AR");
        add("AU");
        add("BR");
        add("CA");
        add("US");
    }};

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<String, String, String, String>> airportCountryNR = env.readTextFile("hdfs:///user/rwaury/input2/ori_por_public.csv").setParallelism(1)
                .flatMap(new AirportCountryExtractor()).setParallelism(1);

        DataSet<Tuple2<String, String>> regionInfo = env.readTextFile("hdfs:///user/rwaury/input2/ori_country_region_info.csv").map(new FlightConnectionJoiner.RegionExtractor());
        //regionInfo.writeAsCsv(outputPath + "regionInfo", "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<Tuple4<String, String, String, String>> airportCountry = airportCountryNR.join(regionInfo).where(2).equalTo(0).with(new RegionJoiner());
        //airportCountry.writeAsCsv(outputPath + "airportCountry", "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull");
        DataSet<Tuple2<Flight, Flight>> twoLegConnections = env.readFile(new FlightOutput.TwoLegFullInputFormat(), outputPath + "twoFull");
        DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), outputPath + "threeFull");

        DataSet<Itinerary> nonStopItineraries = nonStopConnections.flatMap(new FlightExtractor1());
        DataSet<Itinerary> twoLegItineraries = twoLegConnections.flatMap(new FlightExtractor2());
        DataSet<Itinerary> threeLegItineraries = threeLegConnections.flatMap(new FlightExtractor3());
        DataSet<Itinerary> itineraries = nonStopItineraries.union(twoLegItineraries).union(threeLegItineraries);

        DataSet<Tuple3<String, String, Integer>> flightBounds = nonStopConnections.map(new MapFunction<Flight, Tuple3<String, String, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
            @Override
            public Tuple3<String, String, Integer> map(Flight flight) throws Exception {
                Date d = new Date(flight.getDepartureTimestamp());
                return new Tuple3<String, String, Integer>(flight.getAirline() + flight.getFlightNumber(), format.format(d), flight.getMaxCapacity());
            }
        }).groupBy(0,1).min(2);
        flightBounds.writeAsCsv(outputPath + "flightCapacity", "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<String> midtStrings = env.readTextFile("hdfs:///user/rwaury/input2/MIDTTotalHits.csv");
        DataSet<MIDT> midt = midtStrings.flatMap(new MIDTParser()).map(new MIDTCompressor()).groupBy(0,1,2,3,4,5,6,7).reduceGroup(new MIDTGrouper());
        DataSet<Tuple5<String, String, String, Integer, Integer>> ODLowerBound = midt.map(new LowerBoundExtractor()).groupBy(0,1,2).sum(3).andSum(4);

        // group sort and first-1 is necessary to exclude multileg connections that yield the same OD (only the fastest is included)
        DataSet<Itinerary> itinerariesWithMIDT = itineraries.groupBy(0,1,2,3,4,5,6,7).sortGroup(10, Order.ASCENDING).first(1).coGroup(midt).where(0,1,2,3,4,5,6,7).equalTo(0,1,2,3,4,5,6,7).with(new CoGroupFunction<Itinerary, MIDT, Itinerary>() {
            @Override
            public void coGroup(Iterable<Itinerary> itineraries, Iterable<MIDT> midts, Collector<Itinerary> out) throws Exception {
                Iterator<Itinerary> itinIter = itineraries.iterator();
                Iterator<MIDT> midtIter = midts.iterator();
                if(!midtIter.hasNext()) {
                    out.collect(itinIter.next());
                } else if(!itinIter.hasNext()) {
                    out.collect(MIDTToItinerary(midtIter.next()));
                } else {
                    MIDT midt = midtIter.next();
                    Itinerary itin = itinIter.next();
                    itin.f13 = midt.f11;
                    out.collect(itin);
                }
                if(itinIter.hasNext()) {
                    throw new Exception("More than one Itinerary: " + itinIter.next().toString());
                }
                if(midtIter.hasNext()) {
                    throw new Exception("More than one MIDT: " + midtIter.next().toString());
                }

            }
        });

        DataSet<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> APBoundsAgg = midtStrings.flatMap(new MIDTCapacityEmitter()).withBroadcastSet(airportCountry, AP_COUNTRY_MAPPING);
        DataSet<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> APBounds = APBoundsAgg.groupBy(0,1,2,3,4).sum(5).andSum(6);
        //APBounds.writeAsCsv(outputPath + "APBounds", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        DataSet<Tuple5<String, String, String, Integer, Integer>> ODMax = itinerariesWithMIDT.groupBy(0,1,2).reduceGroup(new ODMax());
        //ODMax.writeAsCsv(outputPath + "ODMax", "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<Tuple5<String, String, String, Integer, Integer>> ODBounds = ODLowerBound.coGroup(ODMax).where(0,1,2).equalTo(0,1,2)
                .with(new CoGroupFunction<Tuple5<String, String, String, Integer, Integer>, Tuple5<String, String, String, Integer, Integer>, Tuple5<String, String, String, Integer, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple5<String, String, String, Integer, Integer>> lower,
                                        Iterable<Tuple5<String, String, String, Integer, Integer>> upper,
                                        Collector<Tuple5<String, String, String, Integer, Integer>> out) throws Exception {
                        Tuple5<String, String, String, Integer, Integer> result = new Tuple5<String, String, String, Integer, Integer>();
                        for(Tuple5<String, String, String, Integer, Integer> l : lower) {
                            result.f0 = l.f0;
                            result.f1 = l.f1;
                            result.f2 = l.f2;
                            result.f3 = l.f3;
                            result.f4 = l.f4;
                        }
                        for(Tuple5<String, String, String, Integer, Integer> u : upper) {
                            result.f0 = u.f0;
                            result.f1 = u.f1;
                            result.f2 = u.f2;
                            if(result.f3 == null) {
                                result.f3 = 0;
                            }
                            result.f4 = u.f4;
                        }
                        out.collect(result);
                    }
                });
        ODBounds.writeAsCsv(outputPath + "ODBounds", "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> inOutCapa = nonStopConnections.flatMap(new FlatMapFunction<Flight, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
            @Override
            public void flatMap(Flight flight, Collector<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> out) throws Exception {
                if(flight.getLegCount() > 1) {
                    return;
                }
                if(flight.getDepartureTimestamp() > lastPossibleTimestamp || flight.getDepartureTimestamp() < firstPossibleTimestamp) {
                    return;
                }
                Date date = new Date(flight.getDepartureTimestamp());
                String dayString = format.format(date);
                boolean isInterRegional = !flight.getOriginRegion().equals(flight.getDestinationRegion());
                boolean isInternational = !flight.getOriginCountry().equals(flight.getDestinationCountry());
                boolean isInterState = true;
                if(!isInternational && countriesWithStates.contains(flight.getOriginCountry())) {
                    isInterState = !flight.getOriginState().equals(flight.getDestinationState());
                }
                int capacity = flight.getMaxCapacity();
                Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> outgoing = new Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>
                        (flight.getOriginAirport(), dayString, isInterRegional, isInternational, isInterState, capacity, 0);
                Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> incoming = new Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>
                        (flight.getDestinationAirport(), dayString, isInterRegional, isInternational, isInterState, 0, capacity);
                out.collect(incoming);
                out.collect(outgoing);
            }
        });

        // in and out loads of airports per day and as intercontinental/intracontinental, international/domestic and interstate/innerstate (three boolean flags (f2-4))
        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> outgoingMarginalsAgg = inOutCapa.groupBy(0,1,2,3,4).reduceGroup(new GroupReduceFunction<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>>() {
            @Override
            public void reduce(Iterable<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> tuple7s,
                               Collector<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> out) throws Exception {
                boolean first = true;
                Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> result = new Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>();
                for(Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> t : tuple7s) {
                    if(first) {
                        result.f0 = t.f0;
                        result.f1 = t.f1;
                        result.f2 = t.f2;
                        result.f3 = t.f3;
                        result.f4 = t.f4;
                        result.f5 = 0;
                        result.f6 = 1.0; // Ki(0)
                        result.f7 = true;
                        first = false;
                    }
                    result.f5 += t.f5;
                }
                result.f5 = (int)Math.round(SLF*result.f5);
                out.collect(result);
            }
        });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> incomingMarginalsAgg = inOutCapa.groupBy(0,1,2,3,4).reduceGroup(new GroupReduceFunction<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>>() {
            @Override
            public void reduce(Iterable<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> tuple7s,
                               Collector<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> out) throws Exception {
                boolean first = true;
                Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> result = new Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>();
                for(Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> t : tuple7s) {
                    if(first) {
                        result.f0 = t.f0;
                        result.f1 = t.f1;
                        result.f2 = t.f2;
                        result.f3 = t.f3;
                        result.f4 = t.f4;
                        result.f5 = 0;
                        result.f6 = 1.0; // Kj(0)
                        result.f7 = false;
                        first = false;
                    }
                    result.f5 += t.f6;
                }
                result.f5 = (int)Math.round(SLF*result.f5);
                out.collect(result);
            }
        });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> outgoingMarginals = outgoingMarginalsAgg.join(APBounds, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0,1,2,3,4).equalTo(0,1,2,3,4).with(
                new JoinFunction<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>>() {
                    @Override
                    public Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> join(Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> midtBound) throws Exception {
                        int estimateResidual = Math.max(marginal.f5 - midtBound.f5, 0);
                        return new Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>(marginal.f0, marginal.f1, marginal.f2, marginal.f3, marginal.f4, estimateResidual, marginal.f6, marginal.f7);
                    }
                });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> incomingMarginals = incomingMarginalsAgg.join(APBounds, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0,1,2,3,4).equalTo(0,1,2,3,4).with(
                new JoinFunction<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>>() {
                    @Override
                    public Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> join(Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> midtBound) throws Exception {
                        int estimateResidual = Math.max(marginal.f5 - midtBound.f6, 0);
                        return new Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>(marginal.f0, marginal.f1, marginal.f2, marginal.f3, marginal.f4, estimateResidual, marginal.f6, marginal.f7);
                    }
                });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> nonStop = nonStopConnections.flatMap(new FlatMapFunction<Flight, Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
            @Override
            public void flatMap(Flight value, Collector<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> out) throws Exception {
                if(value.getDepartureTimestamp() > lastPossibleTimestamp || value.getDepartureTimestamp() < firstPossibleTimestamp) {
                    return;
                }
                Date date = new Date(value.getDepartureTimestamp());
                String dayString = format.format(date);
                long duration = value.getArrivalTimestamp() - value.getDepartureTimestamp();
                if (duration <= 0L)
                    throw new Exception("Value error: " + value.toString());
                Integer minutes = (int) (duration / (60L * 1000L));
                boolean isIntercontinental = !value.getOriginRegion().equals(value.getDestinationRegion());;
                boolean isInternational = !value.getOriginCountry().equals(value.getDestinationCountry());
                boolean isInterstate = true;
                if(!isInternational && countriesWithStates.contains(value.getOriginCountry())) {
                    isInterstate = !value.getOriginState().equals(value.getDestinationState());
                }
                out.collect(new Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>
                        (value.getOriginAirport(), value.getDestinationAirport(), isIntercontinental, isInternational, isInterstate, dayString,
                         dist(value.getOriginLatitude(), value.getOriginLongitude(), value.getDestinationLatitude(), value.getDestinationLongitude()), minutes));
            }
        });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> twoLeg = twoLegConnections.flatMap(new FlatMapFunction<Tuple2<Flight, Flight>, Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
            @Override
            public void flatMap(Tuple2<Flight, Flight> value, Collector<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> out) throws Exception {
                if(value.f0.getDepartureTimestamp() > lastPossibleTimestamp || value.f0.getDepartureTimestamp() < firstPossibleTimestamp) {
                    return;
                }
                Date date = new Date(value.f0.getDepartureTimestamp());
                String dayString = format.format(date);
                long flight1 = value.f0.getArrivalTimestamp() - value.f0.getDepartureTimestamp();
                long wait1 = (long) (WAITING_FACTOR * (value.f1.getDepartureTimestamp() - value.f0.getArrivalTimestamp()));
                long flight2 = value.f1.getArrivalTimestamp() - value.f1.getDepartureTimestamp();
                long duration = flight1 + wait1 + flight2;
                if(duration <= 0L)
                    throw new Exception("Value error: " + value.toString());
                Integer minutes = (int) (duration / (60L * 1000L));
                boolean isIntercontinental = !value.f0.getOriginRegion().equals(value.f1.getDestinationRegion());
                boolean isInternational = !value.f0.getOriginCountry().equals(value.f1.getDestinationCountry());
                boolean isInterstate = true;
                if(!isInternational && countriesWithStates.contains(value.f0.getOriginCountry())) {
                    isInterstate = !value.f0.getOriginState().equals(value.f1.getDestinationState());
                }
                out.collect(new Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>
                        (value.f0.getOriginAirport(), value.f1.getDestinationAirport(), isIntercontinental, isInternational, isInterstate, dayString,
                         dist(value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f1.getDestinationLatitude(), value.f1.getDestinationLongitude()), minutes));
            }
        });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> threeLeg = threeLegConnections.flatMap(new FlatMapFunction<Tuple3<Flight, Flight, Flight>, Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
            @Override
            public void flatMap(Tuple3<Flight, Flight, Flight> value, Collector<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> out) throws Exception {
                if(value.f0.getDepartureTimestamp() > lastPossibleTimestamp || value.f0.getDepartureTimestamp() < firstPossibleTimestamp) {
                    return;
                }
                Date date = new Date(value.f0.getDepartureTimestamp());
                String dayString = format.format(date);
                long flight1 = value.f0.getArrivalTimestamp() - value.f0.getDepartureTimestamp();
                long wait1 = (long) (WAITING_FACTOR * (value.f1.getDepartureTimestamp() - value.f0.getArrivalTimestamp()));
                long flight2 = value.f1.getArrivalTimestamp() - value.f1.getDepartureTimestamp();
                long wait2 = (long) (WAITING_FACTOR * (value.f2.getDepartureTimestamp() - value.f1.getArrivalTimestamp()));
                long flight3 = value.f2.getArrivalTimestamp() - value.f2.getDepartureTimestamp();
                long duration = flight1 + wait1 + flight2 + wait2 + flight3;
                if (duration <= 0L)
                    throw new Exception("Value error: " + value.toString());
                Integer minutes = (int) (duration / (60L * 1000L));
                boolean isIntercontinental = !value.f0.getOriginRegion().equals(value.f2.getDestinationRegion());
                boolean isInternational = !value.f0.getOriginCountry().equals(value.f2.getDestinationCountry());
                boolean isInterstate = true;
                if(!isInternational && countriesWithStates.contains(value.f0.getOriginCountry())) {
                    isInterstate = !value.f0.getOriginState().equals(value.f2.getDestinationState());
                }
                out.collect(new Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>
                        (value.f0.getOriginAirport(), value.f2.getDestinationAirport(), isIntercontinental, isInternational, isInterstate, dayString,
                         dist(value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f2.getDestinationLatitude(), value.f2.getDestinationLongitude()), minutes));
            }
        });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> flights = nonStop.union(twoLeg).union(threeLeg);
        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>> distances = flights.groupBy(0,1,2,3,4,5)
                .reduceGroup(new GroupReduceFunction<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>, Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>>() {
                    @Override
                    public void reduce(Iterable<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> values, Collector<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>> out) throws Exception {
                        Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector> result =
                                new Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>();
                        int min = Integer.MAX_VALUE;
                        int max = Integer.MIN_VALUE;
                        int sum = 0;
                        int count = 0;
                        boolean first = true;
                        SerializableVector vector = new SerializableVector(OD_FEATURE_COUNT);
                        for (Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer> t : values) {
                            if (first) {
                                result.f0 = t.f0;
                                result.f1 = t.f1;
                                result.f2 = t.f5;
                                result.f3 = t.f2;
                                result.f4 = t.f3;
                                result.f5 = t.f4;
                                vector.getVector().setEntry(0, t.f6);
                                first = false;
                            }
                            int minutes = t.f7;
                            sum += minutes;
                            count++;
                            if (minutes < min) {
                                min = minutes;
                            }
                            if (minutes > max) {
                                max = minutes;
                            }
                        }
                        Double avg = (double) sum / (double) count;
                        vector.getVector().setEntry(1, (double)min);
                        vector.getVector().setEntry(2, (double)max);
                        vector.getVector().setEntry(3, avg);
                        vector.getVector().setEntry(4, (double)count);
                        result.f6 = vector;
                        out.collect(result);
                    }
                });

        /* IPF START */
        IterativeDataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> initial = outgoingMarginals.union(incomingMarginals).iterate(MAX_ITERATIONS);

        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>> KiFractions = distances
                .join(initial.filter(new IncomingFilter()), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new KJoiner());

        outgoingMarginals = KiFractions.groupBy(0,2,3,4,5).sum(6)
                .join(initial.filter(new OutgoingFilter()), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new KUpdater());

        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>> KjFractions = distances
                .join(outgoingMarginals, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new KJoiner());

        incomingMarginals = KjFractions.groupBy(1,2,3,4,5).sum(6)
                .join(initial.filter(new IncomingFilter()), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new KUpdater());

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> iteration = outgoingMarginals.union(incomingMarginals);
        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> result = initial.closeWith(iteration);
        /* IPF END */

        DataSet<Tuple5<String, String, String, Double, SerializableVector>> trafficMatrix = distances
                .join(result.filter(new OutgoingFilter()), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new TMJoinerOut())
                .join(result.filter(new IncomingFilter()), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new TMJoinerIn())
                .project(0,1,2,6,7);

        DataSet<Tuple5<String, String, String, Double, SerializableVector>> TMWithMIDT = trafficMatrix.coGroup(ODBounds).where(0,1,2).equalTo(0,1,2).with(
                new CoGroupFunction<Tuple5<String, String, String, Double, SerializableVector>, Tuple5<String, String, String, Integer, Integer>, Tuple5<String, String, String, Double, SerializableVector>>() {
            @Override
            public void coGroup(Iterable<Tuple5<String, String, String, Double, SerializableVector>> tm, Iterable<Tuple5<String, String, String, Integer, Integer>> midtBound, Collector<Tuple5<String, String, String, Double, SerializableVector>> out) throws Exception {
                Iterator<Tuple5<String, String, String, Integer, Integer>> midtIter = midtBound.iterator();
                Iterator<Tuple5<String, String, String, Double, SerializableVector>> tmIter = tm.iterator();
                if(!midtIter.hasNext()) {
                    out.collect(tmIter.next());
                } else {
                    Tuple5<String, String, String, Integer, Integer> midt = midtIter.next();
                    if(tmIter.hasNext()) {
                        Tuple5<String, String, String, Double, SerializableVector> tmEntry = tmIter.next();
                        out.collect(new Tuple5<String, String, String, Double, SerializableVector>(tmEntry.f0, tmEntry.f1, tmEntry.f2, Math.min(tmEntry.f3+(double)midt.f3, (double)midt.f4), tmEntry.f4));
                    }
                }
                if(tmIter.hasNext()) {
                    throw new Exception("More than one TM entry: " + tmIter.next().toString());
                }
                if(midtIter.hasNext()) {
                    throw new Exception("More than one MIDT bound: " + midtIter.next().toString());
                }
            }
        });

        TMWithMIDT.project(0,1,2,3).writeAsCsv(outputPath + "trafficMatrix", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        //midt.groupBy(0,1,2).sortGroup(0, Order.ASCENDING).first(10000).writeAsCsv(outputPath + "groupedMIDT", "\n", ",", FileSystem.WriteMode.OVERWRITE);
        DataSet<Tuple4<String, String, String, LogitOptimizable>> trainedLogit = midt.groupBy(0,1,2).reduceGroup(new LogitTrainer());
        //trainedLogit.writeAsCsv(outputPath + "logitResult", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        DataSet<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> TMWithWeights = TMWithMIDT
                .join(trainedLogit, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0,1,2).equalTo(0,1,2).with(new WeightTMJoiner());

        //TMWithWeights.groupBy(0,1,2).sortGroup(2, Order.ASCENDING).first(10000000).writeAsCsv(outputPath + "tmWeights", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        //itineraries.groupBy(0,1,2,3,4,5,6,7).sortGroup(2, Order.ASCENDING).first(1000000000).writeAsCsv(outputPath + "itineraries", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        DataSet<Tuple5<String, String, String, Double, LogitOptimizable>> allWeighted = TMWithWeights.coGroup(TMWithMIDT).where(2).equalTo(2).with(new ODDistanceComparator());

        DataSet<Itinerary> estimate = itinerariesWithMIDT.coGroup(allWeighted).where(0,1,2).equalTo(0,1,2).with(new TrafficEstimator());

        estimate.groupBy(0,1,2).sortGroup(15, Order.DESCENDING).first(1000000000).writeAsCsv(outputPath + "ItineraryEstimate", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        estimate.groupBy(0,1).reduceGroup(new ODSum()).writeAsCsv(outputPath + "ODSum", "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("TrafficAnalysis");
    }

    // IPF operator
    private static class KJoiner implements JoinFunction<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>, Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>> {
        @Override
        public Tuple7<String, String, String, Boolean, Boolean, Boolean, Double> join(Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector> distance, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            double KFraction = marginal.f5*marginal.f6*decayingFunction(distance.f6.getVector().getEntry(0), distance.f6.getVector().getEntry(1), distance.f6.getVector().getEntry(2), distance.f6.getVector().getEntry(3), distance.f6.getVector().getEntry(4), distance.f3, distance.f4, distance.f5);
            if(Double.isNaN(KFraction) || KFraction < 0.0) {
                KFraction = 0.0;
            }
            return new Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>(distance.f0, distance.f1, distance.f2, distance.f3, distance.f4, distance.f5, KFraction);
        }
    }

    // IPF operator
    private static class KUpdater implements JoinFunction<Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> {
        @Override
        public Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> join(Tuple7<String, String, String, Boolean, Boolean, Boolean, Double> Ksum, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal) throws Exception {
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
    private static class TMJoinerOut implements JoinFunction<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>, Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector>> {
        @Override
        public Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector> join(Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector> distance, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            double partialDist = marginal.f5*marginal.f6*decayingFunction(distance.f6.getVector().getEntry(0), distance.f6.getVector().getEntry(1), distance.f6.getVector().getEntry(2), distance.f6.getVector().getEntry(3), distance.f6.getVector().getEntry(4), distance.f3, distance.f4, distance.f5);
            if(Double.isNaN(partialDist) || partialDist < 0.0) {
                partialDist = 0.0;
            }
            return new Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector>(distance.f0, distance.f1, distance.f2, distance.f3, distance.f4, distance.f5, partialDist, distance.f6);
        }
    }

    // IPF result to estimate II
    private static class TMJoinerIn implements JoinFunction<Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>, Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector>> {
        @Override
        public Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector> join(Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector> tmOut, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            double fullDistance = tmOut.f6*marginal.f5*marginal.f6;
            if(Double.isNaN(fullDistance) || fullDistance < 0.0) {
                fullDistance = 0.0;
            }
            return new Tuple8<String, String, String, Boolean, Boolean, Boolean, Double, SerializableVector>(tmOut.f0, tmOut.f1, tmOut.f2, tmOut.f3, tmOut.f4, tmOut.f5, fullDistance, tmOut.f7);
        }
    }

    private static class FlightExtractor1 implements FlatMapFunction<Flight, Itinerary> {
        SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
        @Override
        public void flatMap(Flight flight, Collector<Itinerary> out) throws Exception {
            if(flight.getDepartureTimestamp() > lastPossibleTimestamp || flight.getDepartureTimestamp() < firstPossibleTimestamp) {
                return;
            }
            Date date = new Date(flight.getDepartureTimestamp());
            String dayString = format.format(date);
            Double distance = dist(flight.getOriginLatitude(), flight.getOriginLongitude(), flight.getDestinationLatitude(), flight.getDestinationLongitude());
            Integer travelTime = Math.max((int) ((flight.getArrivalTimestamp() - flight.getDepartureTimestamp())/(60L*1000L)), 1);
            out.collect(new Itinerary(flight.getOriginAirport(), flight.getDestinationAirport(), dayString,
                    flight.getAirline() + flight.getFlightNumber(), "", "", "", "", distance, distance, travelTime, 0,
                    flight.getLegCount(), 0, flight.getMaxCapacity(), -1, -1.0, -1.0, ""));
        }
    }

    private static class FlightExtractor2 implements FlatMapFunction<Tuple2<Flight, Flight>, Itinerary> {
        SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
        @Override
        public void flatMap(Tuple2<Flight, Flight> flight, Collector<Itinerary> out) throws Exception {
            if(flight.f0.getDepartureTimestamp() > lastPossibleTimestamp || flight.f0.getDepartureTimestamp() < firstPossibleTimestamp) {
                return;
            }
            Date date = new Date(flight.f0.getDepartureTimestamp());
            String dayString = format.format(date);
            Double directDistance = dist(flight.f0.getOriginLatitude(), flight.f0.getOriginLongitude(), flight.f1.getDestinationLatitude(), flight.f1.getDestinationLongitude());
            Double travelledDistance = dist(flight.f0.getOriginLatitude(), flight.f0.getOriginLongitude(), flight.f0.getDestinationLatitude(), flight.f0.getDestinationLongitude()) +
                    dist(flight.f1.getOriginLatitude(), flight.f1.getOriginLongitude(), flight.f1.getDestinationLatitude(), flight.f1.getDestinationLongitude());
            Integer travelTime = Math.max((int) ((flight.f1.getArrivalTimestamp() - flight.f0.getDepartureTimestamp())/(60L*1000L)), 1);
            Integer waitingTime = (int) ((flight.f1.getDepartureTimestamp() - flight.f0.getArrivalTimestamp())/(60L*1000L));
            Integer legCount = flight.f0.getLegCount() + flight.f1.getLegCount();
            Integer maxCapacity = Math.min(flight.f0.getMaxCapacity(), flight.f1.getMaxCapacity());
            out.collect(new Itinerary(flight.f0.getOriginAirport(), flight.f1.getDestinationAirport(), dayString,
                    flight.f0.getAirline() + flight.f0.getFlightNumber(), flight.f1.getAirline() + flight.f1.getFlightNumber(), "", "", "",
                    directDistance, travelledDistance, travelTime, waitingTime, legCount, 0, maxCapacity, -1, -1.0, -1.0, ""));
        }
    }

    private static class FlightExtractor3 implements FlatMapFunction<Tuple3<Flight, Flight, Flight>, Itinerary> {
        SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
        @Override
        public void flatMap(Tuple3<Flight, Flight, Flight> flight, Collector<Itinerary> out) throws Exception {
            if(flight.f0.getDepartureTimestamp() > lastPossibleTimestamp || flight.f0.getDepartureTimestamp() < firstPossibleTimestamp) {
                return;
            }
            Date date = new Date(flight.f0.getDepartureTimestamp());
            String dayString = format.format(date);
            Double directDistance = dist(flight.f0.getOriginLatitude(), flight.f0.getOriginLongitude(), flight.f2.getDestinationLatitude(), flight.f2.getDestinationLongitude());
            Double travelledDistance = dist(flight.f0.getOriginLatitude(), flight.f0.getOriginLongitude(), flight.f0.getDestinationLatitude(), flight.f0.getDestinationLongitude()) +
                    dist(flight.f1.getOriginLatitude(), flight.f1.getOriginLongitude(), flight.f1.getDestinationLatitude(), flight.f1.getDestinationLongitude()) +
                    dist(flight.f2.getOriginLatitude(), flight.f2.getOriginLongitude(), flight.f2.getDestinationLatitude(), flight.f2.getDestinationLongitude());
            Integer travelTime = Math.max((int) ((flight.f2.getArrivalTimestamp() - flight.f0.getDepartureTimestamp())/(60L*1000L)), 1);
            Integer waitingTime = (int) (
                    ((flight.f1.getDepartureTimestamp() - flight.f0.getArrivalTimestamp()) +
                    (flight.f2.getDepartureTimestamp() - flight.f1.getArrivalTimestamp())) /
                    (60L*1000L));
            Integer legCount = flight.f0.getLegCount() + flight.f1.getLegCount() + flight.f2.getLegCount();
            Integer maxCapacity = Math.min(flight.f0.getMaxCapacity(), Math.min(flight.f1.getMaxCapacity(), flight.f2.getMaxCapacity()));
            out.collect(new Itinerary(flight.f0.getOriginAirport(), flight.f2.getDestinationAirport(), dayString,
                    flight.f0.getAirline() + flight.f0.getFlightNumber(), flight.f1.getAirline() + flight.f1.getFlightNumber(), flight.f2.getAirline() + flight.f2.getFlightNumber(), "", "",
                    directDistance, travelledDistance, travelTime, waitingTime, legCount, 0, maxCapacity, -1, -1.0, -1.0, ""));
        }
    }

    private static class MIDTCapacityEmitter extends RichFlatMapFunction<String, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> {

        private SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

        private HashMap<String, String> APToRegion;
        private HashMap<String, String> APToCountry;
        private HashMap<String, String> APToState;

        @Override
        public void open(Configuration parameters) {
            Collection<Tuple4<String, String, String, String>> broadcastSet = this.getRuntimeContext().getBroadcastVariable(AP_COUNTRY_MAPPING);
            this.APToRegion = new HashMap<String, String>(broadcastSet.size());
            this.APToCountry = new HashMap<String, String>(broadcastSet.size());
            this.APToState = new HashMap<String, String>(200);

            for(Tuple4<String, String, String, String> t : broadcastSet) {
                this.APToRegion.put(t.f0, t.f1);
                this.APToCountry.put(t.f0, t.f2);
                if(countriesWithStates.contains(t.f2)) {
                    this.APToState.put(t.f0, t.f3);
                }
            }
        }

        @Override
        public void flatMap(String s, Collector<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> out) throws Exception {
            if(s.startsWith("$")) {
                return;
            }
            String[] tmp = s.split(";");
            if(tmp.length < 15) {
                return;
            }
            int pax = Integer.parseInt(tmp[2].trim());
            int segmentCount = Integer.parseInt(tmp[3].trim());
            long departureDay = Long.parseLong(tmp[11].trim())-1;
            long departureTimestamp = firstPossibleTimestamp + (departureDay*24L*60L*60L*1000L);
            Date date = new Date(departureTimestamp);
            String dayString = format.format(date);
            if(departureDay > 6) {
                throw new Exception("Value error: " + s);
            }
            boolean isIntercontinental = false;
            boolean isInternational = false;
            boolean isInterstate = true;
            int offset = 9;
            int counter = 0;
            int index = 6;
            while(counter < segmentCount) {
                String apOut = tmp[index].trim();
                String apIn = tmp[index+1].trim();
                index += offset;
                counter++;
                String outRegion = APToRegion.get(apOut);
                String inRegion = APToRegion.get(apIn);
                String outCountry = APToCountry.get(apOut);
                String inCountry = APToCountry.get(apIn);
                if(outCountry != null && inCountry != null) {
                    isIntercontinental = !outRegion.equals(inRegion);
                    isInternational = !outCountry.equals(inCountry);
                    isInterstate = true;
                    if(!isInternational && countriesWithStates.contains(outCountry)) {
                        String outState = APToState.get(apOut);
                        String inState = APToState.get(apIn);
                        isInterstate = !outState.equals(inState);
                    }
                } else {
                    continue;
                }
                Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> tOut =
                        new Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>(apOut, dayString, isIntercontinental, isInternational, isInterstate, pax, 0);
                Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> tIn =
                        new Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>(apIn, dayString, isIntercontinental, isInternational, isInterstate, 0, pax);
                out.collect(tOut);
                out.collect(tIn);
            }
        }
    }

    // MIDT file to MIDT tuple
    private static class MIDTParser implements FlatMapFunction<String, MIDT> {

        SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

        @Override
        public void flatMap(String s, Collector<MIDT> out) throws Exception {
            if(s.startsWith("$")) {
                return;
            }
            String[] tmp = s.split(";");
            if(tmp.length < 15) {
                return;
            }
            String origin = tmp[0].trim();
            String destination = tmp[1].trim();
            int pax = Integer.parseInt(tmp[2].trim());
            int segmentCount = Integer.parseInt(tmp[3].trim());
            String flight1 = tmp[9].trim() + tmp[10].replaceAll("[^0-9]", "");
            String flight2 = "";
            String flight3 = "";
            String flight4 = "";
            String flight5 = "";
            long departureDay = Long.parseLong(tmp[11].trim())-1;
            if(departureDay > 6) {
                throw new Exception("Value error: " + s);
            }
            int departure = Integer.parseInt(tmp[12].trim());
            int arrival = Integer.parseInt(tmp[14].trim());
            int waitingTime = 0;
            int tmpDep = 0;
            if(segmentCount > 1) {
                flight2 = tmp[18].trim() + tmp[19].replaceAll("[^0-9]", "");
                tmpDep = Integer.parseInt(tmp[21].trim());
                waitingTime += tmpDep - arrival;
                arrival = Integer.parseInt(tmp[23].trim());
            }
            if(segmentCount > 2) {
                flight3 = tmp[27].trim() + tmp[28].replaceAll("[^0-9]", "");
                tmpDep = Integer.parseInt(tmp[30].trim());
                waitingTime += tmpDep - arrival;
                arrival = Integer.parseInt(tmp[32].trim());
            }
            if(segmentCount > 3) {
                flight4 = tmp[36].trim() + tmp[37].replaceAll("[^0-9]", "");
                tmpDep = Integer.parseInt(tmp[39].trim());
                waitingTime += tmpDep - arrival;
                arrival = Integer.parseInt(tmp[41].trim());
            }
            if(segmentCount > 4) {
                flight5 = tmp[45].trim() + tmp[46].replaceAll("[^0-9]", "");
                tmpDep = Integer.parseInt(tmp[48].trim());
                waitingTime += tmpDep - arrival;
                arrival = Integer.parseInt(tmp[49].trim());
            }
            int travelTime = arrival - departure;
            if(travelTime < 1) {
                return;
            }
            long departureTimestamp = firstPossibleTimestamp + (departureDay*24L*60L*60L*1000L) + 1L;
            Date date = new Date(departureTimestamp);
            String dayString = format.format(date);
            MIDT result = new MIDT(origin, destination, dayString,
                    flight1, flight2, flight3, flight4, flight5, travelTime, waitingTime,
                    segmentCount, pax);
            out.collect(result);
        }
    }

    // merge MIDT data from different GDS systems and booking locations
    private static class MIDTGrouper implements GroupReduceFunction<MIDT, MIDT> {

        @Override
        public void reduce(Iterable<MIDT> midts, Collector<MIDT> out) throws Exception {
            int paxSum = 0;
            int count = 0;
            Iterator<MIDT> iterator = midts.iterator();
            MIDT midt = null;
            while(iterator.hasNext()) {
                midt = iterator.next();
                paxSum += midt.f11;
                count++;
            }
            MIDT result = new MIDT(midt.f0, midt.f1, midt.f2, midt.f3, midt.f4, midt.f5, midt.f6, midt.f7, midt.f8, midt.f9, midt.f10, paxSum);
            out.collect(result);
        }
    }

    // emit number of MIDT bookings for an OD
    private static class LowerBoundExtractor implements MapFunction<MIDT, Tuple5<String, String, String, Integer, Integer>> {
        @Override
        public Tuple5<String, String, String, Integer, Integer> map(MIDT midt) throws Exception {
            return new Tuple5<String, String, String, Integer, Integer>(midt.f0, midt.f1, midt.f2, midt.f11, 0);
        }
    }

    // train a logit model with the MIDT data
    private static class LogitTrainer implements GroupReduceFunction<MIDT, Tuple4<String, String, String, LogitOptimizable>> {

        @Override
        public void reduce(Iterable<MIDT> midts, Collector<Tuple4<String, String, String, LogitOptimizable>> out) throws Exception {
            ArrayList<LogitOptimizable.TrainingData> trainingData = new ArrayList<LogitOptimizable.TrainingData>();
            Iterator<MIDT> iterator = midts.iterator();
            MIDT midt = null;
            int minTravelTime = Integer.MAX_VALUE;
            while(iterator.hasNext()) {
                midt = iterator.next();
                if(midt.f8 < minTravelTime) {
                    minTravelTime = midt.f8;
                }
                double percentageWaiting = (midt.f8 == 0 || midt.f9 == 0) ? 0.0 : midt.f9/midt.f8;
                trainingData.add(new LogitOptimizable.TrainingData(midt.f8, percentageWaiting, midt.f10, midt.f11));
            }
            if(trainingData.size() < 2) {
                return;
            }
            LogitOptimizable optimizable = new LogitOptimizable();
            if(minTravelTime < 1) {
                minTravelTime = 1;
            }
            optimizable.setTrainingData(trainingData, minTravelTime);
            LimitedMemoryBFGS optimizer = new LimitedMemoryBFGS(optimizable);
            optimizer.setTolerance(OPTIMIZER_TOLERANCE);
            boolean converged = false;
            try {
                converged = optimizer.optimize(MAX_OPTIMIZER_ITERATIONS);
            } catch (IllegalArgumentException e) {
                // This exception may be thrown if L-BFGS
                // cannot step in the current direction.
                // This condition does not necessarily mean that
                // the optimizer has failed, but it doesn't want
                // to claim to have succeeded...
            } catch (OptimizationException o) {
                // see above
            } catch (Throwable t) {
                throw new Exception("Something went wrong in the optimizer. " + t.getMessage());
            }
            if(!converged) {
                return;
            }
            optimizable.clear(); // push the results in the tuple
            out.collect(new Tuple4<String, String, String, LogitOptimizable>(midt.f0, midt.f1, midt.f2, optimizable));
        }
    }

    // merge TM estimates and training results
    private static class WeightTMJoiner implements JoinFunction<Tuple5<String, String, String, Double, SerializableVector>, Tuple4<String, String, String, LogitOptimizable>,
            Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> {

        @Override
        public Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> join(Tuple5<String, String, String, Double, SerializableVector> tmEntry, Tuple4<String, String, String, LogitOptimizable> logit) throws Exception {
            return new Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>(tmEntry.f0, tmEntry.f1, tmEntry.f2, tmEntry.f3, tmEntry.f4, logit.f3);
        }
    }

    // assign weights to ODs without training data (very slow at the moment, performs day wise NL-join)
    private static class ODDistanceComparator implements
            CoGroupFunction<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>,
                    Tuple5<String, String, String, Double, SerializableVector>,
                    Tuple5<String, String, String, Double, LogitOptimizable>> {

        //private List<Tuple2<String, SerializableMatrix>> matrices = null;

        /*@Override
        public void open(Configuration parameters) {
            //this.matrices = getRuntimeContext().getBroadcastVariable(INVERTED_COVARIANCE_MATRIX);
        }*/

        @Override
        public void coGroup(Iterable<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> weightedODs,
                            Iterable<Tuple5<String, String, String, Double, SerializableVector>> unweightedODs,
                            Collector<Tuple5<String, String, String, Double, LogitOptimizable>> out) throws Exception {

            ArrayList<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> weighted = new ArrayList<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>>();
            for(Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> w : weightedODs) {
                weighted.add(w.copy());
            }
            double distance = 0.0;
            for(Tuple5<String, String, String, Double, SerializableVector> uw : unweightedODs) {
                double minDistance = Double.MAX_VALUE;
                Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> tmp = null;
                for(Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> w : weighted) {
                    distance = uw.f4.getVector().getDistance(w.f4.getVector());
                    if(distance < minDistance) {
                        minDistance = distance;
                        tmp = w;
                    }
                }
                if(tmp == null) {
                    continue;
                }
                out.collect(new Tuple5<String, String, String, Double, LogitOptimizable>(uw.f0, uw.f1, uw.f2, uw.f3, tmp.f5));
            }
        }
    }

    // use trained models to assign OD pax to OD itineraries
    private static class TrafficEstimator implements CoGroupFunction<Itinerary, Tuple5<String, String, String, Double, LogitOptimizable>, Itinerary> {

        @Override
        public void coGroup(Iterable<Itinerary> connections, Iterable<Tuple5<String, String, String, Double, LogitOptimizable>> logitModel,
                            Collector<Itinerary> out) throws Exception {
            Iterator<Tuple5<String, String, String, Double, LogitOptimizable>> logitIter = logitModel.iterator();
            if(!logitIter.hasNext()) {
                return; // no model
            }
            Tuple5<String, String, String, Double, LogitOptimizable> logit = logitIter.next();
            if(logitIter.hasNext()) {
                throw new Exception("More than one logit model: " + logitIter.next().toString());
            }
            double estimate = logit.f3;
            double[] weights = logit.f4.asArray();
            Iterator<Itinerary> connIter = connections.iterator();
            if(!connIter.hasNext()) {
                return; // no connections
            }
            int count = 0;
            int MIDTonlyPax = 0;
            ArrayList<Itinerary> itineraries = new ArrayList<Itinerary>();
            int minTime = Integer.MAX_VALUE;
            while (connIter.hasNext()) {
                Itinerary e = connIter.next().deepCopy();
                if(e.f18.equals("MIDT")) {
                    MIDTonlyPax += e.f13;
                    e.f16 = (double)e.f13;
                    e.f17 = estimate;
                    out.collect(e);
                } else {
                    itineraries.add(e);
                    if (e.f10 < minTime) {
                        minTime = e.f10;
                    }
                }
                count++;
            }
            double estimateWithoutMIDT = estimate - (double) MIDTonlyPax;
            if(minTime < 1) {
                minTime = 1;
            }
            double softmaxSum = 0.0;
            for(Itinerary e : itineraries) {
                softmaxSum += Math.exp(LogitOptimizable.linearPredictorFunction(LogitOptimizable.toArray(e, minTime), weights));
            }
            for(Itinerary e : itineraries) {
                double itineraryEstimate = LogitOptimizable.softmax(e, softmaxSum, weights, minTime)*estimateWithoutMIDT;
                int roundedEstimate = (int)Math.round(itineraryEstimate);
                roundedEstimate = Math.min(Math.max(roundedEstimate, e.f13), e.f14); // enforce lower and upper bound on itinerary level
                if(roundedEstimate > 0) {
                    e.f15 = roundedEstimate;
                    e.f16 = itineraryEstimate;
                    e.f17 = estimate;
                    out.collect(e);
                }
            }

        }
    }

    // get OD capacity from CB result
    private static class ODMax implements GroupReduceFunction<Itinerary, Tuple5<String, String, String, Integer, Integer>> {

        @Override
        public void reduce(Iterable<Itinerary> itineraries, Collector<Tuple5<String, String, String, Integer, Integer>> out) throws Exception {
            HashMap<String, Integer> flightLoad = new HashMap<String, Integer>();
            flightLoad.put("", -1);
            int maxODCapacity = 0;
            boolean first = true;
            Tuple5<String, String, String, Integer, Integer> result = new Tuple5<String, String, String, Integer, Integer>();
            for(Itinerary itinerary : itineraries) {
                if(first) {
                    result.f0 = itinerary.f0;
                    result.f1 = itinerary.f1;
                    result.f2 = itinerary.f2;
                    first = false;
                }
                int increase1 = 0;
                int increase2 = 0;
                int increase3 = 0;
                Integer itinCap = itinerary.f14;
                Integer cap1 = flightLoad.get(itinerary.f3);
                if(cap1 == null) {
                    flightLoad.put(itinerary.f3, itinCap);
                    increase1 = itinCap;
                } else if(cap1 <= itinCap) {
                    increase1 = itinCap-cap1;
                    flightLoad.put(itinerary.f3, itinCap);
                }
                Integer cap2 = flightLoad.get(itinerary.f4);
                if(cap2 == null) {
                    flightLoad.put(itinerary.f4, itinCap);
                } else if(cap2 <= itinCap && cap2 > -1) {
                    increase2 = itinCap-cap2;
                    flightLoad.put(itinerary.f4, itinCap);
                }
                Integer cap3 = flightLoad.get(itinerary.f5);
                if(cap3 == null) {
                    flightLoad.put(itinerary.f5, itinCap);
                } else if(cap3 <= itinCap && cap3 > -1) {
                    increase3 = itinCap-cap3;
                    flightLoad.put(itinerary.f5, itinCap);
                }
                maxODCapacity += Math.max(increase1, Math.max(increase2, increase3));
            }
            result.f3 = 0;
            result.f4 = maxODCapacity;
            out.collect(result);
        }
    }

    // aggregate OD estimate over whole period (only to compare to AirTraffic data)
    private static class ODSum implements GroupReduceFunction<Itinerary, Tuple5<String, String, Integer, Integer, Double>> {

        @Override
        public void reduce(Iterable<Itinerary> iterable, Collector<Tuple5<String, String, Integer, Integer, Double>> out) throws Exception {
            Tuple5<String, String, Integer, Integer, Double> result = new Tuple5<String, String, Integer, Integer, Double>();
            HashSet<String> days = new HashSet<String>(7);
            int itinCount = 0;
            int paxSum = 0;
            double estimateSum = 0.0;
            for(Itinerary t : iterable) {
                if(!days.contains(t.f2)) {
                    days.add(t.f2);
                    estimateSum += t.f17;
                }
                result.f0 = t.f0;
                result.f1 = t.f1;
                paxSum += t.f15;
                itinCount++;
            }
            result.f2 = itinCount;
            result.f3 = paxSum;
            result.f4 = estimateSum;
            out.collect(result);
        }
    }

    // TODO: implement covariance matrix of OD feature vectors to compute Mahalanobis distance between ODs and not Euclidean
    private static class CovarianceMatrix implements GroupReduceFunction<Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>,
            Tuple2<String, SerializableMatrix>> {

        @Override
        public void reduce(Iterable<Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>> iterable,
                           Collector<Tuple2<String, SerializableMatrix>> collector) throws Exception {

        }
    }

    // extract airport -> [region, country, state] mapping (region is added later)
    public static class AirportCountryExtractor implements FlatMapFunction<String, Tuple4<String, String, String, String>> {

        String[] tmp = null;
        String from = null;
        String until = null;

        @Override
        public void flatMap(String value, Collector<Tuple4<String, String, String, String>> out) throws Exception {
            tmp = value.split("\\^");
            if (tmp[0].trim().startsWith("#")) {
                // header
                return;
            }
            if (!tmp[41].trim().equals("A")) {
                // not an airport
                return;
            }
            from = tmp[13].trim();
            until = tmp[14].trim();
            if (!until.equals("")) {
                // expired
                return;
            }
            String iataCode = tmp[0].trim();
            String regionCode = "";
            String countryCode = tmp[16].trim();
            String stateCode = tmp[40].trim();
            out.collect(new Tuple4<String, String, String, String>(iataCode, regionCode, countryCode, stateCode));
        }
    }

    // add region to airport -> [region, country, state] mapping (join key is country)
    public static class RegionJoiner implements JoinFunction<Tuple4<String, String, String, String>,
            Tuple2<String, String>, Tuple4<String, String, String, String>> {

        @Override
        public Tuple4<String, String, String, String>
        join(Tuple4<String, String, String, String> first, Tuple2<String, String> second) throws Exception {
            first.f1 = second.f1;
            return first;
        }
    }

    // get all itineraries that have two or more flights
    public static final class SecondFlight implements FilterFunction<Tuple10<String, String, String, String, String, String, Long, Integer, Integer, Double>> {
        @Override
        public boolean filter(Tuple10<String, String, String, String, String, String, Long, Integer, Integer, Double> tuple) throws Exception {
            return !tuple.f4.isEmpty();
        }
    }

    // get all itineraries that have three or more flights
    public static final class ThirdFlight implements FilterFunction<Tuple10<String, String, String, String, String, String, Long, Integer, Integer, Double>> {
        @Override
        public boolean filter(Tuple10<String, String, String, String, String, String, Long, Integer, Integer, Double> tuple) throws Exception {
            return !tuple.f5.isEmpty();
        }
    }

    // merge multi-leg flights in MIDT to be comparable to CB result
    public static class MIDTCompressor implements MapFunction<MIDT, MIDT> {

        @Override
        public MIDT map(MIDT midt) throws Exception {
            String flight1 = midt.f3;
            String flight2 = midt.f4;
            String flight3 = midt.f5;
            String flight4 = midt.f6;
            String flight5 = midt.f7;
            if(flight1.equals(flight2)) {
                flight2 = flight3;
                flight3 = flight4;
                flight4 = flight5;
                flight5 = "";
            }
            if(flight2.equals(flight3)) {
                flight3 = flight4;
                flight4 = flight5;
                flight5 = "";
            }
            if(flight3.equals(flight4)) {
                flight4 = flight5;
                flight5 = "";
            }
            if(flight4.equals(flight5)) {
                flight5 = "";
            }
            return new MIDT(midt.f0, midt.f1, midt.f2, flight1, flight2, flight3, flight4, flight5, midt.f8, midt.f9, midt.f10, midt.f11);
        }
    }

    // Converts aggregated MIDT to Itinerary with estimate
    private static Itinerary MIDTToItinerary(MIDT midt) {
        return new Itinerary(midt.f0, midt.f1, midt.f2, midt.f3, midt.f4, midt.f5, midt.f6, midt.f7, -1.0, -1.0, midt.f8, midt.f9, midt.f10, midt.f11, midt.f11, midt.f11, -1.0, -1.0, "MIDT");
    }

    /*
     * distance between two coordinates in kilometers
	 */
    private static double dist(double lat1, double long1, double lat2, double long2) {
        final double d2r = Math.PI / 180.0;
        double dlong = (long2 - long1) * d2r;
        double dlat = (lat2 - lat1) * d2r;
        double a = Math.pow(Math.sin(dlat / 2.0), 2.0)
                + Math.cos(lat1 * d2r)
                * Math.cos(lat2 * d2r)
                * Math.pow(Math.sin(dlong / 2.0), 2.0);
        double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));
        double d = 6367.0 * c;
        return d;
    }

    private static double decayingFunction(double minTravelTime, double maxTravelTime, double avgTravelTime, double distance, double count, boolean isIntercontinental, boolean isInternational, boolean isInterstate) {
        double lnMode = 0.0;
        if(isIntercontinental) {
            lnMode = Math.log(5545.0); // JFK-LHR
        } else if(isInternational) {
            lnMode = Math.log(832.0); // HKG-TPE
        } else if(isInterstate) {
            lnMode = Math.log(451.0); // GMP-CJU
        } else {
            lnMode = Math.log(543.0); // SFO-LAX
        }
        double sigma2 = 4.0;
        double sigma = Math.sqrt(sigma2);
        double mu = lnMode + sigma2;
        double x = distance;
        double numerator = Math.exp(-1.0*(Math.pow(Math.log(x)-mu, 2.0)/(2.0*sigma2)));
        double denominator = x*sigma*Math.sqrt(2.0*Math.PI);
        return numerator/denominator;
        //return 1.0/Math.sqrt(minTravelTime*avgTravelTime*distance);
    }

    private static double mahalanobisDistance(ArrayRealVector x, ArrayRealVector y, Array2DRowRealMatrix Sinv) {
        ArrayRealVector xmy = x.subtract(y);
        RealVector SinvXmy = Sinv.operate(xmy);
        double result = xmy.dotProduct(SinvXmy);
        return Math.sqrt(result);
    }

}

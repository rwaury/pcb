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
    private static long lastPossibleTimestamp = 1399939199000L;

    private static String outputPath = "hdfs:///user/rwaury/output2/flights/";

    private static String INVERTED_COVARIANCE_MATRIX = "InvertedCovarianceMatrixBroadcastSet";

    private static final int OD_FEATURE_COUNT = 5;

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull");

        DataSet<Tuple5<String, String, Boolean, Integer, Integer>> inOutCapa = nonStopConnections.flatMap(new FlatMapFunction<Flight, Tuple5<String, String, Boolean, Integer, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

            @Override
            public void flatMap(Flight flight, Collector<Tuple5<String, String, Boolean, Integer, Integer>> out) throws Exception {
                if(flight.getLegCount() > 1) {
                    return;
                }
                if(flight.getDepartureTimestamp() > lastPossibleTimestamp || flight.getDepartureTimestamp() < firstPossibleTimestamp) {
                    return;
                }
                Date date = new Date(flight.getDepartureTimestamp());
                String dayString = format.format(date);
                boolean isInternational = !flight.getOriginCountry().equals(flight.getDestinationCountry());
                int roundedCapacity = flight.getMaxCapacity();
                int capacity = (roundedCapacity > 0) ? roundedCapacity : 1;
                Tuple5<String, String, Boolean, Integer, Integer> outgoing = new Tuple5<String, String, Boolean, Integer, Integer>
                        (flight.getOriginAirport(), dayString, isInternational, capacity, 0);
                Tuple5<String, String, Boolean, Integer, Integer> incoming = new Tuple5<String, String, Boolean, Integer, Integer>
                        (flight.getDestinationAirport(), dayString, isInternational, 0, capacity);
                out.collect(incoming);
                out.collect(outgoing);
            }
        });

        // in and out loads of airports per day and as domestic and international (boolean flag)
        DataSet<Tuple6<String, String, Boolean, Integer, Double, Boolean>> outgoingMarginals = inOutCapa.groupBy(0,1,2).reduceGroup(new GroupReduceFunction<Tuple5<String, String, Boolean, Integer, Integer>, Tuple6<String, String, Boolean, Integer, Double, Boolean>>() {
            @Override
            public void reduce(Iterable<Tuple5<String, String, Boolean, Integer, Integer>> tuple7s,
                               Collector<Tuple6<String, String, Boolean, Integer, Double, Boolean>> out) throws Exception {
                boolean first = true;
                Tuple6<String, String, Boolean, Integer, Double, Boolean> result = new Tuple6<String, String, Boolean, Integer, Double, Boolean>();
                for(Tuple5<String, String, Boolean, Integer, Integer> t : tuple7s) {
                    if(first) {
                        result.f0 = t.f0;
                        result.f1 = t.f1;
                        result.f2 = t.f2;
                        result.f3 = 0;
                        result.f4 = 1.0; // Ki(0)
                        result.f5 = true;
                        first = false;
                    }
                    result.f3 += t.f3;
                }
                result.f3 = (int)Math.round(SLF*result.f3);
                out.collect(result);
            }
        });

        DataSet<Tuple6<String, String, Boolean, Integer, Double, Boolean>> incomingMarginals = inOutCapa.groupBy(0,1,2).reduceGroup(new GroupReduceFunction<Tuple5<String, String, Boolean, Integer, Integer>, Tuple6<String, String, Boolean, Integer, Double, Boolean>>() {
            @Override
            public void reduce(Iterable<Tuple5<String, String, Boolean, Integer, Integer>> tuple7s,
                               Collector<Tuple6<String, String, Boolean, Integer, Double, Boolean>> out) throws Exception {
                boolean first = true;
                Tuple6<String, String, Boolean, Integer, Double, Boolean> result = new Tuple6<String, String, Boolean, Integer, Double, Boolean>();
                for(Tuple5<String, String, Boolean, Integer, Integer> t : tuple7s) {
                    if(first) {
                        result.f0 = t.f0;
                        result.f1 = t.f1;
                        result.f2 = t.f2;
                        result.f3 = 0;
                        result.f4 = 1.0; // Kj(0)
                        result.f5 = false;
                        first = false;
                    }
                    result.f3 += t.f4;
                }
                result.f3 = (int)Math.round(SLF*result.f3);
                out.collect(result);
            }
        });

        DataSet<Tuple2<Flight, Flight>> twoLegConnections = env.readFile(new FlightOutput.TwoLegFullInputFormat(), outputPath + "twoFull");
        DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), outputPath + "threeFull");

        DataSet<Tuple6<String, String, Boolean, String, Double, Integer>> nonStop = nonStopConnections.flatMap(new FlatMapFunction<Flight, Tuple6<String, String, Boolean, String, Double, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
            @Override
            public void flatMap(Flight value, Collector<Tuple6<String, String, Boolean, String, Double, Integer>> out) throws Exception {
                if(value.getDepartureTimestamp() > lastPossibleTimestamp || value.getDepartureTimestamp() < firstPossibleTimestamp) {
                    return;
                }
                Date date = new Date(value.getDepartureTimestamp());
                String dayString = format.format(date);
                long duration = value.getArrivalTimestamp() - value.getDepartureTimestamp();
                if (duration <= 0L)
                    throw new Exception("Value error: " + value.toString());
                Integer minutes = (int) (duration / (60L * 1000L));
                boolean isInternational = !value.getOriginCountry().equals(value.getDestinationCountry());
                out.collect(new Tuple6<String, String, Boolean, String, Double, Integer>
                        (value.getOriginAirport(), value.getDestinationAirport(), isInternational, dayString,
                                dist(value.getOriginLatitude(), value.getOriginLongitude(), value.getDestinationLatitude(), value.getDestinationLongitude()), minutes));
            }
        });

        DataSet<Tuple6<String, String, Boolean, String, Double, Integer>> twoLeg = twoLegConnections.flatMap(new FlatMapFunction<Tuple2<Flight, Flight>, Tuple6<String, String, Boolean, String, Double, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
            @Override
            public void flatMap(Tuple2<Flight, Flight> value, Collector<Tuple6<String, String, Boolean, String, Double, Integer>> out) throws Exception {
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
                boolean isInternational = !value.f0.getOriginCountry().equals(value.f1.getDestinationCountry());
                out.collect(new Tuple6<String, String, Boolean, String, Double, Integer>
                        (value.f0.getOriginAirport(), value.f1.getDestinationAirport(), isInternational, dayString,
                                dist(value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f1.getDestinationLatitude(), value.f1.getDestinationLongitude()), minutes));
            }
        });

        DataSet<Tuple6<String, String, Boolean, String, Double, Integer>> threeLeg = threeLegConnections.flatMap(new FlatMapFunction<Tuple3<Flight, Flight, Flight>, Tuple6<String, String, Boolean, String, Double, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
            @Override
            public void flatMap(Tuple3<Flight, Flight, Flight> value, Collector<Tuple6<String, String, Boolean, String, Double, Integer>> out) throws Exception {
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
                boolean isInternational = !value.f0.getOriginCountry().equals(value.f2.getDestinationCountry());
                out.collect(new Tuple6<String, String, Boolean, String, Double, Integer>
                        (value.f0.getOriginAirport(), value.f2.getDestinationAirport(), isInternational, dayString,
                                dist(value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f2.getDestinationLatitude(), value.f2.getDestinationLongitude()), minutes));
            }
        });

        DataSet<Tuple6<String, String, Boolean, String, Double, Integer>> flights = nonStop.union(twoLeg).union(threeLeg);
        DataSet<Tuple5<String, String, String, Boolean, SerializableVector>> distances = flights.groupBy(0, 1, 2, 3)
                .reduceGroup(new GroupReduceFunction<Tuple6<String, String, Boolean, String, Double, Integer>, Tuple5<String, String, String, Boolean, SerializableVector>>() {
                    @Override
                    public void reduce(Iterable<Tuple6<String, String, Boolean, String, Double, Integer>> values, Collector<Tuple5<String, String, String, Boolean, SerializableVector>> out) throws Exception {
                        Tuple5<String, String, String, Boolean, SerializableVector> result =
                                new Tuple5<String, String, String, Boolean, SerializableVector>();
                        int min = Integer.MAX_VALUE;
                        int max = Integer.MIN_VALUE;
                        int sum = 0;
                        int count = 0;
                        boolean first = true;
                        SerializableVector vector = new SerializableVector(OD_FEATURE_COUNT);
                        for (Tuple6<String, String, Boolean, String, Double, Integer> t : values) {
                            if (first) {
                                result.f0 = t.f0;
                                result.f1 = t.f1;
                                result.f2 = t.f3;
                                result.f3 = t.f2;
                                vector.getVector().setEntry(0, t.f4);
                                first = false;
                            }
                            int minutes = t.f5;
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
                        result.f4 = vector;
                        out.collect(result);
                    }
                });

        IterativeDataSet<Tuple6<String, String, Boolean, Integer, Double, Boolean>> initial = outgoingMarginals.union(incomingMarginals).iterate(MAX_ITERATIONS);

        DataSet<Tuple5<String, String, String, Boolean, Double>> KiFractions = distances
                .join(initial.filter(new IncomingFilter()), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(1, 2, 3).equalTo(0, 1, 2).with(new KJoiner());

        outgoingMarginals = KiFractions.groupBy(0, 2, 3).sum(4)
                .join(initial.filter(new OutgoingFilter()), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0, 2, 3).equalTo(0, 1, 2).with(new KUpdater());

        DataSet<Tuple5<String, String, String, Boolean, Double>> KjFractions = distances
                .join(outgoingMarginals, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0, 2, 3).equalTo(0, 1, 2).with(new KJoiner());

        incomingMarginals = KjFractions.groupBy(1, 2, 3).sum(4)
                .join(initial.filter(new IncomingFilter()), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(1, 2, 3).equalTo(0, 1, 2).with(new KUpdater());

        DataSet<Tuple6<String, String, Boolean, Integer, Double, Boolean>> iteration = outgoingMarginals.union(incomingMarginals);
        DataSet<Tuple6<String, String, Boolean, Integer, Double, Boolean>> result = initial.closeWith(iteration);


        DataSet<Tuple5<String, String, String, Double, SerializableVector>> trafficMatrix = distances
                .join(result.filter(new OutgoingFilter()), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0,2,3).equalTo(0,1,2).with(new TMJoinerOut())
                .join(result.filter(new IncomingFilter()), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(1,2,3).equalTo(0,1,2).with(new TMJoinerIn())
                .project(0, 1, 2, 4, 5);


        trafficMatrix/*.groupBy(2).sortGroup(3, Order.DESCENDING).first(100)*/.writeAsCsv(outputPath + "trafficMatrix", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        DataSet<Itinerary> nonStopItineraries = nonStopConnections.flatMap(new FlightExtractor1());
        DataSet<Itinerary> twoLegItineraries = twoLegConnections.flatMap(new FlightExtractor2());
        DataSet<Itinerary> threeLegItineraries = threeLegConnections.flatMap(new FlightExtractor3());
        DataSet<Itinerary> itineraries = nonStopItineraries.union(twoLegItineraries).union(threeLegItineraries);

        /*itineraries.filter(new FilterFunction<Itinerary>() {
            @Override
            public boolean filter(Itinerary itinerary) throws Exception {
                return itinerary.f2.equals("06052014");
            }
        }).writeAsCsv(outputPath + "itineraries", "\n", ",", FileSystem.WriteMode.OVERWRITE);*/


        DataSet<MIDT> midt = env.readTextFile("hdfs:///user/rwaury/input2/MIDTTotalHits.csv").flatMap(new MIDTParser()).groupBy(0,1,2,3,4,5,6,7).reduceGroup(new MIDTGrouper());
        DataSet<Tuple5<String, String, String, Integer, Integer>> bounds = midt.map(new LowerBoundExtractor());
        bounds.writeAsCsv(outputPath + "bounds", "\n", ",", FileSystem.WriteMode.OVERWRITE);
        //midt.groupBy(0,1,2).sortGroup(0, Order.ASCENDING).first(10000).writeAsCsv(outputPath + "groupedMIDT", "\n", ",", FileSystem.WriteMode.OVERWRITE);
        DataSet<Tuple4<String, String, String, LogitOptimizable>> trainedLogit = midt.groupBy(0,1,2).reduceGroup(new LogitTrainer());
        //trainedLogit.writeAsCsv(outputPath + "logitResult", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        DataSet<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> TMWithWeights = trafficMatrix
                .join(trainedLogit, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0,1,2).equalTo(0,1,2).with(new WeightTMJoiner());



        //TMWithWeights.groupBy(0,1,2).sortGroup(2, Order.ASCENDING).first(10000000).writeAsCsv(outputPath + "tmWeights", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        //itineraries.groupBy(0,1,2,3,4,5,6,7).sortGroup(2, Order.ASCENDING).first(1000000000).writeAsCsv(outputPath + "itineraries", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        DataSet<Tuple5<String, String, String, Double, LogitOptimizable>> allWeighted = TMWithWeights.coGroup(trafficMatrix).where(2).equalTo(2).with(new ODDistanceComparator());

        DataSet<Tuple11<String, String, String, String, String, String, Long, Integer, Itinerary, Integer, Double>> estimate = itineraries.coGroup(allWeighted).where(0,1,2).equalTo(0,1,2).with(new TrafficEstimator());

        estimate.groupBy(0,1).sortGroup(6, Order.DESCENDING).first(1000000000).writeAsCsv(outputPath + "ItineraryEstimate", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        estimate.groupBy(0,1).reduceGroup(new ODSum()).writeAsCsv(outputPath + "ODSum", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        env.execute("TrafficAnalysis");
    }

    private static class KJoiner implements JoinFunction<Tuple5<String, String, String, Boolean, SerializableVector>, Tuple6<String, String, Boolean, Integer, Double, Boolean>, Tuple5<String, String, String, Boolean, Double>> {
        @Override
        public Tuple5<String, String, String, Boolean, Double> join(Tuple5<String, String, String, Boolean, SerializableVector> distance, Tuple6<String, String, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            return new Tuple5<String, String, String, Boolean, Double>(distance.f0, distance.f1, distance.f2, distance.f3,
                    marginal.f3*marginal.f4*decayingFunction(distance.f4.getVector().getEntry(0), distance.f4.getVector().getEntry(1), distance.f4.getVector().getEntry(2), distance.f4.getVector().getEntry(3), distance.f4.getVector().getEntry(4)));
        }
    }

    private static class KUpdater implements JoinFunction<Tuple5<String, String, String, Boolean, Double>, Tuple6<String, String, Boolean, Integer, Double, Boolean>, Tuple6<String, String, Boolean, Integer, Double, Boolean>> {
        @Override
        public Tuple6<String, String, Boolean, Integer, Double, Boolean> join(Tuple5<String, String, String, Boolean, Double> Ksum, Tuple6<String, String, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            marginal.f4 = 1.0/Ksum.f4;
            return marginal;
        }
    }

    private static class OutgoingFilter implements FilterFunction<Tuple6<String, String, Boolean, Integer, Double, Boolean>> {
        @Override
        public boolean filter(Tuple6<String, String, Boolean, Integer, Double, Boolean> tuple) throws Exception {
            return tuple.f5.booleanValue();
        }
    }

    private static class IncomingFilter implements FilterFunction<Tuple6<String, String, Boolean, Integer, Double, Boolean>> {
        @Override
        public boolean filter(Tuple6<String, String, Boolean, Integer, Double, Boolean> tuple) throws Exception {
            return !tuple.f5.booleanValue();
        }
    }

    private static class TMJoinerOut implements JoinFunction<Tuple5<String, String, String, Boolean, SerializableVector>, Tuple6<String, String, Boolean, Integer, Double, Boolean>, Tuple6<String, String, String, Boolean, Double, SerializableVector>> {
        @Override
        public Tuple6<String, String, String, Boolean, Double, SerializableVector> join(Tuple5<String, String, String, Boolean, SerializableVector> distance, Tuple6<String, String, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            return new Tuple6<String, String, String, Boolean, Double, SerializableVector>(distance.f0, distance.f1, distance.f2, distance.f3,
                    marginal.f3*marginal.f4*decayingFunction(distance.f4.getVector().getEntry(0), distance.f4.getVector().getEntry(1), distance.f4.getVector().getEntry(2), distance.f4.getVector().getEntry(3), distance.f4.getVector().getEntry(4)), distance.f4);
        }
    }

    private static class TMJoinerIn implements JoinFunction<Tuple6<String, String, String, Boolean, Double, SerializableVector>, Tuple6<String, String, Boolean, Integer, Double, Boolean>, Tuple6<String, String, String, Boolean, Double, SerializableVector>> {
        @Override
        public Tuple6<String, String, String, Boolean, Double, SerializableVector> join(Tuple6<String, String, String, Boolean, Double, SerializableVector> tmOut, Tuple6<String, String, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            return new Tuple6<String, String, String, Boolean, Double, SerializableVector>(tmOut.f0, tmOut.f1, tmOut.f2, tmOut.f3, tmOut.f4*marginal.f3*marginal.f4, tmOut.f5);
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
            Integer travelTime = (int) ((flight.getArrivalTimestamp() - flight.getDepartureTimestamp())/(60L*1000L));
            out.collect(new Itinerary(flight.getOriginAirport(), flight.getDestinationAirport(), dayString,
                    flight.getAirline() + flight.getFlightNumber(), "", "", "", "", distance, distance, travelTime, 0, flight.getLegCount(), flight.getMaxCapacity(), 0, ""));
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
            Integer travelTime = (int) ((flight.f1.getArrivalTimestamp() - flight.f0.getDepartureTimestamp())/(60L*1000L));
            Integer waitingTime = (int) ((flight.f1.getDepartureTimestamp() - flight.f0.getArrivalTimestamp())/(60L*1000L));
            Integer legCount = flight.f0.getLegCount() + flight.f1.getLegCount();
            Integer maxCapacity = Math.min(flight.f0.getMaxCapacity(), flight.f1.getMaxCapacity());
            out.collect(new Itinerary(flight.f0.getOriginAirport(), flight.f1.getDestinationAirport(), dayString,
                    flight.f0.getAirline() + flight.f0.getFlightNumber(), flight.f1.getAirline() + flight.f1.getFlightNumber(), "", "", "",
                    directDistance, travelledDistance, travelTime, waitingTime, legCount, maxCapacity, 0, ""));
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
            Integer travelTime = (int) ((flight.f2.getArrivalTimestamp() - flight.f0.getDepartureTimestamp())/(60L*1000L));
            Integer waitingTime = (int) (
                    ((flight.f1.getDepartureTimestamp() - flight.f0.getArrivalTimestamp()) +
                    (flight.f2.getDepartureTimestamp() - flight.f1.getArrivalTimestamp())) /
                    (60L*1000L));
            Integer legCount = flight.f0.getLegCount() + flight.f1.getLegCount() + flight.f2.getLegCount();
            Integer maxCapacity = Math.min(flight.f0.getMaxCapacity(), Math.min(flight.f1.getMaxCapacity(), flight.f2.getMaxCapacity()));
            out.collect(new Itinerary(flight.f0.getOriginAirport(), flight.f2.getDestinationAirport(), dayString,
                    flight.f0.getAirline() + flight.f0.getFlightNumber(), flight.f1.getAirline() + flight.f1.getFlightNumber(), flight.f2.getAirline() + flight.f2.getFlightNumber(), "", "",
                    directDistance, travelledDistance, travelTime, waitingTime, legCount, maxCapacity, 0, ""));
        }
    }

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
            //int legsInEntry = (tmp.length-(6+1))/9;
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
            if(travelTime < 0) {
                return;
            }
            long departureTimestamp = firstPossibleTimestamp + (departureDay*24L*60L*60L*1000L);
            Date date = new Date(departureTimestamp);
            String dayString = format.format(date);
            MIDT result = new MIDT(origin, destination, dayString,
                    flight1, flight2, flight3, flight4, flight5, travelTime, waitingTime,
                    segmentCount, pax);
            out.collect(result);
        }
    }

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
                double percentageWaiting = (midt.f8 == 0 || midt.f9 == 0) ? 0 : midt.f9/midt.f8;
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

    private static class WeightTMJoiner implements JoinFunction<Tuple5<String, String, String, Double, SerializableVector>, Tuple4<String, String, String, LogitOptimizable>,
            Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> {

        @Override
        public Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable> join(Tuple5<String, String, String, Double, SerializableVector> tmEntry, Tuple4<String, String, String, LogitOptimizable> logit) throws Exception {
            return new Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>(tmEntry.f0, tmEntry.f1, tmEntry.f2, tmEntry.f3, tmEntry.f4, logit.f3);
        }
    }

    private static class ODDistanceComparator extends
            RichCoGroupFunction<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>,
                    Tuple5<String, String, String, Double, SerializableVector>,
                    Tuple5<String, String, String, Double, LogitOptimizable>> {

        //private List<Tuple2<String, SerializableMatrix>> matrices = null;

        @Override
        public void open(Configuration parameters) {
            //this.matrices = getRuntimeContext().getBroadcastVariable(INVERTED_COVARIANCE_MATRIX);
        }

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

    private static class TrafficEstimator implements CoGroupFunction<Itinerary, Tuple5<String, String, String, Double, LogitOptimizable>,
            Tuple11<String, String, String, String, String, String, Long, Integer, Itinerary, Integer, Double>> {

        @Override
        public void coGroup(Iterable<Itinerary> connections, Iterable<Tuple5<String, String, String, Double, LogitOptimizable>> logitModel,
                            Collector<Tuple11<String, String, String, String, String, String, Long, Integer, Itinerary, Integer, Double>> out) throws Exception {
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
            ArrayList<Itinerary> itineraries = new ArrayList<Itinerary>();
            int minTime = Integer.MAX_VALUE;
            while (connIter.hasNext()) {
                Itinerary e = connIter.next().deepCopy();
                /*if(itineraries.contains(e)) {
                    throw new Exception("Itinerary found twice:" + e.toString());
                }*/
                itineraries.add(e);
                if(e.f10 < minTime) {
                    minTime = e.f10;
                }
                count++;
            }
            if(minTime < 1) {
                minTime = 1;
            }
            double softmaxSum = 0.0;
            for(Itinerary e : itineraries) {
                softmaxSum += Math.exp(LogitOptimizable.linearPredictorFunction(LogitOptimizable.toArray(e, minTime), weights));
            }
            for(Itinerary e : itineraries) {
                double itineraryEstimate = LogitOptimizable.softmax(e, softmaxSum, weights, minTime)*estimate;
                long roundedEstimate = Math.round(itineraryEstimate);
                if(roundedEstimate > 0L) {
                    out.collect(new Tuple11<String, String, String, String, String, String, Long, Integer, Itinerary, Integer, Double>(e.f0, e.f1, e.f2, e.f3, e.f4, e.f5, roundedEstimate, e.f10, e, count, logit.f3));
                }
            }

        }
    }

    private static class ODSum implements GroupReduceFunction<Tuple11<String, String, String, String, String, String, Long, Integer, Itinerary, Integer, Double>,
            Tuple6<String, String, Integer, Integer, Long, Double>> {

        @Override
        public void reduce(Iterable<Tuple11<String, String, String, String, String, String, Long, Integer, Itinerary, Integer, Double>> iterable,
                           Collector<Tuple6<String, String, Integer, Integer, Long, Double>> out) throws Exception {
            Tuple6<String, String, Integer, Integer, Long, Double> result = new Tuple6<String, String, Integer, Integer, Long, Double>();
            HashSet<String> days = new HashSet<String>(7);
            int itinCount = 0;
            long paxSum = 0L;
            double estimateSum = 0.0;
            for(Tuple11<String, String, String, String, String, String, Long, Integer, Itinerary, Integer, Double> t : iterable) {
                if(!days.contains(t.f2)) {
                    days.add(t.f2);
                    estimateSum += t.f10;
                }
                result.f0 = t.f0;
                result.f1 = t.f1;
                paxSum += t.f6;
                itinCount++;
                result.f3 = t.f9;
            }
            result.f2 = itinCount;
            result.f4 = paxSum;
            result.f5 = estimateSum;
            out.collect(result);
        }
    }

    private static class LowerBoundExtractor implements MapFunction<MIDT, Tuple5<String, String, String, Integer, Integer>> {
        @Override
        public Tuple5<String, String, String, Integer, Integer> map(MIDT midt) throws Exception {
            return new Tuple5<String, String, String, Integer, Integer>(midt.f0, midt.f1, midt.f2, midt.f11, 0);
        }
    }

    private static class CovarianceMatrix implements GroupReduceFunction<Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>,
            Tuple2<String, SerializableMatrix>> {

        @Override
        public void reduce(Iterable<Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>> iterable, Collector<Tuple2<String, SerializableMatrix>> collector) throws Exception {

        }
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

    private static double decayingFunction(double minTravelTime, double maxTravelTime, double avgTravelTime, double distance, double count) {
        /*double mu = 3.3;
        double sigma = 0.5;
        double sigma2 = Math.pow(sigma, 2.0);
        double x = distance;
        double numerator = Math.exp((-Math.pow(Math.log(x)-mu, 2.0))/2.0*sigma2);
        double denominator = x*sigma*Math.sqrt(2.0*Math.PI);
        return numerator/denominator;*/
        return 1.0/(avgTravelTime*distance);
        //return Math.exp(-2*(Math.log(distance)-1.8)*(Math.log(distance)-1.8));
    }

    private static double mahalanobisDistance(ArrayRealVector x, ArrayRealVector y, Array2DRowRealMatrix Sinv) {
        ArrayRealVector xmy = x.subtract(y);
        RealVector SinvXmy = Sinv.operate(xmy);
        double result = xmy.dotProduct(SinvXmy);
        return Math.sqrt(result);
    }

}

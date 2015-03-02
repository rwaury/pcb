package com.amadeus.ti.analysis;

import com.amadeus.ti.pcb.CBUtil;
import com.amadeus.ti.pcb.Flight;
import com.amadeus.ti.pcb.FlightOutput;
import com.amadeus.ti.pcb.RegionExtractor;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

public class TrafficAnalysis {

    public static final double OPTIMIZER_TOLERANCE = 0.000001;

    public static final int MAX_OPTIMIZER_ITERATIONS = 1000;

    public static long firstPossibleTimestamp = 1399248000000L;
    public static long lastPossibleTimestamp = 1399852799000L;

    public static final double SLF = 0.795;

    public static final double WAITING_FACTOR = 1.0;

    public static final int MAX_ITERATIONS = 10;

    public static String AP_COUNTRY_MAPPING = "AirportCountryMappingBroadcastSet";

    public static String INVERTED_COVARIANCE_MATRIX = "InvertedCovarianceMatrixBroadcastSet";

    public static final int OD_FEATURE_COUNT = 5;

    public final static ArrayList<String> countriesWithStates = new ArrayList<String>() {{
        add("AR");
        add("AU");
        add("BR");
        add("CA");
        add("US");
    }};

    public final static SimpleDateFormat dayFormat = new SimpleDateFormat("ddMMyyyy");

    private static String oriPath = "hdfs:///user/rwaury/input2/ori_por_public.csv";
    private static String regionPath = "hdfs:///user/rwaury/input2/ori_country_region_info.csv";
    private static String midtPath = "hdfs:///user/rwaury/input2/MIDTTotalHits.csv";
    private static String outputPath = "hdfs:///user/rwaury/output2/flights/";

    private static final JoinOperatorBase.JoinHint JOIN_HINT = JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE;
    private static final FileSystem.WriteMode OVERWRITE = FileSystem.WriteMode.OVERWRITE;

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<String, String, String, String>> airportCountryNR = env.readTextFile(oriPath)
                .flatMap(new AirportCountryExtractor());

        DataSet<Tuple2<String, String>> regionInfo = env.readTextFile(regionPath).map(new RegionExtractor());
        //regionInfo.writeAsCsv(outputPath + "regionInfo", "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<Tuple4<String, String, String, String>> airportCountry = airportCountryNR.join(regionInfo).where(2).equalTo(0).with(new RegionJoiner());
        //airportCountry.writeAsCsv(outputPath + "airportCountry", "\n", ",", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull");
        DataSet<Tuple2<Flight, Flight>> twoLegConnections = env.readFile(new FlightOutput.TwoLegFullInputFormat(), outputPath + "twoFull");
        DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), outputPath + "threeFull");

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
        flightBounds.writeAsCsv(outputPath + "flightCapacity", "\n", ",", OVERWRITE).setParallelism(1);

        DataSet<String> midtStrings = env.readTextFile(midtPath);
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
                    out.collect(TAUtil.MIDTToItinerary(midtIter.next()));
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
        ODBounds.writeAsCsv(outputPath + "ODBounds", "\n", ",", OVERWRITE).setParallelism(1);

        DataSet<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> inOutCapa = nonStopConnections.flatMap(new FlatMapFunction<Flight, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>>() {
            @Override
            public void flatMap(Flight flight, Collector<Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>> out) throws Exception {
                if(flight.getLegCount() > 1) {
                    return;
                }
                if(flight.getDepartureTimestamp() > lastPossibleTimestamp || flight.getDepartureTimestamp() < firstPossibleTimestamp) {
                    return;
                }
                Date date = new Date(flight.getDepartureTimestamp());
                String dayString = dayFormat.format(date);
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

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> outgoingMarginals = outgoingMarginalsAgg.join(APBounds, JOIN_HINT).where(0,1,2,3,4).equalTo(0,1,2,3,4).with(
                new JoinFunction<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>>() {
                    @Override
                    public Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> join(Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> midtBound) throws Exception {
                        int estimateResidual = Math.max(marginal.f5 - midtBound.f5, 0);
                        return new Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>(marginal.f0, marginal.f1, marginal.f2, marginal.f3, marginal.f4, estimateResidual, marginal.f6, marginal.f7);
                    }
                });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> incomingMarginals = incomingMarginalsAgg.join(APBounds, JOIN_HINT).where(0,1,2,3,4).equalTo(0,1,2,3,4).with(
                new JoinFunction<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>>() {
                    @Override
                    public Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> join(Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal, Tuple7<String, String, Boolean, Boolean, Boolean, Integer, Integer> midtBound) throws Exception {
                        int estimateResidual = Math.max(marginal.f5 - midtBound.f6, 0);
                        return new Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>(marginal.f0, marginal.f1, marginal.f2, marginal.f3, marginal.f4, estimateResidual, marginal.f6, marginal.f7);
                    }
                });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> nonStop = nonStopConnections.flatMap(new FlatMapFunction<Flight, Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>>() {
            @Override
            public void flatMap(Flight value, Collector<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> out) throws Exception {
                if(value.getDepartureTimestamp() > lastPossibleTimestamp || value.getDepartureTimestamp() < firstPossibleTimestamp) {
                    return;
                }
                Date date = new Date(value.getDepartureTimestamp());
                String dayString = dayFormat.format(date);
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
                         CBUtil.dist(value.getOriginLatitude(), value.getOriginLongitude(), value.getDestinationLatitude(), value.getDestinationLongitude()), minutes));
            }
        });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> twoLeg = twoLegConnections.flatMap(new FlatMapFunction<Tuple2<Flight, Flight>, Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>>() {
            @Override
            public void flatMap(Tuple2<Flight, Flight> value, Collector<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> out) throws Exception {
                if(value.f0.getDepartureTimestamp() > lastPossibleTimestamp || value.f0.getDepartureTimestamp() < firstPossibleTimestamp) {
                    return;
                }
                Date date = new Date(value.f0.getDepartureTimestamp());
                String dayString = dayFormat.format(date);
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
                         CBUtil.dist(value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f1.getDestinationLatitude(), value.f1.getDestinationLongitude()), minutes));
            }
        });

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> threeLeg = threeLegConnections.flatMap(new FlatMapFunction<Tuple3<Flight, Flight, Flight>, Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>>() {
            @Override
            public void flatMap(Tuple3<Flight, Flight, Flight> value, Collector<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> out) throws Exception {
                if(value.f0.getDepartureTimestamp() > lastPossibleTimestamp || value.f0.getDepartureTimestamp() < firstPossibleTimestamp) {
                    return;
                }
                Date date = new Date(value.f0.getDepartureTimestamp());
                String dayString = dayFormat.format(date);
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
                         CBUtil.dist(value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f2.getDestinationLatitude(), value.f2.getDestinationLongitude()), minutes));
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
                .join(initial.filter(new IncomingFilter()), JOIN_HINT).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new KJoiner());

        outgoingMarginals = KiFractions.groupBy(0,2,3,4,5).sum(6)
                .join(initial.filter(new OutgoingFilter()), JOIN_HINT).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new KUpdater());

        DataSet<Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>> KjFractions = distances
                .join(outgoingMarginals, JOIN_HINT).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new KJoiner());

        incomingMarginals = KjFractions.groupBy(1,2,3,4,5).sum(6)
                .join(initial.filter(new IncomingFilter()), JOIN_HINT).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new KUpdater());

        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> iteration = outgoingMarginals.union(incomingMarginals);
        DataSet<Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>> result = initial.closeWith(iteration);
        /* IPF END */

        DataSet<Tuple5<String, String, String, Double, SerializableVector>> trafficMatrix = distances
                .join(result.filter(new OutgoingFilter()), JOIN_HINT).where(0,2,3,4,5).equalTo(0,1,2,3,4).with(new TMJoinerOut())
                .join(result.filter(new IncomingFilter()), JOIN_HINT).where(1,2,3,4,5).equalTo(0,1,2,3,4).with(new TMJoinerIn())
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

        TMWithMIDT.project(0,1,2,3).writeAsCsv(outputPath + "trafficMatrix", "\n", ",", OVERWRITE);

        //midt.groupBy(0,1,2).sortGroup(0, Order.ASCENDING).first(10000).writeAsCsv(outputPath + "groupedMIDT", "\n", ",", FileSystem.WriteMode.OVERWRITE);
        DataSet<Tuple4<String, String, String, LogitOptimizable>> trainedLogit = midt.groupBy(0,1,2).reduceGroup(new LogitTrainer());
        //trainedLogit.writeAsCsv(outputPath + "logitResult", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        DataSet<Tuple6<String, String, String, Double, SerializableVector, LogitOptimizable>> TMWithWeights = TMWithMIDT
                .join(trainedLogit, JOIN_HINT).where(0,1,2).equalTo(0,1,2).with(new WeightTMJoiner());

        //TMWithWeights.groupBy(0,1,2).sortGroup(2, Order.ASCENDING).first(10000000).writeAsCsv(outputPath + "tmWeights", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        //itineraries.groupBy(0,1,2,3,4,5,6,7).sortGroup(2, Order.ASCENDING).first(1000000000).writeAsCsv(outputPath + "itineraries", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        DataSet<Tuple5<String, String, String, Double, LogitOptimizable>> allWeighted = TMWithWeights.coGroup(TMWithMIDT).where(2).equalTo(2).with(new ODDistanceComparator());

        DataSet<Itinerary> estimate = itinerariesWithMIDT.coGroup(allWeighted).where(0,1,2).equalTo(0,1,2).with(new TrafficEstimator());

        estimate.groupBy(0,1,2).sortGroup(15, Order.DESCENDING).first(1000000000).writeAsCsv(outputPath + "ItineraryEstimate", "\n", ",", OVERWRITE);

        estimate.groupBy(0,1).reduceGroup(new ODSum()).writeAsCsv(outputPath + "ODSum", "\n", ",", OVERWRITE).setParallelism(1);

        env.execute("TrafficAnalysis");
    }

    // IPF operator
    private static class KJoiner implements JoinFunction<Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector>, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean>, Tuple7<String, String, String, Boolean, Boolean, Boolean, Double>> {
        @Override
        public Tuple7<String, String, String, Boolean, Boolean, Boolean, Double> join(Tuple7<String, String, String, Boolean, Boolean, Boolean, SerializableVector> distance, Tuple8<String, String, Boolean, Boolean, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            double KFraction = marginal.f5*marginal.f6*TAUtil.decayingFunction(distance.f6.getVector().getEntry(0), distance.f6.getVector().getEntry(1), distance.f6.getVector().getEntry(2), distance.f6.getVector().getEntry(3), distance.f6.getVector().getEntry(4), distance.f3, distance.f4, distance.f5);
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
            double partialDist = marginal.f5*marginal.f6*TAUtil.decayingFunction(distance.f6.getVector().getEntry(0), distance.f6.getVector().getEntry(1), distance.f6.getVector().getEntry(2), distance.f6.getVector().getEntry(3), distance.f6.getVector().getEntry(4), distance.f3, distance.f4, distance.f5);
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

    // emit number of MIDT bookings for an OD
    private static class LowerBoundExtractor implements MapFunction<MIDT, Tuple5<String, String, String, Integer, Integer>> {
        @Override
        public Tuple5<String, String, String, Integer, Integer> map(MIDT midt) throws Exception {
            return new Tuple5<String, String, String, Integer, Integer>(midt.f0, midt.f1, midt.f2, midt.f11, 0);
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

}

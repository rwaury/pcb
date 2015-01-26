package com.amadeus.pcb.join;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TrafficAnalysis {

    private static final double WAITING_FACTOR = 1.0;

    private static final int MAX_ITERATIONS = 10;

    private static String outputPath = "hdfs:///user/rwaury/output/flights/";

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
                Date date = new Date(flight.getDepartureTimestamp());
                String dayString = format.format(date);
                boolean isInternational = !flight.getOriginCountry().equals(flight.getDestinationCountry());
                Tuple5<String, String, Boolean, Integer, Integer> outgoing = new Tuple5<String, String, Boolean, Integer, Integer>
                        (flight.getOriginAirport(), dayString, isInternational, flight.getMaxCapacity(), 0);
                Tuple5<String, String, Boolean, Integer, Integer> incoming = new Tuple5<String, String, Boolean, Integer, Integer>
                        (flight.getDestinationAirport(), dayString, isInternational, 0, flight.getMaxCapacity());
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
                out.collect(result);
            }
        });

        DataSet<Tuple2<Flight, Flight>> twoLegConnections = env.readFile(new FlightOutput.TwoLegFullInputFormat(), outputPath + "twoFull");
        DataSet<Tuple3<Flight, Flight, Flight>> threeLegConnections = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), outputPath + "threeFull");

        DataSet<Tuple6<String, String, Boolean, Double, String, Integer>> nonStop = nonStopConnections.map(new MapFunction<Flight, Tuple6<String, String, Boolean, Double, String, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

            @Override
            public Tuple6<String, String, Boolean, Double, String, Integer> map(Flight value) throws Exception {
                Date date = new Date(value.getDepartureTimestamp());
                String dayString = format.format(date);
                long duration = value.getArrivalTimestamp() - value.getDepartureTimestamp();
                if(duration <= 0L)
                    throw new Exception("Value error: " + value.toString());
                Integer minutes = (int) (duration / (60L * 1000L));
                boolean isInternational = !value.getOriginCountry().equals(value.getDestinationCountry());
                return new Tuple6<String, String, Boolean, Double, String, Integer>
                (value.getOriginAirport(), value.getDestinationAirport(), isInternational,
                dist(value.getOriginLatitude(), value.getOriginLongitude(), value.getDestinationLatitude(), value.getDestinationLongitude()), dayString, minutes);
            }
        });

        DataSet<Tuple6<String, String, Boolean, Double, String, Integer>> twoLeg = twoLegConnections.map(new MapFunction<Tuple2<Flight, Flight>, Tuple6<String, String, Boolean, Double, String, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

            @Override
            public Tuple6<String, String, Boolean, Double, String, Integer> map(Tuple2<Flight, Flight> value) throws Exception {
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
                return new Tuple6<String, String, Boolean, Double, String, Integer>
                (value.f0.getOriginAirport(), value.f1.getDestinationAirport(), isInternational,
                dist(value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f1.getDestinationLatitude(), value.f1.getDestinationLongitude()), dayString, minutes);
            }
        });

        DataSet<Tuple6<String, String, Boolean, Double, String, Integer>> threeLeg = threeLegConnections.map(new MapFunction<Tuple3<Flight, Flight, Flight>, Tuple6<String, String, Boolean, Double, String, Integer>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

            @Override
            public Tuple6<String, String, Boolean, Double, String, Integer> map(Tuple3<Flight, Flight, Flight> value) throws Exception {
                Date date = new Date(value.f0.getDepartureTimestamp());
                String dayString = format.format(date);
                long flight1 = value.f0.getArrivalTimestamp() - value.f0.getDepartureTimestamp();
                long wait1 = (long)(WAITING_FACTOR * (value.f1.getDepartureTimestamp() - value.f0.getArrivalTimestamp()));
                long flight2 = value.f1.getArrivalTimestamp() - value.f1.getDepartureTimestamp();
                long wait2 = (long)(WAITING_FACTOR * (value.f2.getDepartureTimestamp() - value.f1.getArrivalTimestamp()));
                long flight3 = value.f2.getArrivalTimestamp() - value.f2.getDepartureTimestamp();
                long duration = flight1 + wait1 + flight2 + wait2 + flight3;
                if(duration <= 0L)
                    throw new Exception("Value error: " + value.toString());
                Integer minutes = (int) (duration / (60L * 1000L));
                boolean isInternational = !value.f0.getOriginCountry().equals(value.f2.getDestinationCountry());
                return new Tuple6<String, String, Boolean, Double, String, Integer>
                (value.f0.getOriginAirport(), value.f2.getDestinationAirport(), isInternational,
                dist(value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f2.getDestinationLatitude(), value.f2.getDestinationLongitude()), dayString, minutes);
            }
        });

        DataSet<Tuple6<String, String, Boolean, Double, String, Integer>> flights = nonStop.union(twoLeg).union(threeLeg);
        DataSet<Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>> distances = flights.groupBy(0, 1, 2, 3, 4).reduceGroup(new GroupReduceFunction<Tuple6<String, String, Boolean, Double, String, Integer>, Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple6<String, String, Boolean, Double, String, Integer>> values, Collector<Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>> out) throws Exception {
                Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer> result =
                        new Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>();
                int min = Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;
                int sum = 0;
                int count = 0;
                boolean first = true;
                for (Tuple6<String, String, Boolean, Double, String, Integer> t : values) {
                    if (first) {
                        result.f0 = t.f0;
                        result.f1 = t.f1;
                        result.f2 = t.f4;
                        result.f3 = t.f2;
                        result.f4 = t.f3;
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
                result.f5 = min;
                result.f6 = max;
                result.f7 = avg;
                result.f8 = count;
                out.collect(result);
            }
        });

        IterativeDataSet<Tuple6<String, String, Boolean, Integer, Double, Boolean>> initial = outgoingMarginals.union(incomingMarginals).iterate(MAX_ITERATIONS);

        DataSet<Tuple5<String, String, String, Boolean, Double>> KiFractions = distances.join(initial.filter(new IncomingFilter())).where(1, 2, 3).equalTo(0, 1, 2).with(new KJoiner());
        outgoingMarginals = KiFractions.groupBy(0, 2, 3).sum(4).join(initial.filter(new OutgoingFilter())).where(0, 2, 3).equalTo(0, 1, 2).with(new KUpdater());

        DataSet<Tuple5<String, String, String, Boolean, Double>> KjFractions = distances.join(outgoingMarginals).where(0, 2, 3).equalTo(0, 1, 2).with(new KJoiner());
        incomingMarginals = KjFractions.groupBy(1, 2, 3).sum(4).join(initial.filter(new IncomingFilter())).where(1, 2, 3).equalTo(0, 1, 2).with(new KUpdater());

        DataSet<Tuple6<String, String, Boolean, Integer, Double, Boolean>> iteration = outgoingMarginals.union(incomingMarginals);

        DataSet<Tuple6<String, String, Boolean, Integer, Double, Boolean>> result = initial.closeWith(iteration);


        DataSet<Tuple5<String, String, String, Boolean, Double>> trafficMatrix = distances.join(result.filter(new OutgoingFilter())).where(0,2,3).equalTo(0,1,2).with(new TMJoinerOut()).join(result.filter(new IncomingFilter())).where(1,2,3).equalTo(0,1,2).with(new TMJoinerIn());

        trafficMatrix.project(0,1,2,4)/*.groupBy(2).sortGroup(3, Order.DESCENDING).first(100)*/.writeAsCsv(outputPath + "trafficMatrix", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        /*
        DataSet<Itinerary> nonStopItineraries = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull").map(new FlightExtractor1());
        DataSet<Itinerary> twoLegItineraries = env.readFile(new FlightOutput.TwoLegFullInputFormat(), outputPath + "twoFull").map(new FlightExtractor2());
        DataSet<Itinerary> threeLegItineraries = env.readFile(new FlightOutput.ThreeLegFullInputFormat(), outputPath + "threeFull").map(new FlightExtractor3());

        DataSet<Itinerary> itineraries = nonStopItineraries.union(twoLegItineraries).union(threeLegItineraries);

        itineraries.filter(new FilterFunction<Itinerary>() {
            @Override
            public boolean filter(Itinerary itinerary) throws Exception {
                return itinerary.f2.equals("06052014");
            }
        }).writeAsCsv(outputPath + "itineraries", "\n", ",", FileSystem.WriteMode.OVERWRITE);*/

        env.execute("TrafficAnalysis");
    }

    private static class KJoiner implements JoinFunction<Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>, Tuple6<String, String, Boolean, Integer, Double, Boolean>, Tuple5<String, String, String, Boolean, Double>> {
        @Override
        public Tuple5<String, String, String, Boolean, Double> join(Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer> distance, Tuple6<String, String, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            return new Tuple5<String, String, String, Boolean, Double>(distance.f0, distance.f1, distance.f2, distance.f3, marginal.f3*marginal.f4*decayingFunction((double)distance.f5));
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

    private static class TMJoinerOut implements JoinFunction<Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer>, Tuple6<String, String, Boolean, Integer, Double, Boolean>, Tuple5<String, String, String, Boolean, Double>> {
        @Override
        public Tuple5<String, String, String, Boolean, Double> join(Tuple9<String, String, String, Boolean, Double, Integer, Integer, Double, Integer> distance, Tuple6<String, String, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            return new Tuple5<String, String, String, Boolean, Double>(distance.f0, distance.f1, distance.f2, distance.f3, marginal.f3*marginal.f4*decayingFunction((double)distance.f5));
        }
    }

    private static class TMJoinerIn implements JoinFunction<Tuple5<String, String, String, Boolean, Double>, Tuple6<String, String, Boolean, Integer, Double, Boolean>, Tuple5<String, String, String, Boolean, Double>> {
        @Override
        public Tuple5<String, String, String, Boolean, Double> join(Tuple5<String, String, String, Boolean, Double> tmOut, Tuple6<String, String, Boolean, Integer, Double, Boolean> marginal) throws Exception {
            return new Tuple5<String, String, String, Boolean, Double>(tmOut.f0, tmOut.f1, tmOut.f2, tmOut.f3, tmOut.f4*marginal.f3*marginal.f4);
        }
    }

    private static class FlightExtractor1 implements MapFunction<Flight, Itinerary> {
        SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
        @Override
        public Itinerary map(Flight flight) throws Exception {
            Date date = new Date(flight.getDepartureTimestamp());
            String dayString = format.format(date);
            Double distance = dist(flight.getOriginLatitude(), flight.getOriginLongitude(), flight.getDestinationLatitude(), flight.getDestinationLongitude());
            Integer travelTime = (int) ((flight.getArrivalTimestamp() - flight.getDepartureTimestamp())/(60L*1000L));
            return new Itinerary(flight.getOriginAirport(), flight.getDestinationAirport(), dayString, flight.getDepartureTimestamp(),
                    flight.getAirline() + flight.getFlightNumber(), "", "", distance, distance, travelTime, 0, flight.getLegCount(), flight.getMaxCapacity());
        }
    }

    private static class FlightExtractor2 implements MapFunction<Tuple2<Flight, Flight>, Itinerary> {
        SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
        @Override
        public Itinerary map(Tuple2<Flight, Flight> flight) throws Exception {
            Date date = new Date(flight.f0.getDepartureTimestamp());
            String dayString = format.format(date);
            Double directDistance = dist(flight.f0.getOriginLatitude(), flight.f0.getOriginLongitude(), flight.f1.getDestinationLatitude(), flight.f1.getDestinationLongitude());
            Double travelledDistance = dist(flight.f0.getOriginLatitude(), flight.f0.getOriginLongitude(), flight.f0.getDestinationLatitude(), flight.f0.getDestinationLongitude()) +
                    dist(flight.f1.getOriginLatitude(), flight.f1.getOriginLongitude(), flight.f1.getDestinationLatitude(), flight.f1.getDestinationLongitude());
            Integer travelTime = (int) ((flight.f1.getArrivalTimestamp() - flight.f0.getDepartureTimestamp())/(60L*1000L));
            Integer waitingTime = (int) ((flight.f1.getDepartureTimestamp() - flight.f0.getArrivalTimestamp())/(60L*1000L));
            Integer legCount = flight.f0.getLegCount() + flight.f1.getLegCount();
            Integer maxCapacity = Math.min(flight.f0.getMaxCapacity(), flight.f1.getMaxCapacity());
            return new Itinerary(flight.f0.getOriginAirport(), flight.f1.getDestinationAirport(), dayString, flight.f0.getDepartureTimestamp(),
                    flight.f0.getAirline() + flight.f0.getFlightNumber(), flight.f1.getAirline() + flight.f1.getFlightNumber(), "",
                    directDistance, travelledDistance, travelTime, waitingTime, legCount, maxCapacity);
        }
    }

    private static class FlightExtractor3 implements MapFunction<Tuple3<Flight, Flight, Flight>, Itinerary> {
        SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
        @Override
        public Itinerary map(Tuple3<Flight, Flight, Flight> flight) throws Exception {
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
            return new Itinerary(flight.f0.getOriginAirport(), flight.f2.getDestinationAirport(), dayString, flight.f0.getDepartureTimestamp(),
                    flight.f0.getAirline() + flight.f0.getFlightNumber(), flight.f1.getAirline() + flight.f1.getFlightNumber(), flight.f2.getAirline() + flight.f2.getFlightNumber(),
                    directDistance, travelledDistance, travelTime, waitingTime, legCount, maxCapacity);
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

    private static double decayingFunction(double distance) {
        return 1.0/Math.sqrt(distance);
    }
}

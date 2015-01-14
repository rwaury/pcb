package com.amadeus.pcb.join;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by robert on 14/01/15.
 */
public class TrafficAnalysis {

    private static final double WAITING_FACTOR = 1.0;

    private static final int MAX_ITERATIONS = 5;

    private static String outputPath = "hdfs:///user/rwaury/output/flights/";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Flight> nonStopConnections = env.readFile(new FlightOutput.NonStopFullInputFormat(), outputPath + "oneFull");

        DataSet<Tuple5<String, String, Integer, Integer, Boolean>> inOutCapa = nonStopConnections.flatMap(new FlatMapFunction<Flight, Tuple5<String, String, Integer, Integer, Boolean>>() {
            SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

            @Override
            public void flatMap(Flight flight, Collector<Tuple5<String, String, Integer, Integer, Boolean>> out) throws Exception {
                if(flight.getLegCount() > 1) {
                    return;
                }
                Date date = new Date(flight.getDepartureTimestamp());
                String dayString = format.format(date);
                boolean isInternational = !flight.getOriginCountry().equals(flight.getDestinationCountry());
                Tuple5<String, String, Integer, Integer, Boolean> outgoing = new Tuple5<String, String, Integer, Integer, Boolean>
                        (flight.getOriginAirport(), dayString, flight.getMaxCapacity(), 0, isInternational);
                Tuple5<String, String, Integer, Integer, Boolean> incoming = new Tuple5<String, String, Integer, Integer, Boolean>
                        (flight.getDestinationAirport(), dayString, 0, flight.getMaxCapacity(), isInternational);
                out.collect(incoming);
                out.collect(outgoing);
            }
        });

        // in and out loads of airports per day and as domestic and international (boolean flag)
        DataSet<Tuple5<String, String, Integer, Integer, Boolean>> loads = inOutCapa.groupBy(0,1,4).reduceGroup(new GroupReduceFunction<Tuple5<String, String, Integer, Integer, Boolean>, Tuple5<String, String, Integer, Integer, Boolean>>() {
            @Override
            public void reduce(Iterable<Tuple5<String, String, Integer, Integer, Boolean>> tuple7s,
                               Collector<Tuple5<String, String, Integer, Integer, Boolean>> out) throws Exception {
                boolean first = true;
                Tuple5<String, String, Integer, Integer, Boolean> result = new Tuple5<String, String, Integer, Integer, Boolean>();
                for(Tuple5<String, String, Integer, Integer, Boolean> t : tuple7s) {
                    if(first) {
                        result.f0 = t.f0;
                        result.f1 = t.f1;
                        result.f2 = 0;
                        result.f3 = 0;
                        result.f4 = t.f4;
                        first = false;
                    }
                    result.f2 += t.f2;
                    result.f3 += t.f3;
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
        DataSet<Tuple9<String, String, Boolean, Double, String, Integer, Integer, Double, Integer>> result = flights.groupBy(0, 1, 2, 3, 4).reduceGroup(new GroupReduceFunction<Tuple6<String, String, Boolean, Double, String, Integer>, Tuple9<String, String, Boolean, Double, String, Integer, Integer, Double, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple6<String, String, Boolean, Double, String, Integer>> values, Collector<Tuple9<String, String, Boolean, Double, String, Integer, Integer, Double, Integer>> out) throws Exception {
                Tuple9<String, String, Boolean, Double, String, Integer, Integer, Double, Integer> result =
                        new Tuple9<String, String, Boolean, Double, String, Integer, Integer, Double, Integer>();
                int min = Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;
                int sum = 0;
                int count = 0;
                boolean first = true;
                for (Tuple6<String, String, Boolean, Double, String, Integer> t : values) {
                    if (first) {
                        result.f0 = t.f0;
                        result.f1 = t.f1;
                        result.f2 = t.f2;
                        result.f3 = t.f3;
                        result.f4 = t.f4;
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

        env.execute("TrafficAnalysis");
    }

    /*
     * distance between two coordinates in kilometers
	 */
    private static double dist(double lat1, double long1, double lat2, double long2) {
        double d2r = Math.PI / 180.0;
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
}

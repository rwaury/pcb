package de.tuberlin.dima.ti.analysis;

import de.tuberlin.dima.ti.pcb.CBUtil;
import de.tuberlin.dima.ti.pcb.Flight;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

import java.util.Date;

public class FlightDistanceExtractor {

    public static class FlightDistanceExtractor1 implements FlatMapFunction<Flight, Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> {

        private boolean noPartition;
        private boolean DIPartition;
        private boolean fullPartition;

        public FlightDistanceExtractor1(boolean noPartition, boolean DIPartition, boolean fullPartition) {
            this.noPartition = noPartition;
            this.DIPartition = DIPartition;
            this.fullPartition = fullPartition;
        }

        @Override
        public void flatMap(Flight value, Collector<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> out) throws Exception {
            if(value.getDepartureTimestamp() > TrafficAnalysis.lastPossibleTimestamp ||
               value.getDepartureTimestamp() < TrafficAnalysis.firstPossibleTimestamp) {
                return;
            }
            Date date = new Date(value.getDepartureTimestamp());
            String dayString = TrafficAnalysis.dayFormat.format(date);
            long duration = value.getArrivalTimestamp() - value.getDepartureTimestamp();
            if (duration <= 0L)
                throw new Exception("Value error: " + value.toString());
            Integer minutes = (int) (duration / (60L * 1000L));
            boolean isIntercontinental = !value.getOriginRegion().equals(value.getDestinationRegion());;
            boolean isInternational = !value.getOriginCountry().equals(value.getDestinationCountry());
            boolean isInterstate = true;
            if(!isInternational && TrafficAnalysis.countriesWithStates.contains(value.getOriginCountry())) {
                isInterstate = !value.getOriginState().equals(value.getDestinationState());
            }
            if(noPartition) {
                isIntercontinental = false;
                isInternational = false;
                isInterstate = false;
            }
            if(DIPartition) {
                isIntercontinental = false;
                isInternational = isInternational;
                isInterstate = false;
            }
            out.collect(new Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>
                    (value.getOriginAirport(), value.getDestinationAirport(), isIntercontinental, isInternational, isInterstate, dayString,
                     CBUtil.dist(value.getOriginLatitude(), value.getOriginLongitude(), value.getDestinationLatitude(), value.getDestinationLongitude()), minutes));
        }
    }

    public static class FlightDistanceExtractor2 implements FlatMapFunction<Tuple2<Flight, Flight>, Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> {

        private boolean noPartition;
        private boolean DIPartition;
        private boolean fullPartition;

        public FlightDistanceExtractor2(boolean noPartition, boolean DIPartition, boolean fullPartition) {
            this.noPartition = noPartition;
            this.DIPartition = DIPartition;
            this.fullPartition = fullPartition;
        }

        @Override
        public void flatMap(Tuple2<Flight, Flight> value, Collector<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> out) throws Exception {
            if(value.f0.getDepartureTimestamp() > TrafficAnalysis.lastPossibleTimestamp ||
                    value.f0.getDepartureTimestamp() < TrafficAnalysis.firstPossibleTimestamp) {
                return;
            }
            Date date = new Date(value.f0.getDepartureTimestamp());
            String dayString = TrafficAnalysis.dayFormat.format(date);
            long flight1 = value.f0.getArrivalTimestamp() - value.f0.getDepartureTimestamp();
            long wait1 = (long) (TrafficAnalysis.WAITING_FACTOR * (value.f1.getDepartureTimestamp() - value.f0.getArrivalTimestamp()));
            long flight2 = value.f1.getArrivalTimestamp() - value.f1.getDepartureTimestamp();
            long duration = flight1 + wait1 + flight2;
            if(duration <= 0L)
                throw new Exception("Value error: " + value.toString());
            Integer minutes = (int) (duration / (60L * 1000L));
            boolean isIntercontinental = !value.f0.getOriginRegion().equals(value.f1.getDestinationRegion());
            boolean isInternational = !value.f0.getOriginCountry().equals(value.f1.getDestinationCountry());
            boolean isInterstate = true;
            if(!isInternational && TrafficAnalysis.countriesWithStates.contains(value.f0.getOriginCountry())) {
                isInterstate = !value.f0.getOriginState().equals(value.f1.getDestinationState());
            }
            if(noPartition) {
                isIntercontinental = false;
                isInternational = false;
                isInterstate = false;
            }
            if(DIPartition) {
                isIntercontinental = false;
                isInternational = isInternational;
                isInterstate = false;
            }
            out.collect(new Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>
                    (value.f0.getOriginAirport(), value.f1.getDestinationAirport(), isIntercontinental, isInternational, isInterstate, dayString,
                            CBUtil.dist(value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f1.getDestinationLatitude(), value.f1.getDestinationLongitude()), minutes));
        }
    }

    public static class FlightDistanceExtractor3 implements FlatMapFunction<Tuple3<Flight, Flight, Flight>, Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> {

        private boolean noPartition;
        private boolean DIPartition;
        private boolean fullPartition;

        public FlightDistanceExtractor3(boolean noPartition, boolean DIPartition, boolean fullPartition) {
            this.noPartition = noPartition;
            this.DIPartition = DIPartition;
            this.fullPartition = fullPartition;
        }

        @Override
        public void flatMap(Tuple3<Flight, Flight, Flight> value, Collector<Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>> out) throws Exception {
            if(value.f0.getDepartureTimestamp() > TrafficAnalysis.lastPossibleTimestamp ||
               value.f0.getDepartureTimestamp() < TrafficAnalysis.firstPossibleTimestamp) {
                return;
            }
            Date date = new Date(value.f0.getDepartureTimestamp());
            String dayString = TrafficAnalysis.dayFormat.format(date);
            long flight1 = value.f0.getArrivalTimestamp() - value.f0.getDepartureTimestamp();
            long wait1 = (long) (TrafficAnalysis.WAITING_FACTOR * (value.f1.getDepartureTimestamp() - value.f0.getArrivalTimestamp()));
            long flight2 = value.f1.getArrivalTimestamp() - value.f1.getDepartureTimestamp();
            long wait2 = (long) (TrafficAnalysis.WAITING_FACTOR * (value.f2.getDepartureTimestamp() - value.f1.getArrivalTimestamp()));
            long flight3 = value.f2.getArrivalTimestamp() - value.f2.getDepartureTimestamp();
            long duration = flight1 + wait1 + flight2 + wait2 + flight3;
            if (duration <= 0L)
                throw new Exception("Value error: " + value.toString());
            Integer minutes = (int) (duration / (60L * 1000L));
            boolean isIntercontinental = !value.f0.getOriginRegion().equals(value.f2.getDestinationRegion());
            boolean isInternational = !value.f0.getOriginCountry().equals(value.f2.getDestinationCountry());
            boolean isInterstate = true;
            if(!isInternational && TrafficAnalysis.countriesWithStates.contains(value.f0.getOriginCountry())) {
                isInterstate = !value.f0.getOriginState().equals(value.f2.getDestinationState());
            }
            if(noPartition) {
                isIntercontinental = false;
                isInternational = false;
                isInterstate = false;
            }
            if(DIPartition) {
                isIntercontinental = false;
                isInternational = isInternational;
                isInterstate = false;
            }
            out.collect(new Tuple8<String, String, Boolean, Boolean, Boolean, String, Double, Integer>
                    (value.f0.getOriginAirport(), value.f2.getDestinationAirport(), isIntercontinental, isInternational, isInterstate, dayString,
                     CBUtil.dist(value.f0.getOriginLatitude(), value.f0.getOriginLongitude(), value.f2.getDestinationLatitude(), value.f2.getDestinationLongitude()), minutes));
        }
    }
}

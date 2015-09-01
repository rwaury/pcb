package de.tuberlin.dima.ti.analysis;

import de.tuberlin.dima.ti.pcb.CBUtil;
import de.tuberlin.dima.ti.pcb.Flight;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

public class FlightExtractor {

    public static class FlightExtractor1 implements FlatMapFunction<Flight, Itinerary> {
        SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
        @Override
        public void flatMap(Flight flight, Collector<Itinerary> out) throws Exception {
            if(flight.getDepartureTimestamp() > TrafficAnalysis.lastPossibleTimestamp ||
               flight.getDepartureTimestamp() < TrafficAnalysis.firstPossibleTimestamp) {
                return;
            }
            int numCountries = 2;
            if(CBUtil.isDomestic(flight)) {
                numCountries = 1;
            }
            Date date = new Date(flight.getDepartureTimestamp());
            String dayString = format.format(date);
            Double distance = CBUtil.dist(flight.getOriginLatitude(), flight.getOriginLongitude(), flight.getDestinationLatitude(), flight.getDestinationLongitude());
            Integer travelTime = Math.max((int) ((flight.getArrivalTimestamp() - flight.getDepartureTimestamp())/(60L*1000L)), 1);
            out.collect(new Itinerary(flight.getOriginAirport(), flight.getDestinationAirport(), dayString,
                    flight.getAirline() + flight.getFlightNumber(), "", "", "", "", distance, distance, travelTime, 0,
                    flight.getLegCount(), 0, flight.getMaxCapacity(), -1.0, -1.0, -1.0, "", numCountries, "", ""));
        }
    }

    public static class FlightExtractor2 implements FlatMapFunction<Tuple2<Flight, Flight>, Itinerary> {
        SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
        @Override
        public void flatMap(Tuple2<Flight, Flight> flight, Collector<Itinerary> out) throws Exception {
            if(flight.f0.getDepartureTimestamp() > TrafficAnalysis.lastPossibleTimestamp ||
               flight.f0.getDepartureTimestamp() < TrafficAnalysis.firstPossibleTimestamp) {
                return;
            }
            HashSet<String> countries = new HashSet<String>(3);
            countries.add(flight.f0.getOriginCountry());
            countries.add(flight.f0.getDestinationCountry());
            countries.add(flight.f1.getOriginCountry());
            countries.add(flight.f1.getDestinationCountry());
            Date date = new Date(flight.f0.getDepartureTimestamp());
            String dayString = format.format(date);
            Double directDistance = CBUtil.dist(flight.f0.getOriginLatitude(), flight.f0.getOriginLongitude(), flight.f1.getDestinationLatitude(), flight.f1.getDestinationLongitude());
            Double travelledDistance = CBUtil.dist(flight.f0.getOriginLatitude(), flight.f0.getOriginLongitude(), flight.f0.getDestinationLatitude(), flight.f0.getDestinationLongitude()) +
                    CBUtil.dist(flight.f1.getOriginLatitude(), flight.f1.getOriginLongitude(), flight.f1.getDestinationLatitude(), flight.f1.getDestinationLongitude());
            Integer travelTime = Math.max((int) ((flight.f1.getArrivalTimestamp() - flight.f0.getDepartureTimestamp())/(60L*1000L)), 1);
            Integer waitingTime = (int) ((flight.f1.getDepartureTimestamp() - flight.f0.getArrivalTimestamp())/(60L*1000L));
            Integer legCount = flight.f0.getLegCount() + flight.f1.getLegCount();
            Integer maxCapacity = Math.min(flight.f0.getMaxCapacity(), flight.f1.getMaxCapacity());
            out.collect(new Itinerary(flight.f0.getOriginAirport(), flight.f1.getDestinationAirport(), dayString,
                    flight.f0.getAirline() + flight.f0.getFlightNumber(), flight.f1.getAirline() + flight.f1.getFlightNumber(), "", "", "",
                    directDistance, travelledDistance, travelTime, waitingTime, legCount, 0, maxCapacity, -1.0, -1.0, -1.0, "", Math.max(1, countries.size()), flight.f1.getOriginAirport(), ""));
        }
    }

    public static class FlightExtractor3 implements FlatMapFunction<Tuple3<Flight, Flight, Flight>, Itinerary> {
        SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");
        @Override
        public void flatMap(Tuple3<Flight, Flight, Flight> flight, Collector<Itinerary> out) throws Exception {
            if(flight.f0.getDepartureTimestamp() > TrafficAnalysis.lastPossibleTimestamp ||
               flight.f0.getDepartureTimestamp() < TrafficAnalysis.firstPossibleTimestamp) {
                return;
            }
            HashSet<String> countries = new HashSet<String>(4);
            countries.add(flight.f0.getOriginCountry());
            countries.add(flight.f0.getDestinationCountry());
            countries.add(flight.f1.getOriginCountry());
            countries.add(flight.f1.getDestinationCountry());
            countries.add(flight.f2.getOriginCountry());
            countries.add(flight.f2.getDestinationCountry());
            Date date = new Date(flight.f0.getDepartureTimestamp());
            String dayString = format.format(date);
            Double directDistance = CBUtil.dist(flight.f0.getOriginLatitude(), flight.f0.getOriginLongitude(), flight.f2.getDestinationLatitude(), flight.f2.getDestinationLongitude());
            Double travelledDistance = CBUtil.dist(flight.f0.getOriginLatitude(), flight.f0.getOriginLongitude(), flight.f0.getDestinationLatitude(), flight.f0.getDestinationLongitude()) +
                    CBUtil.dist(flight.f1.getOriginLatitude(), flight.f1.getOriginLongitude(), flight.f1.getDestinationLatitude(), flight.f1.getDestinationLongitude()) +
                    CBUtil.dist(flight.f2.getOriginLatitude(), flight.f2.getOriginLongitude(), flight.f2.getDestinationLatitude(), flight.f2.getDestinationLongitude());
            Integer travelTime = Math.max((int) ((flight.f2.getArrivalTimestamp() - flight.f0.getDepartureTimestamp())/(60L*1000L)), 1);
            Integer waitingTime = (int) (
                    ((flight.f1.getDepartureTimestamp() - flight.f0.getArrivalTimestamp()) +
                            (flight.f2.getDepartureTimestamp() - flight.f1.getArrivalTimestamp())) /
                            (60L*1000L));
            Integer legCount = flight.f0.getLegCount() + flight.f1.getLegCount() + flight.f2.getLegCount();
            Integer maxCapacity = Math.min(flight.f0.getMaxCapacity(), Math.min(flight.f1.getMaxCapacity(), flight.f2.getMaxCapacity()));
            out.collect(new Itinerary(flight.f0.getOriginAirport(), flight.f2.getDestinationAirport(), dayString,
                    flight.f0.getAirline() + flight.f0.getFlightNumber(), flight.f1.getAirline() + flight.f1.getFlightNumber(), flight.f2.getAirline() + flight.f2.getFlightNumber(), "", "",
                    directDistance, travelledDistance, travelTime, waitingTime, legCount, 0, maxCapacity, -1.0, -1.0, -1.0, "", Math.max(1, countries.size()), flight.f1.getOriginAirport(), flight.f2.getOriginAirport()));
        }
    }
}

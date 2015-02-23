package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * creates three-leg connections by joining two-leg connections and pruning them
 */
public class ThreeLegJoiner implements FlatJoinFunction<Tuple2<Flight, Flight>, Tuple2<Flight, Flight>, Tuple3<Flight, Flight, Flight>> {

    @Override
    public void join(Tuple2<Flight, Flight> in1, Tuple2<Flight, Flight> in2, Collector<Tuple3<Flight, Flight, Flight>> out) throws Exception {

        long hub1Time = in1.f1.getDepartureTimestamp() - in1.f0.getArrivalTimestamp();
        long hub2Time = in2.f1.getDepartureTimestamp() - in2.f0.getArrivalTimestamp();
        long hubTime = hub1Time + hub2Time;
        double ODDistance = CBUtil.dist(in1.f0.getOriginLatitude(), in1.f0.getOriginLongitude(),
                in2.f1.getDestinationLatitude(), in2.f1.getDestinationLongitude());
        if (ODDistance < 1000.0) {
            return;
        }
        if (hubTime > (CBUtil.computeMaxCT(ODDistance) * 60L * 1000L)) {
            return;
        }
        if (in1.f0.getOriginAirport().equals(in2.f1.getDestinationAirport())) {
            return;
        }
        if (!CBUtil.geoDetourAcceptable(in1.f0.getOriginLatitude(), in1.f0.getOriginLongitude(),
                in1.f0.getDestinationLatitude(), in1.f0.getDestinationLongitude(),
                in1.f1.getDestinationLatitude(), in1.f1.getDestinationLongitude()) ||
            !CBUtil.geoDetourAcceptable(in2.f0.getOriginLatitude(), in2.f0.getOriginLongitude(),
                in2.f0.getDestinationLatitude(), in2.f0.getDestinationLongitude(),
                in2.f1.getDestinationLatitude(), in2.f1.getDestinationLongitude())) {
            // if a two-leg connection exceeds the geoDetour it is unreasonable to build a
            // three-leg connection from it even if the two-leg connection makes sense
            return;
        }
        if (in1.f0.getAirline().equals(in1.f1.getAirline()) &&
            in1.f0.getAirline().equals(in2.f1.getAirline()) &&
            in1.f0.getFlightNumber().equals(in1.f1.getFlightNumber()) &&
            in1.f0.getFlightNumber().equals(in2.f1.getFlightNumber())) {
            // we already built all multi-leg flights
            return;
        }
        if (in1.f0.getOriginCountry().equals(in2.f1.getDestinationCountry()) &&
            (!in1.f0.getDestinationCountry().equals(in1.f0.getOriginCountry()) ||
             !in1.f1.getDestinationCountry().equals(in1.f0.getOriginCountry()))) {
            // domestic three leg connections may not use foreign hubs
            return;
        }
        if (!CBUtil.geoDetourAcceptable(in1.f0.getOriginLatitude(), in1.f0.getOriginLongitude(),
                in1.f0.getDestinationLatitude(), in1.f0.getDestinationLongitude(),
                in2.f0.getOriginLatitude(), in2.f0.getOriginLongitude(),
                in2.f1.getDestinationLatitude(), in2.f1.getDestinationLongitude())) {
            return;
        }
        // check if the codeshares still work (only compare first and last flight, we already checked if it's valid for the two-leg connections)
        if (in1.f0.getAirline().equals(in2.f1.getAirline())) {
            // same airline
            out.collect(new Tuple3<Flight, Flight, Flight>(in1.f0, in1.f1, in2.f1));
            return;
        } else if (in1.f0.getCodeShareInfo().isEmpty() && in2.f1.getCodeShareInfo().isEmpty()) {
            // not the same airline and no codeshare information
            return;
        } else {
            String[] codeshareAirlines1 = null;
            if (!in1.f0.getCodeShareInfo().isEmpty()) {
                String[] codeshareInfo1 = in1.f0.getCodeShareInfo().split("/");
                codeshareAirlines1 = new String[codeshareInfo1.length + 1];
                for (int i = 0; i < codeshareInfo1.length; i++) {
                    codeshareAirlines1[i] = codeshareInfo1[i].substring(0, 2);
                }
                codeshareAirlines1[codeshareAirlines1.length - 1] = in1.f0.getAirline();
            } else {
                codeshareAirlines1 = new String[]{in1.f0.getAirline()};
            }
            String[] codeshareAirlines2 = null;
            if (!in2.f1.getCodeShareInfo().isEmpty()) {
                String[] codeshareInfo2 = in2.f1.getCodeShareInfo().split("/");
                codeshareAirlines2 = new String[codeshareInfo2.length + 1];
                for (int i = 0; i < codeshareInfo2.length; i++) {
                    codeshareAirlines2[i] = codeshareInfo2[i].substring(0, 2);
                }
                codeshareAirlines2[codeshareAirlines2.length - 1] = in2.f1.getAirline();
            } else {
                codeshareAirlines2 = new String[]{in2.f1.getAirline()};
            }
            for (int i = 0; i < codeshareAirlines1.length; i++) {
                for (int j = 0; j < codeshareAirlines2.length; j++) {
                    if (codeshareAirlines1[i].equals(codeshareAirlines2[j])) {
                        out.collect(new Tuple3<Flight, Flight, Flight>(in1.f0, in1.f1, in2.f1));
                        return;
                    }
                }
            }
        }
    }

}
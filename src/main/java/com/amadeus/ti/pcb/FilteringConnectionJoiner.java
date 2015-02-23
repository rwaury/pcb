package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * creates two-leg flights by joining non-stop flights and pruning the result
 */
public class FilteringConnectionJoiner implements FlatJoinFunction<Flight, Flight, Tuple2<Flight, Flight>> {

    private final String exceptionsGeneral = "ABHIMTDEG";
    private final String exceptionsInternational = "NOQW";
    private final String exceptionsDomestic = "C";

    @Override
    public void join(Flight in1, Flight in2, Collector<Tuple2<Flight, Flight>> out)
            throws Exception {
        // sanity check
        if (!in1.getDestinationCity().equals(in2.getOriginCity())) {
            throw new Exception("Hub city mismatch: " + in1.toString() + " / " + in2.toString());
        }
        long hubTime = in2.getDepartureTimestamp() - in1.getArrivalTimestamp();
        long travelTime = in2.getArrivalTimestamp() - in1.getDepartureTimestamp();
        if (hubTime <= 0L) {
            // arrival before departure
            return;
        }
        if (hubTime < (CBUtil.computeMinCT() * 60L * 1000L)) {
            return;
        }
        double ODDistance = CBUtil.dist(in1.getOriginLatitude(), in1.getOriginLongitude(), in2.getDestinationLatitude(), in2.getDestinationLongitude());
        if (hubTime > ((CBUtil.computeMaxCT(ODDistance)) * 60L * 1000L)) {
            return;
        }
        if (in1.getOriginAirport().equals(in2.getDestinationAirport())) {
            // some multi-leg flights are circular
            return;
        }
        if (in1.getAirline().equals(in2.getAirline()) && in1.getFlightNumber().equals(in2.getFlightNumber())) {
            // multi-leg flight connections have already been built
            return;
        }
        // check traffic restrictions
        if ((exceptionsGeneral.indexOf(in1.getTrafficRestriction()) >= 0) ||
                (exceptionsGeneral.indexOf(in2.getTrafficRestriction()) >= 0)) {
            return;
        } else if (!CBUtil.isDomestic(in2) && exceptionsDomestic.indexOf(in1.getTrafficRestriction()) >= 0) {
            return;
        } else if (!CBUtil.isDomestic(in1) && exceptionsDomestic.indexOf(in2.getTrafficRestriction()) >= 0) {
            return;
        } else if (CBUtil.isDomestic(in2) && exceptionsInternational.indexOf(in1.getTrafficRestriction()) >= 0) {
            return;
        } else if (CBUtil.isDomestic(in1) && exceptionsInternational.indexOf(in2.getTrafficRestriction()) >= 0) {
            return;
        }
        if (CBUtil.isODDomestic(in1, in2)) {
            if (!CBUtil.isDomestic(in1)) {
                if (hubTime > 240L * 60L * 1000L ||
                    !CBUtil.geoDetourAcceptable(in1.getOriginLatitude(), in1.getOriginLongitude(),
                                in1.getDestinationLatitude(), in1.getDestinationLongitude(),
                                in2.getDestinationLatitude(), in2.getDestinationLongitude())) {
                    // for domestic connections with the hub in another country the MaxCT is 240 min
                    // and geoDetour applies
                    return;
                }
            } else {
                if (travelTime > (CBUtil.travelTimeAt100kphInMinutes(ODDistance) * 60L * 1000L)) {
                    // if on a domestic flight we are faster than 100 km/h in a straight line between
                    // origin and destination the connection is built even if geoDetour is exceeded
                    // this is to preserve connection via domestic hubs like TLS-ORY-NCE
                    return;
                }
            }
        } else {
            if (!CBUtil.geoDetourAcceptable(in1.getOriginLatitude(), in1.getOriginLongitude(),
                    in1.getDestinationLatitude(), in1.getDestinationLongitude(),
                    in2.getDestinationLatitude(), in2.getDestinationLongitude())) {
                return;
            }
        }
        if (in1.getAirline().equals(in2.getAirline())) {
            // same airline
            out.collect(new Tuple2<Flight, Flight>(in1, in2));
        } else if (in1.getCodeShareInfo().isEmpty() && in2.getCodeShareInfo().isEmpty()) {
            // not the same airline and no codeshare information
            return;
        } else {
            // check if a connection can be made via codeshares
            String[] codeshareAirlines1 = null;
            if (!in1.getCodeShareInfo().isEmpty()) {
                String[] codeshareInfo1 = in1.getCodeShareInfo().split("/");
                codeshareAirlines1 = new String[codeshareInfo1.length + 1];
                for (int i = 0; i < codeshareInfo1.length; i++) {
                    codeshareAirlines1[i] = codeshareInfo1[i].substring(0, 2);
                }
                codeshareAirlines1[codeshareAirlines1.length - 1] = in1.getAirline();
            } else {
                codeshareAirlines1 = new String[]{in1.getAirline()};
            }
            String[] codeshareAirlines2 = null;
            if (!in2.getCodeShareInfo().isEmpty()) {
                String[] codeshareInfo2 = in2.getCodeShareInfo().split("/");
                codeshareAirlines2 = new String[codeshareInfo2.length + 1];
                for (int i = 0; i < codeshareInfo2.length; i++) {
                    codeshareAirlines2[i] = codeshareInfo2[i].substring(0, 2);
                }
                codeshareAirlines2[codeshareAirlines2.length - 1] = in2.getAirline();
            } else {
                codeshareAirlines2 = new String[]{in2.getAirline()};
            }
            for (int i = 0; i < codeshareAirlines1.length; i++) {
                for (int j = 0; j < codeshareAirlines2.length; j++) {
                    if (codeshareAirlines1[i].equals(codeshareAirlines2[j])) {
                        out.collect(new Tuple2<Flight, Flight>(in1, in2));
                        return;
                    }
                }
            }
        }
    }

}
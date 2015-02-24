package com.amadeus.ti.pcb;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

/**
 * merge flights that have the same flight number (multi-leg flights)
 */
public class MultiLegJoiner implements FlatJoinFunction<Flight, Flight, Flight> {

    private final String exceptions = "AI";
    private final String exceptionsIn = "BG";

    @Override
    public void join(Flight in1, Flight in2, Collector<Flight> out) throws Exception {
        // sanity checks
        if (!in1.getDestinationCity().equals(in2.getOriginCity())) {
            throw new Exception("Hub city mismatch: " + in1.toString() + " / " + in2.toString());
        }
        if (!in1.getAirline().equals(in2.getAirline()) && in1.getFlightNumber().equals(in2.getFlightNumber())) {
            throw new Exception("Flight mismatch: " + in1.toString() + " / " + in2.toString());
        }
        if(!in1.getAircraftType().equals(in2.getAircraftType())) {
            // if planes are changed it is not a valid multi-leg flight
            return;
        }
        if (exceptionsIn.indexOf(in1.getTrafficRestriction()) >= 0) {
            return;
        }
        if (((exceptions.indexOf(in1.getTrafficRestriction()) >= 0) && in2.getTrafficRestriction().equals(CBUtil.EMPTY)) ||
            ((exceptions.indexOf(in2.getTrafficRestriction()) >= 0) && in1.getTrafficRestriction().equals(CBUtil.EMPTY))) {
            return;
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
        Flight result = new Flight();

        result.setOriginAirport(in1.getOriginAirport());
        result.setOriginTerminal(in1.getOriginTerminal());
        result.setOriginCity(in1.getOriginCity());
        result.setOriginState(in1.getOriginState());
        result.setOriginCountry(in1.getOriginCountry());
        result.setOriginRegion(in1.getOriginRegion());
        result.setOriginLatitude(in1.getOriginLatitude());
        result.setOriginLongitude(in1.getOriginLongitude());
        result.setOriginICAO(in1.getOriginICAO());
        result.setDepartureTimestamp(in1.getDepartureTimestamp());
        result.setDepartureWindow(in1.getDepartureWindow());

        result.setDestinationAirport(in2.getDestinationAirport());
        result.setDestinationTerminal(in2.getDestinationTerminal());
        result.setDestinationCity(in2.getDestinationCity());
        result.setDestinationState(in2.getDestinationState());
        result.setDestinationCountry(in2.getDestinationCountry());
        result.setDestinationRegion(in2.getDestinationRegion());
        result.setDestinationLatitude(in2.getDestinationLatitude());
        result.setDestinationLongitude(in2.getDestinationLongitude());
        result.setDestinationICAO(in2.getDestinationICAO());
        result.setArrivalTimestamp(in2.getArrivalTimestamp());
        result.setFirstArrivalWindow(in2.getFirstArrivalWindow());
        result.setSecondArrivalWindow(in2.getSecondArrivalWindow());

        result.setAirline(in1.getAirline());
        result.setFlightNumber(in1.getFlightNumber());
        result.setAircraftType(in1.getAircraftType()); // connection is discarded on aircraft change (see above)
        result.setMaxCapacity(Math.min(in1.getMaxCapacity(), in2.getMaxCapacity()));

        String codeshareInfo = "";
        if (!in1.getCodeShareInfo().isEmpty() && !in2.getCodeShareInfo().isEmpty()) {
            // merge codeshare info
            String[] codeshareInfo1 = in1.getCodeShareInfo().split("/");
            String[] codeshareInfo2 = in2.getCodeShareInfo().split("/");
            for (int i = 0; i < codeshareInfo1.length; i++) {
                for (int j = 0; j < codeshareInfo2.length; j++) {
                    // keep all codeshare info that allows a connection over this multi-leg segment
                    if (codeshareInfo1[i].substring(0, 2).equals(codeshareInfo2[j].substring(0, 2))) {
                        codeshareInfo += codeshareInfo1[i] + "/";
                        codeshareInfo += codeshareInfo2[j] + "/";
                    }
                }
            }
        }
        if (!codeshareInfo.isEmpty()) {
            codeshareInfo = codeshareInfo.substring(0, codeshareInfo.lastIndexOf('/'));
        }
        result.setCodeShareInfo(codeshareInfo);

        if (in1.getTrafficRestriction().equals('I') && in2.getTrafficRestriction().equals('I')) {
            result.setTrafficRestriction(CBUtil.EMPTY);
        } else if (in1.getTrafficRestriction().equals('A') && in2.getTrafficRestriction().equals('A')) {
            result.setTrafficRestriction(CBUtil.EMPTY);
        } else {
            result.setTrafficRestriction(in2.getTrafficRestriction());
        }

        result.setLastAirport(in2.getLastAirport());
        result.setLastTerminal(in2.getLastTerminal());
        result.setLastCity(in2.getLastCity());
        result.setLastState(in2.getLastState());
        result.setLastCountry(in2.getLastCountry());
        result.setLastRegion(in2.getLastRegion());
        result.setLastLatitude(in2.getLastLatitude());
        result.setLastLongitude(in2.getLastLongitude());
        result.setLastICAO(in2.getLastICAO());

        result.setLegCount(in1.getLegCount() + in2.getLegCount());

        out.collect(result);
    }
}